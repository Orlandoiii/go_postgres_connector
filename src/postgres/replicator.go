package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/config"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/observability"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/pipeline"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type Replicator struct {
	decoder      *Decoder
	dispatcher   *pipeline.Dispatcher
	coordinator  *pipeline.LSNCoordinator
	sqlConn      *pgx.Conn
	replConn     *pgconn.PgConn
	listeners    []config.Listener
	slot         string
	currentLSN   pglogrepl.LSN
	lastSendTime time.Time
	logger       observability.Logger
	goFinalLSN   bool
}

func NewReplicator(sqlConn *pgx.Conn, replConn *pgconn.PgConn,
	listeners []config.Listener,
	slot string,
	dispatcher *pipeline.Dispatcher,
	coordinator *pipeline.LSNCoordinator,
	logger observability.Logger,
	goFinalLSN bool,
) (*Replicator, error) {
	return &Replicator{
		decoder:      NewDecoder(logger),
		logger:       logger,
		dispatcher:   dispatcher,
		coordinator:  coordinator,
		sqlConn:      sqlConn,
		replConn:     replConn,
		listeners:    listeners,
		slot:         slot,
		lastSendTime: time.Time{},
		goFinalLSN:   goFinalLSN,
	}, nil
}

func (r *Replicator) Close() error {
	ctx := context.Background()
	r.logger.Trace(ctx, "Cerrando replicator")

	// Cerrar conexión de replicación
	if r.replConn != nil {
		r.logger.Trace(ctx, "Cerrando conexión de replicación")
		if err := r.replConn.Close(ctx); err != nil {
			r.logger.Warn(ctx, "Error cerrando conexión de replicación", err)
		}
		r.replConn = nil
	}

	// Detener dispatcher
	if r.dispatcher != nil {
		r.dispatcher.Stop(ctx)
	}
	return nil
}

func (r *Replicator) handleKeepalive(ctx context.Context, data []byte) (bool, error) {
	r.logger.Trace(ctx, "Procesando keepalive")

	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)

	if err != nil {
		return false, fmt.Errorf("parse keepalive: %w", err)
	}

	r.coordinator.SetWalEnd(pkm.ServerWALEnd)

	r.logger.Trace(ctx, "Keepalive procesado", "server_wal_end", pkm.ServerWALEnd.String())

	if !r.dispatcher.HasPendingEvents() {

		r.currentLSN = r.coordinator.GetWalEnd()

		return pkm.ReplyRequested, nil
	}

	return pkm.ReplyRequested, nil
}

func (r *Replicator) handleXLogData(ctx context.Context, data []byte,
	consumeTime time.Time, tr *pipeline.TransactionEvent) (pglogrepl.LSN, error) {

	r.logger.Trace(ctx, "Procesando XLogData")

	xld, err := pglogrepl.ParseXLogData(data)

	if err != nil {
		r.logger.Error(ctx, "Error parseando XLogData", err)
		return 0, fmt.Errorf("parse XLogData: %w", err)
	}

	r.logger.Debug(ctx, "Datos recibidos",
		"wal_start", xld.WALStart.String(),
		"size", len(xld.WALData),
		"wal_end", xld.ServerWALEnd.String(),
		"server_time", xld.ServerTime.Format("2006-01-02 15:04:05"))

	r.coordinator.SetWalEnd(xld.ServerWALEnd)

	if len(xld.WALData) > 0 {

		logicalMsg, err := pglogrepl.Parse(xld.WALData)

		if err != nil {
			return 0, fmt.Errorf("parse logical message: %w", err)
		}

		result := r.decoder.ProcessLogicalMessage(ctx, logicalMsg, consumeTime, tr)

		if result != nil {
			*tr = *result
		}
	}

	return xld.ServerWALEnd, nil
}

func (r *Replicator) sendStatusUpdate(ctx context.Context, currentLSN pglogrepl.LSN) {

	r.logger.Info(ctx, "Enviando ACK",
		"lsn", currentLSN.String())

	r.logger.Trace(ctx, "Enviando ACK", "lsn",
		currentLSN.String())

	err := pglogrepl.SendStandbyStatusUpdate(ctx, r.replConn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: currentLSN,
			WALFlushPosition: currentLSN,
			WALApplyPosition: currentLSN,
			ClientTime:       time.Now(),
			ReplyRequested:   false,
		})

	if err != nil {
		r.logger.Warn(ctx, "Error enviando ACK", err,
			"lsn", currentLSN.String(),
			"timestamp", time.Now().Format("2006-01-02 15:04:05"))
	} else {
		r.lastSendTime = time.Now()
		r.logger.Trace(ctx, "ACK enviado", "lsn", currentLSN.String())

	}
}

func (r *Replicator) receiveLoop(ctx context.Context) error {

	tr := &pipeline.TransactionEvent{}
	shouldSendStatus := false

	for {
		// Verificar si el contexto fue cancelado
		if ctx.Err() != nil {
			r.logger.Info(ctx, "Contexto cancelado, saliendo del bucle de recepción")
			return ctx.Err()
		}

		consumeTime := time.Now()
		receiveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)

		r.logger.Trace(ctx, "Receiving message")

		msg, err := r.replConn.ReceiveMessage(receiveCtx)
		cancel()

		r.logger.Trace(ctx, "Message received", "message", msg)

		shouldSendStatus = false

		if err != nil {
			// Verificar si el contexto fue cancelado
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				r.logger.Info(ctx, "Contexto cancelado durante recepción de mensaje")
				return ctx.Err()
			}

			if errors.Is(err, context.DeadlineExceeded) {

				r.logger.Trace(ctx, "Timeout received", "last_send_time", r.lastSendTime.Format("2006-01-02 15:04:05"))

				if time.Since(r.lastSendTime) > 5*time.Second {

					r.currentLSN = r.coordinator.GetGlobalLSN()

					if !r.dispatcher.HasPendingEvents() {
						r.logger.Trace(ctx, "No hay eventos pendientes, seteando LSN a WalEnd", "wal_end",
							r.coordinator.GetWalEnd().String())
						r.currentLSN = r.coordinator.GetWalEnd()
					} else {
						r.logger.Trace(ctx, "Hay eventos pendientes, manteniendo LSN actual", "current_lsn",
							r.currentLSN.String())
					}
					r.sendStatusUpdate(ctx, r.currentLSN)

					r.logger.Trace(ctx, "Sending status update", "lsn", r.currentLSN.String())
				}

				continue
			}

			r.logger.Error(ctx, "Error recibiendo mensaje", err)

			return fmt.Errorf("receive message failed: %w", err)
		}

		if msg == nil {

			r.logger.Warn(ctx, "Mensaje recibido es nil", nil)

			continue
		}

		switch msg := msg.(type) {

		case *pgproto3.CopyData:

			switch msg.Data[0] {

			case pglogrepl.PrimaryKeepaliveMessageByteID:

				replyRequested, err := r.handleKeepalive(ctx, msg.Data[1:])

				if err != nil {
					r.logger.Warn(ctx, "Error procesando keepalive", err)
					continue
				}

				if replyRequested {
					shouldSendStatus = true
				}

			case pglogrepl.XLogDataByteID:

				_, err = r.handleXLogData(ctx, msg.Data[1:], consumeTime, tr)

				if err != nil {

					r.logger.Warn(ctx, "Error procesando XLogData", err)

					continue
				}

				if tr != nil && tr.IsCommit {

					r.logger.Trace(ctx, "Dispatching transaction", "transaction_lsn", tr.LSN.String())

					err = r.dispatcher.Dispatch(ctx, tr)

					if err != nil {

						r.logger.Warn(ctx, "Error despachando evento, no avanzando LSN", err)

						continue
					}

					shouldSendStatus = true

					tr = &pipeline.TransactionEvent{}

				}

			default:
				r.logger.Warn(ctx, "Mensaje CopyData desconocido",
					nil,
					"tipo", msg.Data[0],
					"timestamp", time.Now().Format("2006-01-02 15:04:05"))
			}

		default:

			r.logger.Warn(ctx, "Mensaje inesperado",
				nil,
				"type", fmt.Sprintf("%T", msg),
				"timestamp", time.Now().Format("2006-01-02 15:04:05"))
		}

		if shouldSendStatus || time.Since(r.lastSendTime) > 5*time.Second {

			r.logger.Trace(ctx, "End of loop replication loop, Sending status update",
				"should_send_status", shouldSendStatus,
				"last_send_time", r.lastSendTime.Format("2006-01-02 15:04:05"))

			coordinatorLSN := r.coordinator.GetGlobalLSN()

			if coordinatorLSN > 0 && coordinatorLSN > r.currentLSN {
				r.currentLSN = coordinatorLSN
			}
			if !r.dispatcher.HasPendingEvents() {
				r.currentLSN = r.coordinator.GetWalEnd()
			}
			r.sendStatusUpdate(ctx, r.currentLSN)
		}
	}
}

func (r *Replicator) Start(ctx context.Context) error {

	r.logger.Info(ctx, "Creando slot de replicacion")

	err := CreateLogicalSlotIfMissing(ctx, r.sqlConn, r.slot, PgoutputPlugin)

	if err != nil {

		return fmt.Errorf("create slot: %w", err)
	}

	r.logger.Info(ctx, "Verificando tablas en las publicaciones")

	err = VerifyTables(ctx, r.sqlConn, r.listeners)

	if err != nil {
		return fmt.Errorf("verificar tablas: %w", err)
	}

	var pubNames []string

	r.logger.Info(ctx, "Creando publicaciones")

	for _, listener := range r.listeners {

		err := CreatePublicationAndSetTableIdentityFull(ctx, r.sqlConn, listener, r.logger)

		if err != nil {

			r.logger.Warn(ctx, "Error creando publicacion y seteando identidad de la replica", err)

			return err
		}

		pubNames = append(pubNames, strings.TrimSpace(listener.Publication))

	}

	lastLSN, err := GetSlotLSN(ctx, r.sqlConn, r.slot)

	if err != nil && !r.goFinalLSN {
		return fmt.Errorf("get slot LSN: %w", err)
	}

	err = r.sqlConn.Close(ctx)

	if err != nil {
		return fmt.Errorf("close sql connection: %w", err)
	}

	r.logger.Info(ctx, "Identificando sistema")

	sysident, err := pglogrepl.IdentifySystem(ctx, r.replConn)

	if err != nil {
		return fmt.Errorf("identify system: %w", err)
	}

	r.logger.Info(ctx, "Conectado",
		"system_id", sysident.SystemID,
		"timeline", sysident.Timeline,
		"xlog_pos", sysident.XLogPos.String())

	pluginArgs := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", strings.Join(pubNames, ", ")),
	}

	if r.goFinalLSN {
		lastLSN = sysident.XLogPos
	}

	r.currentLSN = lastLSN

	r.logger.Trace(ctx, "Last LSN", "last_lsn", lastLSN.String())

	err = pglogrepl.StartReplication(ctx, r.replConn, r.slot, lastLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArgs,
			Mode:       pglogrepl.LogicalReplication,
		})

	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	r.logger.Info(ctx, "Replicación iniciada",
		"lsn", sysident.XLogPos.String(),
		"tables", GetTableNames(r.listeners))

	r.logger.Info(ctx, "Iniciando bucle de recepcion")

	return r.receiveLoop(ctx)
}
