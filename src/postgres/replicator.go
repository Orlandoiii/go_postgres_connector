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
}

func NewReplicator(sqlConn *pgx.Conn, replConn *pgconn.PgConn,
	listeners []config.Listener,
	slot string,
	dispatcher *pipeline.Dispatcher,
	coordinator *pipeline.LSNCoordinator,
	logger observability.Logger,
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
	}, nil
}

func (r *Replicator) Close() error {
	if r.dispatcher != nil {
		r.dispatcher.Stop(context.Background())
	}
	return nil
}

func (r *Replicator) handleKeepalive(ctx context.Context, data []byte) (bool, error) {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)

	if err != nil {
		return false, fmt.Errorf("parse keepalive: %w", err)
	}

	coordinatorLSN := r.coordinator.GetGlobalLSN()

	if coordinatorLSN > 0 {

		if coordinatorLSN < pkm.ServerWALEnd {
			r.currentLSN = coordinatorLSN
		} else {
			r.currentLSN = pkm.ServerWALEnd
		}

	} else if r.coordinator.HasRegisteredTables() {

		r.logger.Debug(ctx, "Keepalive recibido pero coordinator tiene tablas sin LSN, manteniendo LSN actual",
			"current_lsn", r.currentLSN.String(),
			"server_wal_end", pkm.ServerWALEnd.String())

	} else {

		r.logger.Debug(ctx, "Keepalive recibido pero coordinator no tiene tablas registradas, manteniendo LSN actual",
			"current_lsn", r.currentLSN.String(),
			"server_wal_end", pkm.ServerWALEnd.String())
		r.currentLSN = pkm.ServerWALEnd
	}

	return pkm.ReplyRequested, nil
}

func (r *Replicator) handleXLogData(ctx context.Context, data []byte,
	consumeTime time.Time, tr *pipeline.TransactionEvent) (pglogrepl.LSN, error) {

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

	walEnd := xld.WALStart + pglogrepl.LSN(len(xld.WALData))

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

	return walEnd, nil
}

func (r *Replicator) sendStatusUpdate(ctx context.Context, currentLSN pglogrepl.LSN) {

	r.logger.Info(ctx, "Enviando ACK",
		"lsn", currentLSN.String())

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
	}
}

func (r *Replicator) receiveLoop(ctx context.Context) error {

	tr := &pipeline.TransactionEvent{}
	shouldSendStatus := false

	for {

		consumeTime := time.Now()
		receiveCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		msg, err := r.replConn.ReceiveMessage(receiveCtx)
		cancel()

		shouldSendStatus = false

		if err != nil {

			if errors.Is(err, context.DeadlineExceeded) {

				if time.Since(r.lastSendTime) > 5*time.Second {
					r.sendStatusUpdate(ctx, r.currentLSN)
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

				walEnd, err := r.handleXLogData(ctx, msg.Data[1:], consumeTime, tr)

				if err != nil {

					r.logger.Warn(ctx, "Error procesando XLogData", err)

					continue
				}

				if tr != nil && tr.IsCommit {

					if err := r.dispatcher.Dispatch(ctx, tr); err != nil {

						r.logger.Warn(ctx, "Error despachando evento, no avanzando LSN", err)

						tr = &pipeline.TransactionEvent{}

						continue
					}

					coordinatorLSN := r.coordinator.GetGlobalLSN()

					if coordinatorLSN > 0 {

						r.currentLSN = coordinatorLSN

					} else if r.coordinator.HasRegisteredTables() {

						r.logger.Debug(ctx, "Coordinator tiene tablas registradas pero LSN es 0, manteniendo LSN actual hasta primer reporte",
							"current_lsn", r.currentLSN.String(),
							"tx_lsn", tr.LSN.String())

					} else {

						r.logger.Debug(ctx, "No hay tablas registradas aún, usando walEnd",
							"wal_end", walEnd.String())

						r.currentLSN = walEnd
					}

					tr = &pipeline.TransactionEvent{}

				} else {

					r.logger.Debug(ctx, "Transacción no completada o sin transacción, manteniendo LSN actual",
						"current_lsn", r.currentLSN.String(),
						"wal_end", walEnd.String(),
						"has_transaction", tr != nil,
						"is_commit", tr != nil && tr.IsCommit)
				}

				shouldSendStatus = true

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

			coordinatorLSN := r.coordinator.GetGlobalLSN()

			if coordinatorLSN > 0 && coordinatorLSN > r.currentLSN {
				r.currentLSN = coordinatorLSN
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

	err = pglogrepl.StartReplication(ctx, r.replConn, r.slot, sysident.XLogPos,
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

	r.currentLSN = sysident.XLogPos

	r.logger.Info(ctx, "Iniciando bucle de recepcion")

	return r.receiveLoop(ctx)
}
