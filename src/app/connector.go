package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/config"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/expressions"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/observability"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/pipeline"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/postgres"
	"github.com/SOLUCIONESSYCOM/scribe"
)

type Connector struct {
	logger      observability.Logger
	connManager *postgres.ConnectionManager
	dispatcher  *pipeline.Dispatcher
	coordinator *pipeline.LSNCoordinator
	replicator  *postgres.Replicator
	postgresCfg *config.PostgresConfig
	sinkFactory pipeline.SinkFactory
	goFinalLSN  bool
}

func NewConnector(ctx context.Context) (*Connector, error) {
	postgresCfg, err := config.PostgresCfg()
	if err != nil {
		return nil, fmt.Errorf("load postgres config: %w", err)
	}

	logConfig, err := config.LogCfg()
	if err != nil {
		return nil, fmt.Errorf("load log config: %w", err)
	}

	sc, err := scribe.New(logConfig, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("create scribe: %w", err)
	}

	logger := observability.NewScribeLogger(sc)

	coordinator := pipeline.NewLSNCoordinator(logger)

	var sinkFactory pipeline.SinkFactory
	kafkaCfg, err := config.KafkaCfg()

	if err != nil {
		return nil, fmt.Errorf("load kafka config: %w", err)
	}

	kafkaSinkFactory, err := pipeline.NewKafkaSinkFactory(kafkaCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("create kafka sink factory: %w", err)
	}
	sinkFactory = kafkaSinkFactory
	logger.Info(ctx, "Usando Kafka sink")

	// sinkFactory = pipeline.NewFileSinkFactory("output", logger)
	// logger.Info(ctx, "Usando File sink")

	filterFactory := expressions.NewExpressionFilterFactory(logger)

	dispatcher := pipeline.NewDispatcher(sinkFactory,
		filterFactory,
		postgresCfg.WorkerBufferSize,
		coordinator, postgresCfg.Listeners,
		logger)

	connManager := postgres.NewConnectionManager(postgresCfg, logger)

	return &Connector{
		logger:      logger,
		connManager: connManager,
		dispatcher:  dispatcher,
		coordinator: coordinator,
		postgresCfg: postgresCfg,
		sinkFactory: sinkFactory,
		goFinalLSN:  postgresCfg.GoFinalLSN,
	}, nil
}

func (c *Connector) Start(ctx context.Context) error {

	c.logger.Trace(ctx, "Iniciando Connector con configuración", "goFinalLSN", c.goFinalLSN)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigChan
		c.logger.Info(ctx, "Señal de terminación recibida, cerrando...")
		cancel()
		time.Sleep(150 * time.Millisecond)
	}()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		func() {
			defer c.recoverPanic(ctx, "bucle de conexión")

			c.cleanupConnections(ctx)

			if err := c.connManager.ConnectWithRetry(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				c.logger.Warn(ctx, "No se pudo conectar, reintentando en el siguiente ciclo", nil)
				time.Sleep(5 * time.Second)
				return
			}

			sqlConn, replConn := c.connManager.GetConnections()

			c.cleanupReplicator()

			replicator, err := postgres.NewReplicator(
				sqlConn,
				replConn,
				c.postgresCfg.Listeners,
				c.postgresCfg.SlotName,
				c.dispatcher,
				c.coordinator,
				c.logger,
				c.goFinalLSN,
			)
			if err != nil {
				c.logger.Error(ctx, "Error creando replicator", err)
				c.cleanupConnections(ctx)
				time.Sleep(5 * time.Second)
				return
			}

			c.replicator = replicator
			c.logger.Info(ctx, "Iniciando replicación...")

			err = c.runReplication(ctx, replicator)
			if err != nil {
				c.logger.Error(ctx, "Error en replicación", err)
			}

			c.cleanupReplicator()
			c.cleanupConnections(ctx)

			if ctx.Err() != nil {
				return
			}

			c.logger.Warn(ctx, "Replicación detenida, esperando antes de reintentar...", nil)
			time.Sleep(5 * time.Second)
		}()
	}
}

func (c *Connector) cleanupReplicator() {
	if c.replicator != nil {
		c.replicator.Close()
		c.replicator = nil
	}
}

func (c *Connector) cleanupConnections(ctx context.Context) {
	c.logger.Trace(ctx, "Cerrando conexiones")
	if c.connManager != nil {
		c.connManager.Close(ctx)
	}
}

func (c *Connector) runReplication(ctx context.Context, replicator *postgres.Replicator) error {
	c.logger.Trace(ctx, "Iniciando replicación")
	return replicator.Start(ctx)
}

func (c *Connector) recoverPanic(ctx context.Context, operation string) {
	if r := recover(); r != nil {

		stackTrace := string(debug.Stack())

		c.logger.Error(ctx, fmt.Sprintf("Panic capturado en %s", operation),
			fmt.Errorf("panic: %v", r),
			"operation", operation,
			"panic_value", r,
			"stack_trace", stackTrace)

		c.cleanupReplicator()
		c.cleanupConnections(ctx)
		time.Sleep(5 * time.Second)
	}
}

func (c *Connector) Close(ctx context.Context) error {

	c.logger.Trace(ctx, "Cerrando Connector")

	// Primero detener el dispatcher para que los workers terminen de procesar
	if c.dispatcher != nil {
		c.logger.Trace(ctx, "Deteniendo dispatcher")
		c.dispatcher.Stop(ctx)
	}

	// Luego cerrar el replicator (esto cierra la conexión de replicación)
	if c.replicator != nil {
		c.logger.Trace(ctx, "Cerrando replicator")
		c.replicator.Close()
		c.replicator = nil
	}

	// Cerrar sink factory
	if c.sinkFactory != nil {
		if closer, ok := c.sinkFactory.(interface{ Close() error }); ok {
			c.logger.Trace(ctx, "Cerrando sink factory")
			closer.Close()
		}
	}

	// Finalmente cerrar las conexiones
	if c.connManager != nil {
		c.logger.Trace(ctx, "Cerrando connection manager")
		c.connManager.Close(ctx)
	}

	c.logger.Trace(ctx, "Connector cerrado")
	return nil
}
