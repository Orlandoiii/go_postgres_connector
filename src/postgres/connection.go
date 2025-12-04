package postgres

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/config"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/observability"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type ConnectionManager struct {
	config     *config.PostgresConfig
	logger     observability.Logger
	sqlConn    *pgx.Conn
	replConn   *pgconn.PgConn
	retryDelay time.Duration
	maxRetries int
}

func NewConnectionManager(cfg *config.PostgresConfig,
	logger observability.Logger) *ConnectionManager {

	return &ConnectionManager{
		config:     cfg,
		logger:     logger,
		retryDelay: 5 * time.Second,
		maxRetries: -1, // -1 = infinito
	}
}

func (cm *ConnectionManager) ConnectWithRetry(ctx context.Context) error {

	for attempt := 0; cm.maxRetries < 0 || attempt < cm.maxRetries; attempt++ {

		if attempt == math.MaxInt {

			cm.logger.Error(ctx,
				fmt.Sprintf("No se pudo conectar después de %d intentos reiniciando contador a 60", math.MaxInt), nil)

			attempt = 60

		}

		if attempt > 0 {

			delay := cm.calculateBackoff(attempt)

			cm.logger.Warn(ctx, "Reintentando conexión a PostgreSQL", nil,
				"attempt", attempt,
				"delay", delay.String())

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		err := cm.connect(ctx)

		if err == nil {
			cm.logger.Info(ctx, "Conexión a PostgreSQL establecida exitosamente")

			return nil
		}

		cm.logger.Error(ctx, "Error conectando a PostgreSQL", err,
			"attempt", attempt+1)
	}

	return fmt.Errorf("no se pudo conectar después de %d intentos", cm.maxRetries)
}

func (cm *ConnectionManager) connect(ctx context.Context) error {

	connString := cm.config.ConnectionString()

	connConfig, err := pgx.ParseConfig(connString)
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	sqlConn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return fmt.Errorf("connect sql: %w", err)
	}

	if err := ValidateReplicaConfiguration(ctx, sqlConn); err != nil {
		sqlConn.Close(ctx)
		return fmt.Errorf("validate config: %w", err)
	}

	replDSN := connString + " replication=database"
	replConn, err := pgconn.Connect(ctx, replDSN)
	if err != nil {
		sqlConn.Close(ctx)
		return fmt.Errorf("connect replication: %w", err)
	}

	cm.sqlConn = sqlConn
	cm.replConn = replConn

	return nil
}

func (cm *ConnectionManager) calculateBackoff(attempt int) time.Duration {
	delay := cm.retryDelay * time.Duration(1<<uint(attempt))
	if delay > 60*time.Second {
		delay = 60 * time.Second
	}
	return delay
}

func (cm *ConnectionManager) GetConnections() (*pgx.Conn, *pgconn.PgConn) {
	return cm.sqlConn, cm.replConn
}

func (cm *ConnectionManager) Close(ctx context.Context) {
	if cm.sqlConn != nil {
		cm.sqlConn.Close(ctx)
	}
	if cm.replConn != nil {
		cm.replConn.Close(ctx)
	}
}

func (cm *ConnectionManager) Reconnect(ctx context.Context) error {
	cm.Close(ctx)
	return cm.ConnectWithRetry(ctx)
}

func (cm *ConnectionManager) HealthCheck(ctx context.Context) error {
	if cm.sqlConn == nil {
		return fmt.Errorf("sql connection is nil")
	}

	var result int
	err := cm.sqlConn.QueryRow(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	return nil
}
