package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/app"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/config"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/observability"
	"github.com/SOLUCIONESSYCOM/scribe"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func replicate() {
	ctx := context.Background()

	// Cargar configuración de log para el servicio de métricas
	logConfig, err := config.LogCfg()
	if err != nil {
		panic(fmt.Sprintf("error loading log config: %v", err))
	}

	sc, err := scribe.New(logConfig, nil, nil)
	if err != nil {
		panic(fmt.Sprintf("error creating scribe: %v", err))
	}

	logger := observability.NewScribeLogger(sc)

	// Cargar configuración del servidor
	serverConfig, err := config.ServerCfg()
	if err != nil {
		logger.Error(ctx, "Error loading server config", err)
		panic(fmt.Sprintf("error loading server config: %v", err))
	}

	// Crear servicio de métricas
	metricsService := observability.NewMetricsService()

	// Inicializar métricas del conector
	observability.NewConnectorMetrics(metricsService.GetRegistry())

	// Configurar servidor HTTP con Gin
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	// Endpoint de métricas de Prometheus
	router.GET("/metrics", gin.WrapH(promhttp.HandlerFor(metricsService.GetRegistry(), promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})))

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
		})
	})

	router.GET("/ready", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "ready",
		})
	})

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", serverConfig.HttpPort),
		Handler: router,
	}

	go func() {
		logger.Info(ctx, "Starting metrics server", "port", serverConfig.HttpPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error(ctx, "Metrics server error", err, "port", serverConfig.HttpPort)
		}
	}()

	// Cerrar servidor HTTP al terminar
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		logger.Info(ctx, "Stopping metrics server")
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error(ctx, "Error stopping metrics server", err)
		}
	}()

	logger.Info(ctx, "Metrics server started", "port", serverConfig.HttpPort, "endpoint", fmt.Sprintf("http://localhost:%d/metrics", serverConfig.HttpPort))

	// Crear connector
	connector, err := app.NewConnector(ctx)
	if err != nil {
		panic(fmt.Sprintf("error creating connector: %v", err))
	}
	defer connector.Close(ctx)

	// Manejar señales de terminación
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	// Crear contexto cancelable para el connector
	connectorCtx, connectorCancel := context.WithCancel(ctx)
	defer connectorCancel()

	// Iniciar connector en goroutine
	connectorErrChan := make(chan error, 1)
	go func() {
		if err := connector.Start(connectorCtx); err != nil {
			connectorErrChan <- err
		}
	}()

	// Esperar señal de terminación o error
	select {
	case sig := <-sigChan:
		logger.Info(ctx, "Received termination signal", "signal", sig.String())
		// Cancelar contexto del connector para que termine
		connectorCancel()
		// Esperar a que el connector termine (con timeout)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		select {
		case err := <-connectorErrChan:
			if err != nil && err != context.Canceled {
				logger.Warn(ctx, "Connector stopped with error", err)
			}
		case <-shutdownCtx.Done():
			logger.Warn(ctx, "Timeout waiting for connector to stop", nil)
		}
	case err := <-connectorErrChan:
		logger.Error(ctx, "Connector error", err)
		connectorCancel()
		panic(fmt.Sprintf("connector error: %v", err))
	}
}

func main() {
	fmt.Println("Starting replication...")
	replicate()
	fmt.Println("Replication stopped")
}
