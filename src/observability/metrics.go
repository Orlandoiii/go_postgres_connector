package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// MetricsService maneja las métricas de Prometheus
type MetricsService struct {
	registry *prometheus.Registry
}

// NewMetricsService crea un nuevo servicio de métricas
func NewMetricsService() *MetricsService {
	registry := prometheus.NewRegistry()

	// Registrar métricas estándar de Go
	registry.MustRegister(collectors.NewGoCollector())
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	return &MetricsService{
		registry: registry,
	}
}

// GetRegistry retorna el registro de Prometheus para registrar métricas personalizadas
func (ms *MetricsService) GetRegistry() *prometheus.Registry {
	return ms.registry
}
