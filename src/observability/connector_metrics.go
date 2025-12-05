package observability

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/jackc/pglogrepl"
)

// ConnectorMetrics contiene todas las métricas del conector
type ConnectorMetrics struct {
	// LSN Metrics
	globalLSN      *prometheus.GaugeVec
	walEnd         prometheus.Gauge
	lastKeepalive  prometheus.Gauge

	// Transaction Metrics
	transactionsProcessedTotal *prometheus.CounterVec
	transactionsProcessedByWorker *prometheus.CounterVec
	eventsInProcessByWorker    *prometheus.GaugeVec

	// Worker Metrics
	workerBufferSize *prometheus.GaugeVec

	mu sync.RWMutex
}

var (
	metricsInstance *ConnectorMetrics
	metricsOnce     sync.Once
)

// NewConnectorMetrics crea e inicializa las métricas del conector
func NewConnectorMetrics(registry *prometheus.Registry) *ConnectorMetrics {
	metricsOnce.Do(func() {
		metrics := &ConnectorMetrics{
			globalLSN: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "connector_global_lsn",
					Help: "Global LSN (Log Sequence Number) - mínimo de todos los workers",
				},
				[]string{},
			),
			walEnd: prometheus.NewGauge(
				prometheus.GaugeOpts{
					Name: "connector_wal_end",
					Help: "WAL End position leído del último keepalive de PostgreSQL",
				},
			),
			lastKeepalive: prometheus.NewGauge(
				prometheus.GaugeOpts{
					Name: "connector_last_keepalive_timestamp",
					Help: "Timestamp del último keepalive recibido de PostgreSQL (Unix timestamp)",
				},
			),
			transactionsProcessedTotal: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: "connector_transactions_processed_total",
					Help: "Número total de transacciones procesadas",
				},
				[]string{},
			),
			transactionsProcessedByWorker: prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: "connector_transactions_processed_by_worker_total",
					Help: "Número total de transacciones procesadas por worker",
				},
				[]string{"worker", "type"},
			),
			eventsInProcessByWorker: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "connector_events_in_process_by_worker",
					Help: "Número de eventos actualmente en proceso por worker",
				},
				[]string{"worker", "type"},
			),
			workerBufferSize: prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "connector_worker_buffer_size",
					Help: "Tamaño del buffer de cada worker",
				},
				[]string{"worker", "type"},
			),
		}

		registry.MustRegister(
			metrics.globalLSN,
			metrics.walEnd,
			metrics.lastKeepalive,
			metrics.transactionsProcessedTotal,
			metrics.transactionsProcessedByWorker,
			metrics.eventsInProcessByWorker,
			metrics.workerBufferSize,
		)

		metricsInstance = metrics
	})

	return metricsInstance
}

// GetConnectorMetrics retorna la instancia singleton de métricas
func GetConnectorMetrics() *ConnectorMetrics {
	return metricsInstance
}

// SetGlobalLSN actualiza el LSN global
func (cm *ConnectorMetrics) SetGlobalLSN(lsn pglogrepl.LSN) {
	if cm == nil {
		return
	}
	cm.globalLSN.WithLabelValues().Set(float64(lsn))
}

// SetWalEnd actualiza el WAL End
func (cm *ConnectorMetrics) SetWalEnd(lsn pglogrepl.LSN) {
	if cm == nil {
		return
	}
	cm.walEnd.Set(float64(lsn))
}

// SetLastKeepalive actualiza el timestamp del último keepalive
func (cm *ConnectorMetrics) SetLastKeepalive(timestamp float64) {
	if cm == nil {
		return
	}
	cm.lastKeepalive.Set(timestamp)
}

// IncTransactionsProcessed incrementa el contador de transacciones procesadas
func (cm *ConnectorMetrics) IncTransactionsProcessed() {
	if cm == nil {
		return
	}
	cm.transactionsProcessedTotal.WithLabelValues().Inc()
}

// IncTransactionsProcessedByWorker incrementa el contador por worker
func (cm *ConnectorMetrics) IncTransactionsProcessedByWorker(worker string, workerType string) {
	if cm == nil {
		return
	}
	cm.transactionsProcessedByWorker.WithLabelValues(worker, workerType).Inc()
}

// SetEventsInProcess actualiza el número de eventos en proceso
func (cm *ConnectorMetrics) SetEventsInProcess(worker string, workerType string, count float64) {
	if cm == nil {
		return
	}
	cm.eventsInProcessByWorker.WithLabelValues(worker, workerType).Set(count)
}

// SetWorkerBufferSize actualiza el tamaño del buffer del worker
func (cm *ConnectorMetrics) SetWorkerBufferSize(worker string, workerType string, size float64) {
	if cm == nil {
		return
	}
	cm.workerBufferSize.WithLabelValues(worker, workerType).Set(size)
}

