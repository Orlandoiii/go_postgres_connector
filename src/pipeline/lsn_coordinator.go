package pipeline

import (
	"context"
	"sync"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/observability"

	"github.com/jackc/pglogrepl"
)

type LSNCoordinator struct {
	mu         sync.RWMutex
	targetLSNs map[string]pglogrepl.LSN
	observability.Logger
	walEnd pglogrepl.LSN
	metrics *observability.ConnectorMetrics
}

// NewLSNCoordinator crea un nuevo LSNCoordinator
func NewLSNCoordinator(logger observability.Logger) *LSNCoordinator {
	return &LSNCoordinator{
		mu:         sync.RWMutex{},
		targetLSNs: make(map[string]pglogrepl.LSN),
		Logger:     logger,
		metrics:    observability.GetConnectorMetrics(),
	}
}

func (lc *LSNCoordinator) HasRegisteredTables() bool {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	return len(lc.targetLSNs) > 0
}

func (lc *LSNCoordinator) RegisterTable(workerKey string) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	if _, ok := lc.targetLSNs[workerKey]; ok {
		return
	}

	lc.targetLSNs[workerKey] = pglogrepl.LSN(0)
}

// UnregisterTable elimina una entrada del map de LSNs
// CRITICAL FIX: Previene memory leaks cuando se eliminan workers/tablas
func (lc *LSNCoordinator) UnregisterTable(workerKey string) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	if _, ok := lc.targetLSNs[workerKey]; ok {
		delete(lc.targetLSNs, workerKey)
		lc.Trace(context.Background(), "LSN unregistered", "worker", workerKey)
	}
}

func (lc *LSNCoordinator) ReportLSN(ctx context.Context, workerKey string, lsn pglogrepl.LSN) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	lc.Trace(ctx, "Reportando LSN", "worker", workerKey, "lsn", lsn)

	current, exists := lc.targetLSNs[workerKey]

	if !exists || lsn > current {

		lc.targetLSNs[workerKey] = lsn
	}
}

func (lc *LSNCoordinator) GetGlobalLSN() pglogrepl.LSN {
	lc.mu.RLock()
	defer lc.mu.RUnlock()

	if len(lc.targetLSNs) == 0 {
		if lc.metrics != nil {
			lc.metrics.SetGlobalLSN(pglogrepl.LSN(0))
		}
		return pglogrepl.LSN(0)
	}

	minLsn := pglogrepl.LSN(0)
	hasNonZero := false

	for _, lsn := range lc.targetLSNs {

		if lsn > 0 {

			if !hasNonZero || lsn < minLsn {

				minLsn = lsn

				hasNonZero = true
			}
		}
	}

	// Actualizar métrica sin bloquear
	if lc.metrics != nil {
		lc.metrics.SetGlobalLSN(minLsn)
	}

	return minLsn
}

func (lc *LSNCoordinator) SetWalEnd(walEnd pglogrepl.LSN) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.walEnd = walEnd
	// Actualizar métrica sin bloquear
	if lc.metrics != nil {
		lc.metrics.SetWalEnd(walEnd)
	}
}

func (lc *LSNCoordinator) GetWalEnd() pglogrepl.LSN {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	return lc.walEnd
}
