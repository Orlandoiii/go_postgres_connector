package pipeline

import (
	"sync"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/observability"

	"github.com/jackc/pglogrepl"
)

// LSNCoordinator es el coordinador de los LSNs de las tablas
type LSNCoordinator struct {
	mu         sync.RWMutex
	targetLSNs map[string]pglogrepl.LSN
	observability.Logger
}

// NewLSNCoordinator crea un nuevo LSNCoordinator
func NewLSNCoordinator(logger observability.Logger) *LSNCoordinator {
	return &LSNCoordinator{
		mu:         sync.RWMutex{},
		targetLSNs: make(map[string]pglogrepl.LSN),
		Logger:     logger,
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

func (lc *LSNCoordinator) ReportLSN(workerKey string, lsn pglogrepl.LSN) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	current, exists := lc.targetLSNs[workerKey]

	if !exists || lsn > current {

		lc.targetLSNs[workerKey] = lsn
	}
}

func (lc *LSNCoordinator) GetGlobalLSN() pglogrepl.LSN {
	lc.mu.RLock()
	defer lc.mu.RUnlock()

	if len(lc.targetLSNs) == 0 {
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

	return minLsn
}
