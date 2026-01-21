package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/config"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/observability"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/utils"
)

type Dispatcher struct {
	workers            map[string]*TableWorker
	transactionWorkers map[string]*TransactionWorker // Nuevo: workers para transacciones
	coordinator        *LSNCoordinator
	logger             observability.Logger
	mu                 sync.RWMutex
	sinkFactory        SinkFactory
	listeners          []config.Listener
	workerBufferSize   int
	filter             EventFilterFactory
}

func NewDispatcher(sinkFactory SinkFactory,
	filterFactory EventFilterFactory,
	workerBufferSize int,
	coordinator *LSNCoordinator,
	listeners []config.Listener,
	logger observability.Logger) *Dispatcher {

	return &Dispatcher{
		workers:            make(map[string]*TableWorker),
		transactionWorkers: make(map[string]*TransactionWorker),
		coordinator:        coordinator,
		logger:             logger,
		mu:                 sync.RWMutex{},
		sinkFactory:        sinkFactory,
		listeners:          listeners,
		workerBufferSize:   workerBufferSize,
		filter:             filterFactory,
	}
}

func (d *Dispatcher) findListenerByTable(tableKey string) *config.Listener {

	for _, listener := range d.listeners {

		if listener.Table == tableKey {

			return &listener
		}
	}
	return nil
}

func (d *Dispatcher) getOrCreateWorker(ctx context.Context, workerKey string) *TableWorker {

	d.mu.RLock()
	worker, exists := d.workers[workerKey]
	d.mu.RUnlock()

	if exists {
		return worker
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if worker, exists := d.workers[workerKey]; exists {
		return worker
	}

	d.coordinator.RegisterTable(workerKey)

	sink, err := d.sinkFactory.CreateSink(workerKey)

	if err != nil {

		d.logger.Error(ctx, "Error creating sink", err,
			"table", workerKey)

		return nil
	}

	// Pasar el coordinator al sink si tiene el método SetCoordinator
	if sinkWithCoordinator, ok := sink.(interface{ SetCoordinator(*LSNCoordinator) }); ok {
		sinkWithCoordinator.SetCoordinator(d.coordinator)
	}

	worker = NewTableWorker(workerKey, d.coordinator, sink, d.workerBufferSize, d.logger)

	worker.Start(ctx)

	d.workers[workerKey] = worker

	d.logger.Info(ctx, "Created new worker",
		"worker", workerKey)

	return worker

}

func (d *Dispatcher) processSingleEvent(ctx context.Context,
	changeEventSink *ChangeEventSink,
	pipeline *config.Pipeline,
	workerKey string) error {

	if pipeline == nil {
		return d.persistEvent(ctx, workerKey, "", changeEventSink)
	}

	for _, target := range pipeline.Targets {

		if target.Group != "" && target.SendAsTransaction {
			continue
		}

		if !d.filter.CreateFilter(target.Filter).
			ShouldProcessSingleEvent(ctx, changeEventSink) {

			d.logger.Info(ctx, "Event filtered", nil,
				"worker", workerKey, "target", target.Name)

			continue
		}

		err := d.persistEvent(ctx, workerKey, target.Name, changeEventSink)

		if err != nil {

			d.logger.Error(ctx, "Error persisting event", err,
				"worker", workerKey, "target", target.Name)

			return err
		}
	}

	return nil

}

func (d *Dispatcher) persistEvent(ctx context.Context, tableKey string, targetName string,
	changeEventSink *ChangeEventSink) error {

	workerKey := fmt.Sprintf("%s.%s", tableKey, targetName)

	if utils.StringIsEmptyOrWhitespace(targetName) {
		workerKey = tableKey
	}

	worker := d.getOrCreateWorker(ctx, workerKey)

	if worker == nil {
		return fmt.Errorf("worker not found")
	}

	return worker.Process(ctx, changeEventSink)
}

func (d *Dispatcher) getOrCreateTransactionWorker(ctx context.Context, groupKey string) *TransactionWorker {
	d.mu.RLock()
	worker, exists := d.transactionWorkers[groupKey]
	d.mu.RUnlock()

	if exists {
		return worker
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if worker, exists := d.transactionWorkers[groupKey]; exists {
		return worker
	}

	d.coordinator.RegisterTable(groupKey)

	sink, err := d.sinkFactory.CreateSink(groupKey)
	if err != nil {
		d.logger.Error(ctx, "Error creating sink for transaction worker", err,
			"group", groupKey)
		return nil
	}

	// Pasar el coordinator al sink si tiene el método SetCoordinator
	if sinkWithCoordinator, ok := sink.(interface{ SetCoordinator(*LSNCoordinator) }); ok {
		sinkWithCoordinator.SetCoordinator(d.coordinator)
	}

	worker = NewTransactionWorker(groupKey, d.coordinator, sink, d.workerBufferSize, d.logger)
	worker.Start(ctx)

	d.transactionWorkers[groupKey] = worker

	d.logger.Info(ctx, "Created new transaction worker",
		"group", groupKey)

	return worker
}

func (d *Dispatcher) processGroupedTransaction(ctx context.Context,
	txEvent *TransactionEvent,
	groupKey string,
	targetsByGroup map[string][]config.Target,
	tablesByGroup map[string]map[string]bool) error {

	groupTables := tablesByGroup[groupKey]
	if groupTables == nil {

		groupTables = make(map[string]bool)
	}

	groupOperations := make([]ChangeEvent, 0)
	for _, changeEvent := range txEvent.Operations {
		tableKey := fmt.Sprintf("%s.%s", changeEvent.Schema, changeEvent.Table)
		if groupTables[tableKey] {
			groupOperations = append(groupOperations, changeEvent)
		}
	}

	if len(groupOperations) == 0 {
		return nil
	}

	groupEventsSink := make([]*ChangeEventSink, 0, len(groupOperations))
	for _, changeEvent := range groupOperations {
		changeEventSink := changeEvent.ToChangeEventSink(txEvent.Xid, txEvent.LSN)
		groupEventsSink = append(groupEventsSink, changeEventSink)
	}

	targets := targetsByGroup[groupKey]

	allTargetsPassed := true
	for _, target := range targets {
		filter := d.filter.CreateFilter(target.Filter)

		targetPassed := true
		for _, changeEventSink := range groupEventsSink {
			if !filter.ShouldProcessSingleEvent(ctx, changeEventSink) {
				targetPassed = false
				d.logger.Info(ctx, "Transaction filtered: at least one event in group was filtered",
					nil, "group", groupKey, "target", target.Name,
					"table", fmt.Sprintf("%s.%s", changeEventSink.Schema, changeEventSink.Table))
				break
			}
		}

		if !targetPassed {
			allTargetsPassed = false
			break
		}
	}

	if !allTargetsPassed {
		return nil
	}

	filteredTxEvent := &TransactionEvent{
		Xid:        txEvent.Xid,
		Timestamp:  txEvent.Timestamp,
		LSN:        txEvent.LSN,
		BeginLSN:   txEvent.BeginLSN,
		Operations: groupOperations,
		CommitLSN:  txEvent.CommitLSN,
		IsCommit:   txEvent.IsCommit,
		CommitTime: txEvent.CommitTime,
	}

	worker := d.getOrCreateTransactionWorker(ctx, groupKey)

	if worker != nil {

		err := worker.Process(ctx, filteredTxEvent)

		if err != nil {
			d.logger.Error(ctx, "Error processing grouped transaction", err,
				"group", groupKey)
			return err
		}
	}

	return nil
}

func (d *Dispatcher) Dispatch(ctx context.Context, e *TransactionEvent) error {

	if e == nil || len(e.Operations) == 0 {

		d.logger.Warn(ctx, "No operations to dispatch", nil,
			"event", e)

		return nil
	}

	targetsByGroup := make(map[string][]config.Target)
	tablesByGroup := make(map[string]map[string]bool)
	individualListeners := make(map[string]*config.Listener)

	for _, changeEvent := range e.Operations {

		tableKey := fmt.Sprintf("%s.%s", changeEvent.Schema, changeEvent.Table)

		listener := d.findListenerByTable(tableKey)

		if listener == nil {
			d.logger.Info(ctx, "Event filtered NOT listener found", nil,
				"event", changeEvent, "table", tableKey)
			continue
		}

		if listener.Pipeline != nil {
			hasGroupedTargets := false

			for _, target := range listener.Pipeline.Targets {
				if target.Group != "" && target.SendAsTransaction {

					hasGroupedTargets = true

					if tablesByGroup[target.Group] == nil {
						tablesByGroup[target.Group] = make(map[string]bool)
					}
					tablesByGroup[target.Group][tableKey] = true

					if targetsByGroup[target.Group] == nil {
						targetsByGroup[target.Group] = make([]config.Target, 0)
					}
					found := false
					for _, existingTarget := range targetsByGroup[target.Group] {
						if existingTarget.Name == target.Name {
							found = true
							break
						}
					}
					if !found {
						targetsByGroup[target.Group] = append(targetsByGroup[target.Group], target)
					}
				}
			}
			if !hasGroupedTargets {
				individualListeners[tableKey] = listener
			} else {
				hasIndividualTargets := false
				for _, target := range listener.Pipeline.Targets {
					if target.Group == "" || !target.SendAsTransaction {
						hasIndividualTargets = true
						break
					}
				}
				if hasIndividualTargets {
					individualListeners[tableKey] = listener
				}
			}
		} else {
			individualListeners[tableKey] = listener
		}
	}

	for groupKey := range targetsByGroup {
		err := d.processGroupedTransaction(ctx, e, groupKey, targetsByGroup, tablesByGroup)
		if err != nil {
			d.logger.Error(ctx, "Error processing grouped transaction", err,
				"group", groupKey)
		}
	}

	for _, changeEvent := range e.Operations {
		tableKey := fmt.Sprintf("%s.%s", changeEvent.Schema, changeEvent.Table)
		listener := individualListeners[tableKey]

		if listener == nil {
			continue
		}

		changeEventSink := changeEvent.ToChangeEventSink(e.Xid, e.LSN)

		err := d.processSingleEvent(ctx, changeEventSink, listener.Pipeline, tableKey)

		if err != nil {

			d.logger.Error(ctx, "Error processing event", err,
				"event", changeEvent, "listener", listener.Publication)

			continue
		}

	}

	return nil

}

func (d *Dispatcher) Stop(ctx context.Context) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// CRITICAL FIX: Limpiar workers después de detenerlos para prevenir memory leaks
	workersCount := len(d.workers)
	transactionWorkersCount := len(d.transactionWorkers)

	for workerKey, worker := range d.workers {
		worker.Stop(ctx)

		if worker.sink != nil {
			err := worker.sink.Close()

			if err != nil {
				d.logger.Error(ctx, "Error closing sink", err,
					"table", worker.tableKey)
			}
		}

		// Limpiar entrada del map para liberar memoria
		delete(d.workers, workerKey)

		// Limpiar LSN del coordinator
		d.coordinator.UnregisterTable(workerKey)
	}

	for groupKey, worker := range d.transactionWorkers {
		worker.Stop(ctx)

		if worker.sink != nil {
			err := worker.sink.Close()

			if err != nil {
				d.logger.Error(ctx, "Error closing transaction sink", err,
					"group", worker.groupKey)
			}
		}

		// Limpiar entrada del map para liberar memoria
		delete(d.transactionWorkers, groupKey)

		// Limpiar LSN del coordinator
		d.coordinator.UnregisterTable(groupKey)
	}

	d.logger.Info(ctx, "Dispatcher stopped and cleaned up",
		"workers_cleaned", workersCount,
		"transaction_workers_cleaned", transactionWorkersCount)
}

func (d *Dispatcher) GetTotalPendingEvents() int {
	d.mu.RLock()
	defer d.mu.RUnlock()

	total := 0
	for _, worker := range d.workers {
		total += worker.PendingEvents()
	}
	for _, worker := range d.transactionWorkers {
		total += worker.PendingEvents()
	}
	return total
}

func (d *Dispatcher) HasPendingEvents() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	for _, worker := range d.workers {
		if worker.HasPendingEvents() {
			return true
		}
	}
	for _, worker := range d.transactionWorkers {
		if worker.HasPendingEvents() {
			return true
		}
	}
	return false
}
