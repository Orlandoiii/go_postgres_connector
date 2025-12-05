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
	workers          map[string]*TableWorker
	coordinator      *LSNCoordinator
	logger           observability.Logger
	mu               sync.RWMutex
	sinkFactory      SinkFactory
	listeners        []config.Listener
	workerBufferSize int
	filter           EventFilterFactory
}

func NewDispatcher(sinkFactory SinkFactory,
	filterFactory EventFilterFactory,
	workerBufferSize int,
	coordinator *LSNCoordinator,
	listeners []config.Listener,
	logger observability.Logger) *Dispatcher {

	return &Dispatcher{
		workers:          make(map[string]*TableWorker),
		coordinator:      coordinator,
		logger:           logger,
		mu:               sync.RWMutex{},
		sinkFactory:      sinkFactory,
		listeners:        listeners,
		workerBufferSize: workerBufferSize,
		filter:           filterFactory,
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

func (d *Dispatcher) Dispatch(ctx context.Context, e *TransactionEvent) error {

	if e == nil || len(e.Operations) == 0 {

		d.logger.Warn(ctx, "No operations to dispatch", nil,
			"event", e)

		return nil
	}

	for _, changeEvent := range e.Operations {

		tableKey := fmt.Sprintf("%s.%s", changeEvent.Schema, changeEvent.Table)

		listener := d.findListenerByTable(tableKey)

		if listener == nil {

			d.logger.Info(ctx, "Event filtered NOT listener found", nil,
				"event", changeEvent, "table", tableKey)

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

	for _, worker := range d.workers {
		worker.Stop(ctx)

		if worker.sink != nil {
			err := worker.sink.Close()

			if err != nil {
				d.logger.Error(ctx, "Error closing sink", err,
					"table", worker.tableKey)
			}
		}
	}
}

func (d *Dispatcher) GetTotalPendingEvents() int {
	d.mu.RLock()
	defer d.mu.RUnlock()

	total := 0
	for _, worker := range d.workers {
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
	return false
}
