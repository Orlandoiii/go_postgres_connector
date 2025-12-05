package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/observability"
)

type workerEvent struct {
	changeEvent *ChangeEvent
	txEvent     *TransactionEvent
}

type TableWorker struct {
	tableKey       string
	lsnCoordinator *LSNCoordinator
	sink           EventSink
	eventCh        chan *workerEvent
	wg             sync.WaitGroup
	stopCh         chan struct{}
	observability.Logger
}

func NewTableWorker(tableKey string, lsnCoordinator *LSNCoordinator,
	sink EventSink,
	bufferSize int,
	logger observability.Logger) *TableWorker {

	return &TableWorker{
		tableKey:       tableKey,
		lsnCoordinator: lsnCoordinator,
		sink:           sink,
		eventCh:        make(chan *workerEvent, bufferSize),
		wg:             sync.WaitGroup{},
		stopCh:         make(chan struct{}),
		Logger:         logger,
	}
}

func (tw *TableWorker) processEvent(ctx context.Context, e *workerEvent) error {

	tw.Trace(ctx, "Procesando evento", "worker", tw.tableKey, "lsn", e.txEvent.LSN)

	err := tw.sink.PersistEvent(ctx, e.changeEvent, e.txEvent)

	if err != nil {

		tw.Error(ctx, "Error persisting event", err, "worker", tw.tableKey, "lsn", e.txEvent.LSN)

		return err
	}

	if e.txEvent.LSN > 0 {
		tw.lsnCoordinator.ReportLSN(ctx, tw.tableKey, e.txEvent.LSN)
	}

	return nil

}

func (tw *TableWorker) run(ctx context.Context) {
	defer tw.wg.Done()

	for {
		select {
		case <-ctx.Done():
			tw.Info(ctx, "TableWorker stopped by context done", nil,
				"table", tw.tableKey)
			return
		case <-tw.stopCh:
			tw.Info(ctx, "TableWorker stopped by stop channel", nil,
				"table", tw.tableKey)
			return
		case event := <-tw.eventCh:

			tw.Trace(ctx, "Procesando evento", "worker", tw.tableKey, "lsn", event.txEvent.LSN)

			err := tw.processEvent(ctx, event)

			if err != nil {
				tw.Error(ctx, "Error processing event", err,
					"table", tw.tableKey, "lsn", event.txEvent.LSN)
			}

			tw.Trace(ctx, "Evento procesado", "worker", tw.tableKey, "lsn", event.txEvent.LSN)

		}
	}
}

func (tw *TableWorker) Start(ctx context.Context) {
	tw.wg.Add(1)
	go tw.run(ctx)
}

func (tw *TableWorker) Stop(ctx context.Context) {
	tw.stopCh <- struct{}{}

	close(tw.stopCh)
	close(tw.eventCh)
	tw.wg.Wait()
}

func (tw *TableWorker) Process(ctx context.Context,
	changeEvent *ChangeEvent,
	txEvent *TransactionEvent) error {

	workerEvent := &workerEvent{changeEvent: changeEvent, txEvent: txEvent}

	if tw.eventCh == nil {
		return fmt.Errorf("channel is closed")
	}

	tw.eventCh <- workerEvent

	return nil
}

func (tw *TableWorker) PendingEvents() int {
	if tw.eventCh == nil {
		return 0
	}
	return len(tw.eventCh)
}

func (tw *TableWorker) HasPendingEvents() bool {
	return tw.PendingEvents() > 0
}
