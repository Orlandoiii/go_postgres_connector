package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/observability"
)

type TableWorker struct {
	tableKey       string
	lsnCoordinator *LSNCoordinator
	sink           EventSink
	eventCh        chan *ChangeEventSink
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
		eventCh:        make(chan *ChangeEventSink, bufferSize),
		wg:             sync.WaitGroup{},
		stopCh:         make(chan struct{}),
		Logger:         logger,
	}
}

func (tw *TableWorker) processEvent(ctx context.Context, e *ChangeEventSink) error {

	if e == nil {
		return fmt.Errorf("change event is nil")
	}

	tw.Trace(ctx, "Procesando evento", "worker", tw.tableKey, "lsn", e.Lsn)

	err := tw.sink.PersistSingleEvent(ctx, e)

	if err != nil {

		tw.Error(ctx, "Error persisting event", err, "worker", tw.tableKey, "lsn", e.Lsn)

		return err
	}

	if e.Lsn > 0 {
		tw.lsnCoordinator.ReportLSN(ctx, tw.tableKey, e.Lsn)
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

			err := tw.processEvent(ctx, event)

			if err != nil {
				tw.Error(ctx, "Error processing event", err,
					"table", tw.tableKey, "lsn", event.Lsn)
			}

			tw.Trace(ctx, "Evento procesado", "worker", tw.tableKey, "lsn", event.Lsn)

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
	changeEvent *ChangeEventSink) error {

	if tw.eventCh == nil {
		return fmt.Errorf("channel is closed")
	}

	//Patron para evitar bloqueos en el canal

	select {
	case tw.eventCh <- changeEvent:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return fmt.Errorf("worker buffer full, timeout after 5s")
	}
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
