package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/observability"
)

type TransactionWorker struct {
	groupKey       string
	lsnCoordinator *LSNCoordinator
	sink           EventSink
	eventCh        chan *TransactionEvent
	wg             sync.WaitGroup
	stopCh         chan struct{}
	observability.Logger
}

func NewTransactionWorker(groupKey string, lsnCoordinator *LSNCoordinator,
	sink EventSink,
	bufferSize int,
	logger observability.Logger) *TransactionWorker {

	return &TransactionWorker{
		groupKey:       groupKey,
		lsnCoordinator: lsnCoordinator,
		sink:           sink,
		eventCh:        make(chan *TransactionEvent, bufferSize),
		wg:             sync.WaitGroup{},
		stopCh:         make(chan struct{}),
		Logger:         logger,
	}
}

func (tw *TransactionWorker) processEvent(ctx context.Context, e *TransactionEvent) error {

	if e == nil {
		tw.Error(ctx, "Transaction event is nil", nil, "group", tw.groupKey)

		return fmt.Errorf("transaction event is nil")
	}

	tw.Trace(ctx, "Procesando transacción completa", "group", tw.groupKey, "lsn", e.LSN)

	err := tw.sink.PersistTransaction(ctx, e)

	if err != nil {
		tw.Error(ctx, "Error persisting transaction", err, "group", tw.groupKey, "lsn", e.LSN)

		return err
	}

	if e.LSN > 0 {
		tw.lsnCoordinator.ReportLSN(ctx, tw.groupKey, e.LSN)
	}

	return nil
}

func (tw *TransactionWorker) run(ctx context.Context) {
	defer tw.wg.Done()

	for {
		select {
		case <-ctx.Done():
			tw.Info(ctx, "TransactionWorker stopped by context done", nil,
				"group", tw.groupKey)
			return
		case <-tw.stopCh:
			tw.Info(ctx, "TransactionWorker stopped by stop channel", nil,
				"group", tw.groupKey)
			return
		case event := <-tw.eventCh:
			tw.Trace(ctx, "Procesando transacción", "group", tw.groupKey, "lsn", event.LSN)

			err := tw.processEvent(ctx, event)
			if err != nil {
				tw.Error(ctx, "Error processing transaction", err,
					"group", tw.groupKey, "lsn", event.LSN)
			}

			tw.Trace(ctx, "Transacción procesada", "group", tw.groupKey, "lsn", event.LSN)
		}
	}
}

func (tw *TransactionWorker) Start(ctx context.Context) {
	tw.wg.Add(1)
	go tw.run(ctx)
}

func (tw *TransactionWorker) Stop(ctx context.Context) {
	tw.stopCh <- struct{}{}
	close(tw.stopCh)
	close(tw.eventCh)
	tw.wg.Wait()
}

func (tw *TransactionWorker) Process(ctx context.Context, txEvent *TransactionEvent) error {
	if tw.eventCh == nil {
		return fmt.Errorf("channel is closed")
	}

	tw.eventCh <- txEvent
	return nil
}

func (tw *TransactionWorker) PendingEvents() int {
	if tw.eventCh == nil {
		return 0
	}
	return len(tw.eventCh)
}

func (tw *TransactionWorker) HasPendingEvents() bool {
	return tw.PendingEvents() > 0
}
