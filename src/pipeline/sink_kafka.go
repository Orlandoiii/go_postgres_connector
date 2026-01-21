package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/config"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/kafka"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/observability"
	confluentkafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pglogrepl"
)

type KafkaSinkFactory struct {
	kafkaCfg         *config.KafkaConfig
	logger           observability.Logger
	mu               sync.Mutex
	producers        map[string]*kafka.ProducerService
	deliveryMonitors map[string]*deliveryMonitor
}

type deliveryMonitor struct {
	producer          *kafka.ProducerService
	topic             string
	logger            observability.Logger
	mu                sync.RWMutex
	pendingTx         map[uint32]*pendingTransaction
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	cleanupInterval   time.Duration // Intervalo para limpiar transacciones huérfanas
	txTimeout         time.Duration // Timeout para transacciones pendientes
}

// pendingTransaction rastrea el estado de una transacción pendiente
// Solo se accede desde el deliveryMonitor que ya tiene su propio mutex
type pendingTransaction struct {
	xid         uint32
	pending     int
	confirmed   int
	lsn         pglogrepl.LSN
	tableKey    string
	coordinator *LSNCoordinator
	createdAt   time.Time // Timestamp para limpieza de transacciones huérfanas
}

type messageMetadata struct {
	xid         uint32
	tableKey    string
	coordinator *LSNCoordinator
	lsn         pglogrepl.LSN
}

func NewKafkaSinkFactory(kafkaCfg *config.KafkaConfig, logger observability.Logger) (*KafkaSinkFactory, error) {
	if kafkaCfg == nil {
		return nil, fmt.Errorf("kafka config is required")
	}

	if len(kafkaCfg.BootstrapServers) == 0 {
		return nil, fmt.Errorf("bootstrap servers are required")
	}

	return &KafkaSinkFactory{
		kafkaCfg:         kafkaCfg,
		logger:           logger,
		producers:        make(map[string]*kafka.ProducerService),
		deliveryMonitors: make(map[string]*deliveryMonitor),
	}, nil
}

func (ksf *KafkaSinkFactory) CreateSink(tableKey string) (EventSink, error) {
	ksf.mu.Lock()
	defer ksf.mu.Unlock()

	topic := ksf.getTopicName(tableKey)

	producer, exists := ksf.producers[topic]
	var monitor *deliveryMonitor

	if !exists {
		producerCfg, err := buildProducerConfig(ksf.kafkaCfg)
		if err != nil {
			return nil, fmt.Errorf("build producer config: %w", err)
		}

		prod, err := kafka.NewProducerService(producerCfg, ksf.logger)
		if err != nil {
			return nil, fmt.Errorf("create producer service: %w", err)
		}

		producer = prod
		ksf.producers[topic] = producer

		// CRITICAL FIX: Usar contexto cancelable que se puede cancelar explícitamente
		// El cancel se llama en stop(), pero también se asegura en Close()
		ctx, cancel := context.WithCancel(context.Background())
		mon := &deliveryMonitor{
			producer:        producer,
			topic:           topic,
			logger:          ksf.logger,
			pendingTx:       make(map[uint32]*pendingTransaction),
			ctx:             ctx,
			cancel:          cancel,
			cleanupInterval: 30 * time.Second, // Limpiar transacciones huérfanas cada 30s
			txTimeout:       5 * time.Minute,  // Timeout para transacciones pendientes
		}

		mon.start()
		monitor = mon
		ksf.deliveryMonitors[topic] = monitor
	} else {
		monitor = ksf.deliveryMonitors[topic]
	}

	return &KafkaSink{
		factory:  ksf,
		topic:    topic,
		tableKey: tableKey,
		logger:   ksf.logger,
		producer: producer,
		monitor:  monitor,
		sem:      make(chan struct{}, 100),
	}, nil
}

func (ksf *KafkaSinkFactory) getTopicName(tableKey string) string {
	idx := -1
	for i := len(tableKey) - 1; i >= 0; i-- {
		if tableKey[i] == ':' {
			idx = i
			break
		}
	}

	if idx >= 0 {
		return tableKey[idx+1:]
	}

	return strings.ReplaceAll(tableKey, ".", "_")
}

func (ksf *KafkaSinkFactory) Close() error {
	ksf.mu.Lock()
	defer ksf.mu.Unlock()

	for _, monitor := range ksf.deliveryMonitors {
		monitor.stop()
	}

	for _, producer := range ksf.producers {
		if producer != nil {
			producer.Close()
		}
	}

	return nil
}

func (dm *deliveryMonitor) start() {
	dm.wg.Add(1)
	go dm.run()
}

func (dm *deliveryMonitor) run() {
	defer dm.wg.Done()

	// Ticker para limpiar transacciones huérfanas periódicamente
	cleanupTicker := time.NewTicker(dm.cleanupInterval)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-dm.ctx.Done():
			dm.logger.Trace(dm.ctx, "DeliveryMonitor stopped by context")
			return
		case <-cleanupTicker.C:
			// Limpiar transacciones huérfanas con timeout
			dm.cleanupStaleTransactions()
		case e := <-dm.producer.DeliveryReports:
			if e == nil {
				continue
			}

			switch ev := e.(type) {
			case *confluentkafka.Message:
				if ev.Opaque == nil {
					continue
				}

				metadata, ok := ev.Opaque.(*messageMetadata)
				if !ok {
					continue
				}

				dm.mu.Lock()
				tx, exists := dm.pendingTx[metadata.xid]
				if !exists {
					dm.mu.Unlock()
					continue
				}

				if ev.TopicPartition.Error != nil {
					dm.logger.Error(dm.ctx, "Error produciendo mensaje en Kafka", ev.TopicPartition.Error,
						"topic", dm.topic, "xid", metadata.xid)
					delete(dm.pendingTx, metadata.xid)
					dm.mu.Unlock()
					continue
				}

				// Incrementar contador de confirmados
				tx.confirmed++

				// Si todos los mensajes fueron confirmados, reportar LSN y limpiar
				if tx.confirmed >= tx.pending {
					// Guardar valores antes de eliminar
					lsn := tx.lsn
					tableKey := tx.tableKey
					coordinator := tx.coordinator

					delete(dm.pendingTx, metadata.xid)
					dm.mu.Unlock()

					// Reportar LSN fuera del lock para evitar bloqueos
					if coordinator != nil && lsn > 0 && tableKey != "" {
						coordinator.ReportLSN(dm.ctx, tableKey, lsn)
					}
				} else {
					dm.mu.Unlock()
				}
			}
		}
	}
}

func (dm *deliveryMonitor) stop() {
	// CRITICAL FIX: Asegurar que el contexto se cancele siempre
	// Esto previene que la goroutine quede bloqueada
	if dm.cancel != nil {
		dm.cancel()
	}
	dm.wg.Wait()
	
	// Limpiar transacciones pendientes al detener
	dm.mu.Lock()
	dm.pendingTx = make(map[uint32]*pendingTransaction)
	dm.mu.Unlock()
}

// cleanupStaleTransactions limpia transacciones que han excedido el timeout
// Previene memory leaks de transacciones que nunca se confirman
func (dm *deliveryMonitor) cleanupStaleTransactions() {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	now := time.Now()
	staleCount := 0
	
	for xid, tx := range dm.pendingTx {
		if now.Sub(tx.createdAt) > dm.txTimeout {
			dm.logger.Warn(dm.ctx, "Limpiando transacción huérfana (timeout)",
				nil,
				"topic", dm.topic,
				"xid", xid,
				"age", now.Sub(tx.createdAt).String(),
				"pending", tx.pending,
				"confirmed", tx.confirmed)
			
			// Reportar LSN antes de eliminar para no perder progreso
			if tx.coordinator != nil && tx.lsn > 0 && tx.tableKey != "" {
				// Reportar fuera del lock
				go func(coord *LSNCoordinator, key string, lsn pglogrepl.LSN) {
					coord.ReportLSN(dm.ctx, key, lsn)
				}(tx.coordinator, tx.tableKey, tx.lsn)
			}
			
			delete(dm.pendingTx, xid)
			staleCount++
		}
	}
	
	if staleCount > 0 {
		dm.logger.Info(dm.ctx, "Transacciones huérfanas limpiadas",
			nil,
			"topic", dm.topic,
			"count", staleCount)
	}
}

func (dm *deliveryMonitor) registerTransaction(xid uint32,
	pending int,
	lsn pglogrepl.LSN,
	tableKey string,
	coordinator *LSNCoordinator) {

	dm.mu.Lock()
	defer dm.mu.Unlock()

	dm.pendingTx[xid] = &pendingTransaction{
		xid:         xid,
		pending:     pending,
		confirmed:   0,
		lsn:         lsn,
		tableKey:    tableKey,
		coordinator: coordinator,
		createdAt:   time.Now(), // Timestamp para limpieza de huérfanas
	}
}

func (dm *deliveryMonitor) unregisterTransaction(xid uint32) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	delete(dm.pendingTx, xid)
}

type KafkaSink struct {
	factory     *KafkaSinkFactory
	topic       string
	tableKey    string
	logger      observability.Logger
	producer    *kafka.ProducerService
	monitor     *deliveryMonitor
	sem         chan struct{}
	coordinator *LSNCoordinator
}

func (ks *KafkaSink) SetCoordinator(coordinator *LSNCoordinator) {
	ks.coordinator = coordinator
}

func (ks *KafkaSink) PersistSingleEvent(ctx context.Context,
	changeEvent *ChangeEventSink) error {

	if changeEvent == nil {
		return nil
	}

	jsonData, err := json.Marshal(changeEvent)
	if err != nil {
		return fmt.Errorf("serialize event: %w", err)
	}

	metadata := &messageMetadata{
		xid:         changeEvent.Xid,
		tableKey:    ks.tableKey,
		coordinator: ks.coordinator,
		lsn:         changeEvent.Lsn,
	}

	// Si hay LSN, registrar la transacción antes de enviar
	// Para eventos individuales, siempre es 1 mensaje por transacción
	if changeEvent.Lsn > 0 && ks.coordinator != nil {
		ks.monitor.registerTransaction(changeEvent.Xid, 1, changeEvent.Lsn, ks.tableKey, ks.coordinator)
	}

	ks.sem <- struct{}{}
	go func() {
		defer func() { <-ks.sem }()

		err := ks.producer.ProduceMessageAsync(ks.topic, jsonData, metadata)
		if err != nil {
			// Desregistrar transacción si falla inmediatamente para evitar LSN bloqueado
			if changeEvent.Lsn > 0 && changeEvent.Xid > 0 {
				ks.monitor.unregisterTransaction(changeEvent.Xid)
			}
			ks.logger.Error(ctx, "Error produciendo mensaje en Kafka", err,
				"topic", ks.topic, "xid", changeEvent.Xid)
		}
	}()

	return nil
}

func (ks *KafkaSink) PersistTransaction(ctx context.Context,
	txEvent *TransactionEvent) error {

	if txEvent == nil {
		return fmt.Errorf("transaction event is nil")
	}

	jsonData, err := json.Marshal(txEvent)
	if err != nil {
		return fmt.Errorf("serialize transaction: %w", err)
	}

	metadata := &messageMetadata{
		xid:         txEvent.Xid,
		tableKey:    ks.tableKey,
		coordinator: ks.coordinator,
		lsn:         txEvent.LSN,
	}

	// Para transacciones completas, siempre es 1 mensaje
	if txEvent.LSN > 0 && ks.coordinator != nil {
		ks.monitor.registerTransaction(txEvent.Xid, 1, txEvent.LSN, ks.tableKey, ks.coordinator)
	}

	ks.sem <- struct{}{}
	go func() {
		defer func() { <-ks.sem }()

		err := ks.producer.ProduceMessageAsync(ks.topic, jsonData, metadata)
		if err != nil {
			// Desregistrar transacción si falla inmediatamente para evitar LSN bloqueado
			if txEvent.LSN > 0 && txEvent.Xid > 0 {
				ks.monitor.unregisterTransaction(txEvent.Xid)
			}
			ks.logger.Error(ctx, "Error produciendo transacción en Kafka", err,
				"topic", ks.topic, "xid", txEvent.Xid)
		}
	}()

	return nil
}

func (ks *KafkaSink) Close() error {
	return nil
}

func buildProducerConfig(kafkaCfg *config.KafkaConfig) (*kafka.ProducerConfig, error) {
	var clientID *string
	if kafkaCfg.ClientID != "" {
		clientID = &kafkaCfg.ClientID
	}

	serverConfigs, err := kafka.NewServerConfigs(kafkaCfg.BootstrapServers, clientID)
	if err != nil {
		return nil, err
	}

	return kafka.NewProducerCgfWithSvrCfgs(serverConfigs, nil)
}
