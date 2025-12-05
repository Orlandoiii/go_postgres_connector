package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

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
	producer  *kafka.ProducerService
	topic     string
	logger    observability.Logger
	mu        sync.RWMutex
	pendingTx map[uint32]*pendingTransaction
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
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

		ctx, cancel := context.WithCancel(context.Background())
		mon := &deliveryMonitor{
			producer:  producer,
			topic:     topic,
			logger:    ksf.logger,
			pendingTx: make(map[uint32]*pendingTransaction),
			ctx:       ctx,
			cancel:    cancel,
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

	for {
		select {
		case <-dm.ctx.Done():
			return
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
	dm.cancel()
	dm.wg.Wait()
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
	}
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
