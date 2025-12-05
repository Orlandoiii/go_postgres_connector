package kafka

import (
	"context"
	"errors"
	"strings"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/observability"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type ProducerConfig struct {
	serverConfigs
	*securityConfig

	acks *ACKS

	lingerMs  int
	batchSize int

	retries           int
	deliveryTimeoutMs int
	messageTimeoutMs  int
}

func NewProducerCgfWithSvrCfgs(serverConfigs *serverConfigs,
	securityConfig *securityConfig) (*ProducerConfig, error) {

	if serverConfigs == nil {
		return nil, errors.New("serverConfigs is required")
	}

	acks := ACKsAll
	p := &ProducerConfig{
		serverConfigs:     *serverConfigs,
		securityConfig:    securityConfig,
		acks:              &acks,
		retries:           1,
		deliveryTimeoutMs: 10000,
		messageTimeoutMs:  10000,
	}

	return p, nil
}

func NewProducerCfg(bootstrapServers []string) (*ProducerConfig, error) {

	serverConfigs, err := NewServerConfigs(bootstrapServers, nil)

	if err != nil {
		return nil, err
	}

	return NewProducerCgfWithSvrCfgs(serverConfigs, nil)
}

func (p *ProducerConfig) WithACKs(acks ACKS) (*ProducerConfig, error) {
	if acks != ACKsAll && acks != ACKsLeader && acks != ACKsNone {
		return nil, errors.New("invalid acks value")
	}
	p.acks = &acks
	return p, nil
}

func (p *ProducerConfig) WithLingerMs(lingerMs int) *ProducerConfig {
	if lingerMs < 0 {
		return p
	}
	p.lingerMs = lingerMs
	return p
}

func (p *ProducerConfig) WithBatchSize(batchSize int) *ProducerConfig {
	if batchSize <= 0 {
		return p
	}
	p.batchSize = batchSize
	return p
}

func (p *ProducerConfig) WithRetries(retries int) *ProducerConfig {
	if retries < 0 {
		return p
	}
	p.retries = retries
	return p
}

func (p *ProducerConfig) WithDeliveryTimeoutMs(deliveryTimeoutMs int) *ProducerConfig {
	if deliveryTimeoutMs < 0 {
		return p
	}
	p.deliveryTimeoutMs = deliveryTimeoutMs
	return p
}

func (p *ProducerConfig) WithMessageTimeoutMs(messageTimeoutMs int) *ProducerConfig {
	if messageTimeoutMs < 0 {
		return p
	}
	p.messageTimeoutMs = messageTimeoutMs
	return p
}

func (p *ProducerConfig) Build() (*kafka.ConfigMap, error) {
	configMap := kafka.ConfigMap{}

	configMap.SetKey("bootstrap.servers", strings.Join(p.bootstrapServers, ","))
	configMap.SetKey("acks", int(*p.acks))
	configMap.SetKey("delivery.timeout.ms", p.deliveryTimeoutMs)
	configMap.SetKey("message.timeout.ms", p.messageTimeoutMs)
	configMap.SetKey("retries", p.retries)

	if p.lingerMs > 0 {
		configMap.SetKey("linger.ms", p.lingerMs)
	}

	if p.batchSize > 0 {
		configMap.SetKey("batch.size", p.batchSize)
	}

	if p.securityConfig != nil {
		p.securityConfig.Build(&configMap)
	}

	return &configMap, nil
}

type ProducerService struct {
	Config *ProducerConfig
	*kafka.Producer
	logger          observability.Logger
	DeliveryReports chan kafka.Event
}

func NewProducerService(config *ProducerConfig, logger observability.Logger) (*ProducerService, error) {
	p := &ProducerService{
		Config: config,
		logger: logger,
	}

	cfg, err := config.Build()
	if err != nil {
		return nil, err
	}

	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		return nil, err
	}

	p.Producer = producer
	p.DeliveryReports = producer.Events()

	return p, nil
}

func (s *ProducerService) Close() {
	if s.Producer != nil {
		s.Producer.Close()
	}
}

func (s *ProducerService) ProduceMessageByteSync(ctx context.Context,
	topic string, message []byte) error {

	deliveryChanReport := make(chan kafka.Event)

	err := s.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: int32(kafka.PartitionAny),
		},
		Value: []byte(message),
	}, deliveryChanReport)

	if err != nil {
		close(deliveryChanReport)
		return err
	}

	e := <-deliveryChanReport
	m := e.(*kafka.Message)
	close(deliveryChanReport)

	if m.TopicPartition.Error != nil {
		s.logger.Error(ctx, "Error producing message", m.TopicPartition.Error)
		return m.TopicPartition.Error
	}

	s.Flush(1000)
	return nil
}

func (s *ProducerService) ProduceMessageSync(ctx context.Context, topic string, message string) error {
	return s.ProduceMessageByteSync(ctx, topic, []byte(message))
}

func (s *ProducerService) ProduceMessageAsync(topic string, message []byte, opaque interface{}) error {
	err := s.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: int32(kafka.PartitionAny),
		},
		Value:  message,
		Opaque: opaque,
	}, nil)

	if err != nil {
		return err
	}

	return nil
}

func (s *ProducerService) ProduceMessageByteAsync(ctx context.Context,
	topic string, message []byte, deliveryChanReport chan kafka.Event) error {

	err := s.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: int32(kafka.PartitionAny),
		},
		Value: message,
	}, deliveryChanReport)

	if err != nil {
		s.logger.Error(ctx, "Error producing message", err)
		return err
	}

	return err
}
