package kafka

import (
	"errors"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaEventType string

const (
	KafkaEventTypeAssignedPartitions KafkaEventType = "assigned_partitions"
	KafkaEventTypeRevokedPartitions  KafkaEventType = "revoked_partitions"
	KafkaEventTypeError              KafkaEventType = "error"
	KafkaEventTypeOther              KafkaEventType = "other"
)

type Topic struct {
	Name              string
	Partitions        int
	ReplicationFactor int
}

func NewTopic(name string, partitions int, replicationFactor int) *Topic {
	return &Topic{Name: name, Partitions: partitions, ReplicationFactor: replicationFactor}
}

func (t *Topic) Validate() error {
	if t.Name == "" {
		return errors.New("topic name is required")
	}
	if t.Partitions <= 0 {
		return errors.New("partitions must be greater than 0")
	}
	if t.ReplicationFactor <= 0 {
		return errors.New("replication factor must be greater than 0")
	}
	return nil
}

func (t *Topic) Build() *kafka.TopicSpecification {
	return &kafka.TopicSpecification{
		Topic:             t.Name,
		NumPartitions:     t.Partitions,
		ReplicationFactor: t.ReplicationFactor,
	}
}

type CustomMessage struct {
	Topic          string
	Partition      int32
	Offset         int64
	ConsumeDate    time.Time
	ProcessDate    time.Time
	KafkaHeaders   []kafka.Header
	MessageValue   []byte
	KafkaTimestamp time.Time
}

func NewCustomMessage(message *kafka.Message, consumeDate time.Time,
	processDate time.Time) (*CustomMessage, error) {
	if message.TopicPartition.Topic == nil {
		return nil, errors.New("topic is nil")
	}

	m := &CustomMessage{
		Topic:        *message.TopicPartition.Topic,
		Partition:    message.TopicPartition.Partition,
		Offset:       int64(message.TopicPartition.Offset),
		KafkaHeaders: message.Headers,
		MessageValue: message.Value,
		ConsumeDate:  consumeDate,
		ProcessDate:  processDate,
	}

	return m, nil
}

type CustomError struct {
	EventType          KafkaEventType
	KafkaInternalEvent kafka.Event
	Consumer           *kafka.Consumer
	Err                error
}
