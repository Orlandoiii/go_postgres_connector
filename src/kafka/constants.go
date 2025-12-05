package kafka

type AutoOffsetReset string

const (
	AutoOffsetResetEarliest AutoOffsetReset = "earliest"
	AutoOffsetResetLatest   AutoOffsetReset = "latest"
	AutoOffsetResetNone     AutoOffsetReset = "none"
)

func IsNotValidAutoOffsetReset(offsetReset AutoOffsetReset) bool {
	return offsetReset != AutoOffsetResetEarliest &&
		offsetReset != AutoOffsetResetLatest &&
		offsetReset != AutoOffsetResetNone
}

type PartitionAssignmentStrategy string

const (
	PartitionAssignmentStrategyRange      PartitionAssignmentStrategy = "range"
	PartitionAssignmentStrategyRoundRobin PartitionAssignmentStrategy = "roundrobin"
	PartitionAssignmentStrategySticky     PartitionAssignmentStrategy = "sticky"
)

func IsNotValidPartitionAssignmentStrategy(strategy PartitionAssignmentStrategy) bool {
	return strategy != PartitionAssignmentStrategyRange &&
		strategy != PartitionAssignmentStrategyRoundRobin &&
		strategy != PartitionAssignmentStrategySticky
}

type ACKS int

const (
	ACKsAll    ACKS = -1
	ACKsLeader ACKS = 1
	ACKsNone   ACKS = 0
)

func IsNotValidACKs(acks ACKS) bool {
	return acks != ACKsAll &&
		acks != ACKsLeader &&
		acks != ACKsNone
}
