package kafka

import (
	"errors"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type AdminClientConfig struct {
	serverConfigs
	*securityConfig

	requestTimeoutMs int
	retries          int
	retryBackoffMs   int
	socketTimeoutMs  int
}

func NewAdminCgfWithSvrCfgs(serverConfigs *serverConfigs,
	securityConfig *securityConfig) (*AdminClientConfig, error) {

	if serverConfigs == nil {
		return nil, errors.New("serverConfigs is required")
	}

	a := &AdminClientConfig{
		serverConfigs:    *serverConfigs,
		securityConfig:   securityConfig,
		requestTimeoutMs: 30000,
		retries:          3,
		retryBackoffMs:   100,
		socketTimeoutMs:  60000,
	}

	return a, nil
}

func NewAdminCfg(bootstrapServers []string) (*AdminClientConfig, error) {

	serverConfigs, err := NewServerConfigs(bootstrapServers, nil)

	if err != nil {
		return nil, err
	}

	return NewAdminCgfWithSvrCfgs(serverConfigs, nil)
}

func (a *AdminClientConfig) WithRequestTimeoutMs(timeoutMs int) *AdminClientConfig {
	if timeoutMs > 0 {
		a.requestTimeoutMs = timeoutMs
	}
	return a
}

func (a *AdminClientConfig) WithRetries(retries int) *AdminClientConfig {
	if retries >= 0 {
		a.retries = retries
	}
	return a
}

func (a *AdminClientConfig) WithRetryBackoffMs(backoffMs int) *AdminClientConfig {
	if backoffMs > 0 {
		a.retryBackoffMs = backoffMs
	}
	return a
}

func (a *AdminClientConfig) WithSocketTimeoutMs(timeoutMs int) *AdminClientConfig {
	if timeoutMs > 0 {
		a.socketTimeoutMs = timeoutMs
	}
	return a
}

func (a *AdminClientConfig) Build() (*kafka.ConfigMap, error) {
	configMap := kafka.ConfigMap{}

	configMap.SetKey("bootstrap.servers", strings.Join(a.bootstrapServers, ","))

	configMap.SetKey("request.timeout.ms", a.requestTimeoutMs)
	configMap.SetKey("retries", a.retries)
	configMap.SetKey("retry.backoff.ms", a.retryBackoffMs)
	configMap.SetKey("socket.timeout.ms", a.socketTimeoutMs)

	if a.clientId != nil {
		configMap.SetKey("client.id", *a.clientId)
	}

	if a.securityConfig != nil {
		a.securityConfig.Build(&configMap)
	}

	return &configMap, nil
}
