package kafka

import (
	"errors"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type serverConfigs struct {
	bootstrapServers []string
	clientId         *string
}

func NewServerConfigs(bootstrapServers []string, clientId *string) (*serverConfigs, error) {
	if len(bootstrapServers) == 0 {
		return nil, errors.New("bootstrapServers is required")
	}

	return &serverConfigs{
		bootstrapServers: bootstrapServers,
		clientId:         clientId,
	}, nil
}

type securityConfig struct {
	securityProtocol string

	sslKeystoreLocation   string
	sslKeystorePassword   string
	sslTruststoreLocation string
	sslTruststorePassword string

	saslMechanism string
	saslUsername  string
	saslPassword  string
}

func NewSecurityConfig() *securityConfig {
	return &securityConfig{}
}

func (c *securityConfig) WithSASL(
	mechanism,
	username,
	password string) *securityConfig {

	if mechanism == "" || username == "" || password == "" {
		return c
	}

	c.saslMechanism = mechanism
	c.saslUsername = username
	c.saslPassword = password

	return c
}

func (c *securityConfig) WithSSL(keystoreLocation,
	keystorePassword,
	truststoreLocation,
	truststorePassword string) *securityConfig {

	if keystoreLocation == "" || keystorePassword == "" || truststoreLocation == "" || truststorePassword == "" {
		return c
	}

	c.sslKeystoreLocation = keystoreLocation
	c.sslKeystorePassword = keystorePassword
	c.sslTruststoreLocation = truststoreLocation
	c.sslTruststorePassword = truststorePassword

	return c
}

func (c *securityConfig) Build(configMap *kafka.ConfigMap) {

	if c.securityProtocol != "" {
		configMap.SetKey("security.protocol", c.securityProtocol)
	}

	if c.saslMechanism != "" {
		configMap.SetKey("sasl.mechanisms", c.saslMechanism)
	}
	if c.saslUsername != "" {
		configMap.SetKey("sasl.username", c.saslUsername)
	}
	if c.saslPassword != "" {
		configMap.SetKey("sasl.password", c.saslPassword)
	}

	if c.sslKeystoreLocation != "" {
		configMap.SetKey("ssl.keystore.location", c.sslKeystoreLocation)
	}
	if c.sslKeystorePassword != "" {
		configMap.SetKey("ssl.keystore.password", c.sslKeystorePassword)
	}
	if c.sslTruststoreLocation != "" {
		configMap.SetKey("ssl.truststore.location", c.sslTruststoreLocation)
	}
	if c.sslTruststorePassword != "" {
		configMap.SetKey("ssl.truststore.password", c.sslTruststorePassword)
	}

}
