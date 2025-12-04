package config

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/SOLUCIONESSYCOM/configuro"
	"github.com/SOLUCIONESSYCOM/scribe"
)

var cfg *configuro.AppConfig

var poolKeys = []string{"pool_min_conns", "pool_max_conns"}

type Condition struct {
	Field    string      `json:"Field"`
	Operator string      `json:"Operator"`
	Value    interface{} `json:"Value"`
}

type FilterConfig struct {
	Actions    []string    `json:"Actions,omitempty"`
	Conditions []Condition `json:"Conditions,omitempty"`
	Logic      string      `json:"Logic,omitempty"`
}

type Target struct {
	Name   string       `json:"Name"`
	Filter FilterConfig `json:"Filter,omitempty"`
}

type Pipeline struct {
	Targets []Target `json:"Targets"`
}

type Listener struct {
	Publication string    `json:"Publication"`
	Table       string    `json:"Table"`
	Actions     []string  `json:"Actions"`
	Pipeline    *Pipeline `json:"Pipeline,omitempty"`
}

type postgresConfig struct {
	connectionString string     `json:"-"` // Campo privado, no se deserializa directamente
	User             string     `json:"User"`
	Password         string     `json:"Password"`
	Listeners        []Listener `json:"Listeners"`
	SlotName         string     `json:"SlotName"`
	WorkerBufferSize int        `json:"WorkerBufferSize"`
	GoFinalLSN       bool       `json:"GoFinalLSN"`
}

type postgresConfigJSON struct {
	ConnectionString string     `json:"ConnectionString"`
	User             string     `json:"User"`
	Password         string     `json:"Password"`
	Listeners        []Listener `json:"Listeners"`
	SlotName         string     `json:"SlotName"`
	WorkerBufferSize int        `json:"WorkerBufferSize"`
	GoFinalLSN       bool       `json:"GoFinalLSN"`
}

func (c *postgresConfig) ConnectionString() string {
	connString := ""

	parts := strings.Split(c.connectionString, " ") // Cambiar a connectionString

	values := make(map[string]string)

	for _, part := range parts {
		parts := strings.Split(part, "=")
		if len(parts) == 2 {
			values[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	for key, value := range values {
		if !slices.Contains(poolKeys, strings.ToLower(key)) {
			connString += fmt.Sprintf("%s=%s ", key, value)
		}
	}
	connString += fmt.Sprintf("user=%s password=%s", c.User, c.Password)

	return connString
}

func (c *postgresConfig) ConnectionStringWithPool() string {
	connString := ""

	parts := strings.Split(c.connectionString, " ") // Cambiar a connectionString

	values := make(map[string]string)

	for _, part := range parts {
		parts := strings.Split(part, "=")
		if len(parts) == 2 {
			values[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	for key, value := range values {
		connString += fmt.Sprintf("%s=%s ", key, value)
	}
	connString += fmt.Sprintf("user=%s password=%s", c.User, c.Password)

	return connString
}

func (c *postgresConfig) String() string {
	return fmt.Sprintf("ConnectionString: %s, User: %s, Password: %s, Listeners: %v", c.connectionString, c.User, c.Password, c.Listeners)
}

type PostgresConfig struct {
	*postgresConfig
	Listeners []Listener `json:"Listeners"`
}

type Config struct {
	Postgres postgresConfig `json:"Postgres"`
	Kafka    kafkaConfig    `json:"Kafka,omitempty"`
}

type kafkaConfig struct {
	BootstrapServers []string `json:"BootstrapServers"`
	ClientID         string   `json:"ClientID,omitempty"`
}

type KafkaConfig struct {
	*kafkaConfig
}

func loadConfig() error {
	execPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("error al obtener el path del archivo de configuración: %w", err)
	}

	execDir := filepath.Dir(execPath)
	configPath := filepath.Join(execDir, "config.json")

	cfg, err = configuro.NewFromJsonFiles(true, configPath)
	if err != nil {
		return fmt.Errorf("error al cargar el archivo de configuración: %w", err)
	}
	return nil
}

func PostgresCfg() (*PostgresConfig, error) {

	if cfg == nil || !cfg.IsBeenLoaded() {

		err := loadConfig()

		if err != nil {
			return nil, err
		}

	}

	postgresConfigJson, err := configuro.GetSection[postgresConfigJSON](cfg, "Postgres")
	if err != nil {
		return nil, fmt.Errorf("error al obtener la configuración de la base de datos: %w", err)
	}
	postgresConfig := &postgresConfig{
		connectionString: postgresConfigJson.ConnectionString,
		User:             postgresConfigJson.User,
		Password:         postgresConfigJson.Password,
		Listeners:        postgresConfigJson.Listeners,
		SlotName:         postgresConfigJson.SlotName,
		WorkerBufferSize: postgresConfigJson.WorkerBufferSize,
		GoFinalLSN:       postgresConfigJson.GoFinalLSN,
	}

	return &PostgresConfig{postgresConfig: postgresConfig, Listeners: postgresConfig.Listeners}, nil
}

func LogCfg() (*scribe.ConfigLogger, error) {
	logConfigJson, err := configuro.GetSection[scribe.ConfigLogger](cfg, "Log")
	if err != nil {
		return nil, fmt.Errorf("error al obtener la configuración de la base de datos: %w", err)
	}
	return logConfigJson, nil
}

func KafkaCfg() (*KafkaConfig, error) {
	if cfg == nil || !cfg.IsBeenLoaded() {
		err := loadConfig()
		if err != nil {
			return nil, err
		}
	}

	kafkaConfigJson, err := configuro.GetSection[kafkaConfig](cfg, "Kafka")
	if err != nil {
		return nil, fmt.Errorf("error al obtener la configuración de Kafka: %w", err)
	}

	return &KafkaConfig{kafkaConfig: kafkaConfigJson}, nil
}

func init() {
	err := loadConfig()
	if err != nil {
		fmt.Println("Error al cargar el archivo de configuración:", err)
		panic(err)
	}
}
