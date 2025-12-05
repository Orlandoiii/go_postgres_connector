# PostgreSQL-Kafka Connector

Conector de replicación lógica de PostgreSQL a Kafka que captura cambios en tiempo real mediante Logical Replication de PostgreSQL y los envía a Kafka con capacidades avanzadas de filtrado, agrupación por transacciones y múltiples targets.

## Características Principales

- ✅ **Replicación Lógica en Tiempo Real**: Captura cambios de PostgreSQL usando Logical Replication
- ✅ **Filtrado Avanzado**: Sistema flexible de filtros con múltiples operadores y lógica AND/OR
- ✅ **Agrupación por Transacciones**: Soporte para enviar transacciones completas agrupadas
- ✅ **Múltiples Targets**: Configuración de múltiples destinos (topics) por tabla
- ✅ **Tracking Inteligente de LSN**: Coordinador que rastrea el progreso de cada worker y solo avanza LSN cuando no hay eventos pendientes
- ✅ **Observabilidad**: Logging estructurado y métricas Prometheus
- ✅ **Resiliencia**: Reconexión automática con backoff exponencial

## Requisitos

- Go 1.25.1 o superior
- PostgreSQL 10+ con Logical Replication habilitada
- Kafka 2.0+ (opcional, si se usa Kafka como sink)
- Permisos de replicación en PostgreSQL

## Instalación

```bash
# Clonar el repositorio
git clone <repository-url>
cd go_postgres_connector

# Instalar dependencias
go mod download

# Compilar
go build -o connector.exe ./cmd/connector.go
```

## Configuración

El conector se configura mediante el archivo `config.json` ubicado en el mismo directorio que el ejecutable.

### Estructura de Configuración

```json
{
    "Server": {
        "HttpPort": 9090
    },
    "Log": {
        "Enabled": true,
        "Console": true,
        "MinLevel": "INFO",
        "FilePath": "/path/to/logs"
    },
    "Postgres": {
        "ConnectionString": "host=localhost port=5432 dbname=mydb sslmode=disable",
        "User": "replication_user",
        "Password": "password",
        "SlotName": "cdc_slot",
        "WorkerBufferSize": 320,
        "GoFinalLSN": false,
        "Listeners": [...]
    },
    "Kafka": {
        "BootstrapServers": ["localhost:9092"],
        "ClientID": "postgres_connector"
    }
}
```

### Configuración de PostgreSQL

#### 1. Habilitar Logical Replication

En `postgresql.conf`:
```conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

#### 2. Crear Usuario con Permisos de Replicación

```sql
CREATE USER replication_user WITH REPLICATION PASSWORD 'password';
GRANT CONNECT ON DATABASE mydb TO replication_user;
```

#### 3. Configurar Listeners

Cada listener define qué tablas replicar y cómo procesarlas:

```json
{
    "Publication": "pub_my_table",
    "Table": "schema.table_name",
    "Actions": ["insert", "update", "delete"],
    "Pipeline": {
        "Targets": [
            {
                "Name": "target_name",
                "Group": "group_name",
                "SendAsTransaction": true,
                "Filter": {
                    "Actions": ["insert", "update"],
                    "Conditions": [
                        {
                            "Field": "new_data.status",
                            "Operator": "!=",
                            "Value": null
                        }
                    ],
                    "Logic": "AND"
                }
            }
        ]
    }
}
```

### Parámetros de Configuración

#### Postgres
- `ConnectionString`: String de conexión a PostgreSQL (sin user/password)
- `User`: Usuario de PostgreSQL con permisos de replicación
- `Password`: Contraseña del usuario
- `SlotName`: Nombre del slot de replicación lógica
- `WorkerBufferSize`: Tamaño del buffer de cada worker (default: 320)
- `GoFinalLSN`: Si es `true`, inicia desde el LSN final del slot (útil para empezar desde cero)

#### Listeners
- `Publication`: Nombre de la publicación PostgreSQL
- `Table`: Tabla a replicar en formato `schema.table`
- `Actions`: Acciones a capturar: `["insert", "update", "delete"]`
- `Pipeline`: Configuración opcional de procesamiento

#### Targets
- `Name`: Nombre del target (se usa como nombre del topic en Kafka)
- `Group`: Grupo opcional para agrupar transacciones
- `SendAsTransaction`: Si es `true`, envía la transacción completa agrupada
- `Filter`: Configuración de filtrado

#### Filtros
- `Actions`: Acciones a filtrar
- `Conditions`: Array de condiciones
  - `Field`: Campo a evaluar (formato: `new_data.field_name` o `old_data.field_name`)
  - `Operator`: Operador (`==`, `!=`, `>`, `<`, `>=`, `<=`, `in`, `not_in`, `exists`, `is_null`)
  - `Value`: Valor a comparar
- `Logic`: Lógica de combinación (`AND` o `OR`)

## Uso

```bash
# Ejecutar el conector
./connector.exe

# El conector iniciará:
# - Servidor HTTP en el puerto configurado (default: 9090)
# - Endpoint de métricas: http://localhost:9090/metrics
# - Health check: http://localhost:9090/health
```

## Arquitectura del Proyecto

### Estructura de Paquetes

```
go_postgres_connector/
├── cmd/
│   └── connector.go          # Punto de entrada principal
├── src/
│   ├── app/
│   │   └── connector.go      # Orquestador principal del conector
│   ├── config/
│   │   └── config.go         # Carga y validación de configuración
│   ├── postgres/
│   │   ├── replicator.go     # Manejo de replicación lógica
│   │   ├── decoder.go        # Decodificación de mensajes WAL
│   │   ├── connection.go     # Gestión de conexiones
│   │   └── publication.go    # Gestión de publicaciones
│   ├── pipeline/
│   │   ├── dispatcher.go     # Distribución de eventos a workers
│   │   ├── lsn_coordinator.go # Coordinación de LSN entre workers
│   │   ├── table_worker.go   # Worker para eventos individuales
│   │   ├── transaction_worker.go # Worker para transacciones completas
│   │   └── sink_kafka.go     # Implementación de sink para Kafka
│   ├── kafka/
│   │   ├── producer.go       # Cliente productor de Kafka
│   │   └── admin.go          # Administración de Kafka
│   ├── expressions/
│   │   ├── filter.go         # Sistema de filtrado
│   │   └── expressions.go    # Evaluador de expresiones
│   └── observability/
│       ├── logger.go         # Sistema de logging
│       └── metrics.go        # Métricas Prometheus
└── config.json               # Archivo de configuración
```

### Descripción de Paquetes

#### `cmd/connector.go`
Punto de entrada principal. Inicializa el servidor HTTP para métricas y health checks, y arranca el conector.

#### `src/app/connector.go`
Orquestador principal que:
- Carga la configuración
- Inicializa los componentes (logger, dispatcher, coordinator, etc.)
- Gestiona el ciclo de vida del conector
- Maneja reconexiones automáticas

#### `src/config/`
Carga y parsea la configuración desde `config.json`. Proporciona funciones para acceder a cada sección de configuración.

#### `src/postgres/`
**replicator.go**: Componente central que:
- Crea y gestiona el slot de replicación
- Lee mensajes del WAL de PostgreSQL
- Decodifica mensajes lógicos
- Coordina el avance de LSN

**decoder.go**: Convierte mensajes binarios del WAL en estructuras de datos Go.

**connection.go**: Gestiona conexiones a PostgreSQL con reconexión automática y backoff exponencial.

**publication.go**: Crea y gestiona publicaciones PostgreSQL.

#### `src/pipeline/`
**dispatcher.go**: Distribuye eventos a workers apropiados según la configuración:
- Procesa eventos individuales
- Agrupa transacciones por grupos
- Aplica filtros

**lsn_coordinator.go**: Coordinador centralizado que:
- Rastrea el LSN de cada worker
- Calcula el LSN global mínimo (el más conservador)
- Permite que diferentes workers avancen a diferentes velocidades

**table_worker.go**: Worker que procesa eventos individuales (insert, update, delete) de forma asíncrona.

**transaction_worker.go**: Worker que procesa transacciones completas agrupadas.

**sink_kafka.go**: Implementación del sink que:
- Serializa eventos a JSON
- Envía mensajes a Kafka
- Rastrea confirmaciones de entrega
- Reporta LSN solo cuando todos los mensajes son confirmados

#### `src/kafka/`
Cliente de Kafka que encapsula la configuración y operaciones del productor.

#### `src/expressions/`
Sistema de filtrado flexible que permite:
- Filtrar por acciones (insert, update, delete)
- Aplicar condiciones sobre campos de datos
- Combinar condiciones con lógica AND/OR

#### `src/observability/`
Sistema de logging estructurado y métricas Prometheus para monitoreo.

## Flujo de Datos

Para una explicación detallada del flujo desde que se hace un commit en PostgreSQL hasta que el mensaje es producido en Kafka, ver [FLUJO.md](FLUJO.md).

## Endpoints HTTP

- `GET /health`: Health check básico
- `GET /ready`: Ready check (verifica que el conector esté listo)
- `GET /metrics`: Métricas Prometheus

## Monitoreo

El conector expone métricas Prometheus en `/metrics`. Las métricas incluyen:

### Métricas Estándar
- Métricas estándar de Go (goroutines, memoria, GC)
- Métricas de proceso del sistema

### Métricas del Conector

#### LSN (Log Sequence Number)
- `connector_global_lsn`: LSN global mínimo de todos los workers (indica el progreso más conservador)
- `connector_wal_end`: Posición WAL End leída del último keepalive de PostgreSQL
- `connector_last_keepalive_timestamp`: Timestamp Unix del último keepalive recibido de PostgreSQL

#### Transacciones
- `connector_transactions_processed_total`: Contador total de transacciones procesadas
- `connector_transactions_processed_by_worker_total`: Contador de transacciones procesadas por worker (labels: `worker`, `type`)
  - `type` puede ser: `table` o `transaction`

#### Workers
- `connector_events_in_process_by_worker`: Número de eventos actualmente en proceso por worker (labels: `worker`, `type`)
- `connector_worker_buffer_size`: Tamaño del buffer configurado para cada worker (labels: `worker`, `type`)

### Ejemplo de Consultas Prometheus

```promql
# Tasa de transacciones procesadas por segundo
rate(connector_transactions_processed_total[5m])

# Transacciones procesadas por worker en la última hora
increase(connector_transactions_processed_by_worker_total[1h])

# Lag de LSN (diferencia entre WAL End y Global LSN)
connector_wal_end - connector_global_lsn

# Eventos en cola por worker
connector_events_in_process_by_worker

# Tiempo desde el último keepalive
time() - connector_last_keepalive_timestamp
```

### Alertas Recomendadas

```yaml
# Lag de LSN muy alto (más de 1GB)
- alert: HighLSNLag
  expr: (connector_wal_end - connector_global_lsn) > 1073741824
  for: 5m

# Sin keepalive en los últimos 30 segundos
- alert: NoKeepalive
  expr: (time() - connector_last_keepalive_timestamp) > 30
  for: 1m

# Buffer de worker casi lleno (>80%)
- alert: WorkerBufferHigh
  expr: connector_events_in_process_by_worker / connector_worker_buffer_size > 0.8
  for: 5m
```

## Troubleshooting

### El slot de replicación crece indefinidamente
- Verificar que los workers estén procesando eventos
- Verificar que Kafka esté disponible y recibiendo mensajes
- Revisar logs para errores de procesamiento

### Eventos no llegan a Kafka
- Verificar conectividad a Kafka
- Verificar configuración de bootstrap servers
- Revisar logs para errores de producción

### LSN no avanza
- Verificar que los workers estén reportando LSN
- Verificar que Kafka esté confirmando mensajes
- Revisar el coordinador de LSN en los logs

## Desarrollo

### Estructura del Código
- El código sigue convenciones estándar de Go
- Interfaces bien definidas para extensibilidad
- Separación clara de responsabilidades

### Contribuir
1. Crear una rama para la feature
2. Implementar cambios
3. Agregar tests (cuando estén disponibles)
4. Crear pull request

## Licencia

[Especificar licencia]

## Soporte

[Información de contacto/soporte]

