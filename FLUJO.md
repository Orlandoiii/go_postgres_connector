# Flujo de Datos: De PostgreSQL a Kafka

Este documento explica de manera simple y clara cómo funciona el conector desde que se realiza una operación en PostgreSQL hasta que el mensaje es producido en Kafka.

## Resumen Visual

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ PostgreSQL  │ --> │  Replicator  │ --> │  Dispatcher │ --> │   Workers   │ --> │    Kafka   │
│   (WAL)     │     │  (Decoder)   │     │  (Filter)   │     │  (Sink)     │     │  (Topics)  │
└─────────────┘     └──────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                            │
                            ▼
                    ┌──────────────┐
                    │ LSNCoordinator│
                    │  (Tracking)   │
                    └──────────────┘
```

## Paso a Paso: El Viaje de un Cambio

### 1. Operación en PostgreSQL

**¿Qué pasa?**
Un usuario o aplicación realiza una operación en PostgreSQL (INSERT, UPDATE o DELETE).

**Ejemplo:**
```sql
INSERT INTO users (name, email) VALUES ('Juan', 'juan@example.com');
COMMIT;
```

**Detalles técnicos:**
- PostgreSQL escribe el cambio en el Write-Ahead Log (WAL)
- El WAL es un registro de todas las operaciones de la base de datos
- PostgreSQL mantiene este log para replicación y recuperación

---

### 2. Logical Replication Slot

**¿Qué pasa?**
El conector tiene un "slot" de replicación lógica configurado en PostgreSQL.

**¿Qué es un slot?**
- Es como una "suscripción" que le dice a PostgreSQL: "Quiero recibir todos los cambios de estas tablas"
- PostgreSQL guarda los cambios en el slot hasta que el conector los lea
- Si el conector se desconecta, los cambios se acumulan en el slot

**Detalles técnicos:**
- El slot se crea automáticamente al iniciar el conector
- El nombre del slot se configura en `config.json` (ej: `"SlotName": "cdc_slot"`)

---

### 3. Replicator Lee el WAL

**¿Qué pasa?**
El componente `Replicator` está constantemente leyendo el WAL de PostgreSQL.

**¿Cómo funciona?**
- El Replicator se conecta a PostgreSQL usando una conexión especial de replicación
- PostgreSQL envía los cambios en tiempo real
- Cada cambio incluye:
  - El tipo de operación (INSERT, UPDATE, DELETE)
  - Los datos antiguos (para UPDATE/DELETE)
  - Los datos nuevos (para INSERT/UPDATE)
  - Un número llamado LSN (Log Sequence Number) que identifica la posición en el log

**Detalles técnicos:**
- El Replicator usa el protocolo de Logical Replication de PostgreSQL
- Lee mensajes en formato binario del WAL
- Mantiene un LSN actual que indica hasta dónde ha leído

---

### 4. Decoder Convierte los Datos

**¿Qué pasa?**
El componente `Decoder` recibe los datos binarios y los convierte en estructuras de datos que el conector puede entender.

**¿Qué hace?**
- Convierte los datos binarios en objetos Go
- Identifica qué tabla fue modificada
- Extrae los datos antiguos y nuevos
- Organiza todo en un objeto `ChangeEvent`

**Ejemplo de lo que se crea:**
```go
ChangeEvent{
    Operation: "insert",
    Schema: "public",
    Table: "users",
    NewData: {
        "name": "Juan",
        "email": "juan@example.com"
    }
}
```

**Detalles técnicos:**
- El Decoder mantiene un mapa de "relaciones" (tablas) que ha visto
- Cada mensaje del WAL puede ser: BEGIN, INSERT, UPDATE, DELETE, COMMIT, o RELATION (definición de tabla)

---

### 5. Agrupación por Transacción

**¿Qué pasa?**
El Replicator agrupa todos los cambios que pertenecen a la misma transacción.

**¿Por qué es importante?**
- Una transacción puede tener múltiples operaciones (ej: INSERT en tabla A y UPDATE en tabla B)
- El conector necesita saber cuándo una transacción está completa (COMMIT) antes de procesarla
- Esto permite enviar transacciones completas a Kafka si está configurado así

**Ejemplo:**
```
BEGIN
  INSERT INTO users ...
  UPDATE accounts ...
COMMIT
```
Todo esto se agrupa en un `TransactionEvent` con todas las operaciones.

---

### 6. Dispatcher Recibe la Transacción

**¿Qué pasa?**
Cuando se recibe el COMMIT, el `Dispatcher` recibe la transacción completa.

**¿Qué hace el Dispatcher?**
1. **Identifica las tablas**: Revisa qué tablas fueron modificadas
2. **Busca Listeners**: Para cada tabla, busca si hay un "listener" configurado
3. **Aplica Filtros**: Si hay filtros configurados, los aplica
4. **Decide el Routing**: Decide a qué workers enviar cada evento

**Ejemplo:**
- Si la tabla `users` tiene un listener configurado, el evento se procesa
- Si no tiene listener, se ignora
- Si tiene filtros (ej: "solo INSERT"), se verifica si pasa el filtro

---

### 7. Filtrado (Opcional)

**¿Qué pasa?**
Si hay filtros configurados, el `Evaluator` evalúa cada evento.

**Tipos de filtros:**
- **Por acción**: Solo INSERT, solo UPDATE, etc.
- **Por condición**: Ej: `new_data.status != null`
- **Lógica combinada**: AND, OR entre condiciones

**Ejemplo:**
```json
{
    "Filter": {
        "Actions": ["update"],
        "Conditions": [
            {
                "Field": "new_data.status",
                "Operator": "!=",
                "Value": null
            }
        ]
    }
}
```
Solo procesa UPDATEs donde el campo `status` no sea null.

---

### 8. Routing a Workers

**¿Qué pasa?**
El Dispatcher envía cada evento a un `Worker` apropiado.

**Tipos de Workers:**
- **TableWorker**: Procesa eventos individuales (un INSERT, un UPDATE, etc.)
- **TransactionWorker**: Procesa transacciones completas agrupadas

**¿Cómo se decide?**
- Si `SendAsTransaction: true` y hay un `Group` configurado → TransactionWorker
- Si no → TableWorker

**Ejemplo:**
- Evento individual → `TableWorker` para `users.target1`
- Transacción agrupada → `TransactionWorker` para `transactions_grouped`

---

### 9. Worker Procesa el Evento

**¿Qué pasa?**
El Worker recibe el evento en un canal (buffer) y lo procesa de forma asíncrona.

**¿Cómo funciona?**
- El Worker tiene un buffer (canal) donde se acumulan eventos
- Un goroutine (hilo) procesa eventos del buffer uno por uno
- Cada evento se envía al Sink (Kafka)

**Ventajas:**
- No bloquea el Dispatcher
- Puede procesar múltiples eventos en paralelo
- Si el buffer está lleno, hay un timeout para evitar bloqueos

---

### 10. Sink Serializa y Envía a Kafka

**¿Qué pasa?**
El `KafkaSink` recibe el evento del Worker y lo envía a Kafka.

**Proceso:**
1. **Serialización**: Convierte el evento a JSON
2. **Envío Asíncrono**: Envía el mensaje a Kafka de forma asíncrona
3. **Registro de Transacción**: Si hay LSN, registra la transacción para tracking
4. **Espera Confirmación**: Espera a que Kafka confirme la recepción

**Ejemplo de mensaje JSON:**
```json
{
    "operation": "insert",
    "schema": "public",
    "table": "users",
    "new_data": {
        "name": "Juan",
        "email": "juan@example.com"
    },
    "xid": 12345,
    "lsn": "0/1234567"
}
```

**Topic en Kafka:**
- El nombre del topic se deriva del nombre del target
- Ej: Si el target es `users.target1`, el topic puede ser `users_target1`

---

### 11. Confirmación de Kafka

**¿Qué pasa?**
Kafka confirma que recibió el mensaje.

**¿Cómo se rastrea?**
- El `deliveryMonitor` rastrea todas las transacciones enviadas
- Cuando Kafka confirma, se incrementa un contador
- Cuando todos los mensajes de una transacción son confirmados, se reporta el LSN

**Importante:**
- El LSN solo se reporta cuando Kafka confirma
- Esto asegura que no se pierdan mensajes

---

### 12. Reporte de LSN

**¿Qué pasa?**
Cuando Kafka confirma, el Worker reporta el LSN al `LSNCoordinator`.

**¿Qué es el LSN?**
- LSN = Log Sequence Number
- Es un número que identifica la posición en el WAL de PostgreSQL
- Indica "hasta aquí he procesado correctamente"

**¿Qué hace el LSNCoordinator?**
- Mantiene un registro del LSN de cada Worker
- Calcula el LSN global mínimo (el más conservador)
- Esto asegura que no se avance demasiado rápido

**Ejemplo:**
- Worker 1 reporta LSN: 1000
- Worker 2 reporta LSN: 950
- LSN Global: 950 (el mínimo, el más conservador)

---

### 13. Avance de LSN en PostgreSQL

**¿Qué pasa?**
El Replicator envía un "ACK" (acknowledgment) a PostgreSQL con el LSN actual.

**¿Por qué es importante?**
- Le dice a PostgreSQL: "Ya procesé hasta aquí, puedes limpiar el slot"
- PostgreSQL puede eliminar datos antiguos del slot
- Si no se envía el ACK, el slot crece indefinidamente

**Lógica inteligente:**
- El Replicator solo avanza el LSN cuando:
  1. No hay eventos pendientes en los workers
  2. Todos los mensajes fueron confirmados por Kafka
- Esto asegura que no se pierdan datos

---

## Flujo Completo con Ejemplo

### Escenario: Insertar un Usuario

1. **Aplicación ejecuta:**
   ```sql
   INSERT INTO users (name, email) VALUES ('Juan', 'juan@example.com');
   COMMIT;
   ```

2. **PostgreSQL escribe en WAL:**
   - BEGIN (xid: 12345, LSN: 0/1000)
   - INSERT users (LSN: 0/1001)
   - COMMIT (LSN: 0/1002)

3. **Replicator lee del slot:**
   - Recibe mensajes binarios del WAL

4. **Decoder convierte:**
   - Crea TransactionEvent con la operación INSERT

5. **Dispatcher recibe COMMIT:**
   - Busca listener para tabla `users`
   - Encuentra listener con filtro "solo INSERT"
   - El evento pasa el filtro

6. **Routing:**
   - Envía a TableWorker `users.target1`

7. **Worker procesa:**
   - Serializa a JSON
   - Envía a Kafka topic `users_target1`

8. **Kafka confirma:**
   - deliveryMonitor registra confirmación

9. **LSN reportado:**
   - Worker reporta LSN 0/1002 al Coordinator

10. **ACK a PostgreSQL:**
    - Replicator envía ACK con LSN 0/1002
    - PostgreSQL puede limpiar el slot hasta ese punto

---

## Casos Especiales

### Transacciones Agrupadas

Si `SendAsTransaction: true` está configurado:
- Todos los cambios de una transacción se agrupan
- Se envían como un solo mensaje a Kafka
- El mensaje contiene todas las operaciones de la transacción

### Múltiples Targets

Una tabla puede tener múltiples targets:
- Cada target puede tener filtros diferentes
- Cada target va a un topic diferente en Kafka
- El mismo evento puede ir a múltiples topics

### Filtrado Complejo

Los filtros pueden ser muy complejos:
- Múltiples condiciones
- Lógica AND/OR
- Acceso a datos antiguos y nuevos
- Operadores: ==, !=, >, <, in, exists, etc.

---

## Garantías del Sistema

### ¿Qué garantiza el conector?

1. **Orden**: Los eventos se procesan en el orden del WAL
2. **No pérdida (dentro de límites)**: Si Kafka confirma, el evento fue procesado
3. **At-least-once**: En caso de fallos, puede haber duplicados (no se garantiza exactly-once)

### ¿Qué NO garantiza?

1. **Exactly-once**: Puede haber duplicados en caso de fallos
2. **Inmediatez**: Hay latencia entre el commit y la producción en Kafka
3. **Orden entre topics**: Si un evento va a múltiples topics, no se garantiza orden entre ellos

---

## Resumen para No Técnicos

**En palabras simples:**

1. Alguien hace un cambio en la base de datos PostgreSQL
2. PostgreSQL guarda ese cambio en un "log" (WAL)
3. El conector lee ese log constantemente
4. Cuando encuentra un cambio, lo convierte a un formato entendible
5. Aplica filtros si están configurados
6. Lo envía a un "worker" que lo procesa
7. El worker lo convierte a JSON y lo envía a Kafka
8. Kafka confirma que lo recibió
9. El conector le dice a PostgreSQL "ya procesé esto, puedes limpiarlo"

**Todo esto pasa en milisegundos**, permitiendo que los cambios en PostgreSQL aparezcan casi instantáneamente en Kafka.

---

## Diagrama de Secuencia Simplificado

```
PostgreSQL    Replicator    Decoder    Dispatcher    Worker    Kafka
    |             |            |           |           |         |
    |--WAL------->|            |           |           |         |
    |             |--decode--->|           |           |         |
    |             |            |--event--->|           |         |
    |             |            |           |--route--->|         |
    |             |            |           |           |--send-->|
    |             |            |           |           |<--ACK---|
    |             |            |           |<--LSN-----|         |
    |<--ACK-------|            |           |           |         |
```

---

## Preguntas Frecuentes

**P: ¿Qué pasa si Kafka está caído?**
R: Los mensajes se acumulan en los buffers de los workers. Si el buffer se llena, hay un timeout. El LSN no avanza hasta que Kafka confirme.

**P: ¿Qué pasa si el conector se desconecta?**
R: PostgreSQL guarda los cambios en el slot. Cuando el conector se reconecta, continúa desde donde se quedó.

**P: ¿Puedo filtrar eventos?**
R: Sí, puedes configurar filtros complejos por acción, campo, valor, etc.

**P: ¿Puedo enviar a múltiples topics?**
R: Sí, puedes configurar múltiples targets por tabla, cada uno va a un topic diferente.

**P: ¿Cómo sé si un evento fue procesado?**
R: El conector solo avanza el LSN cuando Kafka confirma. Si el LSN avanza, los eventos fueron procesados.

