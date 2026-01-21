# Análisis en Profundidad de Memory Leaks

## Problemas Críticos Identificados

### 1. **CRÍTICO: Contexto no cancelado en deliveryMonitor (sink_kafka.go:94)**
**Ubicación**: `src/pipeline/sink_kafka.go:94`

**Problema**: Se crea un contexto con `context.WithCancel(context.Background())` pero:
- El `cancel` solo se llama en `stop()`, que solo se invoca cuando se cierra el factory
- Si el factory nunca se cierra o hay un error, el contexto nunca se cancela
- Esto puede causar que la goroutine del `deliveryMonitor` nunca termine
- El contexto mantiene referencias que pueden prevenir el garbage collection

**Impacto**: Memory leak progresivo, especialmente si se crean múltiples sinks

```go
// Línea 94 - PROBLEMA
ctx, cancel := context.WithCancel(context.Background())
```

### 2. **CRÍTICO: Acumulación de workers en Dispatcher (dispatcher.go)**
**Ubicación**: `src/pipeline/dispatcher.go`

**Problema**: 
- Los workers se crean en `getOrCreateWorker()` y `getOrCreateTransactionWorker()`
- Se almacenan en maps (`workers` y `transactionWorkers`)
- **NUNCA se eliminan** del map cuando se detienen
- Si se crean y destruyen workers dinámicamente, el map crece indefinidamente
- Los workers detenidos mantienen referencias a canales, sinks, y otros recursos

**Impacto**: Memory leak progresivo, especialmente con muchas tablas o grupos

### 3. **CRÍTICO: Acumulación de LSNs en LSNCoordinator (lsn_coordinator.go)**
**Ubicación**: `src/pipeline/lsn_coordinator.go`

**Problema**:
- El map `targetLSNs` se llena con `RegisterTable()` pero **nunca se limpia**
- Si se eliminan workers/tablas, sus entradas permanecen en el map
- El map crece indefinidamente con el tiempo

**Impacto**: Memory leak progresivo, especialmente si hay muchas tablas dinámicas

### 4. **MEDIO: Acumulación de transacciones pendientes en deliveryMonitor (sink_kafka.go)**
**Ubicación**: `src/pipeline/sink_kafka.go:229-252`

**Problema**:
- El map `pendingTx` puede acumular transacciones si:
  - Hay errores en la producción de mensajes
  - Los mensajes nunca se confirman (timeout, errores de red)
  - Hay un desbalance entre mensajes enviados y confirmados
- No hay mecanismo de limpieza de transacciones "huérfanas"
- No hay timeout para transacciones pendientes

**Impacto**: Memory leak progresivo bajo condiciones de error

### 5. **MEDIO: Goroutines que pueden no terminar correctamente**
**Ubicación**: Múltiples archivos

**Problemas**:
- `table_worker.go:132` - Goroutine en `Stop()` puede no terminar si hay deadlock
- `transaction_worker.go:132` - Mismo problema
- `sink_kafka.go:295, 337` - Goroutines anónimas que pueden acumularse si hay errores
- `deliveryMonitor.run()` - Puede no terminar si el canal `DeliveryReports` nunca se cierra

**Impacto**: Acumulación de goroutines, memory leak progresivo

### 6. **BAJO: Canales que no se cierran correctamente**
**Ubicación**: `table_worker.go`, `transaction_worker.go`

**Problema**:
- En `Stop()`, se cierra `eventCh` pero luego se intenta cerrar `stopCh` que ya puede estar cerrado
- Si `Stop()` se llama múltiples veces, puede causar panic
- El canal `done` en `Stop()` puede no cerrarse si hay timeout

**Impacto**: Posibles panics y memory leaks menores

### 7. **BAJO: Contextos con timeout en loop (replicator.go:184)**
**Ubicación**: `src/postgres/replicator.go:184`

**Problema**:
- Se crea un contexto con timeout en cada iteración del loop
- Aunque se cancela inmediatamente después, si hay muchos mensajes, se crean muchos contextos
- Debería reutilizarse o tener mejor manejo

**Impacto**: Memory leak menor, pero puede acumularse con alto tráfico

## Problemas Relacionados con el Contexto (Mencionado por el Usuario)

### Contexto Background en sink_kafka.go
El problema más crítico relacionado con contextos es en `sink_kafka.go:94`:
- Se usa `context.Background()` que nunca se cancela automáticamente
- El `cancel` solo se llama cuando se cierra el factory
- Si el factory no se cierra correctamente, el contexto nunca se cancela
- Esto puede causar que las goroutines asociadas nunca terminen

## Correcciones Aplicadas

### ✅ 1. Contexto en deliveryMonitor (sink_kafka.go)
**Corrección aplicada**:
- Se aseguró que el `cancel()` se llame siempre en `stop()`
- Se agregó verificación de nil antes de cancelar
- Se agregó limpieza de transacciones pendientes al detener

### ✅ 2. Limpieza de workers en Dispatcher
**Corrección aplicada**:
- Se agregó `delete()` de workers del map en `Stop()`
- Se agregó limpieza de LSNs del coordinator cuando se eliminan workers
- Se agregó logging para rastrear la limpieza

### ✅ 3. Limpieza de LSNs en LSNCoordinator
**Corrección aplicada**:
- Se agregó método `UnregisterTable()` para eliminar entradas obsoletas
- Se llama automáticamente cuando se eliminan workers

### ✅ 4. Timeout y limpieza de transacciones pendientes
**Corrección aplicada**:
- Se agregó campo `createdAt` a `pendingTransaction` para rastrear edad
- Se agregó método `cleanupStaleTransactions()` que se ejecuta periódicamente
- Se agregó timeout de 5 minutos para transacciones pendientes
- Se agregó limpieza periódica cada 30 segundos

### ✅ 5. Cierre seguro de canales en workers
**Corrección aplicada**:
- Se agregó campo `stopped sync.Once` a `TableWorker` y `TransactionWorker`
- Se usa `sync.Once` para garantizar que los canales solo se cierren una vez
- Se marca canales como nil después de cerrarlos para evitar referencias

## Recomendaciones Adicionales

1. **Monitoreo**: Agregar métricas para rastrear:
   - Número de workers activos
   - Tamaño del map de LSNs
   - Número de transacciones pendientes en deliveryMonitor

2. **Testing**: Agregar tests para:
   - Verificar que los contextos se cancelan correctamente
   - Verificar que los maps se limpian cuando se eliminan workers
   - Verificar que las transacciones huérfanas se limpian después del timeout

3. **Documentación**: Documentar los timeouts y intervalos de limpieza para facilitar el ajuste según necesidades
