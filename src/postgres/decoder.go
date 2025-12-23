package postgres

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/observability"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/pipeline"
	"github.com/jackc/pglogrepl"
)

const UnchangedDataTypeByte = 'u'

type Relation struct {
	Name    string
	Schema  string
	Columns []*pglogrepl.RelationMessageColumn
}

type Decoder struct {
	relations map[uint32]*Relation
	logger    observability.Logger
}

func NewDecoder(logger observability.Logger) *Decoder {
	return &Decoder{
		relations: make(map[uint32]*Relation),
		logger:    logger,
	}
}

// parsePostgreSQLTimestamp parsea timestamps de PostgreSQL de forma robusta
// Maneja múltiples formatos incluyendo fechas BC y zonas horarias no estándar
func parsePostgreSQLTimestamp(strValue string) (time.Time, error) {
	// Limpiar espacios
	strValue = strings.TrimSpace(strValue)

	// Verificar si es una fecha BC (Before Christ)
	isBC := strings.HasSuffix(strValue, " BC")
	if isBC {
		strValue = strings.TrimSuffix(strValue, " BC")
		strValue = strings.TrimSpace(strValue)
		// Para fechas BC muy antiguas (como 0001-12-31), devolver la fecha mínima de Go
		// o un error según sea necesario
	}

	// Regex mejorada para capturar todos los formatos posibles:
	// YYYY-MM-DD HH:MM:SS[.microseconds] [+-]HH[:MM[:SS]]
	// Nota: el espacio antes de la zona horaria es opcional
	re := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})(?:\.(\d+))?\s*([+-])(\d{2})(?::(\d{2}))?(?::(\d{2}))?$`)
	matches := re.FindStringSubmatch(strValue)

	// Si la regex no captura, puede ser porque no hay espacio antes de la zona horaria
	// Intentar con regex alternativa sin espacio requerido
	if matches == nil {
		reAlt := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})(?:\.(\d+))?([+-])(\d{2})(?::(\d{2}))?(?::(\d{2}))?$`)
		matches = reAlt.FindStringSubmatch(strValue)
	}

	if matches != nil {
		dateStr := matches[1]
		timeStr := matches[2]
		microseconds := matches[3]
		tzSign := matches[4]
		tzHourStr := matches[5]
		tzMinStr := matches[6]
		tzSecStr := matches[7]

		// Parsear componentes de zona horaria
		tzHour, _ := strconv.Atoi(tzHourStr)
		tzMin := 0
		tzSec := 0

		if tzMinStr != "" {
			tzMin, _ = strconv.Atoi(tzMinStr)
		}
		if tzSecStr != "" {
			tzSec, _ = strconv.Atoi(tzSecStr)
		}

		// Convertir segundos a minutos (redondeando)
		if tzSec > 0 {
			tzMin += tzSec / 60
			if tzSec%60 >= 30 {
				tzMin++
			}
		}

		// Calcular offset total en minutos
		offsetMinutes := tzHour*60 + tzMin
		if tzSign == "-" {
			offsetMinutes = -offsetMinutes
		}

		// Construir el string base sin zona horaria
		baseStr := dateStr + " " + timeStr
		if microseconds != "" {
			// Normalizar microsegundos a máximo 6 dígitos
			microseconds = strings.TrimRight(microseconds, "0")
			if len(microseconds) > 6 {
				microseconds = microseconds[:6]
			}
			for len(microseconds) < 6 {
				microseconds += "0"
			}
			baseStr += "." + microseconds
		}

		// Construir zona horaria en formato estándar HH:MM
		// Manejar correctamente valores negativos
		isNegative := offsetMinutes < 0
		if isNegative {
			offsetMinutes = -offsetMinutes
		}

		offsetHours := offsetMinutes / 60
		offsetMins := offsetMinutes % 60

		// Construir string de zona horaria con signo correcto
		sign := "+"
		if isNegative {
			sign = "-"
		}
		tzStr := fmt.Sprintf("%s%02d:%02d", sign, offsetHours, offsetMins)

		// Construir el string completo SIN espacio entre hora y zona horaria
		// Go espera: "2006-01-02 15:04:05-07:00" (sin espacio antes del offset)
		format := "2006-01-02 15:04:05"
		if microseconds != "" {
			format += ".000000"
		}
		format += "-07:00"

		// Concatenar sin espacio adicional (la zona horaria va directamente después)
		fullTimeStr := baseStr + tzStr

		parsedTime, err := time.Parse(format, fullTimeStr)
		if err == nil {
			// Para fechas BC, verificar si el año es válido
			if isBC {
				// Si el año es menor a 1, usar la fecha mínima de Go (año 1)
				if parsedTime.Year() < 1 {
					// Intentar con año 1 como mínimo
					baseStrFixed := fmt.Sprintf("0001-%s", baseStr[5:])
					if parsedTime, err = time.Parse(format, baseStrFixed+tzStr); err != nil {
						// Si aún falla, devolver fecha mínima de Go
						return time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), nil
					}
				}
			}
			return parsedTime, nil
		}

		// Si falla, puede ser por el formato de zona horaria, intentar con formato alternativo
		// Algunos formatos de PostgreSQL pueden tener la zona horaria separada
		altFormat := "2006-01-02 15:04:05"
		if microseconds != "" {
			altFormat += ".000000"
		}
		altFormat += " -07:00" // Con espacio antes de la zona horaria

		if parsedTime, err = time.Parse(altFormat, baseStr+" "+tzStr); err == nil {
			if isBC && parsedTime.Year() < 1 {
				// Para fechas BC con año < 1, usar fecha mínima
				return time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), nil
			}
			return parsedTime, nil
		}

		// Si ambos formatos fallan y es una fecha BC muy antigua, devolver fecha mínima
		if isBC {
			// Para fechas BC problemáticas, devolver fecha mínima en lugar de error
			// Esto permite que el procesamiento continúe
			return time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), nil
		}
	}

	// Fallback: intentar formatos estándar en orden de más específico a menos
	formats := []string{
		"2006-01-02 15:04:05.999999-07:00",
		"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05-07",
		"2006-01-02T15:04:05.999999-07:00",
		"2006-01-02T15:04:05-07:00",
		time.RFC3339,
		time.RFC3339Nano,
	}

	for _, format := range formats {
		if t, err := time.Parse(format, strValue); err == nil {
			return t, nil
		}
	}

	// Si todo falla, devolver error descriptivo
	if isBC {
		return time.Time{}, fmt.Errorf("no se pudo parsear fecha BC: %s BC", strValue)
	}

	return time.Time{}, fmt.Errorf("no se pudo parsear timestamp: %s", strValue)
}

// DecodeColumnByType decodifica un valor de columna según su OID de PostgreSQL
func (d *Decoder) DecodeColumnByType(dataType uint32, data []byte) (interface{}, error) {
	if len(data) == 0 {
		return nil, nil
	}

	strValue := string(data)

	switch dataType {
	case 16: // BOOL OID
		return strValue == "t", nil
	case 20: // INT8 (BIGINT) OID
		val, err := strconv.ParseInt(strValue, 10, 64)
		if err != nil {
			return nil, err
		}
		return val, nil
	case 21: // INT2 (SMALLINT) OID
		val, err := strconv.ParseInt(strValue, 10, 16)
		if err != nil {
			return nil, err
		}
		return int16(val), nil
	case 23: // INT4 (INTEGER) OID
		val, err := strconv.ParseInt(strValue, 10, 32)
		if err != nil {
			return nil, err
		}
		return int32(val), nil
	case 700: // FLOAT4 (REAL) OID
		val, err := strconv.ParseFloat(strValue, 32)
		if err != nil {
			return nil, err
		}
		return float32(val), nil
	case 701: // FLOAT8 (DOUBLE PRECISION) OID
		val, err := strconv.ParseFloat(strValue, 64)
		if err != nil {
			return nil, err
		}
		return val, nil
	case 1082: // DATE OID
		return time.Parse("2006-01-02", strValue)
	case 1114: // TIMESTAMP OID
		// Intentar con microsegundos primero, luego sin
		if t, err := time.Parse("2006-01-02 15:04:05.999999", strValue); err == nil {
			return t, nil
		}
		return time.Parse("2006-01-02 15:04:05", strValue)
	case 1184: // TIMESTAMPTZ OID
		// Usar parser robusto que maneja todos los formatos de PostgreSQL
		return parsePostgreSQLTimestamp(strValue)
	case 25, 1043: // TEXT, VARCHAR OIDs
		return strValue, nil
	default:
		// Por defecto, devolver como string
		return strValue, nil
	}
}

// DecodeTuple decodifica una tupla de datos usando las definiciones de columna
func (d *Decoder) DecodeTuple(tupleColumns []*pglogrepl.TupleDataColumn,
	relColumns []*pglogrepl.RelationMessageColumn) (map[string]interface{}, error) {

	decoded := make(map[string]interface{}, len(tupleColumns))

	for i, colData := range tupleColumns {

		if i >= len(relColumns) {
			return nil, fmt.Errorf("indice de columna %d fuera de rango", i)
		}

		colDef := relColumns[i]

		if len(colData.Data) == 0 {
			decoded[colDef.Name] = nil
			continue
		}

		if colData.Data[0] == UnchangedDataTypeByte {
			decoded[colDef.Name] = "<<unchanged>>"
			continue
		}

		// Usar el OID de la definición de columna, no del TupleDataColumn
		dataType, err := d.DecodeColumnByType(colDef.DataType, colData.Data)

		if err != nil {

			d.logger.Error(context.Background(), "Error al decodificar columna", err,
				"column_name", colDef.Name,
				"column_data_type", colDef.DataType,
				"column_data", string(colData.Data))
			return nil, err
		}

		decoded[colDef.Name] = dataType
	}

	return decoded, nil
}

func (d *Decoder) ProcessLogicalMessage(ctx context.Context,
	logicalMsg pglogrepl.Message, consumeTime time.Time,
	tr *pipeline.TransactionEvent) *pipeline.TransactionEvent {

	switch logicalMsg := logicalMsg.(type) {

	case *pglogrepl.BeginMessage:

		d.logger.Info(ctx, "BeginMessage",
			"xid", logicalMsg.Xid,
			"time", logicalMsg.CommitTime.Format("2006-01-02T15:04:05Z07:00"),
			"lsn", logicalMsg.FinalLSN.String())

		return &pipeline.TransactionEvent{
			Xid:        logicalMsg.Xid,
			Timestamp:  logicalMsg.CommitTime,
			BeginLSN:   logicalMsg.FinalLSN.String(),
			Operations: []pipeline.ChangeEvent{},
			IsCommit:   false,
			LSN:        logicalMsg.FinalLSN,
		}

	case *pglogrepl.RelationMessage:
		rel := &Relation{
			Name:    logicalMsg.RelationName,
			Schema:  logicalMsg.Namespace,
			Columns: logicalMsg.Columns,
		}

		d.relations[logicalMsg.RelationID] = rel
		d.logger.Debug(ctx, "RelationMessage",
			"relation_id", logicalMsg.RelationID,
			"schema", rel.Schema,
			"table", rel.Name,
			"columns", len(rel.Columns))

		return tr

	case *pglogrepl.InsertMessage:
		rel, ok := d.relations[logicalMsg.RelationID]
		if !ok {
			d.logger.Error(ctx, "No se encontró la definición de la tabla", nil,
				"relationID", logicalMsg.RelationID)
			return nil
		}

		newData, err := d.DecodeTuple(logicalMsg.Tuple.Columns, rel.Columns)
		if err != nil {
			d.logger.Error(ctx, "Error al decodificar INSERT", err,
				"relationID", logicalMsg.RelationID)
			return nil
		}

		d.logger.Debug(ctx, "InsertMessage",
			"schema", rel.Schema,
			"table", rel.Name,
			"data", newData)

		event := pipeline.ChangeEvent{
			ConsumeTime: consumeTime,
			Operation:   pipeline.EventTypeInsert,
			Schema:      rel.Schema,
			Table:       rel.Name,
			NewData:     newData,
		}

		tr.Operations = append(tr.Operations, event)
		return tr

	case *pglogrepl.UpdateMessage:
		rel, ok := d.relations[logicalMsg.RelationID]
		if !ok {
			d.logger.Error(ctx, "No se encontró la definición de la tabla", nil,
				"relation_id", logicalMsg.RelationID)
			return nil
		}

		var oldData, newData map[string]interface{}
		var err error

		if logicalMsg.OldTuple != nil {
			oldData, err = d.DecodeTuple(logicalMsg.OldTuple.Columns, rel.Columns)
			if err != nil {
				d.logger.Error(ctx, "Error al decodificar UPDATE (Old Data)", err,
					"relation_id", logicalMsg.RelationID)
				return nil
			}
		}

		if logicalMsg.NewTuple != nil {
			newData, err = d.DecodeTuple(logicalMsg.NewTuple.Columns, rel.Columns)
			if err != nil {
				d.logger.Error(ctx, "Error al decodificar UPDATE (New Data)", err,
					"relation_id", logicalMsg.RelationID)
				return nil
			}
		}

		d.logger.Debug(ctx, "UpdateMessage",
			"schema", rel.Schema,
			"table", rel.Name,
			"old_data", oldData,
			"new_data", newData)

		event := pipeline.ChangeEvent{
			ConsumeTime: consumeTime,
			Operation:   pipeline.EventTypeUpdate,
			Schema:      rel.Schema,
			Table:       rel.Name,
			OldData:     oldData,
			NewData:     newData,
		}
		tr.Operations = append(tr.Operations, event)
		return tr

	case *pglogrepl.DeleteMessage:
		rel, ok := d.relations[logicalMsg.RelationID]
		if !ok {
			d.logger.Error(ctx, "No se encontró la definición de la tabla", nil,
				"relation_id", logicalMsg.RelationID)
			return nil
		}

		var oldData map[string]interface{}
		var err error

		if logicalMsg.OldTuple != nil {
			oldData, err = d.DecodeTuple(logicalMsg.OldTuple.Columns, rel.Columns)
			if err != nil {
				d.logger.Error(ctx, "Error al decodificar DELETE", err,
					"relation_id", logicalMsg.RelationID)
				return nil
			}
		}

		d.logger.Debug(ctx, "DeleteMessage",
			"schema", rel.Schema,
			"table", rel.Name,
			"old_data", oldData)

		event := pipeline.ChangeEvent{
			ConsumeTime: consumeTime,
			Operation:   pipeline.EventTypeDelete,
			Schema:      rel.Schema,
			Table:       rel.Name,
			OldData:     oldData,
		}
		tr.Operations = append(tr.Operations, event)
		return tr

	case *pglogrepl.CommitMessage:

		d.logger.Info(ctx, "CommitMessage",
			"commit_lsn", logicalMsg.CommitLSN.String(),
			"commit_time", logicalMsg.CommitTime.Format("2006-01-02T15:04:05Z07:00"))

		tr.CommitLSN = logicalMsg.CommitLSN.String()
		tr.IsCommit = true
		tr.CommitTime = logicalMsg.CommitTime
		tr.LSN = logicalMsg.CommitLSN
		return tr
	}

	return nil
}
