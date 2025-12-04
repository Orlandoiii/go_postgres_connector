package postgres

import (
	"context"
	"fmt"
	"strconv"
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

// Sin Usar actualmente
func (d *Decoder) DecodeColumnByType(dataType uint32, data []byte) (interface{}, error) {

	strValue := string(data)

	switch dataType {
	case 23: // INT4 OID
		return strconv.ParseInt(strValue, 10, 32)
	case 20: // INT8 OID
		return strconv.ParseInt(strValue, 10, 64)
	case 16: // BOOL OID
		return strValue == "t", nil
	case 700: // FLOAT4 OID
		return strconv.ParseFloat(strValue, 32)
	case 701: // FLOAT8 OID
		return strconv.ParseFloat(strValue, 64)
	case 1082: // DATE OID
		return time.Parse("2006-01-02", strValue)
	case 1114: // TIMESTAMP OID
		return time.Parse("2006-01-02 15:04:05", strValue)
	case 1184: // TIMESTAMPTZ OID
		return time.Parse(time.RFC3339, strValue)
	case 25, 1043: // TEXT, VARCHAR OIDs
		return strValue, nil
	default:
		// Por defecto, devolver como string
		return strValue, nil
	}
}

// Pendinte por mejorar tipo de datos de retorno
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

		dataType, err := d.DecodeColumnByType(uint32(colData.DataType), colData.Data)

		if err != nil {

			d.logger.Error(context.Background(), "Error al decodificar columna", err,
				"column_name", colDef.Name,
				"column_data_type", colData.DataType,
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
