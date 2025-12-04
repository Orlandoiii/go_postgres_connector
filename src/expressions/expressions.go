package expressions

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/config"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/observability"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/pipeline"
)

type Evaluator struct {
	config.FilterConfig `json:"FilterConfig"`
}

func NewEvaluator(filterConfig config.FilterConfig, logger observability.Logger) *Evaluator {
	return &Evaluator{FilterConfig: filterConfig}
}

func (e *Evaluator) Evaluate(event *pipeline.ChangeEvent, txEvent *pipeline.TransactionEvent) (bool, error) {

	return e.evaluate(event, txEvent)
}

func (e *Evaluator) evaluate(event *pipeline.ChangeEvent,
	txEvent *pipeline.TransactionEvent) (bool, error) {

	if len(e.FilterConfig.Conditions) == 0 {
		return false, fmt.Errorf("no conditions provided")
	}

	logic := strings.ToUpper(e.FilterConfig.Logic)

	if logic == "" {
		logic = "AND"
	}

	results := make([]bool, 0, len(e.FilterConfig.Conditions))

	for _, condition := range e.FilterConfig.Conditions {

		fieldValue, err := e.getFieldValue(condition.Field, event, txEvent)

		if err != nil {
			return false, err
		}

		result, err := e.compare(fieldValue, condition.Operator, condition.Value)

		if err != nil {
			return false, err
		}

		results = append(results, result)
	}

	return e.applyLogic(results, logic)
}

func (e *Evaluator) getFieldValue(fieldPath string,
	event *pipeline.ChangeEvent, txEvent *pipeline.TransactionEvent) (interface{}, error) {

	parts := strings.Split(fieldPath, ".")

	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid field path: %s (expected format: prefix.field)", fieldPath)
	}

	prefix := strings.TrimSpace(strings.ToLower(parts[0]))

	fieldName := strings.TrimSpace(strings.Join(parts[1:], "."))

	switch prefix {
	case "new_data":
		if event.NewData == nil {
			return nil, nil
		}
		return event.NewData[fieldName], nil

	case "old_data":
		if event.OldData == nil {
			return nil, nil
		}
		return event.OldData[fieldName], nil

	case "operation":
		return string(event.Operation), nil

	case "tx":
		if txEvent == nil {
			return nil, nil
		}
		switch fieldName {
		case "xid":
			return txEvent.Xid, nil
		case "timestamp":
			return txEvent.Timestamp, nil
		default:
			return nil, fmt.Errorf("unknown tx field: %s", fieldName)
		}

	default:
		return nil, fmt.Errorf("unknown prefix: %s (supported: new_data, old_data, operation, tx)", prefix)
	}
}

func (e *Evaluator) compare(fieldValue interface{}, operator string, expectedValue interface{}) (bool, error) {
	operator = strings.ToLower(strings.TrimSpace(operator))

	switch operator {
	case "==", "eq", "equals":
		return e.equals(fieldValue, expectedValue), nil

	case "!=", "ne", "not_equals":
		return !e.equals(fieldValue, expectedValue), nil

	case ">", "gt", "greater_than":
		return e.greaterThan(fieldValue, expectedValue)

	case "<", "lt", "less_than":
		return e.lessThan(fieldValue, expectedValue)

	case ">=", "gte", "greater_than_or_equal":
		gt, err := e.greaterThan(fieldValue, expectedValue)
		if err != nil {
			return false, err
		}
		eq := e.equals(fieldValue, expectedValue)
		return gt || eq, nil

	case "<=", "lte", "less_than_or_equal":
		lt, err := e.lessThan(fieldValue, expectedValue)
		if err != nil {
			return false, err
		}
		eq := e.equals(fieldValue, expectedValue)
		return lt || eq, nil

	case "in", "contains":
		return e.in(fieldValue, expectedValue)

	case "not_in", "not_contains":
		result, err := e.in(fieldValue, expectedValue)
		return !result, err

	case "exists":
		return fieldValue != nil, nil

	case "is_null", "null":
		return fieldValue == nil, nil

	case "is_not_null", "not_null":
		return fieldValue != nil, nil

	default:
		return false, fmt.Errorf("unsupported operator: %s", operator)
	}
}

func (e *Evaluator) equals(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	av := reflect.ValueOf(a)
	bv := reflect.ValueOf(b)

	if av.Kind() == reflect.String && bv.Kind() != reflect.String {
		bv = reflect.ValueOf(fmt.Sprintf("%v", b))
	} else if bv.Kind() == reflect.String && av.Kind() != reflect.String {
		av = reflect.ValueOf(fmt.Sprintf("%v", a))
	}

	if av.Kind() == bv.Kind() {
		return av.Interface() == bv.Interface()
	}

	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func (e *Evaluator) greaterThan(a, b interface{}) (bool, error) {
	if a == nil || b == nil {
		return false, nil
	}

	av := reflect.ValueOf(a)
	bv := reflect.ValueOf(b)

	if av.Kind() == reflect.Float64 || bv.Kind() == reflect.Float64 {
		af := e.toFloat64(a)
		bf := e.toFloat64(b)
		return af > bf, nil
	}

	if av.Kind() == reflect.Int || av.Kind() == reflect.Int64 || bv.Kind() == reflect.Int || bv.Kind() == reflect.Int64 {
		ai := e.toInt64(a)
		bi := e.toInt64(b)
		return ai > bi, nil
	}

	return false, fmt.Errorf("cannot compare %T and %T with > operator", a, b)
}

func (e *Evaluator) lessThan(a, b interface{}) (bool, error) {
	if a == nil || b == nil {
		return false, nil
	}

	av := reflect.ValueOf(a)
	bv := reflect.ValueOf(b)

	if av.Kind() == reflect.Float64 || bv.Kind() == reflect.Float64 {
		af := e.toFloat64(a)
		bf := e.toFloat64(b)
		return af < bf, nil
	}

	if av.Kind() == reflect.Int || av.Kind() == reflect.Int64 || bv.Kind() == reflect.Int || bv.Kind() == reflect.Int64 {
		ai := e.toInt64(a)
		bi := e.toInt64(b)
		return ai < bi, nil
	}

	return false, fmt.Errorf("cannot compare %T and %T with < operator", a, b)
}

func (e *Evaluator) in(fieldValue interface{}, expectedValue interface{}) (bool, error) {
	if fieldValue == nil {
		return false, nil
	}

	expectedSlice, ok := expectedValue.([]interface{})
	if !ok {
		return false, fmt.Errorf("expected value for 'in' operator must be an array, got %T", expectedValue)
	}

	for _, item := range expectedSlice {
		if e.equals(fieldValue, item) {
			return true, nil
		}
	}

	return false, nil
}

func (e *Evaluator) applyLogic(results []bool, logic string) (bool, error) {
	if len(results) == 0 {
		return false, nil
	}

	switch logic {
	case "AND", "&&":
		for _, result := range results {
			if !result {
				return false, nil
			}
		}
		return true, nil

	case "OR", "||":
		for _, result := range results {
			if result {
				return true, nil
			}
		}
		return false, nil

	default:
		return false, fmt.Errorf("unsupported logic operator: %s (supported: AND, OR)", logic)
	}
}

func (e *Evaluator) toFloat64(v interface{}) float64 {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Float64, reflect.Float32:
		return rv.Float()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint())
	default:
		return 0
	}
}

func (e *Evaluator) toInt64(v interface{}) int64 {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint())
	case reflect.Float64, reflect.Float32:
		return int64(rv.Float())
	default:
		return 0
	}
}
