package postgres

import (
	"errors"
	"fmt"
	"strings"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/config"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func SanitizeTable(table string) (string, error) {

	if strings.TrimSpace(table) == "" {

		return "", fmt.Errorf("invalid table %s", table)
	}

	parts := strings.Split(table, ".")

	switch len(parts) {

	case 1:

		tableName := strings.TrimSpace(parts[0])

		if tableName == "" {
			return "", fmt.Errorf("invalid table %v", table)
		}

		return pgx.Identifier{tableName}.Sanitize(), nil

	case 2:

		schema := strings.TrimSpace(parts[0])

		tableName := strings.TrimSpace(parts[1])

		if schema == "" || tableName == "" {
			return "", fmt.Errorf("invalid schema or table name %q", table)
		}
		return pgx.Identifier{schema, tableName}.Sanitize(), nil
	}

	return "", fmt.Errorf("invalid qualified table %q", table)
}

func ParseTableName(table string) (schema, tableName string) {

	tableParts := strings.Split(table, ".")

	if len(tableParts) == 2 {

		return strings.TrimSpace(tableParts[0]), strings.TrimSpace(tableParts[1])
	}

	return "public", strings.TrimSpace(tableParts[0])
}

func IsPgError(err error) (bool, *pgconn.PgError) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return true, pgErr
	}
	return false, nil
}

func IsDuplicateObjectError(err error) bool {
	ok, pgErr := IsPgError(err)
	return ok && pgErr.Code == DuplicateObject
}

func GetTableNames(listeners []config.Listener) []string {

	tables := make([]string, 0, len(listeners))

	for _, listener := range listeners {
		tables = append(tables, listener.Table)
	}

	return tables
}
