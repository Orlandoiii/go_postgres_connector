package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/config"
	"github.com/jackc/pgx/v5"
)

var allowedActions = map[string]bool{
	"insert": true,
	"update": true,
	"delete": true,
}

func ValidateReplicaConfiguration(ctx context.Context, sqlConn *pgx.Conn) error {

	var walLevel string

	err := sqlConn.QueryRow(ctx, VALIDATE_WAL_LEVEL_QUERY).Scan(&walLevel)

	if err != nil {
		return fmt.Errorf("get wal_level: %w", err)
	}

	if walLevel != "logical" {
		return fmt.Errorf("wal_level is not set to logical")
	}

	return nil
}

func ValidatePublicationActions(actions []string) ([]string, error) {

	if len(actions) == 0 {
		return nil, fmt.Errorf("no actions provided")
	}

	if len(actions) > 3 {
		return nil, fmt.Errorf("too many actions provided")
	}

	output := []string{}

	seen := make(map[string]bool)

	for _, action := range actions {

		lowercaseAction := strings.ToLower(action)

		if !allowedActions[lowercaseAction] {

			return nil, fmt.Errorf("invalid action: %s", action)
		}

		if !seen[lowercaseAction] {

			output = append(output, lowercaseAction)

			seen[lowercaseAction] = true
		}
	}

	if len(output) == 0 {

		return nil, fmt.Errorf("no valid actions provided")
	}

	return output, nil
}

func VerifyTables(ctx context.Context, sqlConn *pgx.Conn, listeners []config.Listener) error {

	for _, listener := range listeners {

		schema, table := ParseTableName(listener.Table)

		var count int

		q := fmt.Sprintf(VERIFY_TABLES_QUERY, pgx.Identifier{schema, table}.Sanitize())

		err := sqlConn.QueryRow(ctx, q).Scan(&count)

		if err != nil {
			return fmt.Errorf("verificar tabla: %w", err)
		}

	}

	return nil
}
