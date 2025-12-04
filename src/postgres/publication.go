package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/config"
	"github.com/SOLUCIONESSYCOM/go_postgres_connector/src/observability"
	"github.com/jackc/pgx/v5"
)

func BuildCreatePublicationSQL(pubName string, tables []string,
	actions []string) (string, error) {

	validActions, err := ValidatePublicationActions(actions)

	if err != nil {
		return "", err
	}

	publishVal := strings.Join(validActions, ", ")

	pubIdent := pgx.Identifier{pubName}.Sanitize()

	tableIdents := make([]string, 0, len(tables))

	for _, table := range tables {

		tableIdent, err := SanitizeTable(table)

		if err != nil {
			return "", err
		}

		tableIdents = append(tableIdents, tableIdent)
	}

	q := fmt.Sprintf(CREATE_PUBLICATION_QUERY, pubIdent,
		strings.Join(tableIdents, ", "), publishVal)

	return q, nil
}

func DropPublication(ctx context.Context, sqlConn *pgx.Conn, publication string) error {

	q := fmt.Sprintf(DROP_PUBLICATION_QUERY, pgx.Identifier{publication}.Sanitize())

	_, err := sqlConn.Exec(ctx, q)

	return err
}

func SetTableIdentityFull(ctx context.Context, sqlConn *pgx.Conn, table string) error {

	sanitizedTable, err := SanitizeTable(table)

	if err != nil {
		return err
	}

	q := fmt.Sprintf(SET_TABLE_IDENTITY_FULL_QUERY, sanitizedTable)

	_, err = sqlConn.Exec(ctx, q)

	return err
}

func CreatePublicationAndSetTableIdentityFull(ctx context.Context,
	sqlConn *pgx.Conn, listener config.Listener, logger observability.Logger) error {

	if sqlConn == nil {
		return fmt.Errorf("sql connection is nil")
	}

	logger.Info(ctx, "Dropping publication", "publication", listener.Publication)

	if err := DropPublication(ctx, sqlConn, listener.Publication); err != nil {
		return fmt.Errorf("drop publication: %w", err)
	}

	logger.Info(ctx, "Setting table identity full", "table", listener.Table)
	if err := SetTableIdentityFull(ctx, sqlConn, listener.Table); err != nil {
		return fmt.Errorf("set table identity full: %w", err)
	}

	logger.Info(ctx, "Building publication query", "publication", listener.Publication)

	publicationQuery, err := BuildCreatePublicationSQL(listener.Publication,
		[]string{listener.Table}, listener.Actions)

	if err != nil {
		return err
	}

	logger.Info(ctx, "Executing publication query", "publication", listener.Publication)
	_, err = sqlConn.Exec(ctx, publicationQuery)

	if err != nil {
		if !IsDuplicateObjectError(err) {
			return err
		}
	}

	return nil
}
