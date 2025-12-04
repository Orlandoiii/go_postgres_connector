package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

const PgoutputPlugin = "pgoutput"

func SlotExists(ctx context.Context, sqlConn *pgx.Conn, slotName string) (bool, error) {

	var exists bool

	err := sqlConn.QueryRow(ctx, SLOT_EXISTS_QUERY, slotName).Scan(&exists)

	return exists, err
}

func CreateLogicalSlotIfMissing(ctx context.Context, sqlConn *pgx.Conn, slot, plugin string) error {

	exists, err := SlotExists(ctx, sqlConn, slot)

	if err != nil {
		return fmt.Errorf("check slot exists: %w", err)
	}

	if exists {
		return nil
	}

	_, err = sqlConn.Exec(ctx, CREATE_LOGICAL_SLOT_QUERY, slot, plugin)

	return err
}

func GetSlotLSN(ctx context.Context, sqlConn *pgx.Conn, slotName string) (pglogrepl.LSN, error) {
	var lsnStr *string
	err := sqlConn.QueryRow(ctx, GET_SLOT_LSN_QUERY, slotName).Scan(&lsnStr)

	if err != nil {
		return pglogrepl.LSN(0), fmt.Errorf("get slot LSN: %w", err)
	}

	if lsnStr == nil || *lsnStr == "" {
		return pglogrepl.LSN(0), nil
	}

	lsn, err := pglogrepl.ParseLSN(*lsnStr)
	if err != nil {
		return pglogrepl.LSN(0), fmt.Errorf("parse LSN: %w", err)
	}

	return lsn, nil
}
