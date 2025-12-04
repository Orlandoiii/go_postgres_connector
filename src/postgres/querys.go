package postgres

const VALIDATE_WAL_LEVEL_QUERY = "SELECT setting FROM pg_settings WHERE name = 'wal_level'"

const SLOT_EXISTS_QUERY = "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)"

const CREATE_LOGICAL_SLOT_QUERY = "SELECT slot_name FROM pg_create_logical_replication_slot($1, $2)"

const CREATE_PUBLICATION_QUERY = "CREATE PUBLICATION %s FOR TABLE %s WITH (publish = '%s')"

const DROP_PUBLICATION_QUERY = "DROP PUBLICATION IF EXISTS %s"

const SET_TABLE_IDENTITY_FULL_QUERY = "ALTER TABLE %s REPLICA IDENTITY FULL"

const VERIFY_TABLES_QUERY = "SELECT COUNT(*) FROM %s"

const GET_SLOT_LSN_QUERY = "SELECT COALESCE(confirmed_flush_lsn, restart_lsn) FROM pg_replication_slots WHERE slot_name = $1"
