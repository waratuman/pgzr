const Connection = @import("connection.zig").Connection;

pub const create_wal_batches =
    \\CREATE TABLE IF NOT EXISTS wal_batches (
    \\    id              BIGSERIAL PRIMARY KEY,
    \\    source_id       UUID NOT NULL,
    \\    start_lsn       BIGINT NOT NULL,
    \\    end_lsn         BIGINT NOT NULL,
    \\    data            BYTEA NOT NULL,
    \\    relations       JSONB,
    \\    state           TEXT NOT NULL DEFAULT 'pending',
    \\    complete        BOOLEAN NOT NULL DEFAULT true,
    \\    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    \\    UNIQUE (source_id, start_lsn)
    \\)
;

pub const create_transactions =
    \\CREATE TABLE IF NOT EXISTS transactions (
    \\    id              BIGSERIAL PRIMARY KEY,
    \\    source_id       UUID NOT NULL,
    \\    lsn             BIGINT NOT NULL,
    \\    xid             INTEGER NOT NULL,
    \\    committed_at    TIMESTAMPTZ NOT NULL,
    \\    UNIQUE (source_id, lsn)
    \\)
;

pub const create_relation_snapshots =
    \\CREATE TABLE IF NOT EXISTS relation_snapshots (
    \\    id               BIGSERIAL PRIMARY KEY,
    \\    source_id        UUID NOT NULL,
    \\    lsn              BIGINT NOT NULL,
    \\    rel_oid          INTEGER NOT NULL,
    \\    schema_name      TEXT NOT NULL,
    \\    table_name       TEXT NOT NULL,
    \\    replica_identity SMALLINT NOT NULL,
    \\    columns          JSONB NOT NULL,
    \\    UNIQUE (source_id, rel_oid, lsn)
    \\)
;

pub const create_events =
    \\CREATE TABLE IF NOT EXISTS events (
    \\    id                       BIGSERIAL PRIMARY KEY,
    \\    transaction_id           BIGINT NOT NULL REFERENCES transactions(id),
    \\    rel_oid                  INTEGER NOT NULL,
    \\    type                     CHAR(1) NOT NULL,
    \\    identity_digest          BYTEA,
    \\    previous_identity_digest BYTEA,
    \\    data                     JSONB,
    \\    old_data                 JSONB
    \\)
;

pub const create_events_indexes =
    \\CREATE INDEX IF NOT EXISTS idx_events_transaction_id ON events (transaction_id)
;

pub const create_events_identity_index =
    \\CREATE INDEX IF NOT EXISTS idx_events_identity_digest ON events (identity_digest)
    \\    WHERE identity_digest IS NOT NULL
;

/// Create all tables if they don't already exist.
pub fn ensureSchema(conn: *Connection) !void {
    _ = try conn.simpleQuery(create_wal_batches);
    _ = try conn.simpleQuery(create_transactions);
    _ = try conn.simpleQuery(create_relation_snapshots);
    _ = try conn.simpleQuery(create_events);
    _ = try conn.simpleQuery(create_events_indexes);
    _ = try conn.simpleQuery(create_events_identity_index);
}
