const Connection = @import("connection.zig").Connection;

pub const create_wal_batches =
    \\CREATE TABLE IF NOT EXISTS wal_batches (
    \\    id              BIGSERIAL PRIMARY KEY,
    \\    source_id       UUID NOT NULL,
    \\    begin_lsn       BIGINT NOT NULL,
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

pub const create_wal_batches_pending_index =
    \\CREATE INDEX IF NOT EXISTS idx_wal_batches_pending
    \\    ON wal_batches (created_at)
    \\    WHERE state = 'pending' AND complete = true
;

pub const create_wal_batches_partial_index =
    \\CREATE INDEX IF NOT EXISTS idx_wal_batches_partial
    \\    ON wal_batches (source_id, begin_lsn)
    \\    WHERE state = 'pending' AND complete = false
;

pub const create_transactions =
    \\CREATE TABLE IF NOT EXISTS transactions (
    \\    id              BIGSERIAL NOT NULL,
    \\    source_id       UUID NOT NULL,
    \\    lsn             BIGINT NOT NULL,
    \\    xid             INTEGER NOT NULL,
    \\    committed_at    TIMESTAMPTZ NOT NULL,
    \\    metadata        JSONB,
    \\    PRIMARY KEY (id, committed_at),
    \\    UNIQUE (source_id, lsn, committed_at)
    \\) PARTITION BY RANGE (committed_at)
;

pub const create_transactions_default_partition =
    \\CREATE TABLE IF NOT EXISTS transactions_default
    \\    PARTITION OF transactions DEFAULT
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
    \\    id                       BIGSERIAL NOT NULL,
    \\    transaction_id           BIGINT NOT NULL,
    \\    committed_at             TIMESTAMPTZ NOT NULL,
    \\    rel_oid                  INTEGER NOT NULL,
    \\    type                     CHAR(1) NOT NULL,
    \\    identity_digest          BYTEA,
    \\    previous_identity_digest BYTEA,
    \\    data                     JSONB,
    \\    old_data                 JSONB,
    \\    PRIMARY KEY (id, committed_at),
    \\    FOREIGN KEY (transaction_id, committed_at) REFERENCES transactions(id, committed_at)
    \\) PARTITION BY RANGE (committed_at)
;

pub const create_events_default_partition =
    \\CREATE TABLE IF NOT EXISTS events_default
    \\    PARTITION OF events DEFAULT
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
    _ = try conn.simpleQuery(create_wal_batches_pending_index);
    _ = try conn.simpleQuery(create_wal_batches_partial_index);
    _ = try conn.simpleQuery(create_transactions);
    _ = try conn.simpleQuery(create_transactions_default_partition);
    _ = try conn.simpleQuery(create_relation_snapshots);
    _ = try conn.simpleQuery(create_events);
    _ = try conn.simpleQuery(create_events_default_partition);
    _ = try conn.simpleQuery(create_events_indexes);
    _ = try conn.simpleQuery(create_events_identity_index);
}
