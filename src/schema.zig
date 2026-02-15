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
    \\    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    \\    UNIQUE (source_id, start_lsn)
    \\)
;

pub const create_transactions =
    \\CREATE TABLE IF NOT EXISTS transactions (
    \\    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    \\    source_id       UUID NOT NULL,
    \\    lsn             BIGINT NOT NULL,
    \\    xid             INTEGER NOT NULL,
    \\    committed_at    TIMESTAMPTZ NOT NULL,
    \\    UNIQUE (source_id, lsn)
    \\)
;

pub const create_events =
    \\CREATE TABLE IF NOT EXISTS events (
    \\    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    \\    transaction_id          UUID NOT NULL REFERENCES transactions(id),
    \\    source_id               UUID NOT NULL,
    \\    lsn                     BIGINT NOT NULL,
    \\    type                    SMALLINT NOT NULL,
    \\    schema_name             TEXT NOT NULL,
    \\    table_name              TEXT NOT NULL,
    \\    committed_at            TIMESTAMPTZ NOT NULL,
    \\    identity_digest         BYTEA,
    \\    previous_identity_digest BYTEA
    \\)
;

pub const create_columns =
    \\CREATE TABLE IF NOT EXISTS columns (
    \\    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    \\    event_id        UUID NOT NULL REFERENCES events(id),
    \\    source_id       UUID NOT NULL,
    \\    name            TEXT NOT NULL,
    \\    type_oid        INTEGER NOT NULL,
    \\    type_name       TEXT NOT NULL,
    \\    value           BYTEA,
    \\    previous_value  BYTEA,
    \\    identity        BOOLEAN NOT NULL DEFAULT false,
    \\    ordinal         SMALLINT NOT NULL
    \\)
;

/// Create all tables if they don't already exist.
pub fn ensureSchema(conn: *Connection) !void {
    _ = try conn.simpleQuery(create_wal_batches);
    _ = try conn.simpleQuery(create_transactions);
    _ = try conn.simpleQuery(create_events);
    _ = try conn.simpleQuery(create_columns);
}
