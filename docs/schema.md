# Destination Database Schema

The pgzr pipeline creates four tables in the destination database. These are
created automatically by `ensureSchema` on Ingestor/Processor init.

## wal_batches

Packed WAL data stored by the Ingestor. The Processor reads and deletes these
after processing.

```sql
CREATE TABLE IF NOT EXISTS wal_batches (
    id              BIGSERIAL PRIMARY KEY,
    source_id       UUID NOT NULL,
    start_lsn       BIGINT NOT NULL,
    end_lsn         BIGINT NOT NULL,
    data            BYTEA NOT NULL,
    relations       JSONB,
    state           TEXT NOT NULL DEFAULT 'pending',
    complete        BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_id, start_lsn)
);
```

| Column       | Description |
|--------------|-------------|
| `id`         | Auto-incrementing primary key |
| `source_id`  | UUID identifying the source database |
| `start_lsn`  | WAL LSN of the first message in this batch |
| `end_lsn`    | WAL LSN of the last message in this batch |
| `data`       | Packed binary pgoutput messages (length-prefixed) |
| `relations`  | JSONB snapshot of relation metadata at batch time |
| `state`      | `pending` → `processing` → deleted (or `error`) |
| `complete`   | `false` for partial batches (mid-transaction split), `true` for final |
| `created_at` | Timestamp when the batch was stored |

## transactions

One row per replicated transaction.

```sql
CREATE TABLE IF NOT EXISTS transactions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id       UUID NOT NULL,
    lsn             BIGINT NOT NULL,
    xid             INTEGER NOT NULL,
    committed_at    TIMESTAMPTZ NOT NULL,
    UNIQUE (source_id, lsn)
);
```

| Column        | Description |
|---------------|-------------|
| `id`          | UUID primary key |
| `source_id`   | UUID identifying the source database |
| `lsn`         | Commit LSN of the transaction on the source |
| `xid`         | Transaction ID (XID) on the source |
| `committed_at`| Commit timestamp from the source WAL |

## events

One row per DML operation (INSERT, UPDATE, DELETE, TRUNCATE).

```sql
CREATE TABLE IF NOT EXISTS events (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id           UUID NOT NULL REFERENCES transactions(id),
    source_id                UUID NOT NULL,
    lsn                      BIGINT NOT NULL,
    type                     SMALLINT NOT NULL,
    schema_name              TEXT NOT NULL,
    table_name               TEXT NOT NULL,
    committed_at             TIMESTAMPTZ NOT NULL,
    identity_digest          BYTEA,
    previous_identity_digest BYTEA
);
```

| Column                    | Description |
|---------------------------|-------------|
| `id`                      | UUID primary key |
| `transaction_id`          | FK to the parent transaction |
| `source_id`               | UUID identifying the source database |
| `lsn`                     | Commit LSN of the parent transaction |
| `type`                    | 0 = INSERT, 1 = UPDATE, 2 = DELETE, 3 = TRUNCATE |
| `schema_name`             | Source schema (e.g. `public`) |
| `table_name`              | Source table name |
| `committed_at`            | Commit timestamp from the source WAL |
| `identity_digest`         | SHA-256 of identity column values (new tuple) |
| `previous_identity_digest`| SHA-256 of identity column values (old tuple, for UPDATEs) |

## columns

One row per column per event. For a table with N columns, each INSERT/UPDATE/DELETE
event produces N column rows.

```sql
CREATE TABLE IF NOT EXISTS columns (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID NOT NULL REFERENCES events(id),
    source_id       UUID NOT NULL,
    name            TEXT NOT NULL,
    type_oid        INTEGER NOT NULL,
    type_name       TEXT NOT NULL,
    value           BYTEA,
    previous_value  BYTEA,
    identity        BOOLEAN NOT NULL DEFAULT false,
    ordinal         SMALLINT NOT NULL
);
```

| Column           | Description |
|------------------|-------------|
| `id`             | UUID primary key |
| `event_id`       | FK to the parent event |
| `source_id`      | UUID identifying the source database |
| `name`           | Column name |
| `type_oid`       | PostgreSQL type OID |
| `type_name`      | Human-readable type name (e.g. `int4`, `text`) |
| `value`          | Current value as bytea (text representation), NULL if unchanged/null |
| `previous_value` | Previous value for UPDATEs (requires REPLICA IDENTITY FULL) |
| `identity`       | `true` if this column is part of the replica identity |
| `ordinal`        | 0-based column position in the table |
