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
| `state`      | `pending` -> `processing` -> deleted (or `error`) |
| `complete`   | `false` for partial batches (mid-transaction split), `true` for final |
| `created_at` | Timestamp when the batch was stored |

## transactions

One row per replicated transaction.

```sql
CREATE TABLE IF NOT EXISTS transactions (
    id              BIGSERIAL PRIMARY KEY,
    source_id       UUID NOT NULL,
    lsn             BIGINT NOT NULL,
    xid             INTEGER NOT NULL,
    committed_at    TIMESTAMPTZ NOT NULL,
    UNIQUE (source_id, lsn)
);
```

| Column        | Description |
|---------------|-------------|
| `id`          | Auto-incrementing primary key (BIGSERIAL) |
| `source_id`   | UUID identifying the source database |
| `lsn`         | Commit LSN of the transaction on the source |
| `xid`         | Transaction ID (XID) on the source |
| `committed_at`| Commit timestamp from the source WAL |

## relation_snapshots

Captures the schema of a source relation at a point in time. A new row is
inserted only when the column metadata changes (DDL), so for a stable schema
there is one row per table.

```sql
CREATE TABLE IF NOT EXISTS relation_snapshots (
    id               BIGSERIAL PRIMARY KEY,
    source_id        UUID NOT NULL,
    lsn              BIGINT NOT NULL,
    rel_oid          INTEGER NOT NULL,
    schema_name      TEXT NOT NULL,
    table_name       TEXT NOT NULL,
    replica_identity SMALLINT NOT NULL,
    columns          JSONB NOT NULL,
    UNIQUE (source_id, rel_oid, lsn)
);
```

| Column             | Description |
|--------------------|-------------|
| `id`               | Auto-incrementing primary key |
| `source_id`        | UUID identifying the source database |
| `lsn`              | LSN at which this schema snapshot was captured |
| `rel_oid`          | PostgreSQL relation OID on the source |
| `schema_name`      | Source schema (e.g. `public`) |
| `table_name`       | Source table name |
| `replica_identity` | Replica identity setting (0=default, 1=nothing, 2=full, 3=index) |
| `columns`          | JSONB array of column descriptors (see below) |

### columns JSONB format

```json
[
  {"name": "id", "oid": 23, "type": "int4", "identity": true, "ordinal": 0},
  {"name": "name", "oid": 25, "type": "text", "identity": false, "ordinal": 1}
]
```

To look up the schema active at a given LSN:

```sql
SELECT * FROM relation_snapshots
WHERE source_id = $1 AND rel_oid = $2 AND lsn <= $3
ORDER BY lsn DESC LIMIT 1;
```

## events

One row per DML operation (INSERT, UPDATE, DELETE, TRUNCATE). Column data is
stored inline as JSONB rather than in a separate table.

```sql
CREATE TABLE IF NOT EXISTS events (
    id                       BIGSERIAL PRIMARY KEY,
    transaction_id           BIGINT NOT NULL REFERENCES transactions(id),
    rel_oid                  INTEGER NOT NULL,
    type                     CHAR(1) NOT NULL,
    identity_digest          BYTEA,
    previous_identity_digest BYTEA,
    data                     JSONB,
    old_data                 JSONB
);

CREATE INDEX idx_events_transaction_id ON events (transaction_id);
CREATE INDEX idx_events_identity_digest ON events (identity_digest)
    WHERE identity_digest IS NOT NULL;
```

| Column                    | Description |
|---------------------------|-------------|
| `id`                      | Auto-incrementing primary key (BIGSERIAL) |
| `transaction_id`          | FK to the parent transaction |
| `rel_oid`                 | PostgreSQL relation OID on the source |
| `type`                    | `I` = INSERT, `U` = UPDATE, `D` = DELETE, `T` = TRUNCATE |
| `identity_digest`         | SHA-256 of identity column values (new tuple) |
| `previous_identity_digest`| SHA-256 of identity column values (old tuple, for UPDATEs) |
| `data`                    | JSONB object mapping column names to text values (new tuple) |
| `old_data`                | JSONB object mapping column names to text values (old tuple) |

### data / old_data JSONB format

All values are stored as text strings (what pgoutput provides). SQL NULL
values are represented as JSON null. Unchanged (TOASTed) columns are omitted.

```json
-- INSERT INTO items (id, name, quantity) VALUES (1, 'widget', 10)
-- type = 'I', data:
{"id": "1", "name": "widget", "quantity": "10"}
-- old_data: NULL

-- UPDATE items SET quantity = 99 WHERE id = 1
-- type = 'U', data:
{"id": "1", "name": "widget", "quantity": "99"}
-- old_data:
{"id": "1", "name": "widget", "quantity": "10"}

-- DELETE FROM items WHERE id = 1
-- type = 'D', data: NULL
-- old_data:
{"id": "1", "name": "widget", "quantity": "99"}
```

To get column type information, join to `relation_snapshots`:

```sql
SELECT e.*, rs.columns AS col_types
FROM events e
JOIN transactions t ON t.id = e.transaction_id
JOIN LATERAL (
    SELECT columns FROM relation_snapshots
    WHERE source_id = t.source_id
      AND rel_oid = e.rel_oid
      AND lsn <= t.lsn
    ORDER BY lsn DESC LIMIT 1
) rs ON true
WHERE e.transaction_id = $1;
```
