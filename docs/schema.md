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
    id              BIGSERIAL NOT NULL,
    source_id       UUID NOT NULL,
    lsn             BIGINT NOT NULL,
    xid             INTEGER NOT NULL,
    committed_at    TIMESTAMPTZ NOT NULL,
    metadata        JSONB,
    PRIMARY KEY (id, committed_at),
    UNIQUE (source_id, lsn, committed_at)
) PARTITION BY RANGE (committed_at);

CREATE TABLE IF NOT EXISTS transactions_default
    PARTITION OF transactions DEFAULT;
```

| Column        | Description |
|---------------|-------------|
| `id`          | Auto-incrementing primary key (BIGSERIAL), part of composite PK with `committed_at` |
| `source_id`   | UUID identifying the source database |
| `lsn`         | Commit LSN of the transaction on the source |
| `xid`         | Transaction ID (XID) on the source |
| `committed_at`| Commit timestamp from the source WAL (partition key) |
| `metadata`    | Application-provided JSONB metadata attached to the transaction (see [Metadata](#metadata)) |

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
    id                       BIGSERIAL NOT NULL,
    transaction_id           BIGINT NOT NULL,
    committed_at             TIMESTAMPTZ NOT NULL,
    rel_oid                  INTEGER NOT NULL,
    type                     CHAR(1) NOT NULL,
    identity_digest          BYTEA,
    previous_identity_digest BYTEA,
    data                     JSONB,
    old_data                 JSONB,
    PRIMARY KEY (id, committed_at),
    FOREIGN KEY (transaction_id, committed_at) REFERENCES transactions(id, committed_at)
) PARTITION BY RANGE (committed_at);

CREATE TABLE IF NOT EXISTS events_default
    PARTITION OF events DEFAULT;

CREATE INDEX idx_events_transaction_id ON events (transaction_id);
CREATE INDEX idx_events_identity_digest ON events (identity_digest)
    WHERE identity_digest IS NOT NULL;
```

| Column                    | Description |
|---------------------------|-------------|
| `id`                      | Auto-incrementing primary key (BIGSERIAL), part of composite PK with `committed_at` |
| `transaction_id`          | FK to the parent transaction (composite with `committed_at`) |
| `committed_at`            | Commit timestamp from the source WAL (partition key, denormalized from transactions) |
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
JOIN transactions t ON t.id = e.transaction_id AND t.committed_at = e.committed_at
JOIN LATERAL (
    SELECT columns FROM relation_snapshots
    WHERE source_id = t.source_id
      AND rel_oid = e.rel_oid
      AND lsn <= t.lsn
    ORDER BY lsn DESC LIMIT 1
) rs ON true
WHERE e.transaction_id = $1;
```

## Metadata

Applications can attach arbitrary JSON metadata to transactions (e.g., the
user who made the change, a request ID). The Processor supports two
mechanisms for capturing metadata; both are opt-in via `ProcessorConfig`.

### Via `pg_logical_emit_message`

Set `metadata_message_prefix` in the Processor config. The application calls
`pg_logical_emit_message` within a transaction on the source database:

```sql
BEGIN;
SELECT pg_logical_emit_message(true, 'my_prefix', '{"user":{"id":1,"name":"Alice"}}');
INSERT INTO orders (item, qty) VALUES ('widget', 10);
COMMIT;
```

The Processor matches the message prefix and stores the JSON content in
`transactions.metadata`.

### Via metadata table

Set `metadata_table` in the Processor config. The application creates a table
on the source (included in the publication) and upserts metadata within
transactions:

```sql
CREATE TABLE my_metadata (version int PRIMARY KEY, data jsonb DEFAULT '{}');

BEGIN;
INSERT INTO orders (item, qty) VALUES ('widget', 10);
INSERT INTO my_metadata (version, data)
    VALUES (1, '{"user":{"id":1,"name":"Alice"}}')
    ON CONFLICT (version) DO UPDATE SET data = EXCLUDED.data;
COMMIT;
```

The Processor recognizes writes to this table, extracts the `data` column,
and stores it in `transactions.metadata`. Writes to the metadata table are
**not** stored as events.

### Merging

If both mechanisms are used in the same transaction (or multiple metadata
sources appear), the JSON objects are merged using PostgreSQL's `||` operator
(shallow merge, later values win).

### Querying metadata

```sql
-- Find events where the transaction has user metadata
SELECT e.*, t.metadata
FROM events e
JOIN transactions t ON t.id = e.transaction_id AND t.committed_at = e.committed_at
WHERE t.metadata IS NOT NULL
  AND t.metadata->'user'->>'id' = '1';
```

## Partitioning

Both `transactions` and `events` are partitioned by `committed_at` using
PostgreSQL range partitioning. A default partition is created automatically
to catch all rows. For better performance and retention management, create
time-range partitions:

```sql
-- Monthly partitions
CREATE TABLE transactions_2025_01 PARTITION OF transactions
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE events_2025_01 PARTITION OF events
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

-- To archive/drop old data, detach or drop the partition:
ALTER TABLE events DETACH PARTITION events_2025_01;
DROP TABLE events_2025_01;
```

`wal_batches` and `relation_snapshots` are not partitioned — `wal_batches`
is a transient queue (rows deleted after processing) and `relation_snapshots`
is very low volume (one row per DDL change).
