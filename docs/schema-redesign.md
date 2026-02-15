# Schema Redesign: Eliminating Write Amplification

## Problem

The current schema stores column-level data as individual rows in a `columns`
table. For a table with N columns, every INSERT, UPDATE, or DELETE generates:

- 1 row in `transactions`
- 1 row in `events`
- N rows in `columns`

A table with 20 columns produces 22 rows per DML operation. At scale this
creates severe write amplification, index bloat, and slow processing.

Additionally, type metadata (OID, type name, identity flag) is duplicated on
every column row despite being identical for all events on the same table
until a DDL change occurs.

## Current Schema

```sql
CREATE TABLE transactions (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id    UUID NOT NULL,
    lsn          BIGINT NOT NULL,
    xid          INTEGER NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL,
    UNIQUE (source_id, lsn)
);

CREATE TABLE events (
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

CREATE TABLE columns (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id       UUID NOT NULL REFERENCES events(id),
    source_id      UUID NOT NULL,
    name           TEXT NOT NULL,
    type_oid       INTEGER NOT NULL,
    type_name      TEXT NOT NULL,
    value          BYTEA,
    previous_value BYTEA,
    identity       BOOLEAN NOT NULL DEFAULT false,
    ordinal        SMALLINT NOT NULL
);
```

## Proposed Schema

### `wal_batches` (unchanged)

Staging table for raw WAL data. No changes needed.

```sql
CREATE TABLE wal_batches (
    id         BIGSERIAL PRIMARY KEY,
    source_id  UUID NOT NULL,
    start_lsn  BIGINT NOT NULL,
    end_lsn    BIGINT NOT NULL,
    data       BYTEA NOT NULL,
    state      TEXT NOT NULL DEFAULT 'pending',
    complete   BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_id, start_lsn)
);
```

### `transactions` (minor changes)

- Change PK from UUID to BIGSERIAL (better index locality, 8 bytes vs 16).

```sql
CREATE TABLE transactions (
    id           BIGSERIAL PRIMARY KEY,
    source_id    UUID NOT NULL,
    lsn          BIGINT NOT NULL,
    xid          INTEGER NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL,
    UNIQUE (source_id, lsn)
);
```

### `relation_snapshots` (new)

Captures the schema of a relation at a point in time. A new row is inserted
only when the column metadata changes (DDL), so for a stable schema there is
one row per table.

```sql
CREATE TABLE relation_snapshots (
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

The `columns` JSONB stores an array of column descriptors:

```json
[
  {"name": "id", "oid": 23, "type": "int4", "identity": true, "ordinal": 0},
  {"name": "name", "oid": 25, "type": "text", "identity": false, "ordinal": 1},
  {"name": "quantity", "oid": 23, "type": "int4", "identity": false, "ordinal": 2}
]
```

To look up the schema that was active at a given LSN:

```sql
SELECT * FROM relation_snapshots
WHERE source_id = $1 AND rel_oid = $2 AND lsn <= $3
ORDER BY lsn DESC LIMIT 1;
```

The unique index on `(source_id, rel_oid, lsn)` makes this a single index
scan.

### `events` (major changes)

- Drop `columns` table entirely; fold values into JSONB on events.
- Change PK from UUID to BIGSERIAL.
- Remove redundant `source_id`, `committed_at`, `lsn` (available via
  `transaction_id` join to `transactions`).
- Remove `schema_name` and `table_name` (available via `rel_oid` join to
  `relation_snapshots`).
- Store `rel_oid` to link to the source relation.
- Change `type` from SMALLINT to CHAR(1) for readability.
- Add `data` (new values) and `old_data` (previous values) as JSONB.

```sql
CREATE TABLE events (
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

### `columns` table

**Dropped.** All column data is stored in `events.data` and `events.old_data`.

## JSONB Value Format

The `data` and `old_data` JSONB objects map column names to their text
representation as provided by pgoutput. NULL values are represented as JSON
null. Unchanged (TOASTed) columns are omitted from the object.

```json
-- INSERT INTO items (id, name, quantity) VALUES (1, 'widget', 10)
-- event.type = 'I'
-- event.data:
{"id": "1", "name": "widget", "quantity": "10"}
-- event.old_data: null

-- UPDATE items SET quantity = 99 WHERE id = 1
-- event.type = 'U'
-- event.data:
{"id": "1", "name": "widget", "quantity": "99"}
-- event.old_data:
{"id": "1", "name": "widget", "quantity": "10"}

-- DELETE FROM items WHERE id = 1
-- event.type = 'D'
-- event.data: null
-- event.old_data:
{"id": "1", "name": "widget", "quantity": "99"}
```

All values are stored as text strings because that is what pgoutput provides.
To interpret them as typed values, join to `relation_snapshots` to get the
column type OIDs.

## Type Lookup at a Given LSN

To get the type of a column at the LSN of a specific event:

```sql
SELECT rs.columns
FROM events e
JOIN transactions t ON t.id = e.transaction_id
JOIN relation_snapshots rs
  ON rs.source_id = t.source_id
  AND rs.rel_oid = e.rel_oid
  AND rs.lsn = (
      SELECT MAX(lsn) FROM relation_snapshots
      WHERE source_id = t.source_id
        AND rel_oid = e.rel_oid
        AND lsn <= t.lsn
  )
WHERE e.id = $1;
```

Alternatively, with a lateral join:

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

## Migration

### Step 1: Create new tables

```sql
-- New relation_snapshots table
CREATE TABLE relation_snapshots (
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

-- New events table (temporary name during migration)
CREATE TABLE events_v2 (
    id                       BIGSERIAL PRIMARY KEY,
    transaction_id           BIGINT NOT NULL,
    rel_oid                  INTEGER NOT NULL,
    type                     CHAR(1) NOT NULL,
    identity_digest          BYTEA,
    previous_identity_digest BYTEA,
    data                     JSONB,
    old_data                 JSONB
);

-- New transactions table (temporary name)
CREATE TABLE transactions_v2 (
    id           BIGSERIAL PRIMARY KEY,
    source_id    UUID NOT NULL,
    lsn          BIGINT NOT NULL,
    xid          INTEGER NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL,
    UNIQUE (source_id, lsn)
);
```

### Step 2: Migrate transactions

```sql
INSERT INTO transactions_v2 (source_id, lsn, xid, committed_at)
SELECT source_id, lsn, xid, committed_at
FROM transactions
ORDER BY committed_at;
```

### Step 3: Build relation snapshots from columns data

Since the old schema doesn't track relation OIDs or schema changes over time,
we derive a single snapshot per (source_id, schema_name, table_name) using
the earliest event LSN and the column metadata from the columns table:

```sql
INSERT INTO relation_snapshots (source_id, lsn, rel_oid, schema_name, table_name, replica_identity, columns)
SELECT DISTINCT ON (e.source_id, e.schema_name, e.table_name)
    e.source_id,
    t.lsn,
    0,  -- rel_oid unknown from old data, use 0 as placeholder
    e.schema_name,
    e.table_name,
    0,  -- replica_identity unknown
    (
        SELECT jsonb_agg(
            jsonb_build_object(
                'name', c.name,
                'oid', c.type_oid,
                'type', c.type_name,
                'identity', c.identity,
                'ordinal', c.ordinal
            ) ORDER BY c.ordinal
        )
        FROM columns c
        WHERE c.event_id = e.id
    )
FROM events e
JOIN transactions t ON t.id = e.transaction_id
ORDER BY e.source_id, e.schema_name, e.table_name, t.lsn;
```

Note: The `rel_oid` will be 0 for migrated data since the old schema didn't
track it. New data from the processor will have the correct OID. This is
acceptable because the unique constraint is on `(source_id, rel_oid, lsn)`,
and migrated tables won't collide with new ones once the processor starts
using real OIDs.

### Step 4: Migrate events

```sql
INSERT INTO events_v2 (transaction_id, rel_oid, type, identity_digest, previous_identity_digest, data, old_data)
SELECT
    tv2.id,
    COALESCE(rs.rel_oid, 0),
    CASE e.type
        WHEN 0 THEN 'I'
        WHEN 1 THEN 'U'
        WHEN 2 THEN 'D'
        WHEN 3 THEN 'T'
    END,
    e.identity_digest,
    e.previous_identity_digest,
    -- Aggregate current values into JSONB
    (
        SELECT jsonb_object_agg(c.name, encode(c.value, 'escape'))
        FROM columns c
        WHERE c.event_id = e.id AND c.value IS NOT NULL
    ),
    -- Aggregate previous values into JSONB
    (
        SELECT jsonb_object_agg(c.name, encode(c.previous_value, 'escape'))
        FROM columns c
        WHERE c.event_id = e.id AND c.previous_value IS NOT NULL
    )
FROM events e
JOIN transactions t ON t.id = e.transaction_id
JOIN transactions_v2 tv2 ON tv2.source_id = t.source_id AND tv2.lsn = t.lsn
LEFT JOIN relation_snapshots rs
    ON rs.source_id = t.source_id
    AND rs.schema_name = e.schema_name
    AND rs.table_name = e.table_name
ORDER BY e.id;
```

### Step 5: Swap tables

```sql
BEGIN;
ALTER TABLE events RENAME TO events_old;
ALTER TABLE columns RENAME TO columns_old;
ALTER TABLE transactions RENAME TO transactions_old;

ALTER TABLE events_v2 RENAME TO events;
ALTER TABLE transactions_v2 RENAME TO transactions;

-- Add FK and indexes
ALTER TABLE events ADD CONSTRAINT fk_events_transaction
    FOREIGN KEY (transaction_id) REFERENCES transactions(id);
CREATE INDEX idx_events_transaction_id ON events (transaction_id);
CREATE INDEX idx_events_identity_digest ON events (identity_digest)
    WHERE identity_digest IS NOT NULL;
COMMIT;
```

### Step 6: Drop old tables

After verifying the migration:

```sql
DROP TABLE columns_old;
DROP TABLE events_old;
DROP TABLE transactions_old;
```

## Impact Summary

| Metric | Before | After |
|--------|--------|-------|
| Rows per INSERT (20-col table) | 22 | 3 (txn + snapshot once + event) |
| Rows per UPDATE (20-col table) | 22 | 2 (txn exists, event) |
| Index writes per event | 22+ (columns PK, event_id FK, ...) | 3 (event PK, txn FK, identity) |
| Type info storage | Repeated every event | Once per schema change |
| UUID overhead | 16 bytes x 3 PKs x every row | 8 bytes BIGSERIAL |
| Query: "what changed?" | JOIN events + columns | Single table scan |
| Query: "column type at LSN X?" | Already inline | Index scan on relation_snapshots |
| Query: "value of column Y?" | JOIN + filter | `data->>'Y'` |
