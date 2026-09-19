# PGZR - PostgreSQL Zig Replicator

A pure Zig library implementing PostgreSQL logical replication. No C
dependencies.

## Features

- Pure Zig, zero C dependencies
- TCP, Unix socket, and TLS connections (require, verify-full)
- SCRAM-SHA-256, MD5, and cleartext authentication
- pgoutput protocol versions 1-4 (streaming, two-phase commit)
- WAL ingest and processing pipeline (store and replay WAL as an audit trail)
- Batch splitting for large transactions
- Auto-reconnection with exponential backoff
- Transaction metadata via `pg_logical_emit_message` or metadata table

## Architecture

PGZR provides three layers that can be used independently or composed into a
full pipeline:

### Mode 1: Low-level Replication (Replicator)

Stream raw WAL messages directly from PostgreSQL:

```
┌───────────┐   WAL stream     ┌────────────┐
│ Source DB │ ───────────────> │ Replicator │ ──> raw WAL messages
│ (PG)      │   pgoutput/      │            │     to your code
└───────────┘   test_decoding  └────────────┘
```

### Mode 2: WAL Ingest Pipeline (Ingestor)

Stream WAL from a source database and store packed batches in a destination
database for later processing:

```
┌───────────┐   WAL stream     ┌───────────┐   packed batches   ┌─────────┐
│ Source DB │ ───────────────> │ Ingestor  │ ─────────────────> │ Dest DB │
│ (PG)      │   pgoutput       │           │   wal_batches      │ (PG)    │
└───────────┘                  └───────────┘   table            └─────────┘
```

### Mode 3: Full Pipeline (Ingestor + Processor)

Ingest WAL, store batches, then process them into structured tables:

```
┌───────────┐  WAL  ┌───────────┐  batches  ┌─────────┐  read   ┌───────────┐
│ Source DB │ ────> │ Ingestor  │ ────────> │ Dest DB │ <────── │ Processor │
│ (PG)      │       │           │           │ (PG)    │ ──────> │           │
└───────────┘       └───────────┘           └─────────┘  write  └───────────┘
                                                 │
                                                 v
                                     ┌───────────────────────┐
                                     │ transactions          │
                                     │ events (JSONB data)   │
                                     │ relation_snapshots    │
                                     │ (structured audit log)│
                                     └───────────────────────┘
```

Large transactions are automatically split into partial batches by the
Ingestor (with `complete=false`). The Processor accumulates partial batches
and processes them as a single unit when the final `complete=true` batch
arrives.

## Usage

### Low-level Replication

Stream raw WAL messages from a logical replication slot:

```zig
const std = @import("std");
const pgzr = @import("pgzr");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var repl = try pgzr.Replicator.init(allocator, .{
        .conn = .{
            .host = "127.0.0.1",
            .port = 5432,
            .user = "postgres",
            .database = "mydb",
        },
        .slot_name = "my_slot",
        .options = &.{
            .{ "proto_version", "1" },
            .{ "publication_names", "my_pub" },
        },
    });
    defer repl.deinit();

    while (try repl.next()) |msg| {
        std.debug.print("{s}\n", .{msg.data});
        repl.ack(msg.wal_start);
    }
}
```

### WAL Ingest Pipeline

Stream WAL from a source database and store packed batches in a destination
database for later processing:

```zig
var ingestor = try pgzr.Ingestor.init(allocator, .{
    .source = .{
        .conn = .{
            .host = "127.0.0.1",
            .port = 5432,
            .user = "postgres",
            .database = "source_db",
        },
        .slot_name = "my_slot",
        .options = &.{
            .{ "proto_version", "1" },
            .{ "publication_names", "my_pub" },
        },
    },
    .dest = .{
        .host = "127.0.0.1",
        .port = 5432,
        .user = "postgres",
        .database = "dest_db",
        .replication = false,
    },
    .source_id = "00000000-0000-0000-0000-000000000001",
    .max_batch_size = 4 * 1024 * 1024, // 4 MiB
});
defer ingestor.deinit();

try ingestor.run();
```

### WAL Processor

Process stored WAL batches into structured transactions and events:

```zig
var processor = try pgzr.Processor.init(allocator, .{
    .dest = .{
        .host = "127.0.0.1",
        .port = 5432,
        .user = "postgres",
        .database = "dest_db",
        .replication = false,
    },
    // Optional: capture metadata from pg_logical_emit_message
    .metadata_message_prefix = "my_prefix",
    // Optional: capture metadata from a table
    .metadata_table = "my_metadata",
});
defer processor.deinit();

try processor.run();
```

### Transaction Metadata

Applications can attach arbitrary JSON metadata to transactions (e.g. the user
who made the change, a request ID). The Processor supports two opt-in
mechanisms configured via `ProcessorConfig`:

**Via `pg_logical_emit_message`** — set `metadata_message_prefix`. The
application calls `pg_logical_emit_message` within a transaction on the source:

```sql
BEGIN;
SELECT pg_logical_emit_message(true, 'my_prefix', '{"user":{"id":1,"name":"Alice"}}');
INSERT INTO orders (item, qty) VALUES ('widget', 10);
COMMIT;
```

The Ingestor must include `"messages", "true"` in its pgoutput options for
logical messages to appear in the WAL stream.

**Via metadata table** — set `metadata_table`. The application creates a table
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

Writes to the metadata table are not stored as events — the `data` column is
extracted and stored in `transactions.metadata`. If both mechanisms are used in
the same transaction, their JSON objects are merged with PostgreSQL's `||`
operator.

See [docs/schema.md](docs/schema.md) for the full schema and query examples.

### TLS

Connections support four TLS modes via the `tls` field on `ConnConfig`:

| Mode | Behavior |
|------|----------|
| `.disable` | No TLS (default) |
| `.prefer` | Try TLS, fall back to plaintext if server doesn't support it |
| `.require` | Require TLS; encrypt only, no certificate or hostname verification (matches PostgreSQL `sslmode=require`) |
| `.verify_full` | Require TLS with full certificate chain and hostname verification against system CAs (matches PostgreSQL `sslmode=verify-full`) |

```zig
.conn = .{
    .host = "db.example.com",
    .port = 5432,
    .user = "postgres",
    .database = "mydb",
    .tls = .require,  // encrypt without verification
},
```

For the C ABI, the `tls_mode` integer maps as: 0 = disable, 1 = prefer,
2 = require, 3 = verify-full.

## Prerequisites

- Zig 0.15.2+
- PostgreSQL with `wal_level = logical`
- A logical replication slot (e.g. created with `test_decoding` or `pgoutput`)

## Building

```bash
zig build                  # build library + examples
zig build test             # run unit tests
zig build lib              # build shared library (libpgzr.dylib/so)
zig build example          # run examples/basic.zig
zig build ingest-example   # run examples/ingest.zig
zig build integration-test # run integration tests (requires PostgreSQL)
zig build pipeline-test    # run pipeline integration tests (requires PostgreSQL)
```

### Shared Library (C ABI)

Build `libpgzr.dylib` (macOS) or `libpgzr.so` (Linux) for use from Ruby,
Python, or any language with FFI:

```bash
zig build lib
ls zig-out/lib/libpgzr.*
```

Exported functions:

```
pgzr_ingestor_new(config)   → *Ingestor or NULL
pgzr_ingestor_run(ptr)      → 0 on success, -1 on error
pgzr_ingestor_stop(ptr)     → void (thread-safe)
pgzr_ingestor_free(ptr)     → void

pgzr_processor_new(config)       → *Processor or NULL
pgzr_processor_run(ptr)          → 0 on success, -1 on error
pgzr_processor_process_one(ptr)  → 1 if batch processed, 0 if none, -1 on error
pgzr_processor_stop(ptr)         → void (thread-safe)
pgzr_processor_free(ptr)         → void

pgzr_last_error(out_len)    → pointer to error message
```

The `PgzrIngestConfig` struct accepts an optional `on_flush` callback
(`void (*)(void *context, uint64_t start_lsn, uint64_t end_lsn, size_t msg_count, bool is_complete)`)
and `on_flush_context` pointer. When set, the callback fires after each
successful batch flush with the batch LSN range, message count, and whether
the batch is complete.

`pgzr_ingestor_run` and `pgzr_processor_run` install SIGINT/SIGTERM handlers
that call `stop()` on the active instance. This allows clean shutdown from FFI
hosts (Ruby, Python) where the language's signal handlers can't execute during
a blocking C call. Previous signal handlers are restored when `run` returns.

## Manual Example

```bash
createdb pgzr_test
psql -d pgzr_test -c \
  "SELECT pg_create_logical_replication_slot('pgzr_test_slot', 'test_decoding');"

# In one terminal:
zig build example

# In another terminal:
psql -d pgzr_test -c \
  "CREATE TABLE t (x text); INSERT INTO t VALUES ('hello');"

# The example prints:
#   BEGIN ...
#   table public.t: INSERT: x[text]:'hello'
#   COMMIT ...

# Cleanup:
psql -d pgzr_test -c "SELECT pg_drop_replication_slot('pgzr_test_slot');"
dropdb pgzr_test
```

## Project Structure

```
src/
  root.zig        -- public API re-exports
  replicator.zig  -- high-level replication loop (IDENTIFY_SYSTEM, START_REPLICATION, streaming, feedback)
  connection.zig  -- TCP/Unix socket connection, TLS upgrade, startup handshake, simple query execution
  transport.zig   -- transport abstraction (plain TCP/Unix and TLS)
  protocol.zig    -- wire protocol encoding/decoding
  auth.zig        -- cleartext, MD5, and SCRAM-SHA-256 authentication
  scram.zig       -- SCRAM-SHA-256 (RFC 5802) implementation
  pgoutput.zig    -- pgoutput binary protocol decoder (proto versions 1-4)
  lsn.zig         -- LSN type (parse, format, binary I/O)
  types.zig       -- ConnConfig, ReplicatorConfig, IngestConfig, ProcessorConfig, etc.
  ingest.zig      -- stage 1: stream WAL from source, pack into batches, store in dest
  processor.zig   -- stage 2: read batches, decode pgoutput, write transactions/events/relation_snapshots
  schema.zig      -- DDL for pipeline tables (wal_batches, transactions, events, relation_snapshots)
  query.zig       -- SQL escaping helpers (strings, bytea, UUIDs, timestamps)
  pg_types.zig    -- PostgreSQL OID-to-type-name lookup
  cabi.zig        -- C ABI exports for shared library (Ruby/Python FFI)
examples/
  basic.zig       -- minimal replication example (test_decoding)
  ingest.zig      -- WAL ingest pipeline example (pgoutput)
tests/
  integration.zig -- replicator integration tests
  pipeline.zig    -- ingest + processor pipeline integration tests
benchmark/
  bench.zig       -- pgzr vs pg_replication (Ruby) benchmark
```

## Future Work

- **UUIDv7 transaction IDs** — Replace `transactions.id` (BIGSERIAL) with a
  client-generated UUIDv7. This eliminates the `INSERT RETURNING id`
  round-trip when creating a transaction, since the ID is known before the
  INSERT. The transaction INSERT can then be combined with the events
  multi-row INSERT into a single SQL statement, reducing the per-transaction
  round-trips from 2 to 1. Requires a schema migration:
  `transactions.id` from `BIGSERIAL` to `UUID`, `events.transaction_id` from
  `BIGINT` to `UUID`, and updating the composite foreign key and primary keys.
  UUIDv7's time-ordered prefix preserves index locality on the partitioned
  tables. Zig's `std.crypto.random` provides the entropy source; the
  timestamp prefix comes from `std.time.milliTimestamp()`.

- **Extended query protocol** — The processor currently uses the simple query
  protocol. PostgreSQL's extended query protocol (Parse/Bind/Execute/Sync)
  enables parameterized queries with plan caching and pipeline mode for
  further reducing round-trips. Requires implementing Parse, Bind, Execute,
  Describe, and Sync message encoding in `protocol.zig`, a new pipelined
  execution path in `connection.zig`, and response handling that matches
  multiple result sequences to their corresponding requests. Event INSERTs
  are already batched into a single multi-row INSERT per transaction.

- **io_uring / kqueue** — The transport layer currently uses blocking I/O.
  Using io_uring (Linux) or kqueue (macOS) would allow non-blocking,
  event-driven I/O — enabling a single thread to manage multiple replication
  connections and reduce syscall overhead. Zig's `std.posix` provides the
  building blocks for both.

- **Store source system identifier, database, and timeline** — The Replicator
  already fetches system_id, timeline, and database name via IDENTIFY_SYSTEM.
  Storing these in the destination (e.g. on a sources table or in
  wal_batches/transactions metadata) would allow the pipeline to detect when
  a source has been rebuilt, failed over to a different timeline, or when
  batches from different clusters are accidentally mixed.


## License

All rights reserved. See [LICENSE](LICENSE).
