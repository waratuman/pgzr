# PGZR - PostgreSQL Zig Replicator

A pure Zig library implementing PostgreSQL logical replication. No C
dependencies.

## Features

- Pure Zig, zero C dependencies
- TCP, Unix socket, and TLS connections
- SCRAM-SHA-256, MD5, and cleartext authentication
- pgoutput protocol versions 1-4 (streaming, two-phase commit)
- WAL ingest and processing pipeline (store and replay WAL as an audit trail)
- Batch splitting for large transactions
- Auto-reconnection with exponential backoff

## Architecture

PGZR provides three layers that can be used independently or composed into a
full pipeline:

### Mode 1: Low-level Replication (Replicator)

Stream raw WAL messages directly from PostgreSQL:

```
┌──────────┐    WAL stream     ┌────────────┐
│ Source DB │ ────────────────> │ Replicator │ ──> raw WAL messages
│ (PG)     │   pgoutput/       │            │     to your code
└──────────┘   test_decoding   └────────────┘
```

### Mode 2: WAL Ingest Pipeline (Ingestor)

Stream WAL from a source database and store packed batches in a destination
database for later processing:

```
┌──────────┐    WAL stream     ┌───────────┐   packed batches   ┌─────────┐
│ Source DB │ ────────────────> │ Ingestor  │ ────────────────>  │ Dest DB │
│ (PG)     │   pgoutput        │           │   wal_batches      │ (PG)    │
└──────────┘                   └───────────┘   table             └─────────┘
```

### Mode 3: Full Pipeline (Ingestor + Processor)

Ingest WAL, store batches, then process them into structured tables:

```
┌──────────┐  WAL   ┌───────────┐  batches  ┌─────────┐  read   ┌───────────┐
│ Source DB │ ────>  │ Ingestor  │ ────────> │ Dest DB │ <────── │ Processor │
│ (PG)     │        │           │           │ (PG)    │ ──────> │           │
└──────────┘        └───────────┘           └─────────┘  write  └───────────┘
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
    .source_id = "00000000-0000-0000-0000-000000000001",
});
defer processor.deinit();

try processor.run();
```

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

- **Pipeline mode for dest queries** — Currently the processor uses the simple
  query protocol, issuing one round-trip per SQL statement. PostgreSQL's
  extended query protocol supports pipeline mode (Parse/Bind/Execute/Sync)
  which allows sending an entire batch of queries without waiting for
  individual responses. This would collapse all event and column INSERTs for a
  batch into a single network round-trip. Requires implementing the extended
  query protocol, client-side UUID generation (to remove the `RETURNING id`
  dependency between event and column INSERTs), and pipeline error handling.

- **io_uring / kqueue** — The transport layer currently uses blocking I/O.
  Using io_uring (Linux) or kqueue (macOS) would allow non-blocking,
  event-driven I/O — enabling a single thread to manage multiple replication
  connections and reduce syscall overhead. Zig's `std.posix` provides the
  building blocks for both.

