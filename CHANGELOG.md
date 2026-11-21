# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- **Extended query protocol (Parse/Bind/Execute/Sync).** `Connection`
  gains `prepare` (named server-side prepared statements),
  `execPrepared`/`execPreparedWithResult` (one-round-trip parameterized
  execution), and `Pipeline` — queue several statements (or prepares)
  and send them in a single network flush, with each result row
  delivered to a callback tagged with its statement index.

- `DEVELOPMENT.md` with the performance roadmap and benchmarking notes.

### Changed

- **The processor hot path uses prepared statements and pipelining.**
  Its five statements are prepared once at init (in one pipelined round
  trip); parameters are bound instead of escaped into SQL. `BEGIN` is
  pipelined with the batch claim and `COMMIT` with the batch delete,
  cutting round trips per batch group from 8 to 6, and the existence
  probe is skipped right after a successful claim so the busy path pays
  no probe round trip. The now-unused `queryRows` and
  `execLargeWithResult` query paths are removed.

- **Row-returning query results are captured in a dedicated side
  buffer.** `simpleQuery` and `execPreparedWithResult` copy the first
  row into a growable `result_buf` instead of the second half of
  `recv_buf`, so later protocol reads can no longer overwrite the
  returned columns, realloc them into dangling slices, or hit an
  overlapping `@memcpy` on rows crossing the buffer midpoint.

- **COPY now uses binary format for both `wal_batches` and `events`.**
  Batch payloads go over the wire at 1x size (no hex) and are streamed
  straight from the accumulation buffer with no intermediate copy;
  event rows carry `committed_at` as 8 bytes instead of 29-char text,
  identity digests as raw bytes instead of hex, and JSONB as plain JSON
  with no COPY text-escaping layer.

- **Plain TCP reads are buffered (64 KiB).** The protocol layer
  previously paid poll + header read + body read (~3 syscalls) per WAL
  message; one refill now serves many messages and `hasPending` skips
  the poll while data is buffered. Large bodies still read directly
  into the destination buffer.

  Together: concurrent stress test (100k events, ReleaseFast) ingest
  ~45% faster (273–291 ms vs 496–528 ms); processing unchanged
  (server-bound).

- **C ABI handles allocate with `std.heap.smp_allocator`** instead of
  `page_allocator` (page-granularity, syscall per allocation) — the FFI
  path makes per-event allocations, so this materially speeds up
  production Ruby/Python hosts.

- **Hot byte loops in the processor are table-driven/vectorized.** Hex
  bytea decode of stored batch rows uses a 256-entry table (~0.2 GB/s →
  ~2.9 GB/s measured); the COPY JSON string escape scan skips clean
  16-byte blocks with `@Vector` (~1.5 GB/s → ~39 GB/s on clean text).
  Stress-test processing (100k events, 4 workers, ReleaseFast) improved
  ~15% (542 ms → ~459 ms).

- **Batch rows are stored via `COPY ... FROM STDIN`.** The ingestor no
  longer builds a megabyte-scale INSERT statement (hex bytea inlined into
  SQL the server must lex/parse/plan); it sends one COPY row (binary
  format — see above). Restart-replay duplicates are detected via
  SQLSTATE 23505 and ignored, matching the previous
  `ON CONFLICT DO NOTHING` semantics.

- **Event flushes stream through copy-in mode in ~64 KiB chunks.**
  `Connection` gains `copyInBegin`/`copyInWrite`/`copyInEnd`/`copyInFail`;
  peak memory per flush drops from a full concatenated copy of the batch
  (retained at high-water capacity) to one chunk.

- **Idle processor polls cost one round trip again.** `processOne` probes
  for pending work with a single indexed `SELECT ... LIMIT 1` before
  opening the claim transaction (previously BEGIN + claim + COMMIT per
  empty poll).

- **Truncate events record which tables were truncated:** one `T` event
  per truncated relation (with its `rel_oid`) instead of a single event
  with `rel_oid = 0`.

- **Non-transactional logical messages are no longer captured as
  metadata.** Only `pg_logical_emit_message(true, ...)` (transactional)
  attaches to a transaction; non-transactional messages were previously
  mis-attributed to whatever transaction committed next.

- **Upgraded to Zig 0.17.** All networking, TLS, randomness, clocks, sleeps,
  and child processes now go through the `std.Io` interface. Public init
  functions take an `Io` parameter: `Connection.connect(allocator, io, config)`,
  `Replicator.init(allocator, io, config)`, `Ingestor.init(allocator, io,
  config)`, `Processor.init(allocator, io, config)`. The C ABI is unchanged
  for callers — each handle creates its own `std.Io.Threaded` instance
  internally. Transport read/write error sets collapse to
  `error{ConnectionClosed, ReadFailed, Canceled}` / `error{WriteFailed,
  Canceled}`. TLS verify-full certificate loading is handled by
  `std.crypto.tls.Client` itself and still fails closed
  (`error.TlsCertificateNotVerified`) when no CA bundle is available.

- **Fixed TLS `verify_full` silently downgrading to no verification** when
  the system CA bundle could not be loaded (pre-0.17 code path).

- **Processor error paths no longer leak state across batches.** Claim and
  COMMIT failures reset all per-batch state (pending data, batch ids,
  buffered events, metadata chunks, transaction state); a COMMIT failure no
  longer parks intact batches in `'error'`. `Begin` clears any stale
  buffered events.

- **C ABI handles own copies of all caller strings.** The FFI host may free
  its config struct and strings as soon as `pgzr_*_new` returns.

- **Events are inserted via `COPY ... FROM STDIN`** instead of multi-row
  INSERT statements. The server no longer lexes/parses/plans megabyte-scale
  SQL per flush, and the client emits the COPY stream directly (no SQL
  literal quoting layer; binary format — see above). ~10% end-to-end on
  the 100K-event stress test over a local socket; larger on remote/TLS
  links. Adds `Connection.copyIn` and a `CopyInResponse` protocol handler.

### Fixed

- **`Replicator.init` no longer leaks the connection's buffers when the
  handshake fails after connecting** (e.g. timeline or system-id
  mismatch): the error path closed a stale copy of the connection
  instead of the one whose buffers had grown. A `TlsState` leak in
  `Connection.connect` is also fixed: a failure after a successful TLS
  upgrade (e.g. bad credentials) skipped the state's cleanup, leaking
  per-attempt under `reconnect()` retry loops.

- **Long replication commands no longer overflow fixed stack buffers.**
  `START_REPLICATION` is now built with the allocator (no 4 KB limit on
  slot name plus options, e.g. long publication lists) and embedded
  quotes in option names/values are escaped. `simpleQuery`/`queryRows`
  now use the connection's growable send buffer instead of a fixed 4 KB
  stack buffer, removing the same overflow one layer down.

- **`Replicator.system_id`/`timeline`/`xlogpos`/`db_name` are now owned
  copies.** They previously pointed into the connection's receive buffer
  and became garbage after the next protocol read; they now stay valid
  for the replicator's lifetime (freed in `deinit`).

- **Malformed stored batch data can no longer panic the processor.**
  Corrupt bytes decoded from `wal_batches` now surface as decode errors
  (which park the batch as poison) instead of crashing: negative pgoutput
  tuple lengths and overflowing truncate relation counts return
  `InvalidMessage`/`UnexpectedEnd` rather than tripping integer-cast
  panics; corrupt hex bytea (odd length or invalid digit) is an error
  instead of silently yielding empty data or a misleading `OutOfMemory`;
  out-of-range integers in the relations JSONB snapshot are ignored
  instead of panicking; and a short MD5 auth message fails with
  `InvalidServerResponse` instead of a slice panic.

- **Interleaved streamed transactions are now handled correctly.** With
  proto v2+ streaming, segments of concurrently-streaming large
  transactions interleave in the WAL stream; previously the ingestor's
  single `in_transaction`/`txn_begin_lsn` and the processor's shared
  event buffer silently dropped or mis-attributed events (even a single
  multi-segment stream could lose its earlier partial batches). The
  ingestor now tracks the set of open transactions — a batch group's
  `begin_lsn` is fixed when the first transaction opens, and only a
  close that leaves zero transactions open flushes `complete=true`. The
  processor buffers streamed events and metadata per top-level xid,
  adopting them into the flush path at that transaction's
  `stream_commit`. Streamed messages now expose their embedded (sub)xid,
  so a subtransaction abort discards exactly its own changes instead of
  the whole buffer. Adds a pipeline test that commits two concurrently
  streaming transactions and verifies per-transaction attribution.

- **Transient failures no longer permanently park batches.** A processor
  failure now parks the claimed batches in `state='error'` only when the
  failure is a property of the batch itself: pgoutput decode errors,
  validation errors, or a server error with a non-retryable SQLSTATE.
  Connection loss, deadlocks, serialization failures, statement timeouts,
  lock timeouts, and resource exhaustion (SQLSTATE classes 08, 40, 53, 55,
  57) leave the rows `'pending'` so the next cycle retries them.
  `Connection` now exposes `lastServerErrorCode()` with the SQLSTATE of the
  most recent command's server error.

- **Streamed Relation messages corrupted the ingestor's relation cache.**
  Inside a stream segment (proto v2+), Relation messages carry a leading
  Int32 xid that the ingestor's cache decode did not skip, so the parse
  misaligned and binary garbage entered the relations JSONB — the batch
  INSERT then failed server-side with `invalid byte sequence for encoding
  "UTF8"`. The ingestor now tracks the open stream segment
  (StreamStart/StreamStop) and decodes streamed messages with the stream
  xid, matching the processor's behavior.

## [0.4.2] - 2026-07-08

### Changed

- **Processor batch throughput: fewer round trips per batch.** The batch
  group claim (complete batch + all partials) is now a single CTE
  `UPDATE ... RETURNING` statement instead of one query per partial plus a
  trailing probe; claimed batches are deleted with one `DELETE ... WHERE id
  IN (...)` instead of one per row; and the transaction insert uses
  `ON CONFLICT ... DO UPDATE ... RETURNING id` so the conflict path no
  longer needs a fallback `SELECT`. A minimal batch drops from 7 round
  trips to 4; batches with many transactions roughly halve.
- **New partial indexes on `wal_batches`** (`idx_wal_batches_pending`,
  `idx_wal_batches_partial`) back the claim queries, keeping batch claiming
  O(log n) under backlog instead of a sequential scan per poll. Created
  automatically by `ensureSchema`.
- **SQL literal escaping copies clean runs in bulk** instead of
  byte-at-a-time (string, bytea, and JSON escaping), reducing per-event CPU
  when building multi-row inserts.

### Fixed

- **`Connection.queryRows` drains the trailing `ReadyForQuery` after an
  `ErrorResponse`** instead of returning early, matching the 0.4.1 fix to
  `simpleQuery`/`execLarge` and keeping the connection usable for a
  follow-up `ROLLBACK`.

## [0.4.1] - 2026-06-30

### Added

- **`pgzr_abi_version()` C export.** Returns a `u32` ABI version
  (currently `0x00040000`) that FFI callers can verify at load time to
  detect struct-layout skew before invoking other entry points. Missing
  symbol indicates a pre-0.4.0 library. Bump this constant in lockstep
  with any incompatible change to an exported config struct.

### Fixed

- **Processor no longer double-processes a batch under concurrent zombie
  recovery (#14).** The claim/decode/insert/delete cycle now runs inside a
  single transaction, so the claim's row lock is held for the whole batch.
  A live worker's batch can no longer be requeued by an external reaper
  (the reaper's `UPDATE` blocks on the held lock), and a crashed worker's
  claim rolls back automatically — returning the batch to `pending` with no
  duplicate events and no reaper required for correctness.
- **Connection query helpers drain the trailing `ReadyForQuery` after an
  `ErrorResponse`** instead of returning early, keeping the connection in
  sync so a `ROLLBACK` (or any follow-up query) after a failed statement
  behaves correctly.

## [0.4.0] - 2026-03-12

### Breaking Changes

- **`ProcessorConfig.source_id` removed.** Processors are now source-agnostic
  workers that derive `source_id` from each claimed batch. Remove the
  `source_id` field from any `ProcessorConfig` initialization.

- **`wal_batches` schema change: new `begin_lsn` column.** The column groups
  partial batches belonging to the same transaction. Existing databases require
  a migration:

  ```sql
  ALTER TABLE wal_batches ADD COLUMN begin_lsn BIGINT;
  UPDATE wal_batches SET begin_lsn = start_lsn WHERE begin_lsn IS NULL;
  ALTER TABLE wal_batches ALTER COLUMN begin_lsn SET NOT NULL;
  ```

- **C ABI: `source_id` removed from `PgzrProcessorConfig`.** The field is no
  longer present in the extern struct.

### Added

- Processors can now safely run concurrently. Multiple processor instances
  drain a shared `wal_batches` queue ordered by `created_at` (FIFO across all
  sources). Partial batch chains are claimed atomically by `(source_id,
  begin_lsn)` grouping.

- Relation metadata is now loaded from the `relations` JSONB column stored in
  each batch, making batches self-contained and order-independent.

### Fixed

- Single quotes in text values are now escaped in JSONB output, fixing SQL
  syntax errors for values like `O'Brien` (#7).

## [0.3.5] - 2026-03-12

### Fixed
- Escape JSON control characters (0x00-0x1f) in ingestor relation
  serialization, matching the processor's escaping behavior.
- Document `standard_conforming_strings=on` assumption and UTF-8 requirement
  on SQL string escaping functions.

## [0.3.4] - 2026-03-12

### Fixed
- Single quotes in text values are now escaped in JSONB output, fixing SQL
  syntax errors for values like `O'Brien` (#7).

## [0.3.3] - 2026-03-10

### Changed
- Batch event INSERTs into multi-row statements, reducing per-transaction
  round-trips from N+1 to 2 (~3x throughput improvement)
- Add chunked flushing (threshold: 1000 events) so large transactions don't
  accumulate unbounded memory before writing
- Unify streaming and non-streaming event buffering paths in the processor

### Fixed
- Grow `recv_buf` dynamically when query responses exceed the default 1 MiB
  buffer, fixing `ProtocolError` on large WAL batches (e.g. 10k+ row
  transactions). Buffer shrinks back to default size between queries.
- Deduplicate buffer growth logic between replicator and connection layer

## [0.3.2] - 2026-03-10

### Fixed
- Fix TLS large write data loss and add debug logging
- Check GPA deinit for memory leaks in integration tests

## [0.3.1] - 2026-03-09

### Fixed
- Fix TLS memory leak, SCRAM error reporting, and add read debug logging
- Fix TLS data corruption and poll starvation in replicator

## [0.3.0] - 2026-03-09

### Added
- SSL ingest integration test
- Logging to replicator and ingestor

## [0.2.9] - 2026-03-08

### Added
- Poll-based read timeout to replicator for periodic status updates and stop
  flag checks

## [0.2.8] - 2026-03-08

### Added
- SIGINT/SIGTERM signal handling in C ABI for clean shutdown from FFI hosts
  (Ruby, Python)

## [0.2.7] - 2026-03-07

### Added
- `on_flush` callback to Ingestor and C ABI for batch flush notifications
- TLS integration tests for prefer and require modes

### Fixed
- Fix TLS buffer sizes and errdefer use-after-free
- Fix TLS write deadlock by flushing output buffer to socket
- Fix TLS buffer aliasing between stream and TLS layers
- Fix TLS require mode to skip certificate verification (match PostgreSQL
  `sslmode=require` behavior)

## [0.2.2] - 2026-03-06

### Added
- Transaction metadata support via `pg_logical_emit_message` and metadata table
- `committed_at` column on events, time-based partitioning for transactions and
  events tables
- Schema v2: JSONB events, relation snapshots, drop columns table
- C ABI shared library (`libpgzr.dylib`/`libpgzr.so`) for Ruby/Python FFI
- pgoutput protocol versions 2-4 (streaming transactions, two-phase commit)
- WAL ingest and processor pipeline (Ingestor + Processor)
- Batch splitting for large transactions
- Pipeline benchmark with 20-column wide table
- GitHub Actions CI workflow
- Makefile for standard make/make install workflow

### Fixed
- Fix streaming transaction support (proto v2/v4)
- Fix stale buffer bugs in ingest/pipeline
- Always enable pgoutput messages option in C ABI
- Wire build.zig.zon version to shared library output

## [0.1.0] - 2026-03-04

Initial release.

### Added
- Pure Zig PostgreSQL logical replication client, zero C dependencies
- TCP and Unix domain socket connections
- TLS support with transport abstraction (disable, prefer, require, verify-full)
- SCRAM-SHA-256, MD5, and cleartext authentication
- pgoutput protocol decoder
- Automatic reconnection with exponential backoff
- Start/end position control for replication slots
- Non-replication connection support and query helpers
- SQL escaping, OID-to-type-name lookup, schema DDL
- Integration test suite ported from pg_replication (Ruby)
- Benchmark comparing pgzr vs pg_replication
