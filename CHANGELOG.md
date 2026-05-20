# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- **`pgzr_abi_version()` C export.** Returns a `u32` ABI version
  (currently `0x00040000`) that FFI callers can verify at load time to
  detect struct-layout skew before invoking other entry points. Missing
  symbol indicates a pre-0.4.0 library. Bump this constant in lockstep
  with any incompatible change to an exported config struct.

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
