# Changelog

All notable changes to this project will be documented in this file.

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
