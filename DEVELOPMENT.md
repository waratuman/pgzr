# Development Notes

## Performance Roadmap

Throughput work on the batch pipeline, ordered by expected impact. Items 1-4
are protocol/processor changes; 5-7 are schema and architecture decisions.

Context: as of 0.4.2 the processor claims a whole batch group in one CTE
statement, deletes claimed batches in one statement, resolves transaction ids
in one round trip, and the claim queries are backed by partial indexes on
`wal_batches`. The remaining costs are per-transaction round trips, SQL
parsing overhead on bulk inserts, and destination write amplification.

### 1. COPY instead of INSERT for events

`flushPendingEvents` builds a megabyte-scale multi-row INSERT that the server
must lex, parse, and plan on every flush, and the client pays hex/JSON
escaping into SQL literal syntax. `COPY events FROM STDIN` (text format
first, binary later if needed) skips SQL parsing entirely — typically 2-5x
faster for bulk loads. Needs `CopyInResponse`/`CopyData`/`CopyDone` support
in `connection.zig`. Also removes the 1000-row `flush_threshold` compromise:
rows can stream continuously.

Status: done. Measured ~10% end-to-end on the 100K-event stress test over a
local socket (process phase ~944ms → ~852ms, ReleaseFast); the win grows
with payload size and on remote/TLS links where wire bytes and parse
latency compound.

### 2. Claim K batch groups per cycle + two-pass decode

A workload of many small transactions still pays `insertTransaction` + one
event flush per transaction, and BEGIN/claim/COMMIT per group.

- Claim K groups per cycle (`LIMIT 1` → `LIMIT k` on the group selector in
  `claimAndProcess`): amortizes claim/commit overhead over K transactions and
  makes fewer, larger destination commits (fewer WAL flushes).
- Two-pass decode: scan the batch once collecting all Begin records, insert
  all transactions in one `INSERT ... RETURNING id` (ordinal-keyed), then
  decode events with ids already known. ~3 round trips per batch regardless
  of transaction count.

### 3. Multi-statement Query messages

The simple query protocol allows multiple statements per Query message:
`"BEGIN; <claim CTE>"` and `"<delete>; COMMIT"` collapse 4 round trips to 2
per cycle with no semantic change.

### 4. `synchronous_commit = off` on the processor connection

Safe for the processor specifically: claim, event inserts, and batch delete
are one atomic transaction — a crash that loses the commit reverts the batch
to `pending` and it is reprocessed with no duplicates. Set per-session after
connect.

Do NOT set this on the ingest connection: ingest acks the replication slot
after flush, so a lost commit there is unrecoverable data loss. (If the slot
ack ever moves post-processing — see item 6 — this constraint can be
revisited.)

### 5. Schema costs on `events`

- The FK `events(transaction_id, committed_at) → transactions` costs a
  per-row lookup plus a row lock on the parent for every event insert. The
  processor already guarantees the invariant (it inserts the transaction
  first, on the same connection, in the same transaction). Dropping it is a
  real per-row win at high volume. Measured: ~14% on the 100K-event stress
  test (~852ms → ~735ms process phase, ReleaseFast, COPY path).
- `identity_digest` is SHA-256 per event (`computeIdentityDigest`). If the
  digest exists for change-tracking joins rather than adversarial collision
  resistance, xxhash3/blake3 is 10-50x cheaper per event. Decide what the
  digest's contract is before switching.

### 6. Queue write amplification

Every change is written to the destination three times: `wal_batches` INSERT
(+WAL), `events` INSERT (+WAL), `wal_batches` DELETE (+WAL), plus vacuum
churn on the queue table. In increasing ambition:

- Aggressive autovacuum on `wal_batches`
  (`autovacuum_vacuum_scale_factor=0` + threshold-based): high-churn queue
  tables bloat badly under defaults, and the claim degrades as the heap
  bloats even with the partial indexes.
- `UNLOGGED` wal_batches: eliminates WAL for queue data. Requires moving the
  slot ack from post-flush to post-process (ack only what processors have
  committed to `events`), so a destination crash replays from the slot
  instead of losing batches.
- Direct mode: a combined ingest+process path that decodes the replication
  stream straight into `transactions`/`events` and acks on commit, skipping
  the queue entirely (~1/3 the destination writes). Fits as an alternative
  consumer alongside the existing pipeline for deployments that don't need
  the replay buffer.

### 7. Ingest parallelism ceiling

One slot = one ordered stream = one ingest connection; that is the hard
ceiling on ingest throughput. The escape hatch is sharding by publication
(multiple slots over disjoint table sets, one pipeline each) — but that
surrenders cross-table transaction atomicity, so it is a product decision.
Below the ceiling: ingest stops reading WAL while its flush INSERT
round-trips (double-buffer or a writer thread would overlap), and
hex-encoded bytea doubles its wire bytes (COPY/binary params help here too).

### Measurement

Benchmark with `zig build concurrent-stress-test -Doptimize=ReleaseFast`
(100K events, 4 workers, adversarial reaper) and compare the `process=` time.
Two hard-won caveats:

- **Always benchmark ReleaseFast.** A Debug build is ~5x slower in the
  process phase (4.7s vs 0.9s) because `GeneralPurposeAllocator`'s safety
  checks plus its shared mutex dominate — worker threads serialize on the
  allocator and every protocol-level change disappears into the noise.
- **Check `pg_stat_activity` during a run** before crediting/blaming the
  database: `idle in transaction` + `ClientRead` waits on all workers means
  the bottleneck is client-side CPU/allocations, not the server.

Baseline after item 1 (2026-07-08, local socket, M-series):
process ≈ 852ms for 100K events (~117K events/s across 4 workers).
Re-evaluate the ordering of items 5-7 based on the profile once 2-4 land.

### Longer-term ideas

- Chunk-streamed `processBatch`: the ingestor only splits batches between
  length-prefixed messages, and all transaction state lives on the
  Processor, so claimed rows could be fed through `processBatch` one at a
  time and freed immediately — memory becomes O(largest batch) instead of
  O(largest transaction). Requires a test pinning the "batches never split
  mid-message" invariant.
- Binary COPY format: eliminates hex encoding for bytea and text formatting
  for timestamps entirely (jsonb binary format is version byte + JSON text).
- LISTEN/NOTIFY instead of polling for batch arrival (latency, not
  throughput).
