# PGZR - PostgreSQL Zig Replicator

A pure Zig library implementing PostgreSQL logical replication. No C
dependencies.

## Usage

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
            .{ "include-timestamp", "on" },
        },
    });
    defer repl.deinit();

    while (try repl.next()) |msg| {
        std.debug.print("{s}\n", .{msg.data});
        repl.ack(msg.wal_start);
    }
}
```

## Prerequisites

- Zig 0.15.2+
- PostgreSQL with `wal_level = logical`
- A logical replication slot (e.g. created with `test_decoding` or `pgoutput`)

## Building

```bash
zig build          # build library + example
zig build test     # run unit tests
zig build example  # build and run examples/basic.zig
```

## Integration Test

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
  transport.zig   -- Transport abstraction (plain TCP/Unix and TLS)
  protocol.zig    -- wire protocol encoding/decoding
  auth.zig        -- cleartext, MD5, and SCRAM-SHA-256 authentication
  scram.zig       -- SCRAM-SHA-256 (RFC 5802) implementation
  lsn.zig         -- LSN type (parse, format, binary I/O)
  types.zig       -- ConnConfig, ReplicatorConfig, WalMessage, TlsMode
examples/
  basic.zig       -- minimal working example
```

## Future Work

- Automatic reconnection
- `pgoutput` protocol decoding (for native logical replication)
- `start_position` / `end_position` integration tests

## License

MIT
