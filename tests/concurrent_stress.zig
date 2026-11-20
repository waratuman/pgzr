const std = @import("std");
const pgzr = @import("pgzr");

const SOURCE_DB = "pgzr_stress_source";
const DEST_DB = "pgzr_stress_dest";
const SLOT_NAME = "pgzr_stress_slot";
const PUB_NAME = "pgzr_stress_pub";
const SOURCE_ID = "00000000-0000-0000-0000-000000000002";

/// Io instance for the whole test run, set at the top of main.
var io: std.Io = undefined;

fn getenv(name: [*:0]const u8) ?[]const u8 {
    const p = std.c.getenv(name) orelse return null;
    return std.mem.span(p);
}

fn sleepMs(ms: i64) void {
    io.sleep(.fromMilliseconds(ms), .awake) catch {};
}

const Stopwatch = struct {
    start_ts: std.Io.Timestamp,

    fn start() Stopwatch {
        return .{ .start_ts = std.Io.Clock.awake.now(io) };
    }

    fn reset(self: *Stopwatch) void {
        self.start_ts = std.Io.Clock.awake.now(io);
    }

    fn readMs(self: *const Stopwatch) u64 {
        const d = self.start_ts.durationTo(std.Io.Clock.awake.now(io));
        return @intCast(d.toMilliseconds());
    }
};

fn getUser() []const u8 {
    return getenv("PGUSER") orelse getenv("USER") orelse "postgres";
}

fn runPsql(allocator: std.mem.Allocator, db: []const u8, sql: []const u8) !void {
    const result = try std.process.run(allocator, io, .{
        .argv = &.{ "psql", "-d", db, "-c", sql },
    });
    allocator.free(result.stdout);
    allocator.free(result.stderr);
}

fn runPsqlCapture(allocator: std.mem.Allocator, db: []const u8, sql: []const u8) ![]u8 {
    const run_result = try std.process.run(allocator, io, .{
        .argv = &.{ "psql", "-d", db, "-t", "-A", "-c", sql },
    });
    defer allocator.free(run_result.stdout);
    allocator.free(run_result.stderr);

    const trimmed = std.mem.trimEnd(u8, run_result.stdout, "\n\r ");
    const result = try allocator.alloc(u8, trimmed.len);
    @memcpy(result, trimmed);
    return result;
}

fn queryCount(allocator: std.mem.Allocator, db: []const u8, sql: []const u8) !u32 {
    const output = try runPsqlCapture(allocator, db, sql);
    defer allocator.free(output);
    return std.fmt.parseInt(u32, output, 10) catch 0;
}

fn runCmd(allocator: std.mem.Allocator, args: []const []const u8) void {
    const result = std.process.run(allocator, io, .{ .argv = args }) catch return;
    allocator.free(result.stdout);
    allocator.free(result.stderr);
}

/// Simulates the hub's zombie reaper (issue #14): requeues any batch stuck in
/// 'processing' back to 'pending'. A short statement_timeout keeps it from
/// blocking indefinitely on rows a live worker holds locked, mirroring a reaper
/// that cannot tell a hung worker from a live-but-slow one. Returns the number
/// of batches it actually requeued.
fn reapOnce(allocator: std.mem.Allocator) u32 {
    const out = runPsqlCapture(
        allocator,
        DEST_DB,
        "SET statement_timeout='200ms'; " ++
            "WITH r AS (UPDATE wal_batches SET state='pending' " ++
            "WHERE state='processing' RETURNING 1) SELECT count(*) FROM r",
    ) catch return 0;
    defer allocator.free(out);
    return std.fmt.parseInt(u32, out, 10) catch 0;
}

const Reaper = struct {
    stop_flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    requeued: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    fn run(self: *Reaper, allocator: std.mem.Allocator) void {
        while (!self.stop_flag.load(.acquire)) {
            const n = reapOnce(allocator);
            if (n > 0) _ = self.requeued.fetchAdd(n, .monotonic);
            sleepMs(2);
        }
    }

    fn stop(self: *Reaper) void {
        self.stop_flag.store(true, .release);
    }
};

fn sourceConnConfig() pgzr.ConnConfig {
    return .{
        .host = "127.0.0.1",
        .port = 5432,
        .user = getUser(),
        .database = SOURCE_DB,
    };
}

fn destConnConfig() pgzr.ConnConfig {
    return .{
        .host = "127.0.0.1",
        .port = 5432,
        .user = getUser(),
        .database = DEST_DB,
        .replication = false,
    };
}

fn setup(allocator: std.mem.Allocator) !void {
    runCmd(allocator, &.{ "createdb", SOURCE_DB });
    runCmd(allocator, &.{ "createdb", DEST_DB });

    runPsql(
        allocator,
        SOURCE_DB,
        "CREATE TABLE IF NOT EXISTS items (id serial PRIMARY KEY, name text NOT NULL, quantity int NOT NULL DEFAULT 0)",
    ) catch {};

    runPsql(allocator, SOURCE_DB, "DROP PUBLICATION IF EXISTS " ++ PUB_NAME) catch {};
    try runPsql(allocator, SOURCE_DB, "CREATE PUBLICATION " ++ PUB_NAME ++ " FOR TABLE items");

    runPsql(allocator, SOURCE_DB, "SELECT pg_drop_replication_slot('" ++ SLOT_NAME ++ "')") catch {};
    try runPsql(allocator, SOURCE_DB, "SELECT pg_create_logical_replication_slot('" ++ SLOT_NAME ++ "', 'pgoutput')");

    runPsql(allocator, SOURCE_DB, "TRUNCATE items RESTART IDENTITY") catch {};

    var dest = pgzr.Connection.connect(allocator, io, destConnConfig()) catch |err| {
        std.debug.print("Failed to connect to dest: {}\n", .{err});
        return err;
    };
    defer dest.close();

    pgzr.schema.ensureSchema(&dest) catch |err| {
        std.debug.print("Failed to create schema: {}\n", .{err});
        return err;
    };

    _ = dest.simpleQuery("TRUNCATE events, relation_snapshots, transactions, wal_batches CASCADE") catch {};
}

fn teardown(allocator: std.mem.Allocator) void {
    sleepMs(200);
    runPsql(allocator, SOURCE_DB, "SELECT pg_drop_replication_slot('" ++ SLOT_NAME ++ "')") catch {};
    runPsql(allocator, SOURCE_DB, "DROP PUBLICATION IF EXISTS " ++ PUB_NAME) catch {};
    runCmd(allocator, &.{ "dropdb", "--if-exists", "--force", SOURCE_DB });
    runCmd(allocator, &.{ "dropdb", "--if-exists", "--force", DEST_DB });
}

fn getCurrentWalLsn(allocator: std.mem.Allocator) !pgzr.Lsn {
    const output = try runPsqlCapture(allocator, SOURCE_DB, "SELECT pg_current_wal_lsn()");
    defer allocator.free(output);
    return pgzr.Lsn.parse(output);
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    io = init.io;

    std.debug.print("PGZR Concurrent Processor Stress Test\n", .{});
    std.debug.print("======================================\n", .{});

    std.debug.print("Setting up test databases...\n", .{});
    setup(allocator) catch |err| {
        std.debug.print("Setup failed: {}. Is PostgreSQL running?\n", .{err});
        std.process.exit(1);
    };

    var failed = false;
    runStressTest(allocator) catch {
        failed = true;
    };

    std.debug.print("Cleaning up...\n", .{});
    teardown(allocator);

    if (failed) {
        std.debug.print("\nTest FAILED\n", .{});
        std.process.exit(1);
    } else {
        std.debug.print("\nTest passed!\n", .{});
    }
}

fn runStressTest(allocator: std.mem.Allocator) !void {
    const num_txns: u32 = 50;
    const rows_per_txn: u32 = 2000;
    const num_workers: u32 = 4;
    const expected_events = num_txns * rows_per_txn;

    std.debug.print("  Config: {d} txns x {d} rows = {d} events, {d} workers\n", .{
        num_txns, rows_per_txn, expected_events, num_workers,
    });

    // Phase 1: Insert data
    var timer = Stopwatch.start();

    std.debug.print("  Inserting data... ", .{});
    for (0..num_txns) |txn_i| {
        var buf: [512]u8 = undefined;
        const sql = std.fmt.bufPrint(
            &buf,
            "INSERT INTO items (name, quantity) SELECT 'stress_' || {d} || '_' || repeat('x', 500) || g::text, g FROM generate_series(1, {d}) g",
            .{ txn_i, rows_per_txn },
        ) catch unreachable;
        runPsql(allocator, SOURCE_DB, sql) catch |err| {
            std.debug.print("FAIL (insert txn {d}: {})\n", .{ txn_i, err });
            return err;
        };
    }

    const insert_ms = timer.readMs();
    std.debug.print("done ({d}ms)\n", .{insert_ms});

    // Phase 2: Ingest
    const end_lsn = getCurrentWalLsn(allocator) catch |err| {
        std.debug.print("  FAIL (get LSN: {})\n", .{err});
        return err;
    };

    timer.reset();
    std.debug.print("  Ingesting WAL... ", .{});

    var ingestor = pgzr.Ingestor.init(allocator, io, .{
        .source = .{
            .conn = sourceConnConfig(),
            .slot_name = SLOT_NAME,
            .end_position = end_lsn,
            .options = &.{
                .{ "proto_version", "1" },
                .{ "publication_names", PUB_NAME },
            },
        },
        .dest = destConnConfig(),
        .source_id = SOURCE_ID,
    }) catch |err| {
        std.debug.print("FAIL (init: {})\n", .{err});
        return err;
    };
    defer ingestor.deinit();

    ingestor.run() catch |err| {
        std.debug.print("FAIL (run: {})\n", .{err});
        return err;
    };

    const ingest_ms = timer.readMs();
    const batch_count = try queryCount(allocator, DEST_DB, "SELECT count(*) FROM wal_batches WHERE state='pending'");
    std.debug.print("done ({d}ms, {d} batches)\n", .{ ingest_ms, batch_count });

    if (batch_count == 0) {
        std.debug.print("  FAIL (no batches to process)\n", .{});
        return error.ServerError;
    }

    // Phase 3: Process with concurrent workers
    timer.reset();
    std.debug.print("  Processing with {d} workers... ", .{num_workers});

    const SharedResult = struct {
        batches: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
        errors: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    };

    var results: [4]SharedResult = .{ SharedResult{}, SharedResult{}, SharedResult{}, SharedResult{} };

    const worker = struct {
        fn run(result: *SharedResult, alloc: std.mem.Allocator) void {
            var processor = pgzr.Processor.init(alloc, io, .{
                .dest = destConnConfig(),
            }) catch {
                _ = result.errors.fetchAdd(1, .monotonic);
                return;
            };
            defer processor.deinit();

            while (true) {
                const processed = processor.processOne() catch {
                    _ = result.errors.fetchAdd(1, .monotonic);
                    return;
                };
                if (!processed) break;
                _ = result.batches.fetchAdd(1, .monotonic);
            }
        }
    }.run;

    // Run a zombie reaper concurrently with the workers (issue #14). It
    // aggressively requeues 'processing' batches; the processor must hold the
    // claim's row lock for the whole decode/insert/delete cycle so the reaper
    // can never requeue a live worker's batch and cause double-processing.
    var reaper = Reaper{};
    const reaper_thread = std.Thread.spawn(.{}, Reaper.run, .{ &reaper, allocator }) catch |err| {
        std.debug.print("FAIL (spawn reaper: {})\n", .{err});
        return err;
    };

    var threads: [4]std.Thread = undefined;
    for (0..num_workers) |i| {
        threads[i] = std.Thread.spawn(.{}, worker, .{ &results[i], allocator }) catch |err| {
            std.debug.print("FAIL (spawn thread {d}: {})\n", .{ i, err });
            for (0..i) |j| threads[j].join();
            reaper.stop();
            reaper_thread.join();
            return err;
        };
    }

    for (0..num_workers) |i| threads[i].join();

    reaper.stop();
    reaper_thread.join();

    const process_ms = timer.readMs();

    var total_batches: u32 = 0;
    var total_errors: u32 = 0;
    for (0..num_workers) |i| {
        total_batches += results[i].batches.load(.monotonic);
        total_errors += results[i].errors.load(.monotonic);
    }

    std.debug.print("done ({d}ms)\n", .{process_ms});

    if (total_errors > 0) {
        std.debug.print("  FAIL ({d} processor errors)\n", .{total_errors});
        return error.ServerError;
    }

    // Phase 4: Verify
    std.debug.print("  Verifying... ", .{});

    const remaining = try queryCount(allocator, DEST_DB, "SELECT count(*) FROM wal_batches");
    if (remaining > 0) {
        std.debug.print("FAIL ({d} batches not cleaned up)\n", .{remaining});
        return error.ServerError;
    }

    const event_count = try queryCount(allocator, DEST_DB,
        \\SELECT count(*) FROM events
        \\ WHERE type='I' AND data::text LIKE '%stress_%'
    );
    if (event_count != expected_events) {
        std.debug.print("FAIL (expected {d} events, got {d})\n", .{ expected_events, event_count });
        return error.ServerError;
    }

    const dup_txns = try queryCount(allocator, DEST_DB,
        \\SELECT count(*) FROM (
        \\  SELECT source_id, lsn FROM transactions
        \\  GROUP BY source_id, lsn HAVING count(*) > 1
        \\) dupes
    );
    if (dup_txns > 0) {
        std.debug.print("FAIL ({d} duplicate transactions)\n", .{dup_txns});
        return error.ServerError;
    }

    std.debug.print("OK\n", .{});

    // Summary
    std.debug.print("\n  Results:\n", .{});
    std.debug.print("    Events:  {d}\n", .{event_count});
    std.debug.print("    Batches: {d}\n", .{total_batches});
    std.debug.print("    Workers: ", .{});
    for (0..num_workers) |i| {
        if (i > 0) std.debug.print(", ", .{});
        std.debug.print("p{d}={d}", .{ i, results[i].batches.load(.monotonic) });
    }
    std.debug.print("\n", .{});
    std.debug.print("    Timing:  insert={d}ms ingest={d}ms process={d}ms\n", .{
        insert_ms, ingest_ms, process_ms,
    });
    std.debug.print("    Reaper:  {d} batches requeued\n", .{reaper.requeued.load(.monotonic)});
    std.debug.print("    Duplicates: 0\n", .{});
}
