const std = @import("std");
const pgzr = @import("pgzr");

const SOURCE_DB = "pgzr_stress_source";
const DEST_DB = "pgzr_stress_dest";
const SLOT_NAME = "pgzr_stress_slot";
const PUB_NAME = "pgzr_stress_pub";
const SOURCE_ID = "00000000-0000-0000-0000-000000000002";

fn getUser() []const u8 {
    return std.posix.getenv("PGUSER") orelse std.posix.getenv("USER") orelse "postgres";
}

fn runPsql(allocator: std.mem.Allocator, db: []const u8, sql: []const u8) !void {
    var child = std.process.Child.init(
        &.{ "psql", "-d", db, "-c", sql },
        allocator,
    );
    child.stdin_behavior = .Close;
    child.stdout_behavior = .Ignore;
    child.stderr_behavior = .Ignore;
    _ = try child.spawnAndWait();
}

fn runPsqlCapture(allocator: std.mem.Allocator, db: []const u8, sql: []const u8) ![]u8 {
    var child = std.process.Child.init(
        &.{ "psql", "-d", db, "-t", "-A", "-c", sql },
        allocator,
    );
    child.stdin_behavior = .Close;
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Ignore;
    _ = try child.spawn();

    var stdout_buf: [4096]u8 = undefined;
    var total: usize = 0;
    while (total < stdout_buf.len) {
        const n = child.stdout.?.read(stdout_buf[total..]) catch break;
        if (n == 0) break;
        total += n;
    }
    _ = try child.wait();

    var end = total;
    while (end > 0 and (stdout_buf[end - 1] == '\n' or stdout_buf[end - 1] == '\r' or stdout_buf[end - 1] == ' ')) {
        end -= 1;
    }

    const result = try allocator.alloc(u8, end);
    @memcpy(result, stdout_buf[0..end]);
    return result;
}

fn queryCount(allocator: std.mem.Allocator, db: []const u8, sql: []const u8) !u32 {
    const output = try runPsqlCapture(allocator, db, sql);
    defer allocator.free(output);
    return std.fmt.parseInt(u32, output, 10) catch 0;
}

fn runCmd(allocator: std.mem.Allocator, args: []const []const u8) void {
    var child = std.process.Child.init(args, allocator);
    child.stdin_behavior = .Close;
    child.stdout_behavior = .Ignore;
    child.stderr_behavior = .Ignore;
    _ = child.spawnAndWait() catch {};
}

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

    var dest = pgzr.Connection.connect(allocator, destConnConfig()) catch |err| {
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
    std.Thread.sleep(200 * std.time.ns_per_ms);
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

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        if (gpa.deinit() == .leak) {
            std.debug.print("\nMemory leak detected!\n", .{});
            std.process.exit(1);
        }
    }
    const allocator = gpa.allocator();

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
    var timer = std.time.Timer.start() catch unreachable;

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

    const insert_ms = timer.read() / std.time.ns_per_ms;
    std.debug.print("done ({d}ms)\n", .{insert_ms});

    // Phase 2: Ingest
    const end_lsn = getCurrentWalLsn(allocator) catch |err| {
        std.debug.print("  FAIL (get LSN: {})\n", .{err});
        return err;
    };

    timer.reset();
    std.debug.print("  Ingesting WAL... ", .{});

    var ingestor = pgzr.Ingestor.init(allocator, .{
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

    const ingest_ms = timer.read() / std.time.ns_per_ms;
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
            var processor = pgzr.Processor.init(alloc, .{
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

    var threads: [4]std.Thread = undefined;
    for (0..num_workers) |i| {
        threads[i] = std.Thread.spawn(.{}, worker, .{ &results[i], allocator }) catch |err| {
            std.debug.print("FAIL (spawn thread {d}: {})\n", .{ i, err });
            for (0..i) |j| threads[j].join();
            return err;
        };
    }

    for (0..num_workers) |i| threads[i].join();

    const process_ms = timer.read() / std.time.ns_per_ms;

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
    std.debug.print("    Duplicates: 0\n", .{});
}
