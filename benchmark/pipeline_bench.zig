const std = @import("std");
const pgzr = @import("pgzr");

/// Io instance for the whole run, set at the top of main.
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

    fn read(self: *const Stopwatch) u64 {
        return @intCast(self.start_ts.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds());
    }
};

const SOURCE_DB = "pgzr_bench_source";
const DEST_DB = "pgzr_bench_dest";
const SLOT_NAME = "pgzr_bench_slot";
const PUB_NAME = "pgzr_bench_pub";
const SOURCE_ID = "00000000-0000-0000-0000-000000000001";

fn getUser() []const u8 {
    return getenv("PGUSER") orelse getenv("USER") orelse "postgres";
}

fn runPsql(allocator: std.mem.Allocator, db: []const u8, sql: []const u8) !void {
    const result = try std.process.run(allocator, io, .{
        .argv = &.{ "psql", "-d", db, "-c", sql },
    });
    allocator.free(result.stdout);
    allocator.free(result.stderr);
    if (result.term != .exited or result.term.exited != 0) return error.CommandFailed;
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

fn runCmd(allocator: std.mem.Allocator, args: []const []const u8) void {
    const result = std.process.run(allocator, io, .{ .argv = args }) catch return;
    allocator.free(result.stdout);
    allocator.free(result.stderr);
}

fn setup(allocator: std.mem.Allocator) !void {
    runCmd(allocator, &.{ "createdb", SOURCE_DB });
    runCmd(allocator, &.{ "createdb", DEST_DB });

    // Create a wide table with 20 columns to highlight write amplification
    runPsql(allocator, SOURCE_DB,
        \\CREATE TABLE IF NOT EXISTS wide_table (
        \\    id SERIAL PRIMARY KEY,
        \\    col_01 TEXT NOT NULL DEFAULT 'value_01',
        \\    col_02 TEXT NOT NULL DEFAULT 'value_02',
        \\    col_03 TEXT NOT NULL DEFAULT 'value_03',
        \\    col_04 TEXT NOT NULL DEFAULT 'value_04',
        \\    col_05 TEXT NOT NULL DEFAULT 'value_05',
        \\    col_06 TEXT NOT NULL DEFAULT 'value_06',
        \\    col_07 TEXT NOT NULL DEFAULT 'value_07',
        \\    col_08 TEXT NOT NULL DEFAULT 'value_08',
        \\    col_09 TEXT NOT NULL DEFAULT 'value_09',
        \\    col_10 TEXT NOT NULL DEFAULT 'value_10',
        \\    col_11 TEXT NOT NULL DEFAULT 'value_11',
        \\    col_12 TEXT NOT NULL DEFAULT 'value_12',
        \\    col_13 TEXT NOT NULL DEFAULT 'value_13',
        \\    col_14 TEXT NOT NULL DEFAULT 'value_14',
        \\    col_15 TEXT NOT NULL DEFAULT 'value_15',
        \\    col_16 TEXT NOT NULL DEFAULT 'value_16',
        \\    col_17 TEXT NOT NULL DEFAULT 'value_17',
        \\    col_18 TEXT NOT NULL DEFAULT 'value_18',
        \\    col_19 TEXT NOT NULL DEFAULT 'value_19',
        \\    col_20 TEXT NOT NULL DEFAULT 'value_20'
        \\)
    ) catch {};

    runPsql(allocator, SOURCE_DB, "ALTER TABLE wide_table REPLICA IDENTITY FULL") catch {};
    runPsql(allocator, SOURCE_DB, "DROP PUBLICATION IF EXISTS " ++ PUB_NAME) catch {};
    try runPsql(allocator, SOURCE_DB, "CREATE PUBLICATION " ++ PUB_NAME ++ " FOR TABLE wide_table");
    runPsql(allocator, SOURCE_DB, "SELECT pg_drop_replication_slot('" ++ SLOT_NAME ++ "')") catch {};
    try runPsql(allocator, SOURCE_DB, "SELECT pg_create_logical_replication_slot('" ++ SLOT_NAME ++ "', 'pgoutput')");
    runPsql(allocator, SOURCE_DB, "TRUNCATE wide_table RESTART IDENTITY") catch {};

    // Set up dest schema
    var dest = try pgzr.Connection.connect(allocator, io, .{
        .host = "127.0.0.1",
        .port = 5432,
        .user = getUser(),
        .database = DEST_DB,
        .replication = false,
    });
    defer dest.close();
    try pgzr.schema.ensureSchema(&dest);
}

fn teardown(allocator: std.mem.Allocator) void {
    sleepMs(200);
    runPsql(allocator, SOURCE_DB, "SELECT pg_drop_replication_slot('" ++ SLOT_NAME ++ "')") catch {};
    runPsql(allocator, SOURCE_DB, "DROP PUBLICATION IF EXISTS " ++ PUB_NAME) catch {};
    runCmd(allocator, &.{ "dropdb", "--if-exists", "--force", SOURCE_DB });
    runCmd(allocator, &.{ "dropdb", "--if-exists", "--force", DEST_DB });
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

const CommandFailed = error{CommandFailed};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    io = init.io;

    const args = init.minimal.args.vector;
    const row_count: usize = if (args.len > 1)
        std.fmt.parseUnsigned(usize, std.mem.span(args[1]), 10) catch 1_000
    else
        1_000;

    std.debug.print("PGZR Pipeline Benchmark (20-column wide table)\n", .{});
    std.debug.print("================================================\n", .{});
    std.debug.print("Rows: {d}\n\n", .{row_count});

    // Setup
    std.debug.print("Setting up...\n", .{});
    setup(allocator) catch |err| {
        std.debug.print("Setup failed: {}. Is PostgreSQL running?\n", .{err});
        std.process.exit(1);
    };

    // Phase 1: Insert rows into source
    std.debug.print("Inserting {d} rows into source...\n", .{row_count});
    var buf: [256]u8 = undefined;
    const insert_sql = std.fmt.bufPrint(&buf, "INSERT INTO wide_table (col_01) SELECT 'row_' || g FROM generate_series(1, {d}) g", .{row_count}) catch unreachable;
    runPsql(allocator, SOURCE_DB, insert_sql) catch |err| {
        std.debug.print("Insert failed: {}\n", .{err});
        teardown(allocator);
        std.process.exit(1);
    };

    // Update 20% of rows
    const update_count = row_count / 5;
    if (update_count > 0) {
        std.debug.print("Updating {d} rows...\n", .{update_count});
        const update_sql = std.fmt.bufPrint(&buf, "UPDATE wide_table SET col_01 = 'updated_' || id WHERE id <= {d}", .{update_count}) catch unreachable;
        runPsql(allocator, SOURCE_DB, update_sql) catch {};
    }

    // Delete 10% of rows
    const delete_count = row_count / 10;
    if (delete_count > 0) {
        std.debug.print("Deleting {d} rows...\n", .{delete_count});
        const delete_sql = std.fmt.bufPrint(&buf, "DELETE FROM wide_table WHERE id > {d}", .{row_count - delete_count}) catch unreachable;
        runPsql(allocator, SOURCE_DB, delete_sql) catch {};
    }

    const total_events = row_count + update_count + delete_count;
    std.debug.print("Total DML events: {d} ({d} inserts + {d} updates + {d} deletes)\n\n", .{ total_events, row_count, update_count, delete_count });

    // Get end LSN
    const end_lsn_str = runPsqlCapture(allocator, SOURCE_DB, "SELECT pg_current_wal_lsn()") catch |err| {
        std.debug.print("Failed to get WAL LSN: {}\n", .{err});
        teardown(allocator);
        std.process.exit(1);
    };
    defer allocator.free(end_lsn_str);

    const end_lsn = pgzr.Lsn.parse(end_lsn_str) catch |err| {
        std.debug.print("Failed to parse LSN: {}\n", .{err});
        teardown(allocator);
        std.process.exit(1);
    };

    // Phase 2: Ingest WAL
    std.debug.print("Phase 1: Ingest WAL...\n", .{});
    var ingest_timer = Stopwatch.start();

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
        std.debug.print("Ingestor init failed: {}\n", .{err});
        teardown(allocator);
        std.process.exit(1);
    };
    defer ingestor.deinit();

    ingestor.run() catch |err| {
        std.debug.print("Ingestor run failed: {}\n", .{err});
        teardown(allocator);
        std.process.exit(1);
    };

    const ingest_ns = ingest_timer.read();
    const ingest_s: f64 = @as(f64, @floatFromInt(ingest_ns)) / 1_000_000_000.0;
    std.debug.print("  Ingest time: {d:.4}s\n\n", .{ingest_s});

    // Phase 3: Process batches
    std.debug.print("Phase 2: Process batches...\n", .{});
    var process_timer = Stopwatch.start();

    var processor = pgzr.Processor.init(allocator, io, .{
        .dest = destConnConfig(),
    }) catch |err| {
        std.debug.print("Processor init failed: {}\n", .{err});
        teardown(allocator);
        std.process.exit(1);
    };
    defer processor.deinit();

    var batch_count: u32 = 0;
    while (true) {
        const processed = processor.processOne() catch |err| {
            std.debug.print("Process failed: {}\n", .{err});
            teardown(allocator);
            std.process.exit(1);
        };
        if (!processed) break;
        batch_count += 1;
    }

    const process_ns = process_timer.read();
    const process_s: f64 = @as(f64, @floatFromInt(process_ns)) / 1_000_000_000.0;

    // Phase 4: Query dest for stats
    const txn_count_str = runPsqlCapture(allocator, DEST_DB, "SELECT count(*) FROM transactions") catch "?";
    defer if (!std.mem.eql(u8, txn_count_str, "?")) allocator.free(txn_count_str);

    const event_count_str = runPsqlCapture(allocator, DEST_DB, "SELECT count(*) FROM events") catch "?";
    defer if (!std.mem.eql(u8, event_count_str, "?")) allocator.free(event_count_str);

    const table_sizes = runPsqlCapture(allocator, DEST_DB,
        \\SELECT
        \\  'transactions: ' || pg_size_pretty(pg_total_relation_size('transactions')) ||
        \\  ', events: ' || pg_size_pretty(pg_total_relation_size('events')) ||
        \\  CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'columns')
        \\       THEN ', columns: ' || pg_size_pretty(pg_total_relation_size('columns'))
        \\       ELSE '' END
    ) catch "?";
    defer if (!std.mem.eql(u8, table_sizes, "?")) allocator.free(table_sizes);

    // Results
    std.debug.print("  Process time: {d:.4}s\n", .{process_s});
    std.debug.print("  Batches processed: {d}\n\n", .{batch_count});

    const total_s = ingest_s + process_s;
    const events_per_sec: f64 = if (process_s > 0) @as(f64, @floatFromInt(total_events)) / process_s else 0;

    std.debug.print("Results\n", .{});
    std.debug.print("-------\n", .{});
    std.debug.print("  Transactions: {s}\n", .{txn_count_str});
    std.debug.print("  Events: {s}\n", .{event_count_str});
    std.debug.print("  Total time: {d:.4}s (ingest {d:.4}s + process {d:.4}s)\n", .{ total_s, ingest_s, process_s });
    std.debug.print("  Process throughput: {d:.0} events/s\n", .{events_per_sec});
    std.debug.print("  Table sizes: {s}\n", .{table_sizes});

    teardown(allocator);
}
