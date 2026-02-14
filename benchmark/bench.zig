const std = @import("std");
const pgzr = @import("pgzr");

const DB_NAME = "pgzr_bench";
const SLOT_NAME = "pgzr_bench_slot";

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

    var buf: [256]u8 = undefined;
    var total: usize = 0;
    while (total < buf.len) {
        const n = child.stdout.?.read(buf[total..]) catch break;
        if (n == 0) break;
        total += n;
    }
    _ = try child.wait();

    var end = total;
    while (end > 0 and (buf[end - 1] == '\n' or buf[end - 1] == '\r' or buf[end - 1] == ' ')) {
        end -= 1;
    }

    const result = try allocator.alloc(u8, end);
    @memcpy(result, buf[0..end]);
    return result;
}

fn setup(allocator: std.mem.Allocator) !void {
    var child = std.process.Child.init(
        &.{ "createdb", DB_NAME },
        allocator,
    );
    child.stdin_behavior = .Close;
    child.stdout_behavior = .Ignore;
    child.stderr_behavior = .Ignore;
    _ = try child.spawnAndWait();

    runPsql(allocator, DB_NAME, "CREATE TABLE IF NOT EXISTS bench (id serial PRIMARY KEY, val text)") catch {};
    runPsql(allocator, DB_NAME, "SELECT pg_drop_replication_slot('" ++ SLOT_NAME ++ "')") catch {};
    runPsql(allocator, DB_NAME, "SELECT pg_create_logical_replication_slot('" ++ SLOT_NAME ++ "', 'test_decoding')") catch {};
}

fn teardown(allocator: std.mem.Allocator) void {
    runPsql(allocator, DB_NAME, "SELECT pg_drop_replication_slot('" ++ SLOT_NAME ++ "')") catch {};
    var child = std.process.Child.init(
        &.{ "dropdb", "--if-exists", "--force", DB_NAME },
        allocator,
    );
    child.stdin_behavior = .Close;
    child.stdout_behavior = .Ignore;
    child.stderr_behavior = .Ignore;
    _ = child.spawnAndWait() catch {};
}

fn insertRows(allocator: std.mem.Allocator, n: usize) !void {
    // Use COPY for fast bulk insert
    var buf: [256]u8 = undefined;
    const sql = std.fmt.bufPrint(&buf, "INSERT INTO bench (val) SELECT 'row_' || g FROM generate_series(0, {d}) g", .{n - 1}) catch unreachable;
    runPsql(allocator, DB_NAME, sql) catch |err| {
        std.debug.print("Insert failed: {}\n", .{err});
        return err;
    };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Parse row count from args
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const row_count: usize = if (args.len > 1)
        std.fmt.parseUnsigned(usize, args[1], 10) catch 10_000
    else
        10_000;

    std.debug.print("pgzr (Zig) Benchmark\n", .{});
    std.debug.print("========================================\n", .{});
    std.debug.print("Rows: {d}\n\n", .{row_count});

    setup(allocator) catch |err| {
        std.debug.print("Setup failed: {}. Is PostgreSQL running?\n", .{err});
        std.process.exit(1);
    };

    insertRows(allocator, row_count) catch |err| {
        std.debug.print("Insert failed: {}\n", .{err});
        teardown(allocator);
        std.process.exit(1);
    };

    // Get end LSN
    const lsn_str = runPsqlCapture(allocator, DB_NAME, "SELECT pg_current_wal_lsn()") catch |err| {
        std.debug.print("Failed to get WAL LSN: {}\n", .{err});
        teardown(allocator);
        std.process.exit(1);
    };
    defer allocator.free(lsn_str);

    const end_lsn = pgzr.Lsn.parse(lsn_str) catch |err| {
        std.debug.print("Failed to parse LSN '{s}': {}\n", .{ lsn_str, err });
        teardown(allocator);
        std.process.exit(1);
    };

    std.debug.print("End LSN: {s}\n", .{lsn_str});

    // Benchmark: consume all messages
    var repl = pgzr.Replicator.init(allocator, .{
        .conn = .{
            .host = "127.0.0.1",
            .port = 5432,
            .user = getUser(),
            .database = DB_NAME,
        },
        .slot_name = SLOT_NAME,
        .end_position = end_lsn,
        .options = &.{
            .{ "include-timestamp", "on" },
        },
    }) catch |err| {
        std.debug.print("Replicator init failed: {}\n", .{err});
        teardown(allocator);
        std.process.exit(1);
    };

    var timer = try std.time.Timer.start();

    var msg_count: u64 = 0;
    while (true) {
        const msg = repl.next() catch break orelse break;
        msg_count += 1;
        repl.ack(msg.wal_start);
    }

    const elapsed_ns = timer.read();
    const elapsed_s: f64 = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;

    repl.deinit();
    teardown(allocator);

    const throughput: f64 = if (elapsed_s > 0) @as(f64, @floatFromInt(msg_count)) / elapsed_s else 0;
    std.debug.print("Messages: {d}\n", .{msg_count});
    std.debug.print("Time: {d:.4} seconds\n", .{elapsed_s});
    std.debug.print("Throughput: {d:.0} messages/second\n\n", .{throughput});
}
