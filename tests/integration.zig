const std = @import("std");
const pgzr = @import("pgzr");

const DB_NAME = "pgzr_integ_test";
const SLOT_NAME = "pgzr_integ_slot";

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

fn setup(allocator: std.mem.Allocator) !void {
    // Create database (ignore errors if it already exists)
    var child = std.process.Child.init(
        &.{ "createdb", DB_NAME },
        allocator,
    );
    child.stdin_behavior = .Close;
    child.stdout_behavior = .Ignore;
    child.stderr_behavior = .Ignore;
    _ = try child.spawnAndWait();

    // Create table
    runPsql(allocator, DB_NAME, "CREATE TABLE IF NOT EXISTS integ_test (id serial PRIMARY KEY, val text)") catch {};

    // Drop slot if it exists, then create it
    runPsql(allocator, DB_NAME, "SELECT pg_drop_replication_slot('" ++ SLOT_NAME ++ "')") catch {};
    try runPsql(allocator, DB_NAME, "SELECT pg_create_logical_replication_slot('" ++ SLOT_NAME ++ "', 'test_decoding')");

    // Drain any existing changes
    try runPsql(allocator, DB_NAME, "SELECT pg_logical_slot_peek_changes('" ++ SLOT_NAME ++ "', NULL, NULL)");
}

fn teardown(allocator: std.mem.Allocator) void {
    runPsql(allocator, DB_NAME, "SELECT pg_drop_replication_slot('" ++ SLOT_NAME ++ "')") catch {};
    var child = std.process.Child.init(
        &.{ "dropdb", DB_NAME },
        allocator,
    );
    child.stdin_behavior = .Close;
    child.stdout_behavior = .Ignore;
    child.stderr_behavior = .Ignore;
    _ = child.spawnAndWait() catch {};
}

fn insertRow(allocator: std.mem.Allocator, val: []const u8) !void {
    var buf: [256]u8 = undefined;
    const sql = std.fmt.bufPrint(&buf, "INSERT INTO integ_test (val) VALUES ('{s}')", .{val}) catch
        return error.OutOfMemory;
    try runPsql(allocator, DB_NAME, sql);
}

fn connConfig() pgzr.ConnConfig {
    return .{
        .host = "127.0.0.1",
        .port = 5432,
        .user = getUser(),
        .database = DB_NAME,
    };
}

// =========================================================================
// Test 1: Basic replication
// =========================================================================
fn testBasicReplication(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: basic replication... ", .{});

    var repl = pgzr.Replicator.init(allocator, .{
        .conn = connConfig(),
        .slot_name = SLOT_NAME,
        .options = &.{
            .{ "include-timestamp", "on" },
        },
    }) catch |err| {
        std.debug.print("FAIL (connect: {})\n", .{err});
        return err;
    };
    defer repl.deinit();

    // Insert a row while streaming
    try insertRow(allocator, "hello");

    // Collect messages until we see BEGIN, the INSERT change, and COMMIT
    var got_begin = false;
    var got_insert = false;
    var got_commit = false;
    var msg_count: u32 = 0;

    while (msg_count < 100) : (msg_count += 1) {
        const msg = repl.next() catch |err| {
            std.debug.print("FAIL (next: {})\n", .{err});
            return err;
        } orelse break;

        const data = msg.data;
        if (std.mem.startsWith(u8, data, "BEGIN")) got_begin = true;
        if (std.mem.indexOf(u8, data, "INSERT") != null) got_insert = true;
        if (std.mem.startsWith(u8, data, "COMMIT")) {
            got_commit = true;
            repl.ack(msg.wal_start);
            break;
        }
        repl.ack(msg.wal_start);
    }

    if (!got_begin or !got_insert or !got_commit) {
        std.debug.print("FAIL (missing messages: begin={} insert={} commit={})\n", .{
            got_begin, got_insert, got_commit,
        });
        return error.ServerError;
    }

    std.debug.print("OK\n", .{});
}

// =========================================================================
// Test 2: start_position
// =========================================================================
fn testStartPosition(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: start_position... ", .{});

    // Phase 1: Stream and record an LSN after the first insert
    var repl1 = pgzr.Replicator.init(allocator, .{
        .conn = connConfig(),
        .slot_name = SLOT_NAME,
        .options = &.{
            .{ "include-timestamp", "on" },
        },
    }) catch |err| {
        std.debug.print("FAIL (connect1: {})\n", .{err});
        return err;
    };

    try insertRow(allocator, "first");
    try insertRow(allocator, "second");

    // Read until we see "first" INSERT's COMMIT, record LSN
    var marker_lsn = pgzr.Lsn.zero;
    var msg_count: u32 = 0;
    var saw_first = false;

    while (msg_count < 200) : (msg_count += 1) {
        const msg = repl1.next() catch break orelse break;
        const data = msg.data;

        if (std.mem.indexOf(u8, data, "'first'") != null) {
            saw_first = true;
        }
        if (saw_first and std.mem.startsWith(u8, data, "COMMIT")) {
            marker_lsn = msg.wal_start;
            repl1.ack(msg.wal_start);
            break;
        }
        repl1.ack(msg.wal_start);
    }

    repl1.deinit();

    if (marker_lsn.value == 0) {
        std.debug.print("FAIL (couldn't find marker LSN)\n", .{});
        return error.ServerError;
    }

    // Phase 2: Start from marker_lsn, should only see "second"
    var repl2 = pgzr.Replicator.init(allocator, .{
        .conn = connConfig(),
        .slot_name = SLOT_NAME,
        .start_position = marker_lsn,
        .options = &.{
            .{ "include-timestamp", "on" },
        },
    }) catch |err| {
        std.debug.print("FAIL (connect2: {})\n", .{err});
        return err;
    };
    defer repl2.deinit();

    var saw_second = false;
    var saw_first_again = false;
    msg_count = 0;

    while (msg_count < 200) : (msg_count += 1) {
        const msg = repl2.next() catch break orelse break;
        const data = msg.data;

        if (std.mem.indexOf(u8, data, "'first'") != null) saw_first_again = true;
        if (std.mem.indexOf(u8, data, "'second'") != null) saw_second = true;
        if (saw_second and std.mem.startsWith(u8, data, "COMMIT")) {
            repl2.ack(msg.wal_start);
            break;
        }
        repl2.ack(msg.wal_start);
    }

    if (saw_first_again) {
        std.debug.print("FAIL (saw 'first' after start_position)\n", .{});
        return error.ServerError;
    }
    if (!saw_second) {
        std.debug.print("FAIL (didn't see 'second')\n", .{});
        return error.ServerError;
    }

    std.debug.print("OK\n", .{});
}

// =========================================================================
// Test 3: end_position
// =========================================================================
fn testEndPosition(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: end_position... ", .{});

    // Insert rows and get the current WAL position
    try insertRow(allocator, "end_test_1");
    try insertRow(allocator, "end_test_2");
    try insertRow(allocator, "end_test_3");

    // Start replication and find the LSN of end_test_2's COMMIT
    var repl1 = pgzr.Replicator.init(allocator, .{
        .conn = connConfig(),
        .slot_name = SLOT_NAME,
        .options = &.{
            .{ "include-timestamp", "on" },
        },
    }) catch |err| {
        std.debug.print("FAIL (connect1: {})\n", .{err});
        return err;
    };

    var end_lsn = pgzr.Lsn.zero;
    var msg_count: u32 = 0;
    var saw_end_test_2 = false;

    while (msg_count < 200) : (msg_count += 1) {
        const msg = repl1.next() catch break orelse break;
        const data = msg.data;

        if (std.mem.indexOf(u8, data, "'end_test_2'") != null) {
            saw_end_test_2 = true;
        }
        if (saw_end_test_2 and std.mem.startsWith(u8, data, "COMMIT")) {
            end_lsn = msg.wal_start;
            repl1.ack(msg.wal_start);
            // Keep consuming to get end_test_3
            while (msg_count < 300) : (msg_count += 1) {
                const msg2 = repl1.next() catch break orelse break;
                if (std.mem.startsWith(u8, msg2.data, "COMMIT")) {
                    repl1.ack(msg2.wal_start);
                    break;
                }
                repl1.ack(msg2.wal_start);
            }
            break;
        }
        repl1.ack(msg.wal_start);
    }

    repl1.deinit();

    if (end_lsn.value == 0) {
        std.debug.print("FAIL (couldn't find end LSN)\n", .{});
        return error.ServerError;
    }

    // Now re-stream with end_position set to the end_test_2 COMMIT LSN.
    // We need to re-create the slot to replay.
    runPsql(allocator, DB_NAME, "SELECT pg_drop_replication_slot('" ++ SLOT_NAME ++ "')") catch {};
    runPsql(allocator, DB_NAME, "SELECT pg_create_logical_replication_slot('" ++ SLOT_NAME ++ "', 'test_decoding')") catch {
        std.debug.print("FAIL (recreate slot)\n", .{});
        return error.ServerError;
    };

    // Re-insert so there's data in the new slot
    try insertRow(allocator, "after_end_1");
    try insertRow(allocator, "after_end_2");

    var repl2 = pgzr.Replicator.init(allocator, .{
        .conn = connConfig(),
        .slot_name = SLOT_NAME,
        .end_position = end_lsn,
        .options = &.{
            .{ "include-timestamp", "on" },
        },
    }) catch |err| {
        std.debug.print("FAIL (connect2: {})\n", .{err});
        return err;
    };
    defer repl2.deinit();

    var saw_after_end_2 = false;
    msg_count = 0;

    while (msg_count < 200) : (msg_count += 1) {
        const msg = repl2.next() catch break orelse {
            // null = end_position reached
            break;
        };
        const data = msg.data;

        if (std.mem.indexOf(u8, data, "'after_end_2'") != null) {
            saw_after_end_2 = true;
        }
        repl2.ack(msg.wal_start);
    }

    // If end_position works, we should NOT have seen after_end_2
    // (it was inserted after end_lsn, so its WAL position is past it)
    if (saw_after_end_2) {
        std.debug.print("FAIL (saw data past end_position)\n", .{});
        return error.ServerError;
    }

    std.debug.print("OK\n", .{});
}

// =========================================================================
// Main
// =========================================================================
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("PGZR Integration Tests\n", .{});
    std.debug.print("======================\n", .{});

    // Setup
    std.debug.print("Setting up test database...\n", .{});
    setup(allocator) catch |err| {
        std.debug.print("Setup failed: {}. Is PostgreSQL running?\n", .{err});
        std.process.exit(1);
    };

    var failures: u32 = 0;

    // Run tests
    testBasicReplication(allocator) catch {
        failures += 1;
    };
    testStartPosition(allocator) catch {
        failures += 1;
    };
    testEndPosition(allocator) catch {
        failures += 1;
    };

    // Teardown
    std.debug.print("Cleaning up...\n", .{});
    teardown(allocator);

    if (failures > 0) {
        std.debug.print("\n{d} test(s) FAILED\n", .{failures});
        std.process.exit(1);
    } else {
        std.debug.print("\nAll tests passed!\n", .{});
    }
}
