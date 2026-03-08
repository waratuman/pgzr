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

/// Run a psql command and capture stdout output.
fn runPsqlCapture(allocator: std.mem.Allocator, db: []const u8, sql: []const u8) ![]u8 {
    var child = std.process.Child.init(
        &.{ "psql", "-d", db, "-t", "-A", "-c", sql },
        allocator,
    );
    child.stdin_behavior = .Close;
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Ignore;
    _ = try child.spawn();

    var stdout_buf: [256]u8 = undefined;
    var total: usize = 0;
    while (total < stdout_buf.len) {
        const n = child.stdout.?.read(stdout_buf[total..]) catch break;
        if (n == 0) break;
        total += n;
    }
    _ = try child.wait();

    // Trim trailing whitespace/newline
    var end = total;
    while (end > 0 and (stdout_buf[end - 1] == '\n' or stdout_buf[end - 1] == '\r' or stdout_buf[end - 1] == ' ')) {
        end -= 1;
    }

    const result = try allocator.alloc(u8, end);
    @memcpy(result, stdout_buf[0..end]);
    return result;
}

/// Get the current WAL insert LSN from PostgreSQL.
fn getCurrentWalInsertLsn(allocator: std.mem.Allocator) !pgzr.Lsn {
    const output = try runPsqlCapture(allocator, DB_NAME, "SELECT pg_current_wal_insert_lsn()");
    defer allocator.free(output);
    return pgzr.Lsn.parse(output);
}

/// Get the current WAL LSN from PostgreSQL.
fn getCurrentWalLsn(allocator: std.mem.Allocator) !pgzr.Lsn {
    const output = try runPsqlCapture(allocator, DB_NAME, "SELECT pg_current_wal_lsn()");
    defer allocator.free(output);
    return pgzr.Lsn.parse(output);
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

    // Create tables before the slot so DDL doesn't appear in the stream
    runPsql(allocator, DB_NAME, "CREATE TABLE IF NOT EXISTS integ_test (id serial PRIMARY KEY, val text)") catch {};
    runPsql(allocator, DB_NAME, "CREATE TABLE IF NOT EXISTS teas (kind TEXT)") catch {};

    // Drop slot if it exists, then create it
    runPsql(allocator, DB_NAME, "SELECT pg_drop_replication_slot('" ++ SLOT_NAME ++ "')") catch {};
    try runPsql(allocator, DB_NAME, "SELECT pg_create_logical_replication_slot('" ++ SLOT_NAME ++ "', 'test_decoding')");

    // Drain any existing changes
    try runPsql(allocator, DB_NAME, "SELECT pg_logical_slot_peek_changes('" ++ SLOT_NAME ++ "', NULL, NULL)");
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

/// Re-create the slot (drop + create) and truncate test tables.
/// Useful between tests that consume the slot and need a fresh starting point.
fn resetSlot(allocator: std.mem.Allocator) void {
    // Brief pause to allow PostgreSQL to fully release the slot
    std.Thread.sleep(200 * std.time.ns_per_ms);
    runPsql(allocator, DB_NAME, "SELECT pg_drop_replication_slot('" ++ SLOT_NAME ++ "')") catch {};
    runPsql(allocator, DB_NAME, "TRUNCATE teas, integ_test") catch {};
    runPsql(allocator, DB_NAME, "SELECT pg_create_logical_replication_slot('" ++ SLOT_NAME ++ "', 'test_decoding')") catch {};
}

fn insertRow(allocator: std.mem.Allocator, val: []const u8) !void {
    var buf: [512]u8 = undefined;
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
// Test 1: Basic replication (ported from test_replication)
// =========================================================================
fn testBasicReplication(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: basic replication... ", .{});

    // Insert 3 rows with Japanese text (matching Ruby test_replication)
    try runPsql(allocator, DB_NAME,
        \\INSERT INTO teas VALUES ('煎茶'), ('蕎麦茶'), ('魔茶')
    );

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

    // Verify LSNs are 0 before replication starts
    if (repl.last_server_lsn.value != 0 or
        repl.last_received_lsn.value != 0 or
        repl.last_processed_lsn.value != 0)
    {
        std.debug.print("FAIL (LSNs not zero before start)\n", .{});
        return error.ServerError;
    }

    // Collect messages until we see the INSERT transaction's COMMIT
    var results: [32][]const u8 = undefined;
    var result_count: usize = 0;
    var msg_count: u32 = 0;
    var insert_commit_seen = false;

    while (msg_count < 200) : (msg_count += 1) {
        const msg = repl.next() catch |err| {
            std.debug.print("FAIL (next: {})\n", .{err});
            return err;
        } orelse break;

        if (result_count < results.len) {
            const copy = allocator.alloc(u8, msg.data.len) catch {
                std.debug.print("FAIL (alloc)\n", .{});
                return error.ServerError;
            };
            @memcpy(copy, msg.data);
            results[result_count] = copy;
            result_count += 1;
        }

        repl.ack(msg.wal_start);

        // Stop after we see a COMMIT that follows an INSERT
        if (std.mem.startsWith(u8, msg.data, "COMMIT") and insert_commit_seen) break;
        if (std.mem.indexOf(u8, msg.data, "INSERT") != null) insert_commit_seen = true;
    }

    defer for (results[0..result_count]) |r| allocator.free(r);

    // Search for patterns across all collected messages
    var got_begin = false;
    var got_commit = false;
    const teas = [_][]const u8{ "煎茶", "蕎麦茶", "魔茶" };
    var tea_found = [_]bool{ false, false, false };

    for (results[0..result_count]) |r| {
        if (std.mem.startsWith(u8, r, "BEGIN")) got_begin = true;
        if (std.mem.startsWith(u8, r, "COMMIT")) got_commit = true;
        for (teas, 0..) |tea, i| {
            if (std.mem.indexOf(u8, r, tea) != null) tea_found[i] = true;
        }
    }

    if (!got_begin) {
        std.debug.print("FAIL (no BEGIN found)\n", .{});
        return error.ServerError;
    }
    if (!got_commit) {
        std.debug.print("FAIL (no COMMIT found)\n", .{});
        return error.ServerError;
    }
    for (teas, 0..) |tea, i| {
        if (!tea_found[i]) {
            std.debug.print("FAIL (missing tea '{s}')\n", .{tea});
            return error.ServerError;
        }
    }

    std.debug.print("OK\n", .{});
}

// =========================================================================
// Test 2: start_position (ported from test_replication_with_startpos)
// =========================================================================
fn testStartPosition(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: start_position... ", .{});

    // Insert first batch
    try runPsql(allocator, DB_NAME,
        \\INSERT INTO teas VALUES ('煎茶'), ('蕎麦茶'), ('魔茶')
    );

    // Get WAL insert LSN after first batch
    const startpos = getCurrentWalInsertLsn(allocator) catch |err| {
        std.debug.print("FAIL (get start LSN: {})\n", .{err});
        return err;
    };

    // Insert second batch
    try runPsql(allocator, DB_NAME, "INSERT INTO teas (kind) VALUES ('ハーブティー')");

    // Get WAL LSN after second batch (for end_position)
    const endpos = getCurrentWalLsn(allocator) catch |err| {
        std.debug.print("FAIL (get end LSN: {})\n", .{err});
        return err;
    };

    var repl = pgzr.Replicator.init(allocator, .{
        .conn = connConfig(),
        .slot_name = SLOT_NAME,
        .start_position = startpos,
        .end_position = endpos,
        .options = &.{
            .{ "include-timestamp", "on" },
        },
    }) catch |err| {
        std.debug.print("FAIL (connect: {})\n", .{err});
        return err;
    };
    defer repl.deinit();

    // Verify LSNs are 0 before replication starts
    if (repl.last_server_lsn.value != 0 or
        repl.last_received_lsn.value != 0 or
        repl.last_processed_lsn.value != 0)
    {
        std.debug.print("FAIL (LSNs not zero before start)\n", .{});
        return error.ServerError;
    }

    var results_buf: [16][]u8 = undefined;
    var result_count: usize = 0;
    var msg_count: u32 = 0;

    while (msg_count < 200) : (msg_count += 1) {
        const msg = repl.next() catch break orelse break;

        if (result_count < results_buf.len) {
            const copy = allocator.alloc(u8, msg.data.len) catch break;
            @memcpy(copy, msg.data);
            results_buf[result_count] = copy;
            result_count += 1;
        }

        repl.ack(msg.wal_start);
    }

    defer for (results_buf[0..result_count]) |r| allocator.free(r);
    const results = results_buf[0..result_count];

    // Verify we got the second batch (BEGIN, INSERT ハーブティー, COMMIT)
    if (result_count < 3) {
        std.debug.print("FAIL (only got {d} messages, expected at least 3)\n", .{result_count});
        return error.ServerError;
    }

    // Verify BEGIN
    if (!std.mem.startsWith(u8, results[0], "BEGIN")) {
        std.debug.print("FAIL (first message not BEGIN)\n", .{});
        return error.ServerError;
    }

    // Verify ハーブティー is present
    var found_herb_tea = false;
    for (results) |r| {
        if (std.mem.indexOf(u8, r, "ハーブティー") != null) {
            found_herb_tea = true;
            break;
        }
    }
    if (!found_herb_tea) {
        std.debug.print("FAIL (didn't find ハーブティー)\n", .{});
        return error.ServerError;
    }

    // Verify first batch teas are NOT present
    const excluded_teas = [_][]const u8{ "煎茶", "蕎麦茶", "魔茶" };
    for (excluded_teas) |tea| {
        for (results) |r| {
            if (std.mem.indexOf(u8, r, tea) != null) {
                std.debug.print("FAIL (found excluded tea '{s}')\n", .{tea});
                return error.ServerError;
            }
        }
    }

    // Verify COMMIT at end
    if (!std.mem.startsWith(u8, results[result_count - 1], "COMMIT")) {
        std.debug.print("FAIL (last message not COMMIT)\n", .{});
        return error.ServerError;
    }

    std.debug.print("OK\n", .{});
}

// =========================================================================
// Test 3: end_position (ported from test_replication_with_endpos)
// =========================================================================
fn testEndPosition(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: end_position... ", .{});

    // Insert first batch
    try runPsql(allocator, DB_NAME,
        \\INSERT INTO teas VALUES ('煎茶'), ('蕎麦茶'), ('魔茶')
    );

    // Get WAL insert LSN after first batch (this will be our end position)
    const endpos = getCurrentWalInsertLsn(allocator) catch |err| {
        std.debug.print("FAIL (get end LSN: {})\n", .{err});
        return err;
    };

    // Insert second batch (should be excluded)
    try runPsql(allocator, DB_NAME, "INSERT INTO teas (kind) VALUES ('ハーブティー')");

    var repl = pgzr.Replicator.init(allocator, .{
        .conn = connConfig(),
        .slot_name = SLOT_NAME,
        .end_position = endpos,
        .options = &.{
            .{ "include-timestamp", "on" },
        },
    }) catch |err| {
        std.debug.print("FAIL (connect: {})\n", .{err});
        return err;
    };
    defer repl.deinit();

    // Verify LSNs are 0 before start
    if (repl.last_server_lsn.value != 0 or
        repl.last_received_lsn.value != 0 or
        repl.last_processed_lsn.value != 0)
    {
        std.debug.print("FAIL (LSNs not zero before start)\n", .{});
        return error.ServerError;
    }

    var results_buf: [16][]u8 = undefined;
    var result_count: usize = 0;
    var msg_count: u32 = 0;

    while (msg_count < 200) : (msg_count += 1) {
        const msg = repl.next() catch break orelse break;

        if (result_count < results_buf.len) {
            const copy = allocator.alloc(u8, msg.data.len) catch break;
            @memcpy(copy, msg.data);
            results_buf[result_count] = copy;
            result_count += 1;
        }

        repl.ack(msg.wal_start);
    }

    defer for (results_buf[0..result_count]) |r| allocator.free(r);
    const results = results_buf[0..result_count];

    // Verify first batch is present
    if (result_count < 5) {
        std.debug.print("FAIL (only got {d} messages, expected at least 5)\n", .{result_count});
        return error.ServerError;
    }

    if (!std.mem.startsWith(u8, results[0], "BEGIN")) {
        std.debug.print("FAIL (first message not BEGIN)\n", .{});
        return error.ServerError;
    }

    const expected_teas = [_][]const u8{ "煎茶", "蕎麦茶", "魔茶" };
    for (expected_teas) |tea| {
        var found = false;
        for (results) |r| {
            if (std.mem.indexOf(u8, r, tea) != null) {
                found = true;
                break;
            }
        }
        if (!found) {
            std.debug.print("FAIL (missing expected tea '{s}')\n", .{tea});
            return error.ServerError;
        }
    }

    // Verify second batch (ハーブティー) is NOT present
    for (results) |r| {
        if (std.mem.indexOf(u8, r, "ハーブティー") != null) {
            std.debug.print("FAIL (found excluded tea ハーブティー past end_position)\n", .{});
            return error.ServerError;
        }
    }

    std.debug.print("OK\n", .{});
}

// =========================================================================
// Test 4: last_server_lsn (ported from test_last_server_lsn)
// =========================================================================
fn testLastServerLsn(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: last_server_lsn... ", .{});

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

    // LSN should be 0 before receiving any messages
    if (repl.last_server_lsn.value != 0 or
        repl.last_received_lsn.value != 0 or
        repl.last_processed_lsn.value != 0)
    {
        std.debug.print("FAIL (LSNs not zero before start)\n", .{});
        return error.ServerError;
    }

    // Insert data so there's something to stream
    try runPsql(allocator, DB_NAME,
        \\INSERT INTO teas VALUES ('煎茶'), ('蕎麦茶'), ('魔茶')
    );

    // Read until COMMIT
    var msg_count: u32 = 0;
    while (msg_count < 200) : (msg_count += 1) {
        const msg = repl.next() catch break orelse break;
        repl.ack(msg.wal_start);
        if (std.mem.startsWith(u8, msg.data, "COMMIT")) break;
    }

    // After receiving messages, last_server_lsn should be non-zero
    if (repl.last_server_lsn.value == 0) {
        std.debug.print("FAIL (last_server_lsn still zero after messages)\n", .{});
        return error.ServerError;
    }

    // last_received_lsn should also be non-zero
    if (repl.last_received_lsn.value == 0) {
        std.debug.print("FAIL (last_received_lsn still zero after messages)\n", .{});
        return error.ServerError;
    }

    // last_processed_lsn should be non-zero after ack
    if (repl.last_processed_lsn.value == 0) {
        std.debug.print("FAIL (last_processed_lsn still zero after ack)\n", .{});
        return error.ServerError;
    }

    std.debug.print("OK\n", .{});
}

// =========================================================================
// Test 5: Timeline mismatch (ported from test_timeline)
// =========================================================================
fn testTimelineMismatch(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: timeline mismatch... ", .{});

    const result = pgzr.Replicator.init(allocator, .{
        .conn = connConfig(),
        .slot_name = SLOT_NAME,
        .expected_timeline = 2, // Server is timeline 1
        .options = &.{
            .{ "include-timestamp", "on" },
        },
    });

    if (result) |*repl| {
        var r = repl.*;
        r.deinit();
        std.debug.print("FAIL (expected TimelineMismatch error, got success)\n", .{});
        return error.ServerError;
    } else |err| {
        if (err != error.TimelineMismatch) {
            std.debug.print("FAIL (expected TimelineMismatch, got {})\n", .{err});
            return error.ServerError;
        }
    }

    std.debug.print("OK\n", .{});
}

// =========================================================================
// Test 6: SystemId mismatch (ported from test_systemid)
// =========================================================================
fn testSystemIdMismatch(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: systemid mismatch... ", .{});

    const result = pgzr.Replicator.init(allocator, .{
        .conn = connConfig(),
        .slot_name = SLOT_NAME,
        .expected_systemid = "2", // Bogus system ID
        .options = &.{
            .{ "include-timestamp", "on" },
        },
    });

    if (result) |*repl| {
        var r = repl.*;
        r.deinit();
        std.debug.print("FAIL (expected SystemIdMismatch error, got success)\n", .{});
        return error.ServerError;
    } else |err| {
        if (err != error.SystemIdMismatch) {
            std.debug.print("FAIL (expected SystemIdMismatch, got {})\n", .{err});
            return error.ServerError;
        }
    }

    std.debug.print("OK\n", .{});
}

// =========================================================================
// Test 7: start_position formats (ported from test_start_position)
// =========================================================================
fn testStartPositionFormats(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: start_position formats... ", .{});

    // Test with integer value 2
    {
        var repl = pgzr.Replicator.init(allocator, .{
            .conn = connConfig(),
            .slot_name = SLOT_NAME,
            .start_position = pgzr.Lsn{ .value = 2 },
            .options = &.{
                .{ "include-timestamp", "on" },
            },
        }) catch |err| {
            std.debug.print("FAIL (start_position=2: {})\n", .{err});
            return err;
        };
        repl.deinit();
    }

    // Test with zero
    {
        var repl = pgzr.Replicator.init(allocator, .{
            .conn = connConfig(),
            .slot_name = SLOT_NAME,
            .start_position = pgzr.Lsn.zero,
            .options = &.{
                .{ "include-timestamp", "on" },
            },
        }) catch |err| {
            std.debug.print("FAIL (start_position=0/0: {})\n", .{err});
            return err;
        };
        repl.deinit();
    }

    // Test with max value (FFFFFFFF/FFFFFFFF)
    {
        var repl = pgzr.Replicator.init(allocator, .{
            .conn = connConfig(),
            .slot_name = SLOT_NAME,
            .start_position = pgzr.Lsn{ .value = std.math.maxInt(u64) },
            .options = &.{
                .{ "include-timestamp", "on" },
            },
        }) catch |err| {
            std.debug.print("FAIL (start_position=max: {})\n", .{err});
            return err;
        };
        repl.deinit();
    }

    std.debug.print("OK\n", .{});
}

// =========================================================================
// Test 8: Feedback (ported from test_feedback_callback)
// =========================================================================
fn testFeedback(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: feedback... ", .{});

    // Insert data first
    try runPsql(allocator, DB_NAME, "INSERT INTO teas (kind) VALUES ('煎茶')");

    var repl = pgzr.Replicator.init(allocator, .{
        .conn = connConfig(),
        .slot_name = SLOT_NAME,
        .status_interval_ms = 100, // Short interval to trigger feedback quickly
        .options = &.{
            .{ "include-timestamp", "on" },
        },
    }) catch |err| {
        std.debug.print("FAIL (connect: {})\n", .{err});
        return err;
    };
    defer repl.deinit();

    // Process messages; the short status_interval_ms should cause
    // feedback to be sent during the loop without errors
    var msg_count: u32 = 0;
    while (msg_count < 200) : (msg_count += 1) {
        const msg = repl.next() catch |err| {
            std.debug.print("FAIL (next: {})\n", .{err});
            return err;
        } orelse break;

        repl.ack(msg.wal_start);
        if (std.mem.startsWith(u8, msg.data, "COMMIT")) break;
    }

    // Verify last_processed_lsn advances after ack
    if (repl.last_processed_lsn.value == 0) {
        std.debug.print("FAIL (last_processed_lsn still zero after ack)\n", .{});
        return error.ServerError;
    }

    // Explicitly send a status update to verify it works
    repl.sendStatus() catch |err| {
        std.debug.print("FAIL (sendStatus: {})\n", .{err});
        return err;
    };

    std.debug.print("OK\n", .{});
}

// =========================================================================
// Test 9: Stop (ported from test_stop)
// =========================================================================
fn testStop(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: stop... ", .{});

    try runPsql(allocator, DB_NAME, "INSERT INTO teas (kind) VALUES ('煎茶')");

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

    // Verify stop is not requested initially
    if (repl.isStopRequested()) {
        std.debug.print("FAIL (stop_requested before stop)\n", .{});
        return error.ServerError;
    }

    // Read some messages first
    var got_any = false;
    var msg_count: u32 = 0;
    while (msg_count < 200) : (msg_count += 1) {
        const msg = repl.next() catch break orelse break;
        got_any = true;
        repl.ack(msg.wal_start);
        if (std.mem.startsWith(u8, msg.data, "COMMIT")) break;
    }

    if (!got_any) {
        std.debug.print("FAIL (no messages received before stop)\n", .{});
        return error.ServerError;
    }

    // Now stop the replicator
    repl.stop();

    // Verify stop_requested is true
    if (!repl.isStopRequested()) {
        std.debug.print("FAIL (stop_requested not set after stop)\n", .{});
        return error.ServerError;
    }

    // next() should return null after stop
    const after_stop = repl.next() catch |err| {
        std.debug.print("FAIL (next after stop: {})\n", .{err});
        return err;
    };
    if (after_stop != null) {
        std.debug.print("FAIL (next returned non-null after stop)\n", .{});
        return error.ServerError;
    }

    std.debug.print("OK\n", .{});
}

// =========================================================================
// Test 10: Replicate async (ported from test_replicate_async)
// =========================================================================
fn testReplicateAsync(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: replicate async... ", .{});

    try runPsql(allocator, DB_NAME, "INSERT INTO teas (kind) VALUES ('煎茶')");

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

    // Track messages received from the worker thread
    var received = std.atomic.Value(u32).init(0);

    // Spawn replication in a separate thread
    const thread = std.Thread.spawn(.{}, struct {
        fn run(r: *pgzr.Replicator, recv_count: *std.atomic.Value(u32)) void {
            var count: u32 = 0;
            while (count < 200) : (count += 1) {
                const msg = r.next() catch break orelse break;
                recv_count.store(recv_count.load(.acquire) + 1, .release);
                r.ack(msg.wal_start);
                if (std.mem.startsWith(u8, msg.data, "COMMIT")) {
                    // After first COMMIT, continue to drain then break
                    continue;
                }
            }
        }
    }.run, .{ &repl, &received }) catch |err| {
        std.debug.print("FAIL (spawn thread: {})\n", .{err});
        repl.deinit();
        return err;
    };

    // Wait a bit for thread to start processing
    std.Thread.sleep(500 * std.time.ns_per_ms);

    // Request stop
    repl.stop();

    // Wait for thread to finish
    thread.join();

    // Should have received some messages
    if (received.load(.acquire) == 0) {
        std.debug.print("FAIL (no messages received in async thread)\n", .{});
        repl.deinit();
        return error.ServerError;
    }

    repl.deinit();

    std.debug.print("OK\n", .{});
}

// =========================================================================
// Test 10: TLS prefer mode (falls back to plain if SSL off, succeeds if on)
// =========================================================================
fn testTlsPrefer(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: TLS prefer mode... ", .{});

    var config = connConfig();
    config.tls = .prefer;

    // prefer mode should always succeed: TLS if server supports it, plain otherwise
    var repl = pgzr.Replicator.init(allocator, .{
        .conn = config,
        .slot_name = SLOT_NAME,
        .options = &.{
            .{ "include-timestamp", "on" },
        },
    }) catch |err| {
        std.debug.print("FAIL (connect: {})\n", .{err});
        return err;
    };
    defer repl.deinit();

    std.debug.print("OK\n", .{});
}

// =========================================================================
// Test 11: TLS require mode
// =========================================================================
fn testTlsRequire(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: TLS require mode... ", .{});

    // Check if PostgreSQL has SSL enabled
    const ssl_output = runPsqlCapture(allocator, DB_NAME, "SHOW ssl") catch {
        std.debug.print("SKIP (cannot query ssl setting)\n", .{});
        return;
    };
    defer allocator.free(ssl_output);

    const ssl_enabled = std.mem.eql(u8, ssl_output, "on");

    var config = connConfig();
    config.tls = .require;

    const result = pgzr.Replicator.init(allocator, .{
        .conn = config,
        .slot_name = SLOT_NAME,
        .options = &.{
            .{ "include-timestamp", "on" },
        },
    });

    if (ssl_enabled) {
        // SSL is on — require mode should succeed
        if (result) |*repl| {
            var r = repl.*;
            r.deinit();
            std.debug.print("OK (connected with TLS)\n", .{});
        } else |err| {
            std.debug.print("FAIL (SSL enabled but got: {})\n", .{err});
            return err;
        }
    } else {
        // SSL is off — require mode should fail with TlsNotSupported
        if (result) |*repl| {
            var r = repl.*;
            r.deinit();
            std.debug.print("FAIL (expected TlsNotSupported, got success)\n", .{});
            return error.ServerError;
        } else |err| {
            if (err == error.TlsNotSupported) {
                std.debug.print("OK (correctly rejected: TlsNotSupported)\n", .{});
            } else {
                std.debug.print("FAIL (expected TlsNotSupported, got {})\n", .{err});
                return err;
            }
        }
    }
}

// =========================================================================
// Main
// =========================================================================
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        if (gpa.deinit() == .leak) {
            std.debug.print("\nMemory leak detected!\n", .{});
            std.process.exit(1);
        }
    }
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

    // Run tests (reset slot between tests that consume it)
    testBasicReplication(allocator) catch {
        failures += 1;
    };
    resetSlot(allocator);

    testStartPosition(allocator) catch {
        failures += 1;
    };
    resetSlot(allocator);

    testEndPosition(allocator) catch {
        failures += 1;
    };
    resetSlot(allocator);

    testLastServerLsn(allocator) catch {
        failures += 1;
    };
    resetSlot(allocator);

    testTimelineMismatch(allocator) catch {
        failures += 1;
    };
    resetSlot(allocator);

    testSystemIdMismatch(allocator) catch {
        failures += 1;
    };
    resetSlot(allocator);

    testStartPositionFormats(allocator) catch {
        failures += 1;
    };
    resetSlot(allocator);

    testFeedback(allocator) catch {
        failures += 1;
    };
    resetSlot(allocator);

    testStop(allocator) catch {
        failures += 1;
    };
    resetSlot(allocator);

    // TODO: testReplicateAsync doesn't actually test async — it inserts before
    // spawning the thread, so it's equivalent to testStop. Needs rewriting to
    // insert rows from the main thread while replication runs in another thread.
    // testReplicateAsync(allocator) catch {
    //     failures += 1;
    // };

    testTlsPrefer(allocator) catch {
        failures += 1;
    };
    resetSlot(allocator);

    testTlsRequire(allocator) catch {
        failures += 1;
    };
    resetSlot(allocator);

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
