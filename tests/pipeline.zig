const std = @import("std");
const pgzr = @import("pgzr");

const SOURCE_DB = "pgzr_pipeline_source";
const DEST_DB = "pgzr_pipeline_dest";
const SLOT_NAME = "pgzr_pipeline_slot";
const PUB_NAME = "pgzr_pipeline_pub";
const SOURCE_ID = "00000000-0000-0000-0000-000000000001";

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

    var stdout_buf: [256]u8 = undefined;
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

fn getCurrentWalLsn(allocator: std.mem.Allocator) !pgzr.Lsn {
    const output = try runPsqlCapture(allocator, SOURCE_DB, "SELECT pg_current_wal_lsn()");
    defer allocator.free(output);
    return pgzr.Lsn.parse(output);
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
    // Create databases (ignore errors if they already exist)
    runCmd(allocator, &.{ "createdb", SOURCE_DB });
    runCmd(allocator, &.{ "createdb", DEST_DB });

    // Create test table in source
    runPsql(allocator, SOURCE_DB,
        "CREATE TABLE IF NOT EXISTS items (id serial PRIMARY KEY, name text NOT NULL, quantity int NOT NULL DEFAULT 0)",
    ) catch {};

    // Create publication (drop first if exists)
    runPsql(allocator, SOURCE_DB,
        "DROP PUBLICATION IF EXISTS " ++ PUB_NAME,
    ) catch {};
    try runPsql(allocator, SOURCE_DB,
        "CREATE PUBLICATION " ++ PUB_NAME ++ " FOR TABLE items",
    );

    // Drop slot if it exists, then create it
    runPsql(allocator, SOURCE_DB,
        "SELECT pg_drop_replication_slot('" ++ SLOT_NAME ++ "')",
    ) catch {};
    try runPsql(allocator, SOURCE_DB,
        "SELECT pg_create_logical_replication_slot('" ++ SLOT_NAME ++ "', 'pgoutput')",
    );

    // Truncate source table
    runPsql(allocator, SOURCE_DB, "TRUNCATE items RESTART IDENTITY") catch {};

    // Ensure schema exists in dest, then clean tables
    var dest = pgzr.Connection.connect(allocator, destConnConfig()) catch |err| {
        std.debug.print("Failed to connect to dest: {}\n", .{err});
        return err;
    };
    defer dest.close();

    pgzr.schema.ensureSchema(&dest) catch |err| {
        std.debug.print("Failed to create schema: {}\n", .{err});
        return err;
    };

    _ = dest.simpleQuery("TRUNCATE columns, events, transactions, wal_batches CASCADE") catch {};
}

fn teardown(allocator: std.mem.Allocator) void {
    std.Thread.sleep(200 * std.time.ns_per_ms);
    runPsql(allocator, SOURCE_DB,
        "SELECT pg_drop_replication_slot('" ++ SLOT_NAME ++ "')",
    ) catch {};
    runPsql(allocator, SOURCE_DB,
        "DROP PUBLICATION IF EXISTS " ++ PUB_NAME,
    ) catch {};
    runCmd(allocator, &.{ "dropdb", "--if-exists", "--force", SOURCE_DB });
    runCmd(allocator, &.{ "dropdb", "--if-exists", "--force", DEST_DB });
}

// =========================================================================
// Test 1: Ingest stores WAL batches
// =========================================================================
fn testIngestStoresBatches(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: ingest stores WAL batches... ", .{});

    // Insert data into source
    try runPsql(allocator, SOURCE_DB,
        "INSERT INTO items (name, quantity) VALUES ('widget', 10), ('gadget', 5)",
    );

    // Get WAL position after insert for end_position
    const end_lsn = getCurrentWalLsn(allocator) catch |err| {
        std.debug.print("FAIL (get LSN: {})\n", .{err});
        return err;
    };

    // Run ingestor with end_position to stop automatically
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
        std.debug.print("FAIL (init ingestor: {})\n", .{err});
        return err;
    };
    defer ingestor.deinit();

    ingestor.run() catch |err| {
        std.debug.print("FAIL (ingestor run: {})\n", .{err});
        return err;
    };

    // Verify batches were stored in dest
    const count = try queryCount(allocator, DEST_DB,
        "SELECT count(*) FROM wal_batches WHERE source_id='" ++ SOURCE_ID ++ "'",
    );
    if (count == 0) {
        std.debug.print("FAIL (no batches stored)\n", .{});
        return error.ServerError;
    }

    // Verify all batches are complete
    const incomplete_count = try queryCount(allocator, DEST_DB,
        "SELECT count(*) FROM wal_batches WHERE source_id='" ++ SOURCE_ID ++ "' AND complete = false",
    );
    if (incomplete_count > 0) {
        std.debug.print("FAIL ({d} incomplete batches)\n", .{incomplete_count});
        return error.ServerError;
    }

    std.debug.print("OK ({d} batch(es))\n", .{count});
}

// =========================================================================
// Test 2: Processor parses batches into transactions/events/columns
// =========================================================================
fn testProcessorParsesBatches(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: processor parses batches... ", .{});

    // Process the batches stored by the previous test
    var processor = pgzr.Processor.init(allocator, .{
        .dest = destConnConfig(),
        .source_id = SOURCE_ID,
    }) catch |err| {
        std.debug.print("FAIL (init processor: {})\n", .{err});
        return err;
    };
    defer processor.deinit();

    // Process all pending batches
    var batch_count: u32 = 0;
    while (true) {
        const processed = processor.processOne() catch |err| {
            std.debug.print("FAIL (processOne: {})\n", .{err});
            return err;
        };
        if (!processed) break;
        batch_count += 1;
        if (batch_count > 100) {
            std.debug.print("FAIL (too many batches)\n", .{});
            return error.ServerError;
        }
    }

    if (batch_count == 0) {
        std.debug.print("FAIL (no batches processed)\n", .{});
        return error.ServerError;
    }

    // Verify transactions were created
    const txn_count = try queryCount(allocator, DEST_DB,
        "SELECT count(*) FROM transactions WHERE source_id='" ++ SOURCE_ID ++ "'",
    );
    if (txn_count == 0) {
        std.debug.print("FAIL (no transactions)\n", .{});
        return error.ServerError;
    }

    // Verify events were created (2 inserts)
    const event_count = try queryCount(allocator, DEST_DB,
        "SELECT count(*) FROM events WHERE source_id='" ++ SOURCE_ID ++ "' AND type=0",
    );
    if (event_count != 2) {
        std.debug.print("FAIL (expected 2 insert events, got {d})\n", .{event_count});
        return error.ServerError;
    }

    // Verify columns were created (id, name, quantity = 3 columns per event * 2 events = 6)
    const col_count = try queryCount(allocator, DEST_DB,
        "SELECT count(*) FROM columns WHERE source_id='" ++ SOURCE_ID ++ "'",
    );
    if (col_count != 6) {
        std.debug.print("FAIL (expected 6 columns, got {d})\n", .{col_count});
        return error.ServerError;
    }

    // Verify batches were cleaned up (deleted after processing)
    const remaining_count = try queryCount(allocator, DEST_DB,
        "SELECT count(*) FROM wal_batches WHERE source_id='" ++ SOURCE_ID ++ "'",
    );
    if (remaining_count > 0) {
        std.debug.print("FAIL ({d} batches not cleaned up)\n", .{remaining_count});
        return error.ServerError;
    }

    std.debug.print("OK ({d} txn, {d} events, {d} columns)\n", .{ txn_count, event_count, col_count });
}

// =========================================================================
// Test 3: End-to-end with UPDATE and DELETE
// =========================================================================
fn testUpdateAndDelete(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: update and delete events... ", .{});

    // Set REPLICA IDENTITY FULL so we get old tuple on updates/deletes
    runPsql(allocator, SOURCE_DB, "ALTER TABLE items REPLICA IDENTITY FULL") catch {};

    // Insert, update, delete
    try runPsql(allocator, SOURCE_DB,
        "INSERT INTO items (name, quantity) VALUES ('tempitem', 1)",
    );
    try runPsql(allocator, SOURCE_DB,
        "UPDATE items SET quantity = 99 WHERE name = 'tempitem'",
    );
    try runPsql(allocator, SOURCE_DB,
        "DELETE FROM items WHERE name = 'tempitem'",
    );

    // Get end LSN
    const end_lsn = getCurrentWalLsn(allocator) catch |err| {
        std.debug.print("FAIL (get LSN: {})\n", .{err});
        return err;
    };

    // Ingest
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
        std.debug.print("FAIL (init ingestor: {})\n", .{err});
        return err;
    };
    defer ingestor.deinit();

    ingestor.run() catch |err| {
        std.debug.print("FAIL (ingestor run: {})\n", .{err});
        return err;
    };

    // Process
    var processor = pgzr.Processor.init(allocator, .{
        .dest = destConnConfig(),
        .source_id = SOURCE_ID,
    }) catch |err| {
        std.debug.print("FAIL (init processor: {})\n", .{err});
        return err;
    };
    defer processor.deinit();

    var batch_count: u32 = 0;
    while (true) {
        const processed = processor.processOne() catch |err| {
            std.debug.print("FAIL (processOne: {})\n", .{err});
            return err;
        };
        if (!processed) break;
        batch_count += 1;
        if (batch_count > 100) break;
    }

    // Verify events: inserts, updates, deletes
    const insert_count = try queryCount(allocator, DEST_DB,
        "SELECT count(*) FROM events WHERE source_id='" ++ SOURCE_ID ++ "' AND type=0",
    );
    // Previous test created 2 inserts, this test adds 1 more
    if (insert_count < 3) {
        std.debug.print("FAIL (expected >= 3 insert events, got {d})\n", .{insert_count});
        return error.ServerError;
    }

    const update_count = try queryCount(allocator, DEST_DB,
        "SELECT count(*) FROM events WHERE source_id='" ++ SOURCE_ID ++ "' AND type=1",
    );
    if (update_count < 1) {
        std.debug.print("FAIL (expected >= 1 update event, got {d})\n", .{update_count});
        return error.ServerError;
    }

    const delete_count = try queryCount(allocator, DEST_DB,
        "SELECT count(*) FROM events WHERE source_id='" ++ SOURCE_ID ++ "' AND type=2",
    );
    if (delete_count < 1) {
        std.debug.print("FAIL (expected >= 1 delete event, got {d})\n", .{delete_count});
        return error.ServerError;
    }

    std.debug.print("OK (inserts={d}, updates={d}, deletes={d})\n", .{
        insert_count, update_count, delete_count,
    });
}

// =========================================================================
// Test 4: Column values are correctly stored
// =========================================================================
fn testColumnValues(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: column values stored correctly... ", .{});

    // Check that update events have previous_value set
    const prev_val_count = try queryCount(allocator, DEST_DB,
        \\SELECT count(*) FROM columns c
        \\ JOIN events e ON e.id = c.event_id
        \\ WHERE e.source_id='00000000-0000-0000-0000-000000000001'
        \\   AND e.type = 1
        \\   AND c.previous_value IS NOT NULL
    );
    if (prev_val_count == 0) {
        std.debug.print("FAIL (no previous_value on update columns)\n", .{});
        return error.ServerError;
    }

    // Verify column names exist
    const name_col_count = try queryCount(allocator, DEST_DB,
        \\SELECT count(*) FROM columns
        \\ WHERE source_id='00000000-0000-0000-0000-000000000001'
        \\   AND name = 'name'
    );
    if (name_col_count == 0) {
        std.debug.print("FAIL (no 'name' column found)\n", .{});
        return error.ServerError;
    }

    std.debug.print("OK ({d} columns with previous_value)\n", .{prev_val_count});
}

// =========================================================================
// Main
// =========================================================================
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("PGZR Pipeline Integration Tests\n", .{});
    std.debug.print("================================\n", .{});

    std.debug.print("Setting up test databases...\n", .{});
    setup(allocator) catch |err| {
        std.debug.print("Setup failed: {}. Is PostgreSQL running?\n", .{err});
        std.process.exit(1);
    };

    var failures: u32 = 0;

    // Tests run sequentially -- later tests depend on data from earlier ones
    testIngestStoresBatches(allocator) catch {
        failures += 1;
    };

    testProcessorParsesBatches(allocator) catch {
        failures += 1;
    };

    testUpdateAndDelete(allocator) catch {
        failures += 1;
    };

    testColumnValues(allocator) catch {
        failures += 1;
    };

    std.debug.print("Cleaning up...\n", .{});
    teardown(allocator);

    if (failures > 0) {
        std.debug.print("\n{d} test(s) FAILED\n", .{failures});
        std.process.exit(1);
    } else {
        std.debug.print("\nAll tests passed!\n", .{});
    }
}
