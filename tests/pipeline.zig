const std = @import("std");
const pgzr = @import("pgzr");

/// Io instance for the whole test run, set at the top of main.
var io: std.Io = undefined;

fn getenv(name: [*:0]const u8) ?[]const u8 {
    const p = std.c.getenv(name) orelse return null;
    return std.mem.span(p);
}

fn sleepMs(ms: i64) void {
    io.sleep(.fromMilliseconds(ms), .awake) catch {};
}

const SOURCE_DB = "pgzr_pipeline_source";
const DEST_DB = "pgzr_pipeline_dest";
const SLOT_NAME = "pgzr_pipeline_slot";
const PUB_NAME = "pgzr_pipeline_pub";
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
    const result = std.process.run(allocator, io, .{ .argv = args }) catch return;
    allocator.free(result.stdout);
    allocator.free(result.stderr);
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
    runPsql(
        allocator,
        SOURCE_DB,
        "CREATE TABLE IF NOT EXISTS items (id serial PRIMARY KEY, name text NOT NULL, quantity int NOT NULL DEFAULT 0)",
    ) catch {};

    // Create publication (drop first if exists)
    runPsql(
        allocator,
        SOURCE_DB,
        "DROP PUBLICATION IF EXISTS " ++ PUB_NAME,
    ) catch {};
    try runPsql(
        allocator,
        SOURCE_DB,
        "CREATE PUBLICATION " ++ PUB_NAME ++ " FOR TABLE items",
    );

    // Drop slot if it exists, then create it
    runPsql(
        allocator,
        SOURCE_DB,
        "SELECT pg_drop_replication_slot('" ++ SLOT_NAME ++ "')",
    ) catch {};
    try runPsql(
        allocator,
        SOURCE_DB,
        "SELECT pg_create_logical_replication_slot('" ++ SLOT_NAME ++ "', 'pgoutput')",
    );

    // Truncate source table
    runPsql(allocator, SOURCE_DB, "TRUNCATE items RESTART IDENTITY") catch {};

    // Ensure schema exists in dest, then clean tables
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
    runPsql(
        allocator,
        SOURCE_DB,
        "SELECT pg_drop_replication_slot('" ++ SLOT_NAME ++ "')",
    ) catch {};
    runPsql(
        allocator,
        SOURCE_DB,
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
    try runPsql(
        allocator,
        SOURCE_DB,
        "INSERT INTO items (name, quantity) VALUES ('widget', 10), ('gadget', 5)",
    );

    // Get WAL position after insert for end_position
    const end_lsn = getCurrentWalLsn(allocator) catch |err| {
        std.debug.print("FAIL (get LSN: {})\n", .{err});
        return err;
    };

    // Run ingestor with end_position to stop automatically
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
        std.debug.print("FAIL (init ingestor: {})\n", .{err});
        return err;
    };
    defer ingestor.deinit();

    ingestor.run() catch |err| {
        std.debug.print("FAIL (ingestor run: {})\n", .{err});
        return err;
    };

    // Verify batches were stored in dest
    const count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM wal_batches WHERE source_id='" ++ SOURCE_ID ++ "'",
    );
    if (count == 0) {
        std.debug.print("FAIL (no batches stored)\n", .{});
        return error.ServerError;
    }

    // Verify all batches are complete
    const incomplete_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM wal_batches WHERE source_id='" ++ SOURCE_ID ++ "' AND complete = false",
    );
    if (incomplete_count > 0) {
        std.debug.print("FAIL ({d} incomplete batches)\n", .{incomplete_count});
        return error.ServerError;
    }

    std.debug.print("OK ({d} batch(es))\n", .{count});
}

// =========================================================================
// Test 2: Processor parses batches into transactions/events with JSONB data
// =========================================================================
fn testProcessorParsesBatches(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: processor parses batches... ", .{});

    // Process the batches stored by the previous test
    var processor = pgzr.Processor.init(allocator, io, .{
        .dest = destConnConfig(),
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
    const txn_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM transactions WHERE source_id='" ++ SOURCE_ID ++ "'",
    );
    if (txn_count == 0) {
        std.debug.print("FAIL (no transactions)\n", .{});
        return error.ServerError;
    }

    // Verify events were created (2 inserts)
    const event_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM events WHERE type='I'",
    );
    if (event_count != 2) {
        std.debug.print("FAIL (expected 2 insert events, got {d})\n", .{event_count});
        return error.ServerError;
    }

    // Verify events have JSONB data
    const data_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM events WHERE data IS NOT NULL AND data != 'null'::jsonb",
    );
    if (data_count != 2) {
        std.debug.print("FAIL (expected 2 events with data, got {d})\n", .{data_count});
        return error.ServerError;
    }

    // Verify relation_snapshots were created
    const snapshot_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM relation_snapshots WHERE source_id='" ++ SOURCE_ID ++ "'",
    );
    if (snapshot_count == 0) {
        std.debug.print("FAIL (no relation snapshots)\n", .{});
        return error.ServerError;
    }

    // Verify batches were cleaned up (deleted after processing)
    const remaining_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM wal_batches WHERE source_id='" ++ SOURCE_ID ++ "'",
    );
    if (remaining_count > 0) {
        std.debug.print("FAIL ({d} batches not cleaned up)\n", .{remaining_count});
        return error.ServerError;
    }

    std.debug.print("OK ({d} txn, {d} events, {d} snapshots)\n", .{ txn_count, event_count, snapshot_count });
}

// =========================================================================
// Test 3: End-to-end with UPDATE and DELETE
// =========================================================================
fn testUpdateAndDelete(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: update and delete events... ", .{});

    // Set REPLICA IDENTITY FULL so we get old tuple on updates/deletes
    runPsql(allocator, SOURCE_DB, "ALTER TABLE items REPLICA IDENTITY FULL") catch {};

    // Insert, update, delete
    try runPsql(
        allocator,
        SOURCE_DB,
        "INSERT INTO items (name, quantity) VALUES ('tempitem', 1)",
    );
    try runPsql(
        allocator,
        SOURCE_DB,
        "UPDATE items SET quantity = 99 WHERE name = 'tempitem'",
    );
    try runPsql(
        allocator,
        SOURCE_DB,
        "DELETE FROM items WHERE name = 'tempitem'",
    );

    // Get end LSN
    const end_lsn = getCurrentWalLsn(allocator) catch |err| {
        std.debug.print("FAIL (get LSN: {})\n", .{err});
        return err;
    };

    // Ingest
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
        std.debug.print("FAIL (init ingestor: {})\n", .{err});
        return err;
    };
    defer ingestor.deinit();

    ingestor.run() catch |err| {
        std.debug.print("FAIL (ingestor run: {})\n", .{err});
        return err;
    };

    // Process
    var processor = pgzr.Processor.init(allocator, io, .{
        .dest = destConnConfig(),
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
    const insert_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM events WHERE type='I'",
    );
    // Previous test created 2 inserts, this test adds 1 more
    if (insert_count < 3) {
        std.debug.print("FAIL (expected >= 3 insert events, got {d})\n", .{insert_count});
        return error.ServerError;
    }

    const update_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM events WHERE type='U'",
    );
    if (update_count < 1) {
        std.debug.print("FAIL (expected >= 1 update event, got {d})\n", .{update_count});
        return error.ServerError;
    }

    const delete_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM events WHERE type='D'",
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
// Test 4: JSONB data and old_data values are correctly stored
// =========================================================================
fn testJsonbValues(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: JSONB data values stored correctly... ", .{});

    // Check that update events have old_data set
    const old_data_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM events WHERE type='U' AND old_data IS NOT NULL AND old_data != 'null'::jsonb",
    );
    if (old_data_count == 0) {
        std.debug.print("FAIL (no old_data on update events)\n", .{});
        return error.ServerError;
    }

    // Verify data contains expected column names
    const name_col_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM events WHERE data ? 'name'",
    );
    if (name_col_count == 0) {
        std.debug.print("FAIL (no events with 'name' key in data)\n", .{});
        return error.ServerError;
    }

    // Verify data contains expected column names
    const quantity_col_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM events WHERE data ? 'quantity'",
    );
    if (quantity_col_count == 0) {
        std.debug.print("FAIL (no events with 'quantity' key in data)\n", .{});
        return error.ServerError;
    }

    // Verify delete events have old_data but no data
    const delete_data_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM events WHERE type='D' AND old_data IS NOT NULL AND (data IS NULL)",
    );
    if (delete_data_count == 0) {
        std.debug.print("FAIL (delete events should have old_data and null data)\n", .{});
        return error.ServerError;
    }

    std.debug.print("OK ({d} updates with old_data, {d} events with name key)\n", .{ old_data_count, name_col_count });
}

// =========================================================================
// Test 5: Relation snapshots contain column metadata
// =========================================================================
fn testRelationSnapshots(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: relation snapshots... ", .{});

    // Verify snapshot has columns JSONB
    const snapshot_with_cols = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM relation_snapshots WHERE jsonb_array_length(columns) > 0",
    );
    if (snapshot_with_cols == 0) {
        std.debug.print("FAIL (no snapshots with columns)\n", .{});
        return error.ServerError;
    }

    // Verify snapshot contains 'name' column metadata
    const has_name = try queryCount(allocator, DEST_DB,
        \\SELECT count(*) FROM relation_snapshots
        \\ WHERE columns @> '[{"name": "name"}]'
    );
    if (has_name == 0) {
        std.debug.print("FAIL (snapshot missing 'name' column)\n", .{});
        return error.ServerError;
    }

    // Verify snapshot table_name is 'items'
    const items_snap = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM relation_snapshots WHERE table_name = 'items'",
    );
    if (items_snap == 0) {
        std.debug.print("FAIL (no snapshot for 'items' table)\n", .{});
        return error.ServerError;
    }

    std.debug.print("OK ({d} snapshots with columns)\n", .{snapshot_with_cols});
}

// =========================================================================
// Test 6: Metadata via pg_logical_emit_message
// =========================================================================
fn testMetadataViaMessage(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: metadata via pg_logical_emit_message... ", .{});

    // Clear dest tables for a clean test
    runPsql(
        allocator,
        DEST_DB,
        "TRUNCATE events, relation_snapshots, transactions, wal_batches CASCADE",
    ) catch {};

    // Truncate items so we get a clean count
    runPsql(allocator, SOURCE_DB, "TRUNCATE items RESTART IDENTITY") catch {};

    // Insert data with a logical message in the same transaction
    try runPsql(allocator, SOURCE_DB,
        \\BEGIN;
        \\SELECT pg_logical_emit_message(true, 'test_metadata', '{"user":{"id":42,"name":"Alice"}}');
        \\INSERT INTO items (name, quantity) VALUES ('meta_widget', 7);
        \\COMMIT;
    );

    // Get end LSN
    const end_lsn = getCurrentWalLsn(allocator) catch |err| {
        std.debug.print("FAIL (get LSN: {})\n", .{err});
        return err;
    };

    // Ingest with messages=true to receive pg_logical_emit_message
    var ingestor = pgzr.Ingestor.init(allocator, io, .{
        .source = .{
            .conn = sourceConnConfig(),
            .slot_name = SLOT_NAME,
            .end_position = end_lsn,
            .options = &.{
                .{ "proto_version", "1" },
                .{ "publication_names", PUB_NAME },
                .{ "messages", "true" },
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

    // Process with metadata_message_prefix configured
    var processor = pgzr.Processor.init(allocator, io, .{
        .dest = destConnConfig(),
        .metadata_message_prefix = "test_metadata",
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

    // Verify transaction has metadata
    const meta_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM transactions WHERE metadata IS NOT NULL",
    );
    if (meta_count == 0) {
        std.debug.print("FAIL (no transactions with metadata)\n", .{});
        return error.ServerError;
    }

    // Verify metadata content
    const user_meta = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM transactions WHERE metadata->'user'->>'id' = '42'",
    );
    if (user_meta == 0) {
        std.debug.print("FAIL (metadata missing user.id=42)\n", .{});
        return error.ServerError;
    }

    // Verify the insert event was still created
    const event_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM events WHERE type='I'",
    );
    if (event_count == 0) {
        std.debug.print("FAIL (no insert events)\n", .{});
        return error.ServerError;
    }

    std.debug.print("OK ({d} txn with metadata, {d} events)\n", .{ meta_count, event_count });
}

// =========================================================================
// Test 7: Metadata via metadata table
// =========================================================================
fn testMetadataViaTable(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: metadata via metadata table... ", .{});

    // Clear dest tables
    runPsql(
        allocator,
        DEST_DB,
        "TRUNCATE events, relation_snapshots, transactions, wal_batches CASCADE",
    ) catch {};

    // Truncate items
    runPsql(allocator, SOURCE_DB, "TRUNCATE items RESTART IDENTITY") catch {};

    // Create metadata table on source
    runPsql(
        allocator,
        SOURCE_DB,
        "CREATE TABLE IF NOT EXISTS test_metadata_table (version int PRIMARY KEY, data jsonb DEFAULT '{}')",
    ) catch {};

    // Add to publication
    runPsql(
        allocator,
        SOURCE_DB,
        "ALTER PUBLICATION " ++ PUB_NAME ++ " ADD TABLE test_metadata_table",
    ) catch {};

    // Insert data with metadata table upsert in same transaction
    try runPsql(allocator, SOURCE_DB,
        \\BEGIN;
        \\INSERT INTO items (name, quantity) VALUES ('table_meta_widget', 3);
        \\INSERT INTO test_metadata_table (version, data)
        \\    VALUES (1, '{"request_id":"abc-123","actor":"Bob"}')
        \\    ON CONFLICT (version) DO UPDATE SET data = EXCLUDED.data;
        \\COMMIT;
    );

    // Get end LSN
    const end_lsn = getCurrentWalLsn(allocator) catch |err| {
        std.debug.print("FAIL (get LSN: {})\n", .{err});
        return err;
    };

    // Ingest
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
        std.debug.print("FAIL (init ingestor: {})\n", .{err});
        return err;
    };
    defer ingestor.deinit();

    ingestor.run() catch |err| {
        std.debug.print("FAIL (ingestor run: {})\n", .{err});
        return err;
    };

    // Process with metadata_table configured
    var processor = pgzr.Processor.init(allocator, io, .{
        .dest = destConnConfig(),
        .metadata_table = "test_metadata_table",
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

    // Verify transaction has metadata
    const meta_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM transactions WHERE metadata IS NOT NULL",
    );
    if (meta_count == 0) {
        std.debug.print("FAIL (no transactions with metadata)\n", .{});
        return error.ServerError;
    }

    // Verify metadata content
    const actor_meta = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM transactions WHERE metadata->>'actor' = 'Bob'",
    );
    if (actor_meta == 0) {
        std.debug.print("FAIL (metadata missing actor=Bob)\n", .{});
        return error.ServerError;
    }

    // Verify the metadata table write was NOT stored as an event
    const meta_event_count = try queryCount(allocator, DEST_DB,
        \\SELECT count(*) FROM events e
        \\ JOIN relation_snapshots rs ON rs.source_id = '00000000-0000-0000-0000-000000000001'
        \\   AND rs.table_name = 'test_metadata_table'
        \\   AND rs.rel_oid = e.rel_oid
    );
    if (meta_event_count > 0) {
        std.debug.print("FAIL (metadata table writes should not be stored as events, got {d})\n", .{meta_event_count});
        return error.ServerError;
    }

    // Verify the items insert event WAS stored
    const item_event_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM events WHERE type='I'",
    );
    if (item_event_count == 0) {
        std.debug.print("FAIL (no insert events for items)\n", .{});
        return error.ServerError;
    }

    std.debug.print("OK ({d} txn with metadata, {d} item events, 0 metadata table events)\n", .{ meta_count, item_event_count });
}

// =========================================================================
// Test 8: Proto v2 streaming — events buffered until commit
// =========================================================================
fn testStreamingEvents(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: proto v2 streaming events... ", .{});

    // Set low work_mem to force streaming for small transactions
    runPsql(allocator, SOURCE_DB, "ALTER SYSTEM SET logical_decoding_work_mem = '64kB'") catch {};
    runPsql(allocator, SOURCE_DB, "SELECT pg_reload_conf()") catch {};

    // Clear dest
    runPsql(
        allocator,
        DEST_DB,
        "TRUNCATE events, relation_snapshots, transactions, wal_batches CASCADE",
    ) catch {};
    runPsql(allocator, SOURCE_DB, "TRUNCATE items RESTART IDENTITY") catch {};

    // Insert enough data to exceed logical_decoding_work_mem (64kB)
    try runPsql(allocator, SOURCE_DB,
        \\BEGIN;
        \\INSERT INTO items (name, quantity) SELECT repeat('x', 1000) || g, g FROM generate_series(1, 100) g;
        \\COMMIT;
    );

    const end_lsn = getCurrentWalLsn(allocator) catch |err| {
        std.debug.print("FAIL (get LSN: {})\n", .{err});
        return err;
    };

    // Ingest with proto v2 + streaming
    var ingestor = pgzr.Ingestor.init(allocator, io, .{
        .source = .{
            .conn = sourceConnConfig(),
            .slot_name = SLOT_NAME,
            .end_position = end_lsn,
            .options = &.{
                .{ "proto_version", "2" },
                .{ "publication_names", PUB_NAME },
                .{ "streaming", "on" },
            },
        },
        .dest = destConnConfig(),
        .source_id = SOURCE_ID,
    }) catch |err| {
        std.debug.print("FAIL (init ingestor: {})\n", .{err});
        // Reset work_mem before returning
        runPsql(allocator, SOURCE_DB, "ALTER SYSTEM RESET logical_decoding_work_mem") catch {};
        runPsql(allocator, SOURCE_DB, "SELECT pg_reload_conf()") catch {};
        return err;
    };
    defer ingestor.deinit();

    ingestor.run() catch |err| {
        std.debug.print("FAIL (ingestor run: {})\n", .{err});
        runPsql(allocator, SOURCE_DB, "ALTER SYSTEM RESET logical_decoding_work_mem") catch {};
        runPsql(allocator, SOURCE_DB, "SELECT pg_reload_conf()") catch {};
        return err;
    };

    // Process
    var processor = pgzr.Processor.init(allocator, io, .{
        .dest = destConnConfig(),
    }) catch |err| {
        std.debug.print("FAIL (init processor: {})\n", .{err});
        runPsql(allocator, SOURCE_DB, "ALTER SYSTEM RESET logical_decoding_work_mem") catch {};
        runPsql(allocator, SOURCE_DB, "SELECT pg_reload_conf()") catch {};
        return err;
    };
    defer processor.deinit();

    var batch_count: u32 = 0;
    while (true) {
        const processed = processor.processOne() catch |err| {
            std.debug.print("FAIL (processOne: {})\n", .{err});
            runPsql(allocator, SOURCE_DB, "ALTER SYSTEM RESET logical_decoding_work_mem") catch {};
            runPsql(allocator, SOURCE_DB, "SELECT pg_reload_conf()") catch {};
            return err;
        };
        if (!processed) break;
        batch_count += 1;
        if (batch_count > 100) break;
    }

    // Reset work_mem
    runPsql(allocator, SOURCE_DB, "ALTER SYSTEM RESET logical_decoding_work_mem") catch {};
    runPsql(allocator, SOURCE_DB, "SELECT pg_reload_conf()") catch {};

    // Verify transaction created
    const txn_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM transactions",
    );
    if (txn_count == 0) {
        std.debug.print("FAIL (no transactions)\n", .{});
        return error.ServerError;
    }

    // Verify all events captured
    const event_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM events WHERE type='I'",
    );
    if (event_count < 100) {
        std.debug.print("FAIL (expected >= 100 insert events, got {d})\n", .{event_count});
        return error.ServerError;
    }

    std.debug.print("OK ({d} txn, {d} events, {d} batches)\n", .{ txn_count, event_count, batch_count });
}

// =========================================================================
// Test 9: Proto v2 streaming — metadata via pg_logical_emit_message
// =========================================================================
fn testStreamingMetadataViaMessage(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: proto v2 streaming with metadata via message... ", .{});

    // Set low work_mem to force streaming
    runPsql(allocator, SOURCE_DB, "ALTER SYSTEM SET logical_decoding_work_mem = '64kB'") catch {};
    runPsql(allocator, SOURCE_DB, "SELECT pg_reload_conf()") catch {};

    // Clear dest
    runPsql(
        allocator,
        DEST_DB,
        "TRUNCATE events, relation_snapshots, transactions, wal_batches CASCADE",
    ) catch {};
    runPsql(allocator, SOURCE_DB, "TRUNCATE items RESTART IDENTITY") catch {};

    // Insert with metadata + enough data to trigger streaming
    try runPsql(allocator, SOURCE_DB,
        \\BEGIN;
        \\SELECT pg_logical_emit_message(true, 'test_metadata', '{"stream_test":true,"user":{"id":99}}');
        \\INSERT INTO items (name, quantity) SELECT repeat('y', 1000) || g, g FROM generate_series(1, 100) g;
        \\COMMIT;
    );

    const end_lsn = getCurrentWalLsn(allocator) catch |err| {
        std.debug.print("FAIL (get LSN: {})\n", .{err});
        return err;
    };

    // Ingest with proto v2 + streaming + messages
    var ingestor = pgzr.Ingestor.init(allocator, io, .{
        .source = .{
            .conn = sourceConnConfig(),
            .slot_name = SLOT_NAME,
            .end_position = end_lsn,
            .options = &.{
                .{ "proto_version", "2" },
                .{ "publication_names", PUB_NAME },
                .{ "streaming", "on" },
                .{ "messages", "true" },
            },
        },
        .dest = destConnConfig(),
        .source_id = SOURCE_ID,
    }) catch |err| {
        std.debug.print("FAIL (init ingestor: {})\n", .{err});
        runPsql(allocator, SOURCE_DB, "ALTER SYSTEM RESET logical_decoding_work_mem") catch {};
        runPsql(allocator, SOURCE_DB, "SELECT pg_reload_conf()") catch {};
        return err;
    };
    defer ingestor.deinit();

    ingestor.run() catch |err| {
        std.debug.print("FAIL (ingestor run: {})\n", .{err});
        runPsql(allocator, SOURCE_DB, "ALTER SYSTEM RESET logical_decoding_work_mem") catch {};
        runPsql(allocator, SOURCE_DB, "SELECT pg_reload_conf()") catch {};
        return err;
    };

    // Process with metadata prefix
    var processor = pgzr.Processor.init(allocator, io, .{
        .dest = destConnConfig(),
        .metadata_message_prefix = "test_metadata",
    }) catch |err| {
        std.debug.print("FAIL (init processor: {})\n", .{err});
        runPsql(allocator, SOURCE_DB, "ALTER SYSTEM RESET logical_decoding_work_mem") catch {};
        runPsql(allocator, SOURCE_DB, "SELECT pg_reload_conf()") catch {};
        return err;
    };
    defer processor.deinit();

    var batch_count: u32 = 0;
    while (true) {
        const processed = processor.processOne() catch |err| {
            std.debug.print("FAIL (processOne: {})\n", .{err});
            runPsql(allocator, SOURCE_DB, "ALTER SYSTEM RESET logical_decoding_work_mem") catch {};
            runPsql(allocator, SOURCE_DB, "SELECT pg_reload_conf()") catch {};
            return err;
        };
        if (!processed) break;
        batch_count += 1;
        if (batch_count > 100) break;
    }

    // Reset work_mem
    runPsql(allocator, SOURCE_DB, "ALTER SYSTEM RESET logical_decoding_work_mem") catch {};
    runPsql(allocator, SOURCE_DB, "SELECT pg_reload_conf()") catch {};

    // Verify metadata
    const meta_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM transactions WHERE metadata IS NOT NULL",
    );
    if (meta_count == 0) {
        std.debug.print("FAIL (no transactions with metadata)\n", .{});
        return error.ServerError;
    }

    const user_meta = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM transactions WHERE metadata->'user'->>'id' = '99'",
    );
    if (user_meta == 0) {
        std.debug.print("FAIL (metadata missing user.id=99)\n", .{});
        return error.ServerError;
    }

    // Verify events
    const event_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM events WHERE type='I'",
    );
    if (event_count < 100) {
        std.debug.print("FAIL (expected >= 100 insert events, got {d})\n", .{event_count});
        return error.ServerError;
    }

    std.debug.print("OK ({d} txn with metadata, {d} events, {d} batches)\n", .{ meta_count, event_count, batch_count });
}

// =========================================================================
// Test: duplicate batch COPY surfaces SQLSTATE 23505 (restart-replay
// dedupe path in the ingestor relies on this).
// =========================================================================
fn testDuplicateBatchCopy(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: duplicate batch COPY reports 23505... ", .{});

    var conn = pgzr.Connection.connect(allocator, io, destConnConfig()) catch |err| {
        std.debug.print("FAIL (connect: {})\n", .{err});
        return err;
    };
    defer conn.close();

    runPsql(allocator, DEST_DB, "DELETE FROM wal_batches WHERE source_id = '00000000-0000-0000-0000-00000000dead'") catch {};

    const cmd = "COPY wal_batches (source_id, begin_lsn, start_lsn, end_lsn, data, relations, complete) FROM STDIN";
    const row = "00000000-0000-0000-0000-00000000dead\t1\t1\t2\t\\\\x00\t\\N\tt\n";

    conn.copyIn(allocator, cmd, row) catch |err| {
        std.debug.print("FAIL (first copy: {})\n", .{err});
        return err;
    };

    // Second copy of the same (source_id, start_lsn) must fail with a
    // unique violation, and the connection must stay usable.
    if (conn.copyIn(allocator, cmd, row)) |_| {
        std.debug.print("FAIL (duplicate copy unexpectedly succeeded)\n", .{});
        return error.ServerError;
    } else |err| {
        if (err != error.ServerError) {
            std.debug.print("FAIL (duplicate copy: {})\n", .{err});
            return err;
        }
        const code = conn.lastServerErrorCode() orelse {
            std.debug.print("FAIL (no SQLSTATE recorded)\n", .{});
            return error.ServerError;
        };
        if (!std.mem.eql(u8, code, "23505")) {
            std.debug.print("FAIL (expected 23505, got {s})\n", .{code});
            return error.ServerError;
        }
    }

    // Connection still in sync after the failed COPY
    const result = conn.simpleQuery("SELECT 1") catch |err| {
        std.debug.print("FAIL (post-error query: {})\n", .{err});
        return err;
    };
    if (result.column_count == 0) {
        std.debug.print("FAIL (post-error query returned no row)\n", .{});
        return error.ServerError;
    }

    runPsql(allocator, DEST_DB, "DELETE FROM wal_batches WHERE source_id = '00000000-0000-0000-0000-00000000dead'") catch {};

    std.debug.print("OK\n", .{});
}

// =========================================================================
// Test 9b: Interleaved streamed transactions — two concurrent large
// transactions stream interleaved segments; each must commit with exactly
// its own events.
// =========================================================================
fn testInterleavedStreams(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: interleaved streamed transactions... ", .{});

    runPsql(allocator, SOURCE_DB, "ALTER SYSTEM SET logical_decoding_work_mem = '64kB'") catch {};
    runPsql(allocator, SOURCE_DB, "SELECT pg_reload_conf()") catch {};
    defer {
        runPsql(allocator, SOURCE_DB, "ALTER SYSTEM RESET logical_decoding_work_mem") catch {};
        runPsql(allocator, SOURCE_DB, "SELECT pg_reload_conf()") catch {};
    }

    runPsql(
        allocator,
        DEST_DB,
        "TRUNCATE events, relation_snapshots, transactions, wal_batches CASCADE",
    ) catch {};
    runPsql(allocator, SOURCE_DB, "TRUNCATE items RESTART IDENTITY") catch {};

    // Two concurrent sessions, each writing >64kB in two chunks with
    // sleeps so their WAL (and thus their stream segments) interleave.
    const Worker = struct {
        fn run(alloc: std.mem.Allocator, script: []const u8) void {
            runPsql(alloc, SOURCE_DB, script) catch {};
        }
    };
    const script_a =
        \\BEGIN;
        \\INSERT INTO items (name, quantity) SELECT 'a' || repeat('x', 1000) || g, g FROM generate_series(1, 75) g;
        \\SELECT pg_sleep(1.0);
        \\INSERT INTO items (name, quantity) SELECT 'a' || repeat('x', 1000) || g, g FROM generate_series(76, 150) g;
        \\COMMIT;
    ;
    const script_b =
        \\BEGIN;
        \\INSERT INTO items (name, quantity) SELECT 'b' || repeat('y', 1000) || g, g FROM generate_series(1, 75) g;
        \\SELECT pg_sleep(1.0);
        \\INSERT INTO items (name, quantity) SELECT 'b' || repeat('y', 1000) || g, g FROM generate_series(76, 150) g;
        \\COMMIT;
    ;
    const t_a = std.Thread.spawn(.{}, Worker.run, .{ allocator, script_a }) catch |err| {
        std.debug.print("FAIL (spawn: {})\n", .{err});
        return err;
    };
    sleepMs(400);
    const t_b = std.Thread.spawn(.{}, Worker.run, .{ allocator, script_b }) catch |err| {
        std.debug.print("FAIL (spawn: {})\n", .{err});
        t_a.join();
        return err;
    };
    t_a.join();
    t_b.join();

    const end_lsn = getCurrentWalLsn(allocator) catch |err| {
        std.debug.print("FAIL (get LSN: {})\n", .{err});
        return err;
    };

    var ingestor = pgzr.Ingestor.init(allocator, io, .{
        .source = .{
            .conn = sourceConnConfig(),
            .slot_name = SLOT_NAME,
            .end_position = end_lsn,
            .options = &.{
                .{ "proto_version", "2" },
                .{ "publication_names", PUB_NAME },
                .{ "streaming", "on" },
            },
        },
        .dest = destConnConfig(),
        .source_id = SOURCE_ID,
        .max_batch_size = 256 * 1024,
    }) catch |err| {
        std.debug.print("FAIL (init ingestor: {})\n", .{err});
        return err;
    };
    defer ingestor.deinit();

    ingestor.run() catch |err| {
        std.debug.print("FAIL (ingestor run: {})\n", .{err});
        return err;
    };

    var processor = pgzr.Processor.init(allocator, io, .{
        .dest = destConnConfig(),
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

    // All of this test's events captured, and each attributed to exactly
    // its own transaction (the shared slot also carries unrelated
    // transactions from earlier tests, so scope every check to our rows).
    const a_events = try queryCount(allocator, DEST_DB, "SELECT count(*) FROM events WHERE type='I' AND data->>'name' LIKE 'ax%'");
    const b_events = try queryCount(allocator, DEST_DB, "SELECT count(*) FROM events WHERE type='I' AND data->>'name' LIKE 'by%'");
    if (a_events != 150 or b_events != 150) {
        std.debug.print("FAIL (expected 150+150 insert events, got a={d} b={d})\n", .{ a_events, b_events });
        return error.ServerError;
    }
    const a_txns = try queryCount(allocator, DEST_DB, "SELECT count(DISTINCT transaction_id) FROM events WHERE data->>'name' LIKE 'ax%'");
    const b_txns = try queryCount(allocator, DEST_DB, "SELECT count(DISTINCT transaction_id) FROM events WHERE data->>'name' LIKE 'by%'");
    const ab_txns = try queryCount(allocator, DEST_DB, "SELECT count(DISTINCT transaction_id) FROM events WHERE data->>'name' LIKE 'ax%' OR data->>'name' LIKE 'by%'");
    if (a_txns != 1 or b_txns != 1 or ab_txns != 2) {
        std.debug.print("FAIL (attribution: a={d} b={d} a+b={d})\n", .{ a_txns, b_txns, ab_txns });
        return error.ServerError;
    }
    const leftover = try queryCount(allocator, DEST_DB, "SELECT count(*) FROM wal_batches");
    if (leftover != 0) {
        std.debug.print("FAIL ({d} unprocessed wal_batches)\n", .{leftover});
        return error.ServerError;
    }

    std.debug.print("OK (2 txn, 300 events, {d} batch groups, correct attribution)\n", .{batch_count});
}

// =========================================================================
// Test 10: Proto v4 — metadata via pg_logical_emit_message
// =========================================================================
fn testProtoV4MetadataViaMessage(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: proto v4 with metadata via message... ", .{});

    // Clear dest
    runPsql(
        allocator,
        DEST_DB,
        "TRUNCATE events, relation_snapshots, transactions, wal_batches CASCADE",
    ) catch {};
    runPsql(allocator, SOURCE_DB, "TRUNCATE items RESTART IDENTITY") catch {};

    // Insert with metadata
    try runPsql(allocator, SOURCE_DB,
        \\BEGIN;
        \\SELECT pg_logical_emit_message(true, 'test_metadata', '{"proto":"v4","version":4}');
        \\INSERT INTO items (name, quantity) VALUES ('v4_test', 44);
        \\COMMIT;
    );

    const end_lsn = getCurrentWalLsn(allocator) catch |err| {
        std.debug.print("FAIL (get LSN: {})\n", .{err});
        return err;
    };

    // Ingest with proto v4 + messages
    var ingestor = pgzr.Ingestor.init(allocator, io, .{
        .source = .{
            .conn = sourceConnConfig(),
            .slot_name = SLOT_NAME,
            .end_position = end_lsn,
            .options = &.{
                .{ "proto_version", "4" },
                .{ "publication_names", PUB_NAME },
                .{ "messages", "true" },
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

    // Process with metadata prefix
    var processor = pgzr.Processor.init(allocator, io, .{
        .dest = destConnConfig(),
        .metadata_message_prefix = "test_metadata",
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

    // Verify metadata
    const meta_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM transactions WHERE metadata IS NOT NULL AND metadata->>'proto' = 'v4'",
    );
    if (meta_count == 0) {
        std.debug.print("FAIL (no transactions with v4 metadata)\n", .{});
        return error.ServerError;
    }

    // Verify events
    const event_count = try queryCount(
        allocator,
        DEST_DB,
        "SELECT count(*) FROM events WHERE type='I'",
    );
    if (event_count == 0) {
        std.debug.print("FAIL (no insert events)\n", .{});
        return error.ServerError;
    }

    std.debug.print("OK ({d} txn with metadata, {d} events)\n", .{ meta_count, event_count });
}

// =========================================================================
// Test 11: Concurrent processors — two processors drain the queue safely
// =========================================================================
fn testConcurrentProcessors(allocator: std.mem.Allocator) !void {
    std.debug.print("  Test: concurrent processors... ", .{});

    // Clear dest
    try runPsql(
        allocator,
        DEST_DB,
        "TRUNCATE events, relation_snapshots, transactions, wal_batches CASCADE",
    );
    runPsql(allocator, SOURCE_DB, "TRUNCATE items RESTART IDENTITY") catch {};

    // Verify clean slate
    const pre_events = try queryCount(allocator, DEST_DB, "SELECT count(*) FROM events");
    if (pre_events > 0) {
        std.debug.print("FAIL (truncate did not clear events, {d} remaining)\n", .{pre_events});
        return error.ServerError;
    }

    // Insert 10 separate transactions so there are multiple batches to claim
    for (0..10) |i| {
        var buf: [256]u8 = undefined;
        const sql = std.fmt.bufPrint(&buf, "INSERT INTO items (name, quantity) VALUES ('concurrent_{d}', {d})", .{ i, i }) catch unreachable;
        runPsql(allocator, SOURCE_DB, sql) catch {};
    }

    // Get end LSN
    const end_lsn = getCurrentWalLsn(allocator) catch |err| {
        std.debug.print("FAIL (get LSN: {})\n", .{err});
        return err;
    };

    // Ingest all batches
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
        std.debug.print("FAIL (init ingestor: {})\n", .{err});
        return err;
    };
    defer ingestor.deinit();

    ingestor.run() catch |err| {
        std.debug.print("FAIL (ingestor run: {})\n", .{err});
        return err;
    };

    // Verify we have batches to process
    const batch_count = try queryCount(allocator, DEST_DB, "SELECT count(*) FROM wal_batches WHERE state='pending'");
    if (batch_count == 0) {
        std.debug.print("FAIL (no batches to process)\n", .{});
        return error.ServerError;
    }

    // Run two processors concurrently in separate threads
    const SharedResult = struct {
        batches: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
        errors: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    };

    var r1 = SharedResult{};
    var r2 = SharedResult{};

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

    const t1 = std.Thread.spawn(.{}, worker, .{ &r1, allocator }) catch |err| {
        std.debug.print("FAIL (spawn thread 1: {})\n", .{err});
        return err;
    };
    const t2 = std.Thread.spawn(.{}, worker, .{ &r2, allocator }) catch |err| {
        std.debug.print("FAIL (spawn thread 2: {})\n", .{err});
        t1.join();
        return err;
    };

    t1.join();
    t2.join();

    const p1_batches = r1.batches.load(.monotonic);
    const p2_batches = r2.batches.load(.monotonic);
    const p1_errors = r1.errors.load(.monotonic);
    const p2_errors = r2.errors.load(.monotonic);

    if (p1_errors > 0 or p2_errors > 0) {
        std.debug.print("FAIL (processor errors: p1={d} p2={d})\n", .{ p1_errors, p2_errors });
        return error.ServerError;
    }

    const total_batches = p1_batches + p2_batches;
    if (total_batches != batch_count) {
        std.debug.print("FAIL (expected {d} batches processed, got {d} (p1={d} p2={d}))\n", .{
            batch_count, total_batches, p1_batches, p2_batches,
        });
        return error.ServerError;
    }

    // Verify no batches remain
    const remaining = try queryCount(allocator, DEST_DB, "SELECT count(*) FROM wal_batches");
    if (remaining > 0) {
        std.debug.print("FAIL ({d} batches not cleaned up)\n", .{remaining});
        return error.ServerError;
    }

    // Verify all 10 insert events were created (no duplicates, no misses)
    // Filter by data containing 'concurrent_' to exclude events from WAL
    // that the replication slot replays from prior tests
    const event_count = try queryCount(allocator, DEST_DB,
        \\SELECT count(*) FROM events
        \\ WHERE type='I' AND data::text LIKE '%concurrent_%'
    );
    if (event_count != 10) {
        std.debug.print("FAIL (expected 10 insert events, got {d})\n", .{event_count});
        return error.ServerError;
    }

    // Verify no duplicate transactions
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

    std.debug.print("OK ({d} batches: p1={d} p2={d}, {d} events, no duplicates)\n", .{
        total_batches, p1_batches, p2_batches, event_count,
    });
}

// (stress test moved to tests/concurrent_stress.zig — run via `zig build concurrent-stress-test`)

// (stress test moved to tests/concurrent_stress.zig — run via `zig build concurrent-stress-test`)

// =========================================================================
// Main
// =========================================================================
pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    io = init.io;

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

    testJsonbValues(allocator) catch {
        failures += 1;
    };

    testRelationSnapshots(allocator) catch {
        failures += 1;
    };

    testMetadataViaMessage(allocator) catch {
        failures += 1;
    };

    testMetadataViaTable(allocator) catch {
        failures += 1;
    };

    testStreamingEvents(allocator) catch {
        failures += 1;
    };

    testStreamingMetadataViaMessage(allocator) catch {
        failures += 1;
    };

    testProtoV4MetadataViaMessage(allocator) catch {
        failures += 1;
    };

    testInterleavedStreams(allocator) catch {
        failures += 1;
    };

    testDuplicateBatchCopy(allocator) catch {
        failures += 1;
    };

    testConcurrentProcessors(allocator) catch {
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
