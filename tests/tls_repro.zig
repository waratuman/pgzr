const std = @import("std");
const pgzr = @import("pgzr");

const SOURCE_DB = "pgzr_tls_repro_source";
const DEST_DB = "pgzr_tls_repro_dest";
const SLOT_NAME = "pgzr_tls_repro_slot";
const PUB_NAME = "pgzr_tls_repro_pub";
const SOURCE_ID = "00000000-0000-0000-0000-000000000001";

const Case = struct {
    name: []const u8,
    source_tls: pgzr.TlsMode,
    dest_tls: pgzr.TlsMode,
    proto_version: []const u8,
};

fn getUser() []const u8 {
    return std.posix.getenv("PGUSER") orelse std.posix.getenv("USER") orelse "postgres";
}

fn runCmd(allocator: std.mem.Allocator, argv: []const []const u8) !void {
    var child = std.process.Child.init(argv, allocator);
    child.stdin_behavior = .Close;
    child.stdout_behavior = .Inherit;
    child.stderr_behavior = .Inherit;
    const term = try child.spawnAndWait();
    if (term.Exited != 0) return error.ChildProcessFailed;
}

fn runPsql(allocator: std.mem.Allocator, db: []const u8, sql: []const u8) !void {
    try runCmd(allocator, &.{ "psql", "-d", db, "-v", "ON_ERROR_STOP=1", "-c", sql });
}

fn setup(allocator: std.mem.Allocator) !void {
    runCmd(allocator, &.{ "dropdb", "--if-exists", "--force", SOURCE_DB }) catch {};
    runCmd(allocator, &.{ "dropdb", "--if-exists", "--force", DEST_DB }) catch {};
    try runCmd(allocator, &.{ "createdb", SOURCE_DB });
    try runCmd(allocator, &.{ "createdb", DEST_DB });

    try runPsql(
        allocator,
        SOURCE_DB,
        "CREATE TABLE IF NOT EXISTS tls_repro_items (id serial PRIMARY KEY, name text NOT NULL);",
    );
    runPsql(allocator, SOURCE_DB, "DROP PUBLICATION IF EXISTS " ++ PUB_NAME) catch {};
    try runPsql(allocator, SOURCE_DB, "CREATE PUBLICATION " ++ PUB_NAME ++ " FOR TABLE tls_repro_items;");
    runPsql(allocator, SOURCE_DB, "SELECT pg_drop_replication_slot('" ++ SLOT_NAME ++ "');") catch {};
    try runPsql(allocator, SOURCE_DB, "SELECT pg_create_logical_replication_slot('" ++ SLOT_NAME ++ "', 'pgoutput');");

    // Ensure destination schema exists for Ingestor.init -> ensureSchema.
    var dest = try pgzr.Connection.connect(allocator, .{
        .host = "127.0.0.1",
        .port = 5432,
        .user = getUser(),
        .database = DEST_DB,
        .replication = false,
        .tls = .disable,
    });
    defer dest.close();
    try pgzr.schema.ensureSchema(&dest);
}

fn teardown(allocator: std.mem.Allocator) void {
    runPsql(allocator, SOURCE_DB, "SELECT pg_drop_replication_slot('" ++ SLOT_NAME ++ "');") catch {};
    runPsql(allocator, SOURCE_DB, "DROP PUBLICATION IF EXISTS " ++ PUB_NAME) catch {};
    runCmd(allocator, &.{ "dropdb", "--if-exists", "--force", SOURCE_DB }) catch {};
    runCmd(allocator, &.{ "dropdb", "--if-exists", "--force", DEST_DB }) catch {};
}

fn runCase(allocator: std.mem.Allocator, c: Case) void {
    std.debug.print("\n[{s}] source_tls={s} dest_tls={s} proto={s}\n", .{
        c.name,
        @tagName(c.source_tls),
        @tagName(c.dest_tls),
        c.proto_version,
    });

    var ingestor = pgzr.Ingestor.init(allocator, .{
        .source = .{
            .conn = .{
                .host = "127.0.0.1",
                .port = 5432,
                .user = getUser(),
                .database = SOURCE_DB,
                .tls = c.source_tls,
            },
            .slot_name = SLOT_NAME,
            .options = &.{
                .{ "proto_version", c.proto_version },
                .{ "publication_names", PUB_NAME },
            },
        },
        .dest = .{
            .host = "127.0.0.1",
            .port = 5432,
            .user = getUser(),
            .database = DEST_DB,
            .replication = false,
            .tls = c.dest_tls,
        },
        .source_id = SOURCE_ID,
        .max_batch_size = 4 * 1024 * 1024,
    }) catch |err| {
        std.debug.print("init error: {}\n", .{err});
        return;
    };
    defer ingestor.deinit();

    std.debug.print("init ok\n", .{});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("TLS repro matrix (set PGZR_TLS_DEBUG=1 for phase logs)\n", .{});
    try setup(allocator);
    defer teardown(allocator);

    const cases = [_]Case{
        .{ .name = "both tls", .source_tls = .prefer, .dest_tls = .require, .proto_version = "4" },
        .{ .name = "both tls proto1", .source_tls = .prefer, .dest_tls = .require, .proto_version = "1" },
        .{ .name = "source tls only", .source_tls = .prefer, .dest_tls = .disable, .proto_version = "4" },
        .{ .name = "dest tls only", .source_tls = .disable, .dest_tls = .require, .proto_version = "4" },
        .{ .name = "dest tls only proto1", .source_tls = .disable, .dest_tls = .require, .proto_version = "1" },
        .{ .name = "both no tls", .source_tls = .disable, .dest_tls = .disable, .proto_version = "4" },
    };

    for (cases) |c| runCase(allocator, c);
}
