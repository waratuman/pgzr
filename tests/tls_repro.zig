const std = @import("std");
const pgzr = @import("pgzr");

const Case = struct {
    name: []const u8,
    source_tls: pgzr.TlsMode,
    dest_tls: pgzr.TlsMode,
    proto_version: []const u8,
};

const ReproConfig = struct {
    source_host: []const u8,
    source_port: u16,
    source_user: []const u8,
    source_db: []const u8,
    dest_host: []const u8,
    dest_port: u16,
    dest_user: []const u8,
    dest_db: []const u8,
    slot_name: []const u8,
    publication_name: []const u8,
    source_id: []const u8,
    skip_setup: bool,
};

fn envOrDefault(name: []const u8, default: []const u8) []const u8 {
    return std.posix.getenv(name) orelse default;
}

fn envU16(name: []const u8, default: u16) u16 {
    const raw = std.posix.getenv(name) orelse return default;
    return std.fmt.parseInt(u16, raw, 10) catch default;
}

fn envBool(name: []const u8, default: bool) bool {
    const raw = std.posix.getenv(name) orelse return default;
    if (std.mem.eql(u8, raw, "1")) return true;
    if (std.mem.eql(u8, raw, "true")) return true;
    if (std.mem.eql(u8, raw, "yes")) return true;
    if (std.mem.eql(u8, raw, "0")) return false;
    if (std.mem.eql(u8, raw, "false")) return false;
    if (std.mem.eql(u8, raw, "no")) return false;
    return default;
}

fn loadConfig() ReproConfig {
    const user = envOrDefault("PGZR_TLS_REPRO_USER", envOrDefault("PGUSER", envOrDefault("USER", "postgres")));
    const source_host = envOrDefault("PGZR_TLS_REPRO_SOURCE_HOST", envOrDefault("PGHOST", "127.0.0.1"));
    const dest_host = envOrDefault("PGZR_TLS_REPRO_DEST_HOST", source_host);
    const source_port = envU16("PGZR_TLS_REPRO_SOURCE_PORT", envU16("PGPORT", 5432));
    const dest_port = envU16("PGZR_TLS_REPRO_DEST_PORT", source_port);

    return .{
        .source_host = source_host,
        .source_port = source_port,
        .source_user = user,
        .source_db = envOrDefault("PGZR_TLS_REPRO_SOURCE_DB", "pgzr_tls_repro_source"),
        .dest_host = dest_host,
        .dest_port = dest_port,
        .dest_user = user,
        .dest_db = envOrDefault("PGZR_TLS_REPRO_DEST_DB", "pgzr_tls_repro_dest"),
        .slot_name = envOrDefault("PGZR_TLS_REPRO_SLOT", "pgzr_tls_repro_slot"),
        .publication_name = envOrDefault("PGZR_TLS_REPRO_PUBLICATION", "pgzr_tls_repro_pub"),
        .source_id = envOrDefault("PGZR_TLS_REPRO_SOURCE_ID", "00000000-0000-0000-0000-000000000001"),
        .skip_setup = envBool("PGZR_TLS_REPRO_SKIP_SETUP", false),
    };
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

fn runPsqlFmt(allocator: std.mem.Allocator, db: []const u8, comptime fmt: []const u8, args: anytype) !void {
    const sql = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(sql);
    try runPsql(allocator, db, sql);
}

fn setup(allocator: std.mem.Allocator, cfg: ReproConfig) !void {
    if (cfg.skip_setup) {
        std.debug.print("setup skipped (PGZR_TLS_REPRO_SKIP_SETUP=1)\n", .{});
        return;
    }

    runCmd(allocator, &.{ "dropdb", "--if-exists", "--force", cfg.source_db }) catch {};
    runCmd(allocator, &.{ "dropdb", "--if-exists", "--force", cfg.dest_db }) catch {};
    try runCmd(allocator, &.{ "createdb", cfg.source_db });
    try runCmd(allocator, &.{ "createdb", cfg.dest_db });

    try runPsql(
        allocator,
        cfg.source_db,
        "CREATE TABLE IF NOT EXISTS tls_repro_items (id serial PRIMARY KEY, name text NOT NULL);",
    );
    runPsqlFmt(allocator, cfg.source_db, "DROP PUBLICATION IF EXISTS {s}", .{cfg.publication_name}) catch {};
    try runPsqlFmt(allocator, cfg.source_db, "CREATE PUBLICATION {s} FOR TABLE tls_repro_items;", .{cfg.publication_name});
    runPsqlFmt(allocator, cfg.source_db, "SELECT pg_drop_replication_slot('{s}');", .{cfg.slot_name}) catch {};
    try runPsqlFmt(allocator, cfg.source_db, "SELECT pg_create_logical_replication_slot('{s}', 'pgoutput');", .{cfg.slot_name});

    var dest = try pgzr.Connection.connect(allocator, .{
        .host = cfg.dest_host,
        .port = cfg.dest_port,
        .user = cfg.dest_user,
        .database = cfg.dest_db,
        .replication = false,
        .tls = .disable,
    });
    defer dest.close();
    try pgzr.schema.ensureSchema(&dest);
}

fn teardown(allocator: std.mem.Allocator, cfg: ReproConfig) void {
    if (cfg.skip_setup) return;

    runPsqlFmt(allocator, cfg.source_db, "SELECT pg_drop_replication_slot('{s}');", .{cfg.slot_name}) catch {};
    runPsqlFmt(allocator, cfg.source_db, "DROP PUBLICATION IF EXISTS {s}", .{cfg.publication_name}) catch {};
    runCmd(allocator, &.{ "dropdb", "--if-exists", "--force", cfg.source_db }) catch {};
    runCmd(allocator, &.{ "dropdb", "--if-exists", "--force", cfg.dest_db }) catch {};
}

fn runCase(allocator: std.mem.Allocator, cfg: ReproConfig, c: Case) void {
    std.debug.print("\n[{s}] source_tls={s} dest_tls={s} proto={s}\n", .{
        c.name,
        @tagName(c.source_tls),
        @tagName(c.dest_tls),
        c.proto_version,
    });

    var ingestor = pgzr.Ingestor.init(allocator, .{
        .source = .{
            .conn = .{
                .host = cfg.source_host,
                .port = cfg.source_port,
                .user = cfg.source_user,
                .database = cfg.source_db,
                .tls = c.source_tls,
            },
            .slot_name = cfg.slot_name,
            .options = &.{
                .{ "proto_version", c.proto_version },
                .{ "publication_names", cfg.publication_name },
            },
        },
        .dest = .{
            .host = cfg.dest_host,
            .port = cfg.dest_port,
            .user = cfg.dest_user,
            .database = cfg.dest_db,
            .replication = false,
            .tls = c.dest_tls,
        },
        .source_id = cfg.source_id,
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
    const cfg = loadConfig();

    std.debug.print("TLS repro matrix (set PGZR_TLS_DEBUG=1 for phase logs)\n", .{});
    std.debug.print(
        "source={s}:{d}/{s} dest={s}:{d}/{s} slot={s} pub={s} skip_setup={}\n",
        .{
            cfg.source_host,
            cfg.source_port,
            cfg.source_db,
            cfg.dest_host,
            cfg.dest_port,
            cfg.dest_db,
            cfg.slot_name,
            cfg.publication_name,
            cfg.skip_setup,
        },
    );

    try setup(allocator, cfg);
    defer teardown(allocator, cfg);

    const cases = [_]Case{
        .{ .name = "both tls", .source_tls = .prefer, .dest_tls = .require, .proto_version = "4" },
        .{ .name = "both tls proto1", .source_tls = .prefer, .dest_tls = .require, .proto_version = "1" },
        .{ .name = "source tls only", .source_tls = .prefer, .dest_tls = .disable, .proto_version = "4" },
        .{ .name = "dest tls only", .source_tls = .disable, .dest_tls = .require, .proto_version = "4" },
        .{ .name = "dest tls only proto1", .source_tls = .disable, .dest_tls = .require, .proto_version = "1" },
        .{ .name = "both no tls", .source_tls = .disable, .dest_tls = .disable, .proto_version = "4" },
    };

    for (cases) |c| runCase(allocator, cfg, c);
}
