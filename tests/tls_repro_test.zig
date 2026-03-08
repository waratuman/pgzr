const std = @import("std");
const pgzr = @import("pgzr");

pub const std_options: std.Options = .{
    .log_level = .debug,
};

fn getEnv(key: []const u8) ?[]const u8 {
    return std.posix.getenv(key);
}

fn getEnvRequired(key: []const u8) []const u8 {
    return std.posix.getenv(key) orelse {
        std.debug.print("Missing required env var: {s}\n", .{key});
        std.process.exit(1);
    };
}

fn getEnvPort(key: []const u8) u16 {
    const val = getEnvRequired(key);
    return std.fmt.parseUnsigned(u16, val, 10) catch {
        std.debug.print("Invalid port in {s}: {s}\n", .{ key, val });
        std.process.exit(1);
    };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const source_host = getEnvRequired("PGZR_TLS_REPRO_SOURCE_HOST");
    const source_port = getEnvPort("PGZR_TLS_REPRO_SOURCE_PORT");
    const source_user = getEnvRequired("PGZR_TLS_REPRO_SOURCE_USER");
    const source_pass = getEnvRequired("PGZR_TLS_REPRO_SOURCE_PASS");
    const source_db = getEnvRequired("PGZR_TLS_REPRO_SOURCE_DB");

    const dest_host = getEnvRequired("PGZR_TLS_REPRO_DEST_HOST");
    const dest_port = getEnvPort("PGZR_TLS_REPRO_DEST_PORT");
    const dest_db = getEnvRequired("PGZR_TLS_REPRO_DEST_DB");
    const dest_user = getEnvRequired("PGZR_TLS_REPRO_USER");
    const dest_pass = getEnvRequired("PGZR_TLS_REPRO_PASSWORD");

    const slot = getEnvRequired("PGZR_TLS_REPRO_SLOT");
    const publication = getEnvRequired("PGZR_TLS_REPRO_PUBLICATION");
    const source_id = getEnv("PGZR_TLS_REPRO_SOURCE_ID") orelse "00000000-0000-0000-0000-000000000001";

    const debug = getEnv("PGZR_TLS_DEBUG") != null;

    std.debug.print("TLS Replication Reproduction Test\n", .{});
    std.debug.print("=================================\n", .{});
    std.debug.print("Source: {s}@{s}:{d}/{s}\n", .{ source_user, source_host, source_port, source_db });
    std.debug.print("Dest:   {s}@{s}:{d}/{s}\n", .{ dest_user, dest_host, dest_port, dest_db });
    std.debug.print("Slot:   {s}  Publication: {s}\n", .{ slot, publication });
    std.debug.print("TLS:    require (no cert verification)\n", .{});
    if (debug) std.debug.print("Debug:  enabled\n", .{});
    std.debug.print("\n", .{});

    var timer = std.time.Timer.start() catch unreachable;

    var ingestor = pgzr.Ingestor.init(allocator, .{
        .source = .{
            .conn = .{
                .host = source_host,
                .port = source_port,
                .user = source_user,
                .password = source_pass,
                .database = source_db,
                .tls = .require,
            },
            .slot_name = slot,
            .options = &.{
                .{ "proto_version", "1" },
                .{ "publication_names", publication },
            },
        },
        .dest = .{
            .host = dest_host,
            .port = dest_port,
            .user = dest_user,
            .password = dest_pass,
            .database = dest_db,
            .replication = false,
            .tls = .require,
        },
        .source_id = source_id,
        .max_batch_size = 4 * 1024 * 1024,
    }) catch |err| {
        std.debug.print("Init failed: {}\n", .{err});
        return err;
    };
    defer ingestor.deinit();

    const init_ms = @as(f64, @floatFromInt(timer.read())) / 1_000_000.0;
    std.debug.print("Connected (TLS) in {d:.1}ms. Ingesting...\n\n", .{init_ms});

    ingestor.run() catch |err| {
        const elapsed_ms = @as(f64, @floatFromInt(timer.read())) / 1_000_000.0;
        std.debug.print("Run error after {d:.1}ms: {}\n", .{ elapsed_ms, err });
        return err;
    };

    const elapsed_ms = @as(f64, @floatFromInt(timer.read())) / 1_000_000.0;
    std.debug.print("\nDone in {d:.1}ms\n", .{elapsed_ms});
}
