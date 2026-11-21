const std = @import("std");
const pgzr = @import("pgzr");

fn getenv(name: [*:0]const u8) ?[]const u8 {
    const p = std.c.getenv(name) orelse return null;
    return std.mem.span(p);
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    const user = getenv("PGUSER") orelse getenv("USER") orelse "postgres";

    std.debug.print("SSL Ingest Test: 50k rows (~17MB) with TLS require\n", .{});
    std.debug.print("==================================================\n\n", .{});

    const start_ts = std.Io.Clock.awake.now(io);

    var ingestor = pgzr.Ingestor.init(allocator, io, .{
        .source = .{
            .conn = .{
                .host = "127.0.0.1",
                .port = 5432,
                .user = user,
                .database = "pgzr_ssl_test",
                .tls = .require,
            },
            .slot_name = "pgzr_ssl_slot",
            .options = &.{
                .{ "proto_version", "1" },
                .{ "publication_names", "pgzr_ssl_pub" },
            },
        },
        .dest = .{
            .host = "127.0.0.1",
            .port = 5432,
            .user = user,
            .database = "pgzr_ssl_dest",
            .replication = false,
            .tls = .require,
        },
        .source_id = "00000000-0000-0000-0000-000000000001",
        .max_batch_size = 4 * 1024 * 1024,
    }) catch |err| {
        std.debug.print("Init failed: {}\n", .{err});
        return err;
    };
    defer ingestor.deinit();

    std.debug.print("Connected (TLS). Ingesting...\n\n", .{});

    ingestor.run() catch |err| {
        std.debug.print("Run error: {}\n", .{err});
        return err;
    };

    const elapsed_ns: u64 = @intCast(start_ts.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds());
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    std.debug.print("\nDone in {d:.1}ms\n", .{elapsed_ms});
}
