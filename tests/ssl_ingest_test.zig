const std = @import("std");
const pgzr = @import("pgzr");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        if (gpa.deinit() == .leak) {
            std.debug.print("\nMemory leak detected!\n", .{});
            std.process.exit(1);
        }
    }
    const allocator = gpa.allocator();

    const user = std.posix.getenv("PGUSER") orelse std.posix.getenv("USER") orelse "postgres";

    std.debug.print("SSL Ingest Test: 50k rows (~17MB) with TLS require\n", .{});
    std.debug.print("==================================================\n\n", .{});

    var timer = std.time.Timer.start() catch unreachable;

    var ingestor = pgzr.Ingestor.init(allocator, .{
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

    const elapsed_ns = timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    std.debug.print("\nDone in {d:.1}ms\n", .{elapsed_ms});
}
