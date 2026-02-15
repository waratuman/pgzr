const std = @import("std");
const pgzr = @import("pgzr");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const user = std.posix.getenv("PGUSER") orelse std.posix.getenv("USER") orelse "postgres";
    const source_db = std.posix.getenv("SOURCE_DB") orelse "pgzr_source";
    const dest_db = std.posix.getenv("DEST_DB") orelse "pgzr_dest";
    const slot_name = std.posix.getenv("SLOT_NAME") orelse "pgzr_ingest_slot";
    const source_id = std.posix.getenv("SOURCE_ID") orelse "00000000-0000-0000-0000-000000000001";

    // Stage 1: Ingest — replicate WAL from source into dest as packed batches
    var ingestor = pgzr.Ingestor.init(allocator, .{
        .source = .{
            .conn = .{
                .host = "127.0.0.1",
                .port = 5432,
                .user = user,
                .database = source_db,
            },
            .slot_name = slot_name,
            .options = &.{
                .{ "proto_version", "1" },
                .{ "publication_names", "pgzr_pub" },
            },
        },
        .dest = .{
            .host = "127.0.0.1",
            .port = 5432,
            .user = user,
            .database = dest_db,
            .replication = false,
        },
        .source_id = source_id,
        .max_batch_size = 4 * 1024 * 1024, // 4 MiB
    }) catch |err| {
        std.debug.print("Failed to initialize ingestor: {}\n", .{err});
        return;
    };
    defer ingestor.deinit();

    std.debug.print("Ingestor connected. Streaming WAL from '{s}' into '{s}'...\n", .{ source_db, dest_db });
    std.debug.print("Source ID: {s}\n", .{source_id});
    std.debug.print("Slot: {s}\n", .{slot_name});
    std.debug.print("Press Ctrl+C to stop.\n\n", .{});

    ingestor.run() catch |err| {
        std.debug.print("Ingestor error: {}\n", .{err});
        return;
    };

    std.debug.print("Ingestor finished.\n", .{});
}
