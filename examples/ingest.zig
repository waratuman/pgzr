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
    const source_db = getenv("SOURCE_DB") orelse "pgzr_source";
    const dest_db = getenv("DEST_DB") orelse "pgzr_dest";
    const slot_name = getenv("SLOT_NAME") orelse "pgzr_ingest_slot";
    const source_id = getenv("SOURCE_ID") orelse "00000000-0000-0000-0000-000000000001";

    // Stage 1: Ingest — replicate WAL from source into dest as packed batches
    var ingestor = pgzr.Ingestor.init(allocator, io, .{
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
