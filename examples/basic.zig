const std = @import("std");
const pgzr = @import("pgzr");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    var repl = pgzr.Replicator.init(allocator, io, .{
        .conn = .{
            .host = "127.0.0.1",
            .port = 5432,
            .user = "postgres",
            .database = "pgzr_test",
        },
        .slot_name = "pgzr_test_slot",
        .options = &.{
            .{ "include-timestamp", "on" },
        },
    }) catch |err| {
        std.debug.print("Failed to connect: {}\n", .{err});
        return;
    };
    defer repl.deinit();

    std.debug.print("Connected. Streaming from slot 'pgzr_test_slot'...\n", .{});

    while (true) {
        const msg = repl.next() catch |err| {
            std.debug.print("Error: {}\n", .{err});
            break;
        } orelse break;

        std.debug.print("{s}\n", .{msg.data});
        repl.ack(msg.wal_start);
    }

    std.debug.print("Replication ended.\n", .{});
}
