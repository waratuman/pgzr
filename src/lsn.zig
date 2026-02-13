const std = @import("std");

pub const Lsn = struct {
    value: u64,

    pub const zero: Lsn = .{ .value = 0 };

    /// Parse "X/X" text format (e.g. "0/16B3748" or "3B/6C036B08").
    pub fn parse(text: []const u8) error{InvalidLsn}!Lsn {
        const slash = std.mem.indexOfScalar(u8, text, '/') orelse
            return error.InvalidLsn;
        const hi = std.fmt.parseUnsigned(u32, text[0..slash], 16) catch
            return error.InvalidLsn;
        const lo = std.fmt.parseUnsigned(u32, text[slash + 1 ..], 16) catch
            return error.InvalidLsn;
        return .{ .value = (@as(u64, hi) << 32) | @as(u64, lo) };
    }

    /// Format as "X/X" (uppercase hex, no leading zeros per segment).
    pub fn format(self: Lsn, writer: anytype) !void {
        const hi: u32 = @truncate(self.value >> 32);
        const lo: u32 = @truncate(self.value);
        try writer.print("{X}/{X}", .{ hi, lo });
    }

    /// Increment by 1 (for "last written + 1" semantics in standby update).
    pub fn inc(self: Lsn) Lsn {
        return .{ .value = self.value +| 1 };
    }

    /// Read from 8 big-endian bytes.
    pub fn readBig(buf: *const [8]u8) Lsn {
        return .{ .value = std.mem.readInt(u64, buf, .big) };
    }

    /// Write as 8 big-endian bytes.
    pub fn writeBig(self: Lsn, buf: *[8]u8) void {
        std.mem.writeInt(u64, buf, self.value, .big);
    }
};

test "parse and format round-trip" {
    const lsn = try Lsn.parse("0/16B3748");
    var buf: [32]u8 = undefined;
    const s = try std.fmt.bufPrint(&buf, "{f}", .{lsn});
    try std.testing.expectEqualStrings("0/16B3748", s);
    try std.testing.expectEqual(@as(u64, 0x16B3748), lsn.value);
}

test "parse high/low segments" {
    const lsn = try Lsn.parse("3B/6C036B08");
    try std.testing.expectEqual(@as(u64, 0x3B_6C036B08), lsn.value);
    var buf: [32]u8 = undefined;
    const s = try std.fmt.bufPrint(&buf, "{f}", .{lsn});
    try std.testing.expectEqualStrings("3B/6C036B08", s);
}

test "parse zero" {
    const lsn = try Lsn.parse("0/0");
    try std.testing.expectEqual(@as(u64, 0), lsn.value);
}

test "parse max" {
    const lsn = try Lsn.parse("FFFFFFFF/FFFFFFFF");
    try std.testing.expectEqual(@as(u64, 0xFFFFFFFFFFFFFFFF), lsn.value);
}

test "parse invalid" {
    try std.testing.expectError(error.InvalidLsn, Lsn.parse("invalid"));
    try std.testing.expectError(error.InvalidLsn, Lsn.parse("no_slash"));
    try std.testing.expectError(error.InvalidLsn, Lsn.parse("/"));
}

test "readBig and writeBig round-trip" {
    const original = Lsn{ .value = 0x0000003B6C036B08 };
    var buf: [8]u8 = undefined;
    original.writeBig(&buf);
    const restored = Lsn.readBig(&buf);
    try std.testing.expectEqual(original.value, restored.value);
}

test "inc" {
    const lsn = Lsn{ .value = 42 };
    try std.testing.expectEqual(@as(u64, 43), lsn.inc().value);
}

test "inc saturates" {
    const lsn = Lsn{ .value = std.math.maxInt(u64) };
    try std.testing.expectEqual(std.math.maxInt(u64), lsn.inc().value);
}
