const std = @import("std");

/// Escape a text value for use in a SQL string literal.
/// Doubles any single quotes and wraps in single quotes: 'value'
/// Caller owns the returned slice.
pub fn escapeString(allocator: std.mem.Allocator, value: []const u8) ![]u8 {
    // Count single quotes to determine output size
    var quote_count: usize = 0;
    for (value) |c| {
        if (c == '\'') quote_count += 1;
    }

    // 2 for surrounding quotes + len + extra quotes for escaping
    const out_len = 2 + value.len + quote_count;
    const buf = try allocator.alloc(u8, out_len);

    buf[0] = '\'';
    var pos: usize = 1;
    for (value) |c| {
        if (c == '\'') {
            buf[pos] = '\'';
            pos += 1;
        }
        buf[pos] = c;
        pos += 1;
    }
    buf[pos] = '\'';

    return buf;
}

/// Escape a bytea value as a hex-encoded literal: '\x...'
/// Caller owns the returned slice.
pub fn escapeBytea(allocator: std.mem.Allocator, value: []const u8) ![]u8 {
    // '\x' + 2 hex chars per byte + closing quote
    const out_len = 4 + value.len * 2;
    const buf = try allocator.alloc(u8, out_len);

    buf[0] = '\'';
    buf[1] = '\\';
    buf[2] = 'x';
    const hex = "0123456789abcdef";
    for (value, 0..) |byte, i| {
        buf[3 + i * 2] = hex[byte >> 4];
        buf[3 + i * 2 + 1] = hex[byte & 0x0f];
    }
    buf[out_len - 1] = '\'';

    return buf;
}

/// Format an integer as a SQL literal (no quotes).
/// Caller owns the returned slice.
pub fn formatInt(allocator: std.mem.Allocator, value: i64) ![]u8 {
    var tmp: [20]u8 = undefined;
    const slice = std.fmt.bufPrint(&tmp, "{d}", .{value}) catch unreachable;
    const buf = try allocator.alloc(u8, slice.len);
    @memcpy(buf, slice);
    return buf;
}

/// Format a PostgreSQL timestamp (microseconds since 2000-01-01 00:00:00 UTC)
/// as an ISO 8601 SQL timestamp literal: '2024-01-15 12:34:56.789012+00'
/// Caller owns the returned slice.
pub fn formatTimestamp(allocator: std.mem.Allocator, pg_usec: i64) ![]u8 {
    // PostgreSQL epoch is 2000-01-01 00:00:00 UTC
    // Unix epoch offset: seconds between 1970-01-01 and 2000-01-01
    const pg_epoch_offset_us: i64 = 946_684_800 * 1_000_000;
    const unix_us = pg_usec + pg_epoch_offset_us;
    const unix_sec = @divFloor(unix_us, 1_000_000);
    const frac_us: u64 = @intCast(@mod(unix_us, 1_000_000));

    const epoch_secs: u64 = @intCast(unix_sec);
    const es = std.time.epoch.EpochSeconds{ .secs = epoch_secs };
    const day = es.getEpochDay();
    const yd = day.calculateYearDay();
    const md = yd.calculateMonthDay();
    const ds = es.getDaySeconds();

    var buf: [40]u8 = undefined;
    const result = std.fmt.bufPrint(&buf, "'{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}+00'", .{
        yd.year,
        @as(u32, @intFromEnum(md.month)),
        @as(u32, md.day_index) + 1,
        ds.getHoursIntoDay(),
        ds.getMinutesIntoHour(),
        ds.getSecondsIntoMinute(),
        frac_us,
    }) catch unreachable;

    const out = try allocator.alloc(u8, result.len);
    @memcpy(out, result);
    return out;
}

/// Write SQL NULL literal.
/// Caller owns the returned slice.
pub fn formatNull(allocator: std.mem.Allocator) ![]u8 {
    const buf = try allocator.alloc(u8, 4);
    @memcpy(buf, "NULL");
    return buf;
}

/// Write a UUID string as a SQL literal: 'xxxxxxxx-xxxx-...'
/// Input must be 36-byte UUID string. No validation performed.
/// Caller owns the returned slice.
pub fn escapeUuid(allocator: std.mem.Allocator, uuid: []const u8) ![]u8 {
    const buf = try allocator.alloc(u8, uuid.len + 2);
    buf[0] = '\'';
    @memcpy(buf[1 .. 1 + uuid.len], uuid);
    buf[buf.len - 1] = '\'';
    return buf;
}

// ── Tests ─────────────────────────────────────────────────────────────

test "escapeString: simple" {
    const result = try escapeString(std.testing.allocator, "hello");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("'hello'", result);
}

test "escapeString: with single quotes" {
    const result = try escapeString(std.testing.allocator, "it's a test");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("'it''s a test'", result);
}

test "escapeString: empty" {
    const result = try escapeString(std.testing.allocator, "");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("''", result);
}

test "escapeBytea: simple" {
    const result = try escapeBytea(std.testing.allocator, &[_]u8{ 0xde, 0xad, 0xbe, 0xef });
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("'\\xdeadbeef'", result);
}

test "escapeBytea: empty" {
    const result = try escapeBytea(std.testing.allocator, &[_]u8{});
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("'\\x'", result);
}

test "formatInt: positive" {
    const result = try formatInt(std.testing.allocator, 42);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("42", result);
}

test "formatInt: negative" {
    const result = try formatInt(std.testing.allocator, -100);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("-100", result);
}

test "formatInt: zero" {
    const result = try formatInt(std.testing.allocator, 0);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("0", result);
}

test "formatTimestamp: known value" {
    // 2024-01-15 12:00:00.000000 UTC
    // Seconds from 2000-01-01 to 2024-01-15 12:00:00:
    // 24 years, accounting for leap years
    const pg_usec: i64 = 758_635_200 * 1_000_000; // pre-calculated
    const result = try formatTimestamp(std.testing.allocator, pg_usec);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("'2024-01-15 12:00:00.000000+00'", result);
}

test "formatTimestamp: with microseconds" {
    const pg_usec: i64 = 758_635_200 * 1_000_000 + 123456;
    const result = try formatTimestamp(std.testing.allocator, pg_usec);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("'2024-01-15 12:00:00.123456+00'", result);
}

test "formatNull" {
    const result = try formatNull(std.testing.allocator);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("NULL", result);
}

test "escapeUuid" {
    const result = try escapeUuid(std.testing.allocator, "550e8400-e29b-41d4-a716-446655440000");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("'550e8400-e29b-41d4-a716-446655440000'", result);
}
