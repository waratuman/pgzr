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
/// Handles dates before Unix epoch (1970) by clamping to epoch.
/// Caller owns the returned slice.
pub fn formatTimestamp(allocator: std.mem.Allocator, pg_usec: i64) ![]u8 {
    // PostgreSQL epoch is 2000-01-01 00:00:00 UTC
    // Unix epoch offset: seconds between 1970-01-01 and 2000-01-01
    const pg_epoch_offset_us: i64 = 946_684_800 * 1_000_000;
    const unix_us = pg_usec + pg_epoch_offset_us;

    // Clamp to Unix epoch — pre-1970 timestamps are rare edge cases from pgoutput
    // and would panic on the u64 cast. We preserve them as epoch (1970-01-01).
    const clamped_us = @max(unix_us, 0);
    const unix_sec = @divFloor(clamped_us, @as(i64, 1_000_000));
    const frac_us: u64 = @intCast(@mod(clamped_us, 1_000_000));

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
/// Validates UUID format (36 chars: 8-4-4-4-12 hex with dashes).
/// Returns error.InvalidUuid if the input is not a valid UUID string.
/// Caller owns the returned slice.
pub fn escapeUuid(allocator: std.mem.Allocator, uuid: []const u8) ![]u8 {
    if (!isValidUuid(uuid)) return error.InvalidUuid;
    const buf = try allocator.alloc(u8, uuid.len + 2);
    buf[0] = '\'';
    @memcpy(buf[1 .. 1 + uuid.len], uuid);
    buf[buf.len - 1] = '\'';
    return buf;
}

fn isValidUuid(s: []const u8) bool {
    if (s.len != 36) return false;
    const dash_positions = [_]usize{ 8, 13, 18, 23 };
    for (s, 0..) |c, i| {
        var is_dash_pos = false;
        for (dash_positions) |dp| {
            if (i == dp) {
                is_dash_pos = true;
                break;
            }
        }
        if (is_dash_pos) {
            if (c != '-') return false;
        } else {
            switch (c) {
                '0'...'9', 'a'...'f', 'A'...'F' => {},
                else => return false,
            }
        }
    }
    return true;
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

test "escapeUuid: rejects empty string" {
    try std.testing.expectError(error.InvalidUuid, escapeUuid(std.testing.allocator, ""));
}

test "escapeUuid: rejects wrong length" {
    try std.testing.expectError(error.InvalidUuid, escapeUuid(std.testing.allocator, "550e8400-e29b-41d4-a716"));
}

test "escapeUuid: rejects missing dashes" {
    try std.testing.expectError(error.InvalidUuid, escapeUuid(std.testing.allocator, "550e8400xe29bx41d4xa716x446655440000"));
}

test "escapeUuid: rejects non-hex characters" {
    try std.testing.expectError(error.InvalidUuid, escapeUuid(std.testing.allocator, "550e8400-e29b-41d4-a716-44665544000g"));
}

test "escapeUuid: rejects SQL injection attempt" {
    try std.testing.expectError(error.InvalidUuid, escapeUuid(std.testing.allocator, "'; DROP TABLE users; --aaaaaaaaaaaaaa"));
}

test "escapeUuid: accepts uppercase hex" {
    const result = try escapeUuid(std.testing.allocator, "550E8400-E29B-41D4-A716-446655440000");
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("'550E8400-E29B-41D4-A716-446655440000'", result);
}

test "formatTimestamp: pre-epoch clamps to 1970" {
    // A timestamp representing a date before 1970 (negative unix_us)
    // PG epoch is 2000-01-01. -946_684_800_000_001 us would be just before 1970-01-01
    const pg_usec: i64 = -946_684_800 * 1_000_000 - 1;
    const result = try formatTimestamp(std.testing.allocator, pg_usec);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("'1970-01-01 00:00:00.000000+00'", result);
}

test "formatTimestamp: exactly unix epoch" {
    // PG timestamp for 1970-01-01 00:00:00 UTC
    const pg_usec: i64 = -946_684_800 * 1_000_000;
    const result = try formatTimestamp(std.testing.allocator, pg_usec);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("'1970-01-01 00:00:00.000000+00'", result);
}

test "formatTimestamp: pg epoch (2000-01-01)" {
    const result = try formatTimestamp(std.testing.allocator, 0);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("'2000-01-01 00:00:00.000000+00'", result);
}

test "isValidUuid" {
    try std.testing.expect(isValidUuid("550e8400-e29b-41d4-a716-446655440000"));
    try std.testing.expect(isValidUuid("00000000-0000-0000-0000-000000000000"));
    try std.testing.expect(!isValidUuid(""));
    try std.testing.expect(!isValidUuid("not-a-uuid"));
    try std.testing.expect(!isValidUuid("550e8400e29b41d4a716446655440000")); // no dashes
    try std.testing.expect(!isValidUuid("550e8400-e29b-41d4-a716-44665544000")); // too short
}

// ── OOM tests ─────────────────────────────────────────────────────────

test "escapeString: OOM" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, escapeString(failing.allocator(), "hello"));
    try std.testing.expect(failing.has_induced_failure);
}

test "escapeBytea: OOM" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, escapeBytea(failing.allocator(), &[_]u8{ 0xde, 0xad }));
    try std.testing.expect(failing.has_induced_failure);
}

test "formatInt: OOM" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, formatInt(failing.allocator(), 42));
    try std.testing.expect(failing.has_induced_failure);
}

test "formatTimestamp: OOM" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, formatTimestamp(failing.allocator(), 0));
    try std.testing.expect(failing.has_induced_failure);
}

test "formatNull: OOM" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, formatNull(failing.allocator()));
    try std.testing.expect(failing.has_induced_failure);
}

test "escapeUuid: OOM" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, escapeUuid(failing.allocator(), "550e8400-e29b-41d4-a716-446655440000"));
    try std.testing.expect(failing.has_induced_failure);
}
