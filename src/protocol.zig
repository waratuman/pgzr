const std = @import("std");
const Lsn = @import("lsn.zig").Lsn;
const Transport = @import("transport.zig").Transport;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
pub const PROTOCOL_VERSION: u32 = 196608; // 3.0

// Backend message types
pub const MSG_AUTH: u8 = 'R';
pub const MSG_PARAM_STATUS: u8 = 'S';
pub const MSG_BACKEND_KEY: u8 = 'K';
pub const MSG_READY: u8 = 'Z';
pub const MSG_ROW_DESC: u8 = 'T';
pub const MSG_DATA_ROW: u8 = 'D';
pub const MSG_CMD_COMPLETE: u8 = 'C';
pub const MSG_ERROR: u8 = 'E';
pub const MSG_NOTICE: u8 = 'N';
pub const MSG_COPY_BOTH: u8 = 'W';
pub const MSG_COPY_DATA: u8 = 'd';
pub const MSG_COPY_DONE: u8 = 'c';

// Auth subtypes
pub const AUTH_OK: u32 = 0;
pub const AUTH_CLEARTEXT: u32 = 3;
pub const AUTH_MD5: u32 = 5;
pub const AUTH_SASL: u32 = 10;
pub const AUTH_SASL_CONTINUE: u32 = 11;
pub const AUTH_SASL_FINAL: u32 = 12;

// Replication streaming subtypes (inside CopyData payload)
pub const XLOG_DATA: u8 = 'w';
pub const KEEPALIVE: u8 = 'k';
pub const STANDBY_STATUS: u8 = 'r';

// ---------------------------------------------------------------------------
// Message header
// ---------------------------------------------------------------------------
pub const MessageHeader = struct {
    msg_type: u8,
    /// Length INCLUDING the 4 length bytes, EXCLUDING the type byte.
    length: u32,

    /// Number of body bytes (length minus the 4-byte length field itself).
    pub fn bodyLen(self: MessageHeader) u32 {
        return self.length -| 4;
    }
};

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

pub const ReadError = Transport.ReadError;

/// Read exactly `buf.len` bytes from the transport.
pub fn readExact(transport: Transport, buf: []u8) ReadError!void {
    var total: usize = 0;
    while (total < buf.len) {
        const n = try transport.read(buf[total..]);
        if (n == 0) return error.ConnectionClosed;
        total += n;
    }
}

/// Read a message header (1 byte type + 4 byte length).
pub fn readHeader(transport: Transport) ReadError!MessageHeader {
    var buf: [5]u8 = undefined;
    try readExact(transport, &buf);
    return .{
        .msg_type = buf[0],
        .length = std.mem.readInt(u32, buf[1..5], .big),
    };
}

pub const ReadBodyError = ReadError || error{ProtocolError};

/// Read the body of a message given its header into `buf`.
/// Returns the slice of body bytes.
pub fn readBody(transport: Transport, header: MessageHeader, buf: []u8) ReadBodyError![]u8 {
    const body_len = header.bodyLen();
    if (body_len > buf.len) return error.ProtocolError;
    try readExact(transport, buf[0..body_len]);
    return buf[0..body_len];
}

// ---------------------------------------------------------------------------
// Writing helpers
// ---------------------------------------------------------------------------

pub const MsgWriter = struct {
    buf: []u8,
    pos: usize = 0,

    pub fn writeByte(self: *MsgWriter, b: u8) void {
        self.buf[self.pos] = b;
        self.pos += 1;
    }

    pub fn writeInt16(self: *MsgWriter, v: u16) void {
        std.mem.writeInt(u16, self.buf[self.pos..][0..2], v, .big);
        self.pos += 2;
    }

    pub fn writeInt32(self: *MsgWriter, v: u32) void {
        std.mem.writeInt(u32, self.buf[self.pos..][0..4], v, .big);
        self.pos += 4;
    }

    pub fn writeInt64(self: *MsgWriter, v: u64) void {
        std.mem.writeInt(u64, self.buf[self.pos..][0..8], v, .big);
        self.pos += 8;
    }

    pub fn writeString(self: *MsgWriter, s: []const u8) void {
        @memcpy(self.buf[self.pos..][0..s.len], s);
        self.pos += s.len;
        self.buf[self.pos] = 0;
        self.pos += 1;
    }

    pub fn writeBytes(self: *MsgWriter, s: []const u8) void {
        @memcpy(self.buf[self.pos..][0..s.len], s);
        self.pos += s.len;
    }

    /// Patch the Int32 length field at `len_offset` with the distance
    /// from `len_offset` to the current position.
    pub fn patchLength(self: *MsgWriter, len_offset: usize) void {
        const length: u32 = @intCast(self.pos - len_offset);
        std.mem.writeInt(u32, self.buf[len_offset..][0..4], length, .big);
    }

    pub fn getWritten(self: *const MsgWriter) []const u8 {
        return self.buf[0..self.pos];
    }
};

// ---------------------------------------------------------------------------
// Encoding functions
// ---------------------------------------------------------------------------

/// Build a StartupMessage (no type byte; length + version + params + \0).
pub fn encodeStartup(
    buf: []u8,
    user: []const u8,
    database: []const u8,
) []const u8 {
    var w = MsgWriter{ .buf = buf };
    const len_offset: usize = 0;
    w.writeInt32(0); // placeholder for length
    w.writeInt32(PROTOCOL_VERSION);
    w.writeString("user");
    w.writeString(user);
    w.writeString("database");
    w.writeString(database);
    w.writeString("replication");
    w.writeString("database");
    w.writeByte(0); // final terminator
    w.patchLength(len_offset);
    return w.getWritten();
}

/// Build a PasswordMessage ('p' + length + password\0).
pub fn encodePassword(buf: []u8, password: []const u8) []const u8 {
    var w = MsgWriter{ .buf = buf };
    w.writeByte('p');
    const len_offset = w.pos;
    w.writeInt32(0);
    w.writeString(password);
    w.patchLength(len_offset);
    return w.getWritten();
}

/// Build a simple Query message ('Q' + length + query\0).
pub fn encodeQuery(buf: []u8, query: []const u8) []const u8 {
    var w = MsgWriter{ .buf = buf };
    w.writeByte('Q');
    const len_offset = w.pos;
    w.writeInt32(0);
    w.writeString(query);
    w.patchLength(len_offset);
    return w.getWritten();
}

/// Build a CopyData message containing a Standby Status Update.
pub fn encodeStandbyStatus(
    buf: []u8,
    written: Lsn,
    flushed: Lsn,
    applied: Lsn,
    timestamp: i64,
    reply_requested: bool,
) []const u8 {
    var w = MsgWriter{ .buf = buf };
    w.writeByte('d'); // CopyData wrapper
    const len_offset = w.pos;
    w.writeInt32(0); // placeholder

    w.writeByte('r'); // Standby status update
    w.writeInt64(written.value);
    w.writeInt64(flushed.value);
    w.writeInt64(applied.value);
    w.writeInt64(@bitCast(timestamp));
    w.writeByte(if (reply_requested) 1 else 0);

    w.patchLength(len_offset);
    return w.getWritten();
}

/// Build a SASLInitialResponse message ('p' + length + mechanism\0 + Int32(data.len) + data).
pub fn encodeSASLInitialResponse(buf: []u8, mechanism: []const u8, data: []const u8) []const u8 {
    var w = MsgWriter{ .buf = buf };
    w.writeByte('p');
    const len_offset = w.pos;
    w.writeInt32(0); // placeholder
    w.writeString(mechanism); // mechanism name + \0
    w.writeInt32(@intCast(data.len));
    w.writeBytes(data);
    w.patchLength(len_offset);
    return w.getWritten();
}

/// Build a SASLResponse message ('p' + length + data).
pub fn encodeSASLResponse(buf: []u8, data: []const u8) []const u8 {
    var w = MsgWriter{ .buf = buf };
    w.writeByte('p');
    const len_offset = w.pos;
    w.writeInt32(0); // placeholder
    w.writeBytes(data);
    w.patchLength(len_offset);
    return w.getWritten();
}

// ---------------------------------------------------------------------------
// Decoding helpers
// ---------------------------------------------------------------------------

/// Extract a null-terminated string from `data` starting at `offset`.
pub fn readCString(data: []const u8, offset: usize) error{ProtocolError}!struct { str: []const u8, next: usize } {
    const end = std.mem.indexOfScalarPos(u8, data, offset, 0) orelse
        return error.ProtocolError;
    return .{ .str = data[offset..end], .next = end + 1 };
}

pub const ErrorInfo = struct {
    severity: []const u8 = "",
    code: []const u8 = "",
    message: []const u8 = "",
    detail: []const u8 = "",
};

/// Parse an ErrorResponse/NoticeResponse body into its fields.
pub fn parseError(body: []const u8) ErrorInfo {
    var info = ErrorInfo{};
    var pos: usize = 0;
    while (pos < body.len) {
        const field_type = body[pos];
        if (field_type == 0) break;
        pos += 1;
        const result = readCString(body, pos) catch break;
        switch (field_type) {
            'S' => info.severity = result.str,
            'C' => info.code = result.str,
            'M' => info.message = result.str,
            'D' => info.detail = result.str,
            else => {},
        }
        pos = result.next;
    }
    return info;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "encodeStartup" {
    var buf: [4096]u8 = undefined;
    const msg = encodeStartup(&buf, "postgres", "mydb");

    // Length field (first 4 bytes, big-endian)
    const length = std.mem.readInt(u32, msg[0..4], .big);
    try std.testing.expectEqual(length, @as(u32, @intCast(msg.len)));

    // Protocol version
    const version = std.mem.readInt(u32, msg[4..8], .big);
    try std.testing.expectEqual(PROTOCOL_VERSION, version);

    // Should contain "user\0postgres\0database\0mydb\0replication\0database\0\0"
    try std.testing.expect(std.mem.indexOf(u8, msg, "user") != null);
    try std.testing.expect(std.mem.indexOf(u8, msg, "postgres") != null);
    try std.testing.expect(std.mem.indexOf(u8, msg, "database") != null);
    try std.testing.expect(std.mem.indexOf(u8, msg, "mydb") != null);
    try std.testing.expect(std.mem.indexOf(u8, msg, "replication") != null);
    // Last byte should be the double-null terminator
    try std.testing.expectEqual(@as(u8, 0), msg[msg.len - 1]);
}

test "encodeQuery" {
    var buf: [4096]u8 = undefined;
    const msg = encodeQuery(&buf, "IDENTIFY_SYSTEM");

    try std.testing.expectEqual(@as(u8, 'Q'), msg[0]);
    const length = std.mem.readInt(u32, msg[1..5], .big);
    try std.testing.expectEqual(length, @as(u32, @intCast(msg.len - 1)));
    // Query string starts at offset 5
    try std.testing.expect(std.mem.indexOf(u8, msg[5..], "IDENTIFY_SYSTEM") != null);
}

test "encodePassword" {
    var buf: [4096]u8 = undefined;
    const msg = encodePassword(&buf, "secret");

    try std.testing.expectEqual(@as(u8, 'p'), msg[0]);
    const length = std.mem.readInt(u32, msg[1..5], .big);
    try std.testing.expectEqual(length, @as(u32, @intCast(msg.len - 1)));
}

test "encodeStandbyStatus" {
    var buf: [4096]u8 = undefined;
    const lsn = Lsn{ .value = 100 };
    const msg = encodeStandbyStatus(&buf, lsn, lsn, lsn, 12345, false);

    try std.testing.expectEqual(@as(u8, 'd'), msg[0]); // CopyData
    // Inside: 'r' byte at offset 5
    try std.testing.expectEqual(@as(u8, 'r'), msg[5]);
    // Total CopyData body: 1 + 8 + 8 + 8 + 8 + 1 = 34 bytes
    const length = std.mem.readInt(u32, msg[1..5], .big);
    try std.testing.expectEqual(@as(u32, 38), length); // 4 (length field) + 34
}

test "readCString" {
    const data = "hello\x00world\x00";
    const r1 = try readCString(data, 0);
    try std.testing.expectEqualStrings("hello", r1.str);
    try std.testing.expectEqual(@as(usize, 6), r1.next);

    const r2 = try readCString(data, r1.next);
    try std.testing.expectEqualStrings("world", r2.str);
}

test "readCString no null" {
    const data = "no null terminator";
    try std.testing.expectError(error.ProtocolError, readCString(data, 0));
}

test "parseError" {
    // Simulate an ErrorResponse body: S\0severity\0 C\0code\0 M\0message\0 \0
    const body = "SERROR\x00C42P01\x00Mrelation does not exist\x00\x00";
    const info = parseError(body);
    try std.testing.expectEqualStrings("ERROR", info.severity);
    try std.testing.expectEqualStrings("42P01", info.code);
    try std.testing.expectEqualStrings("relation does not exist", info.message);
}

test "MsgWriter" {
    var buf: [64]u8 = undefined;
    var w = MsgWriter{ .buf = &buf };
    w.writeByte(0xAB);
    w.writeInt32(0x12345678);
    w.writeString("hi");

    const written = w.getWritten();
    try std.testing.expectEqual(@as(usize, 8), written.len); // 1 + 4 + 2 + 1
    try std.testing.expectEqual(@as(u8, 0xAB), written[0]);
    try std.testing.expectEqual(@as(u32, 0x12345678), std.mem.readInt(u32, written[1..5], .big));
    try std.testing.expectEqualStrings("hi", written[5..7]);
    try std.testing.expectEqual(@as(u8, 0), written[7]);
}
