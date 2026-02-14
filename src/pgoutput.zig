const std = @import("std");

/// Column data from a TupleData payload.
pub const ColumnData = union(enum) {
    /// Column is NULL.
    null_value,
    /// Column is unchanged (TOASTed value not sent).
    unchanged,
    /// Text-formatted column value (borrowed slice from message buffer).
    text: []const u8,
    /// Binary-formatted column value (borrowed slice from message buffer).
    binary: []const u8,
};

/// Relation column metadata.
pub const Column = struct {
    flags: u8,
    name: []const u8,
    type_oid: u32,
    type_modifier: i32,
};

pub const Begin = struct {
    final_lsn: u64,
    timestamp: i64,
    xid: u32,
};

pub const Commit = struct {
    flags: u8,
    lsn: u64,
    end_lsn: u64,
    timestamp: i64,
};

pub const Relation = struct {
    oid: u32,
    namespace: []const u8,
    name: []const u8,
    replica_identity: u8,
    columns: []const Column,
};

pub const Insert = struct {
    relation_oid: u32,
    new_tuple: []const ColumnData,
};

pub const Update = struct {
    relation_oid: u32,
    old_tuple: ?[]const ColumnData,
    new_tuple: []const ColumnData,
};

pub const Delete = struct {
    relation_oid: u32,
    old_tuple: []const ColumnData,
};

pub const Truncate = struct {
    options: u8,
    relation_count: u32,
    /// Raw big-endian packed OID data. Use `relationOid(index)` to read individual OIDs.
    oid_data: []const u8,

    pub fn relationOid(self: Truncate, index: u32) DecodeError!u32 {
        const offset = index * 4;
        if (offset + 4 > self.oid_data.len) return error.UnexpectedEnd;
        return std.mem.readInt(u32, self.oid_data[offset..][0..4], .big);
    }
};

pub const TypeInfo = struct {
    oid: u32,
    namespace: []const u8,
    name: []const u8,
};

pub const Origin = struct {
    lsn: u64,
    name: []const u8,
};

pub const LogicalMessage = struct {
    flags: u8,
    lsn: u64,
    prefix: []const u8,
    content: []const u8,
};

pub const PgoutputMessage = union(enum) {
    begin: Begin,
    commit: Commit,
    relation: Relation,
    insert: Insert,
    update: Update,
    delete: Delete,
    truncate: Truncate,
    type_info: TypeInfo,
    origin: Origin,
    message: LogicalMessage,
};

pub const DecodeError = error{
    InvalidMessage,
    UnexpectedEnd,
    UnknownMessageType,
};

/// Decode a pgoutput binary message from the WAL data payload.
/// All returned slices borrow from the input `data` buffer.
/// The caller must provide scratch buffers for columns and tuple data.
pub fn decode(
    data: []const u8,
    col_buf: []Column,
    tuple_buf: []ColumnData,
    tuple_buf2: []ColumnData,
) DecodeError!PgoutputMessage {
    if (data.len == 0) return error.InvalidMessage;

    return switch (data[0]) {
        'B' => .{ .begin = try decodeBegin(data[1..]) },
        'C' => .{ .commit = try decodeCommit(data[1..]) },
        'R' => .{ .relation = try decodeRelation(data[1..], col_buf) },
        'I' => .{ .insert = try decodeInsert(data[1..], tuple_buf) },
        'U' => .{ .update = try decodeUpdate(data[1..], tuple_buf, tuple_buf2) },
        'D' => .{ .delete = try decodeDelete(data[1..], tuple_buf) },
        'T' => .{ .truncate = try decodeTruncate(data[1..]) },
        'Y' => .{ .type_info = try decodeTypeInfo(data[1..]) },
        'O' => .{ .origin = try decodeOrigin(data[1..]) },
        'M' => .{ .message = try decodeLogicalMessage(data[1..]) },
        else => error.UnknownMessageType,
    };
}

fn readU8(data: []const u8, pos: *usize) DecodeError!u8 {
    if (pos.* >= data.len) return error.UnexpectedEnd;
    const v = data[pos.*];
    pos.* += 1;
    return v;
}

fn readI32(data: []const u8, pos: *usize) DecodeError!i32 {
    if (pos.* + 4 > data.len) return error.UnexpectedEnd;
    const v = std.mem.readInt(i32, data[pos.*..][0..4], .big);
    pos.* += 4;
    return v;
}

fn readU32(data: []const u8, pos: *usize) DecodeError!u32 {
    if (pos.* + 4 > data.len) return error.UnexpectedEnd;
    const v = std.mem.readInt(u32, data[pos.*..][0..4], .big);
    pos.* += 4;
    return v;
}

fn readU16(data: []const u8, pos: *usize) DecodeError!u16 {
    if (pos.* + 2 > data.len) return error.UnexpectedEnd;
    const v = std.mem.readInt(u16, data[pos.*..][0..2], .big);
    pos.* += 2;
    return v;
}

fn readU64(data: []const u8, pos: *usize) DecodeError!u64 {
    if (pos.* + 8 > data.len) return error.UnexpectedEnd;
    const v = std.mem.readInt(u64, data[pos.*..][0..8], .big);
    pos.* += 8;
    return v;
}

fn readI64(data: []const u8, pos: *usize) DecodeError!i64 {
    if (pos.* + 8 > data.len) return error.UnexpectedEnd;
    const v = std.mem.readInt(i64, data[pos.*..][0..8], .big);
    pos.* += 8;
    return v;
}

fn readCString(data: []const u8, pos: *usize) DecodeError![]const u8 {
    const start = pos.*;
    const end = std.mem.indexOfScalarPos(u8, data, start, 0) orelse
        return error.UnexpectedEnd;
    pos.* = end + 1;
    return data[start..end];
}

fn readBytes(data: []const u8, pos: *usize, n: usize) DecodeError![]const u8 {
    if (pos.* + n > data.len) return error.UnexpectedEnd;
    const slice = data[pos.* .. pos.* + n];
    pos.* += n;
    return slice;
}

fn decodeBegin(data: []const u8) DecodeError!Begin {
    var pos: usize = 0;
    const final_lsn = try readU64(data, &pos);
    const timestamp = try readI64(data, &pos);
    const xid = try readU32(data, &pos);
    return .{ .final_lsn = final_lsn, .timestamp = timestamp, .xid = xid };
}

fn decodeCommit(data: []const u8) DecodeError!Commit {
    var pos: usize = 0;
    const flags = try readU8(data, &pos);
    const lsn = try readU64(data, &pos);
    const end_lsn = try readU64(data, &pos);
    const timestamp = try readI64(data, &pos);
    return .{ .flags = flags, .lsn = lsn, .end_lsn = end_lsn, .timestamp = timestamp };
}

fn decodeRelation(data: []const u8, col_buf: []Column) DecodeError!Relation {
    var pos: usize = 0;
    const oid = try readU32(data, &pos);
    const namespace = try readCString(data, &pos);
    const name = try readCString(data, &pos);
    const replica_identity = try readU8(data, &pos);
    const col_count = try readU16(data, &pos);

    if (col_count > col_buf.len) return error.InvalidMessage;

    for (0..col_count) |i| {
        const flags = try readU8(data, &pos);
        const col_name = try readCString(data, &pos);
        const type_oid = try readU32(data, &pos);
        const type_mod = try readI32(data, &pos);
        col_buf[i] = .{
            .flags = flags,
            .name = col_name,
            .type_oid = type_oid,
            .type_modifier = type_mod,
        };
    }

    return .{
        .oid = oid,
        .namespace = namespace,
        .name = name,
        .replica_identity = replica_identity,
        .columns = col_buf[0..col_count],
    };
}

fn decodeTupleData(data: []const u8, pos: *usize, buf: []ColumnData) DecodeError![]const ColumnData {
    const col_count = try readU16(data, pos);
    if (col_count > buf.len) return error.InvalidMessage;

    for (0..col_count) |i| {
        const col_type = try readU8(data, pos);
        switch (col_type) {
            'n' => buf[i] = .null_value,
            'u' => buf[i] = .unchanged,
            't' => {
                const len: usize = @intCast(try readI32(data, pos));
                const val = try readBytes(data, pos, len);
                buf[i] = .{ .text = val };
            },
            'b' => {
                const len: usize = @intCast(try readI32(data, pos));
                const val = try readBytes(data, pos, len);
                buf[i] = .{ .binary = val };
            },
            else => return error.InvalidMessage,
        }
    }

    return buf[0..col_count];
}

fn decodeInsert(data: []const u8, tuple_buf: []ColumnData) DecodeError!Insert {
    var pos: usize = 0;
    const relation_oid = try readU32(data, &pos);
    const marker = try readU8(data, &pos);
    if (marker != 'N') return error.InvalidMessage;
    const new_tuple = try decodeTupleData(data, &pos, tuple_buf);
    return .{ .relation_oid = relation_oid, .new_tuple = new_tuple };
}

fn decodeUpdate(data: []const u8, tuple_buf: []ColumnData, tuple_buf2: []ColumnData) DecodeError!Update {
    var pos: usize = 0;
    const relation_oid = try readU32(data, &pos);

    var old_tuple: ?[]const ColumnData = null;
    const first_marker = try readU8(data, &pos);

    if (first_marker == 'K' or first_marker == 'O') {
        old_tuple = try decodeTupleData(data, &pos, tuple_buf2);
        const new_marker = try readU8(data, &pos);
        if (new_marker != 'N') return error.InvalidMessage;
    } else if (first_marker != 'N') {
        return error.InvalidMessage;
    }

    const new_tuple = try decodeTupleData(data, &pos, tuple_buf);
    return .{ .relation_oid = relation_oid, .old_tuple = old_tuple, .new_tuple = new_tuple };
}

fn decodeDelete(data: []const u8, tuple_buf: []ColumnData) DecodeError!Delete {
    var pos: usize = 0;
    const relation_oid = try readU32(data, &pos);
    const marker = try readU8(data, &pos);
    if (marker != 'K' and marker != 'O') return error.InvalidMessage;
    const old_tuple = try decodeTupleData(data, &pos, tuple_buf);
    return .{ .relation_oid = relation_oid, .old_tuple = old_tuple };
}

fn decodeTruncate(data: []const u8) DecodeError!Truncate {
    var pos: usize = 0;
    const rel_count = try readU32(data, &pos);
    const options = try readU8(data, &pos);
    const oids_start = pos;
    const oids_byte_len = rel_count * 4;
    if (pos + oids_byte_len > data.len) return error.UnexpectedEnd;
    return .{
        .options = options,
        .relation_count = rel_count,
        .oid_data = data[oids_start..][0..oids_byte_len],
    };
}

fn decodeTypeInfo(data: []const u8) DecodeError!TypeInfo {
    var pos: usize = 0;
    const oid = try readU32(data, &pos);
    const namespace = try readCString(data, &pos);
    const name = try readCString(data, &pos);
    return .{ .oid = oid, .namespace = namespace, .name = name };
}

fn decodeOrigin(data: []const u8) DecodeError!Origin {
    var pos: usize = 0;
    const lsn = try readU64(data, &pos);
    const name = try readCString(data, &pos);
    return .{ .lsn = lsn, .name = name };
}

fn decodeLogicalMessage(data: []const u8) DecodeError!LogicalMessage {
    var pos: usize = 0;
    const flags = try readU8(data, &pos);
    const lsn = try readU64(data, &pos);
    const prefix = try readCString(data, &pos);
    const content_len: usize = @intCast(try readU32(data, &pos));
    const content = try readBytes(data, &pos, content_len);
    return .{ .flags = flags, .lsn = lsn, .prefix = prefix, .content = content };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "decode Begin" {
    // 'B' + Int64(lsn=100) + Int64(timestamp=200) + Int32(xid=42)
    var buf: [21]u8 = undefined;
    buf[0] = 'B';
    std.mem.writeInt(u64, buf[1..9], 100, .big);
    std.mem.writeInt(i64, buf[9..17], 200, .big);
    std.mem.writeInt(u32, buf[17..21], 42, .big);

    var col_buf: [0]Column = undefined;
    var tuple_buf: [0]ColumnData = undefined;
    var tuple_buf2: [0]ColumnData = undefined;
    const msg = try decode(&buf, &col_buf, &tuple_buf, &tuple_buf2);
    const begin = msg.begin;
    try std.testing.expectEqual(@as(u64, 100), begin.final_lsn);
    try std.testing.expectEqual(@as(i64, 200), begin.timestamp);
    try std.testing.expectEqual(@as(u32, 42), begin.xid);
}

test "decode Commit" {
    // 'C' + Int8(flags=0) + Int64(lsn=100) + Int64(end_lsn=200) + Int64(timestamp=300)
    var buf: [26]u8 = undefined;
    buf[0] = 'C';
    buf[1] = 0;
    std.mem.writeInt(u64, buf[2..10], 100, .big);
    std.mem.writeInt(u64, buf[10..18], 200, .big);
    std.mem.writeInt(i64, buf[18..26], 300, .big);

    var col_buf: [0]Column = undefined;
    var tuple_buf: [0]ColumnData = undefined;
    var tuple_buf2: [0]ColumnData = undefined;
    const msg = try decode(&buf, &col_buf, &tuple_buf, &tuple_buf2);
    const commit = msg.commit;
    try std.testing.expectEqual(@as(u64, 100), commit.lsn);
    try std.testing.expectEqual(@as(u64, 200), commit.end_lsn);
    try std.testing.expectEqual(@as(i64, 300), commit.timestamp);
}

test "decode Relation" {
    // 'R' + Int32(oid=16384) + "public\0" + "users\0" + Int8(replica_id='d') + Int16(col_count=2)
    // + col1: Int8(flags=1) + "id\0" + Int32(type_oid=23) + Int32(type_mod=-1)
    // + col2: Int8(flags=0) + "name\0" + Int32(type_oid=25) + Int32(type_mod=-1)
    var buf: [128]u8 = undefined;
    var pos: usize = 0;
    buf[pos] = 'R';
    pos += 1;
    std.mem.writeInt(u32, buf[pos..][0..4], 16384, .big);
    pos += 4;
    @memcpy(buf[pos..][0..7], "public\x00");
    pos += 7;
    @memcpy(buf[pos..][0..6], "users\x00");
    pos += 6;
    buf[pos] = 'd';
    pos += 1;
    std.mem.writeInt(u16, buf[pos..][0..2], 2, .big);
    pos += 2;
    // col1
    buf[pos] = 1;
    pos += 1;
    @memcpy(buf[pos..][0..3], "id\x00");
    pos += 3;
    std.mem.writeInt(u32, buf[pos..][0..4], 23, .big);
    pos += 4;
    std.mem.writeInt(i32, buf[pos..][0..4], -1, .big);
    pos += 4;
    // col2
    buf[pos] = 0;
    pos += 1;
    @memcpy(buf[pos..][0..5], "name\x00");
    pos += 5;
    std.mem.writeInt(u32, buf[pos..][0..4], 25, .big);
    pos += 4;
    std.mem.writeInt(i32, buf[pos..][0..4], -1, .big);
    pos += 4;

    var col_buf: [16]Column = undefined;
    var tuple_buf: [16]ColumnData = undefined;
    var tuple_buf2: [16]ColumnData = undefined;
    const msg = try decode(buf[0..pos], &col_buf, &tuple_buf, &tuple_buf2);
    const rel = msg.relation;
    try std.testing.expectEqual(@as(u32, 16384), rel.oid);
    try std.testing.expectEqualStrings("public", rel.namespace);
    try std.testing.expectEqualStrings("users", rel.name);
    try std.testing.expectEqual(@as(u8, 'd'), rel.replica_identity);
    try std.testing.expectEqual(@as(usize, 2), rel.columns.len);
    try std.testing.expectEqualStrings("id", rel.columns[0].name);
    try std.testing.expectEqual(@as(u32, 23), rel.columns[0].type_oid);
    try std.testing.expectEqualStrings("name", rel.columns[1].name);
    try std.testing.expectEqual(@as(u32, 25), rel.columns[1].type_oid);
}

test "decode Insert" {
    // 'I' + Int32(oid=16384) + 'N' + TupleData(2 cols: text "42", text "alice")
    var buf: [128]u8 = undefined;
    var pos: usize = 0;
    buf[pos] = 'I';
    pos += 1;
    std.mem.writeInt(u32, buf[pos..][0..4], 16384, .big);
    pos += 4;
    buf[pos] = 'N';
    pos += 1;
    // TupleData: Int16(col_count=2)
    std.mem.writeInt(u16, buf[pos..][0..2], 2, .big);
    pos += 2;
    // col1: 't' + Int32(2) + "42"
    buf[pos] = 't';
    pos += 1;
    std.mem.writeInt(i32, buf[pos..][0..4], 2, .big);
    pos += 4;
    @memcpy(buf[pos..][0..2], "42");
    pos += 2;
    // col2: 't' + Int32(5) + "alice"
    buf[pos] = 't';
    pos += 1;
    std.mem.writeInt(i32, buf[pos..][0..4], 5, .big);
    pos += 4;
    @memcpy(buf[pos..][0..5], "alice");
    pos += 5;

    var col_buf: [16]Column = undefined;
    var tuple_buf: [16]ColumnData = undefined;
    var tuple_buf2: [16]ColumnData = undefined;
    const msg = try decode(buf[0..pos], &col_buf, &tuple_buf, &tuple_buf2);
    const ins = msg.insert;
    try std.testing.expectEqual(@as(u32, 16384), ins.relation_oid);
    try std.testing.expectEqual(@as(usize, 2), ins.new_tuple.len);
    try std.testing.expectEqualStrings("42", ins.new_tuple[0].text);
    try std.testing.expectEqualStrings("alice", ins.new_tuple[1].text);
}

test "decode Delete" {
    // 'D' + Int32(oid=16384) + 'K' + TupleData(1 col: text "42")
    var buf: [64]u8 = undefined;
    var pos: usize = 0;
    buf[pos] = 'D';
    pos += 1;
    std.mem.writeInt(u32, buf[pos..][0..4], 16384, .big);
    pos += 4;
    buf[pos] = 'K';
    pos += 1;
    std.mem.writeInt(u16, buf[pos..][0..2], 1, .big);
    pos += 2;
    buf[pos] = 't';
    pos += 1;
    std.mem.writeInt(i32, buf[pos..][0..4], 2, .big);
    pos += 4;
    @memcpy(buf[pos..][0..2], "42");
    pos += 2;

    var col_buf: [16]Column = undefined;
    var tuple_buf: [16]ColumnData = undefined;
    var tuple_buf2: [16]ColumnData = undefined;
    const msg = try decode(buf[0..pos], &col_buf, &tuple_buf, &tuple_buf2);
    const del = msg.delete;
    try std.testing.expectEqual(@as(u32, 16384), del.relation_oid);
    try std.testing.expectEqual(@as(usize, 1), del.old_tuple.len);
    try std.testing.expectEqualStrings("42", del.old_tuple[0].text);
}

test "decode NULL and unchanged columns" {
    // 'I' + Int32(oid=1) + 'N' + TupleData(3 cols: null, unchanged, text "x")
    var buf: [64]u8 = undefined;
    var pos: usize = 0;
    buf[pos] = 'I';
    pos += 1;
    std.mem.writeInt(u32, buf[pos..][0..4], 1, .big);
    pos += 4;
    buf[pos] = 'N';
    pos += 1;
    std.mem.writeInt(u16, buf[pos..][0..2], 3, .big);
    pos += 2;
    buf[pos] = 'n';
    pos += 1;
    buf[pos] = 'u';
    pos += 1;
    buf[pos] = 't';
    pos += 1;
    std.mem.writeInt(i32, buf[pos..][0..4], 1, .big);
    pos += 4;
    buf[pos] = 'x';
    pos += 1;

    var col_buf: [16]Column = undefined;
    var tuple_buf: [16]ColumnData = undefined;
    var tuple_buf2: [16]ColumnData = undefined;
    const msg = try decode(buf[0..pos], &col_buf, &tuple_buf, &tuple_buf2);
    const ins = msg.insert;
    try std.testing.expectEqual(@as(usize, 3), ins.new_tuple.len);
    try std.testing.expect(ins.new_tuple[0] == .null_value);
    try std.testing.expect(ins.new_tuple[1] == .unchanged);
    try std.testing.expectEqualStrings("x", ins.new_tuple[2].text);
}

test "decode Origin" {
    var buf: [64]u8 = undefined;
    buf[0] = 'O';
    std.mem.writeInt(u64, buf[1..9], 12345, .big);
    @memcpy(buf[9..][0..7], "origin\x00");

    var col_buf: [0]Column = undefined;
    var tuple_buf: [0]ColumnData = undefined;
    var tuple_buf2: [0]ColumnData = undefined;
    const msg = try decode(buf[0..16], &col_buf, &tuple_buf, &tuple_buf2);
    const orig = msg.origin;
    try std.testing.expectEqual(@as(u64, 12345), orig.lsn);
    try std.testing.expectEqualStrings("origin", orig.name);
}

test "unknown message type" {
    const buf = [_]u8{'Z'};
    var col_buf: [0]Column = undefined;
    var tuple_buf: [0]ColumnData = undefined;
    var tuple_buf2: [0]ColumnData = undefined;
    try std.testing.expectError(error.UnknownMessageType, decode(&buf, &col_buf, &tuple_buf, &tuple_buf2));
}
