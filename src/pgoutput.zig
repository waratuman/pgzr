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
    xid: ?u32 = null,
};

pub const Insert = struct {
    relation_oid: u32,
    new_tuple: []const ColumnData,
    xid: ?u32 = null,
};

pub const Update = struct {
    relation_oid: u32,
    old_tuple: ?[]const ColumnData,
    new_tuple: []const ColumnData,
    xid: ?u32 = null,
};

pub const Delete = struct {
    relation_oid: u32,
    old_tuple: []const ColumnData,
    xid: ?u32 = null,
};

pub const Truncate = struct {
    options: u8,
    relation_count: u32,
    /// Raw big-endian packed OID data. Use `relationOid(index)` to read individual OIDs.
    oid_data: []const u8,
    xid: ?u32 = null,

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
    xid: ?u32 = null,
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
    xid: ?u32 = null,
};

// Proto v2: Streaming large in-progress transactions (PG14+)
pub const StreamStart = struct {
    xid: u32,
    first_segment: bool,
};

pub const StreamStop = struct {};

pub const StreamCommit = struct {
    xid: u32,
    flags: u8,
    lsn: u64,
    end_lsn: u64,
    timestamp: i64,
};

pub const StreamAbort = struct {
    xid: u32,
    sub_xid: u32,
    abort_lsn: ?u64 = null,
    abort_timestamp: ?i64 = null,
};

// Proto v3: Two-phase commit (PG15+)
pub const BeginPrepare = struct {
    lsn: u64,
    end_lsn: u64,
    timestamp: i64,
    xid: u32,
    gid: []const u8,
};

pub const PrepareMsg = struct {
    flags: u8,
    lsn: u64,
    end_lsn: u64,
    timestamp: i64,
    xid: u32,
    gid: []const u8,
};

pub const CommitPrepared = struct {
    flags: u8,
    lsn: u64,
    end_lsn: u64,
    timestamp: i64,
    xid: u32,
    gid: []const u8,
};

pub const RollbackPrepared = struct {
    flags: u8,
    end_lsn: u64,
    rollback_end_lsn: u64,
    prepare_timestamp: i64,
    rollback_timestamp: i64,
    xid: u32,
    gid: []const u8,
};

pub const StreamPrepare = struct {
    flags: u8,
    lsn: u64,
    end_lsn: u64,
    timestamp: i64,
    xid: u32,
    gid: []const u8,
};

pub const PgoutputMessage = union(enum) {
    // Proto v1
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
    // Proto v2: streaming
    stream_start: StreamStart,
    stream_stop: StreamStop,
    stream_commit: StreamCommit,
    stream_abort: StreamAbort,
    // Proto v3: two-phase commit
    begin_prepare: BeginPrepare,
    prepare: PrepareMsg,
    commit_prepared: CommitPrepared,
    rollback_prepared: RollbackPrepared,
    stream_prepare: StreamPrepare,
};

pub const DecodeError = error{
    InvalidMessage,
    UnexpectedEnd,
    UnknownMessageType,
};

/// Decode a pgoutput binary message from the WAL data payload.
/// All returned slices borrow from the input `data` buffer.
/// The caller must provide scratch buffers for columns and tuple data.
///
/// `stream_xid`: when non-null, indicates we are inside a streaming context
/// (between StreamStart and StreamStop). In proto_version 2+, DML messages
/// within a stream have an extra Int32 xid prepended before their normal
/// fields. The caller manages stream state and passes the xid from StreamStart.
pub fn decode(
    data: []const u8,
    col_buf: []Column,
    tuple_buf: []ColumnData,
    tuple_buf2: []ColumnData,
    stream_xid: ?u32,
) DecodeError!PgoutputMessage {
    if (data.len == 0) return error.InvalidMessage;

    // For streamed DML messages, skip the leading Int32 xid
    const xid_offset: usize = if (stream_xid != null) 4 else 0;

    return switch (data[0]) {
        'B' => .{ .begin = try decodeBegin(data[1..]) },
        'C' => .{ .commit = try decodeCommit(data[1..]) },
        'R' => blk: {
            var rel = try decodeRelation(data[1 + xid_offset ..], col_buf);
            rel.xid = stream_xid;
            break :blk .{ .relation = rel };
        },
        'I' => blk: {
            var ins = try decodeInsert(data[1 + xid_offset ..], tuple_buf);
            ins.xid = stream_xid;
            break :blk .{ .insert = ins };
        },
        'U' => blk: {
            var upd = try decodeUpdate(data[1 + xid_offset ..], tuple_buf, tuple_buf2);
            upd.xid = stream_xid;
            break :blk .{ .update = upd };
        },
        'D' => blk: {
            var del = try decodeDelete(data[1 + xid_offset ..], tuple_buf);
            del.xid = stream_xid;
            break :blk .{ .delete = del };
        },
        'T' => blk: {
            var trunc = try decodeTruncate(data[1 + xid_offset ..]);
            trunc.xid = stream_xid;
            break :blk .{ .truncate = trunc };
        },
        'Y' => blk: {
            var ti = try decodeTypeInfo(data[1 + xid_offset ..]);
            ti.xid = stream_xid;
            break :blk .{ .type_info = ti };
        },
        'O' => .{ .origin = try decodeOrigin(data[1..]) },
        'M' => blk: {
            var msg = try decodeLogicalMessage(data[1 + xid_offset ..]);
            msg.xid = stream_xid;
            break :blk .{ .message = msg };
        },
        // Proto v2: streaming
        'S' => .{ .stream_start = try decodeStreamStart(data[1..]) },
        'E' => .{ .stream_stop = .{} },
        'c' => .{ .stream_commit = try decodeStreamCommit(data[1..]) },
        'A' => .{ .stream_abort = try decodeStreamAbort(data[1..]) },
        // Proto v3: two-phase commit
        'b' => .{ .begin_prepare = try decodeBeginPrepare(data[1..]) },
        'P' => .{ .prepare = try decodePrepare(data[1..]) },
        'K' => .{ .commit_prepared = try decodeCommitPrepared(data[1..]) },
        'r' => .{ .rollback_prepared = try decodeRollbackPrepared(data[1..]) },
        'p' => .{ .stream_prepare = try decodeStreamPrepare(data[1..]) },
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
// Proto v2: Streaming decoders
// ---------------------------------------------------------------------------

fn decodeStreamStart(data: []const u8) DecodeError!StreamStart {
    var pos: usize = 0;
    const xid = try readU32(data, &pos);
    const first = try readU8(data, &pos);
    return .{ .xid = xid, .first_segment = first != 0 };
}

fn decodeStreamCommit(data: []const u8) DecodeError!StreamCommit {
    var pos: usize = 0;
    const xid = try readU32(data, &pos);
    const flags = try readU8(data, &pos);
    const lsn = try readU64(data, &pos);
    const end_lsn = try readU64(data, &pos);
    const timestamp = try readI64(data, &pos);
    return .{ .xid = xid, .flags = flags, .lsn = lsn, .end_lsn = end_lsn, .timestamp = timestamp };
}

fn decodeStreamAbort(data: []const u8) DecodeError!StreamAbort {
    var pos: usize = 0;
    const xid = try readU32(data, &pos);
    const sub_xid = try readU32(data, &pos);
    // Proto v4 extension: abort_lsn and abort_timestamp if data remains
    var abort_lsn: ?u64 = null;
    var abort_timestamp: ?i64 = null;
    if (pos + 16 <= data.len) {
        abort_lsn = try readU64(data, &pos);
        abort_timestamp = try readI64(data, &pos);
    }
    return .{ .xid = xid, .sub_xid = sub_xid, .abort_lsn = abort_lsn, .abort_timestamp = abort_timestamp };
}

// ---------------------------------------------------------------------------
// Proto v3: Two-phase commit decoders
// ---------------------------------------------------------------------------

fn decodeBeginPrepare(data: []const u8) DecodeError!BeginPrepare {
    var pos: usize = 0;
    const lsn = try readU64(data, &pos);
    const end_lsn = try readU64(data, &pos);
    const timestamp = try readI64(data, &pos);
    const xid = try readU32(data, &pos);
    const gid = try readCString(data, &pos);
    return .{ .lsn = lsn, .end_lsn = end_lsn, .timestamp = timestamp, .xid = xid, .gid = gid };
}

fn decodePrepare(data: []const u8) DecodeError!PrepareMsg {
    var pos: usize = 0;
    const flags = try readU8(data, &pos);
    const lsn = try readU64(data, &pos);
    const end_lsn = try readU64(data, &pos);
    const timestamp = try readI64(data, &pos);
    const xid = try readU32(data, &pos);
    const gid = try readCString(data, &pos);
    return .{ .flags = flags, .lsn = lsn, .end_lsn = end_lsn, .timestamp = timestamp, .xid = xid, .gid = gid };
}

fn decodeCommitPrepared(data: []const u8) DecodeError!CommitPrepared {
    var pos: usize = 0;
    const flags = try readU8(data, &pos);
    const lsn = try readU64(data, &pos);
    const end_lsn = try readU64(data, &pos);
    const timestamp = try readI64(data, &pos);
    const xid = try readU32(data, &pos);
    const gid = try readCString(data, &pos);
    return .{ .flags = flags, .lsn = lsn, .end_lsn = end_lsn, .timestamp = timestamp, .xid = xid, .gid = gid };
}

fn decodeRollbackPrepared(data: []const u8) DecodeError!RollbackPrepared {
    var pos: usize = 0;
    const flags = try readU8(data, &pos);
    const end_lsn = try readU64(data, &pos);
    const rollback_end_lsn = try readU64(data, &pos);
    const prepare_timestamp = try readI64(data, &pos);
    const rollback_timestamp = try readI64(data, &pos);
    const xid = try readU32(data, &pos);
    const gid = try readCString(data, &pos);
    return .{
        .flags = flags,
        .end_lsn = end_lsn,
        .rollback_end_lsn = rollback_end_lsn,
        .prepare_timestamp = prepare_timestamp,
        .rollback_timestamp = rollback_timestamp,
        .xid = xid,
        .gid = gid,
    };
}

fn decodeStreamPrepare(data: []const u8) DecodeError!StreamPrepare {
    var pos: usize = 0;
    const flags = try readU8(data, &pos);
    const lsn = try readU64(data, &pos);
    const end_lsn = try readU64(data, &pos);
    const timestamp = try readI64(data, &pos);
    const xid = try readU32(data, &pos);
    const gid = try readCString(data, &pos);
    return .{ .flags = flags, .lsn = lsn, .end_lsn = end_lsn, .timestamp = timestamp, .xid = xid, .gid = gid };
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
    const msg = try decode(&buf, &col_buf, &tuple_buf, &tuple_buf2, null);
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
    const msg = try decode(&buf, &col_buf, &tuple_buf, &tuple_buf2, null);
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
    const msg = try decode(buf[0..pos], &col_buf, &tuple_buf, &tuple_buf2, null);
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
    const msg = try decode(buf[0..pos], &col_buf, &tuple_buf, &tuple_buf2, null);
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
    const msg = try decode(buf[0..pos], &col_buf, &tuple_buf, &tuple_buf2, null);
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
    const msg = try decode(buf[0..pos], &col_buf, &tuple_buf, &tuple_buf2, null);
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
    const msg = try decode(buf[0..16], &col_buf, &tuple_buf, &tuple_buf2, null);
    const orig = msg.origin;
    try std.testing.expectEqual(@as(u64, 12345), orig.lsn);
    try std.testing.expectEqualStrings("origin", orig.name);
}

test "unknown message type" {
    const buf = [_]u8{'Z'};
    var col_buf: [0]Column = undefined;
    var tuple_buf: [0]ColumnData = undefined;
    var tuple_buf2: [0]ColumnData = undefined;
    try std.testing.expectError(error.UnknownMessageType, decode(&buf, &col_buf, &tuple_buf, &tuple_buf2, null));
}

// ---------------------------------------------------------------------------
// Proto v2-v4 tests
// ---------------------------------------------------------------------------

test "decode StreamStart" {
    var buf: [6]u8 = undefined;
    buf[0] = 'S';
    std.mem.writeInt(u32, buf[1..5], 100, .big);
    buf[5] = 1;

    var col_buf: [0]Column = undefined;
    var tuple_buf: [0]ColumnData = undefined;
    var tuple_buf2: [0]ColumnData = undefined;
    const msg = try decode(&buf, &col_buf, &tuple_buf, &tuple_buf2, null);
    const ss = msg.stream_start;
    try std.testing.expectEqual(@as(u32, 100), ss.xid);
    try std.testing.expect(ss.first_segment);
}

test "decode StreamStop" {
    const buf = [_]u8{'E'};
    var col_buf: [0]Column = undefined;
    var tuple_buf: [0]ColumnData = undefined;
    var tuple_buf2: [0]ColumnData = undefined;
    const msg = try decode(&buf, &col_buf, &tuple_buf, &tuple_buf2, null);
    try std.testing.expect(msg == .stream_stop);
}

test "decode StreamCommit" {
    var buf: [30]u8 = undefined;
    buf[0] = 'c';
    std.mem.writeInt(u32, buf[1..5], 100, .big);
    buf[5] = 0;
    std.mem.writeInt(u64, buf[6..14], 200, .big);
    std.mem.writeInt(u64, buf[14..22], 300, .big);
    std.mem.writeInt(i64, buf[22..30], 400, .big);

    var col_buf: [0]Column = undefined;
    var tuple_buf: [0]ColumnData = undefined;
    var tuple_buf2: [0]ColumnData = undefined;
    const msg = try decode(&buf, &col_buf, &tuple_buf, &tuple_buf2, null);
    const sc = msg.stream_commit;
    try std.testing.expectEqual(@as(u32, 100), sc.xid);
    try std.testing.expectEqual(@as(u64, 200), sc.lsn);
    try std.testing.expectEqual(@as(u64, 300), sc.end_lsn);
    try std.testing.expectEqual(@as(i64, 400), sc.timestamp);
}

test "decode StreamAbort v2" {
    var buf: [9]u8 = undefined;
    buf[0] = 'A';
    std.mem.writeInt(u32, buf[1..5], 100, .big);
    std.mem.writeInt(u32, buf[5..9], 101, .big);

    var col_buf: [0]Column = undefined;
    var tuple_buf: [0]ColumnData = undefined;
    var tuple_buf2: [0]ColumnData = undefined;
    const msg = try decode(&buf, &col_buf, &tuple_buf, &tuple_buf2, null);
    const sa = msg.stream_abort;
    try std.testing.expectEqual(@as(u32, 100), sa.xid);
    try std.testing.expectEqual(@as(u32, 101), sa.sub_xid);
    try std.testing.expect(sa.abort_lsn == null);
    try std.testing.expect(sa.abort_timestamp == null);
}

test "decode StreamAbort v4 with abort lsn/timestamp" {
    var buf: [25]u8 = undefined;
    buf[0] = 'A';
    std.mem.writeInt(u32, buf[1..5], 100, .big);
    std.mem.writeInt(u32, buf[5..9], 101, .big);
    std.mem.writeInt(u64, buf[9..17], 500, .big);
    std.mem.writeInt(i64, buf[17..25], 600, .big);

    var col_buf: [0]Column = undefined;
    var tuple_buf: [0]ColumnData = undefined;
    var tuple_buf2: [0]ColumnData = undefined;
    const msg = try decode(&buf, &col_buf, &tuple_buf, &tuple_buf2, null);
    const sa = msg.stream_abort;
    try std.testing.expectEqual(@as(u32, 100), sa.xid);
    try std.testing.expectEqual(@as(u32, 101), sa.sub_xid);
    try std.testing.expectEqual(@as(u64, 500), sa.abort_lsn.?);
    try std.testing.expectEqual(@as(i64, 600), sa.abort_timestamp.?);
}

test "decode BeginPrepare" {
    var buf: [128]u8 = undefined;
    var pos: usize = 0;
    buf[pos] = 'b';
    pos += 1;
    std.mem.writeInt(u64, buf[pos..][0..8], 100, .big);
    pos += 8;
    std.mem.writeInt(u64, buf[pos..][0..8], 200, .big);
    pos += 8;
    std.mem.writeInt(i64, buf[pos..][0..8], 300, .big);
    pos += 8;
    std.mem.writeInt(u32, buf[pos..][0..4], 42, .big);
    pos += 4;
    @memcpy(buf[pos..][0..8], "my_txn\x00\x00");
    pos += 7; // null-terminated string "my_txn"

    var col_buf: [0]Column = undefined;
    var tuple_buf: [0]ColumnData = undefined;
    var tuple_buf2: [0]ColumnData = undefined;
    const msg = try decode(buf[0..pos], &col_buf, &tuple_buf, &tuple_buf2, null);
    const bp = msg.begin_prepare;
    try std.testing.expectEqual(@as(u64, 100), bp.lsn);
    try std.testing.expectEqual(@as(u64, 200), bp.end_lsn);
    try std.testing.expectEqual(@as(i64, 300), bp.timestamp);
    try std.testing.expectEqual(@as(u32, 42), bp.xid);
    try std.testing.expectEqualStrings("my_txn", bp.gid);
}

test "decode CommitPrepared" {
    var buf: [128]u8 = undefined;
    var pos: usize = 0;
    buf[pos] = 'K';
    pos += 1;
    buf[pos] = 0; // flags
    pos += 1;
    std.mem.writeInt(u64, buf[pos..][0..8], 100, .big);
    pos += 8;
    std.mem.writeInt(u64, buf[pos..][0..8], 200, .big);
    pos += 8;
    std.mem.writeInt(i64, buf[pos..][0..8], 300, .big);
    pos += 8;
    std.mem.writeInt(u32, buf[pos..][0..4], 42, .big);
    pos += 4;
    @memcpy(buf[pos..][0..7], "my_txn\x00");
    pos += 7;

    var col_buf: [0]Column = undefined;
    var tuple_buf: [0]ColumnData = undefined;
    var tuple_buf2: [0]ColumnData = undefined;
    const msg = try decode(buf[0..pos], &col_buf, &tuple_buf, &tuple_buf2, null);
    const cp = msg.commit_prepared;
    try std.testing.expectEqual(@as(u64, 100), cp.lsn);
    try std.testing.expectEqual(@as(u32, 42), cp.xid);
    try std.testing.expectEqualStrings("my_txn", cp.gid);
}

test "decode streamed Insert with xid" {
    // Streamed Insert: 'I' + Int32(xid=99) + Int32(oid=16384) + 'N' + TupleData(1 col: text "hi")
    var buf: [128]u8 = undefined;
    var pos: usize = 0;
    buf[pos] = 'I';
    pos += 1;
    std.mem.writeInt(u32, buf[pos..][0..4], 99, .big); // xid (skipped by decoder)
    pos += 4;
    std.mem.writeInt(u32, buf[pos..][0..4], 16384, .big);
    pos += 4;
    buf[pos] = 'N';
    pos += 1;
    std.mem.writeInt(u16, buf[pos..][0..2], 1, .big);
    pos += 2;
    buf[pos] = 't';
    pos += 1;
    std.mem.writeInt(i32, buf[pos..][0..4], 2, .big);
    pos += 4;
    @memcpy(buf[pos..][0..2], "hi");
    pos += 2;

    var col_buf: [16]Column = undefined;
    var tuple_buf: [16]ColumnData = undefined;
    var tuple_buf2: [16]ColumnData = undefined;
    const msg = try decode(buf[0..pos], &col_buf, &tuple_buf, &tuple_buf2, 99);
    const ins = msg.insert;
    try std.testing.expectEqual(@as(u32, 16384), ins.relation_oid);
    try std.testing.expectEqual(@as(u32, 99), ins.xid.?);
    try std.testing.expectEqualStrings("hi", ins.new_tuple[0].text);
}

test "decode non-streamed Insert has null xid" {
    var buf: [128]u8 = undefined;
    var pos: usize = 0;
    buf[pos] = 'I';
    pos += 1;
    std.mem.writeInt(u32, buf[pos..][0..4], 16384, .big);
    pos += 4;
    buf[pos] = 'N';
    pos += 1;
    std.mem.writeInt(u16, buf[pos..][0..2], 1, .big);
    pos += 2;
    buf[pos] = 't';
    pos += 1;
    std.mem.writeInt(i32, buf[pos..][0..4], 2, .big);
    pos += 4;
    @memcpy(buf[pos..][0..2], "hi");
    pos += 2;

    var col_buf: [16]Column = undefined;
    var tuple_buf: [16]ColumnData = undefined;
    var tuple_buf2: [16]ColumnData = undefined;
    const msg = try decode(buf[0..pos], &col_buf, &tuple_buf, &tuple_buf2, null);
    const ins = msg.insert;
    try std.testing.expectEqual(@as(u32, 16384), ins.relation_oid);
    try std.testing.expect(ins.xid == null);
}
