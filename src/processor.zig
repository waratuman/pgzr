const std = @import("std");
const Connection = @import("connection.zig").Connection;
const pgoutput = @import("pgoutput.zig");
const types = @import("types.zig");
const Lsn = @import("lsn.zig").Lsn;
const query_mod = @import("query.zig");
const pg_types = @import("pg_types.zig");
const schema_mod = @import("schema.zig");
const Sha256 = std.crypto.hash.sha2.Sha256;

pub const Processor = struct {
    dest: Connection,
    allocator: std.mem.Allocator,
    config: types.ProcessorConfig,

    // Relation cache (rebuilt from binary messages in each batch)
    relations: std.AutoHashMap(u32, OwnedRelation),

    // pgoutput decode scratch buffers
    col_buf: [256]pgoutput.Column,
    tuple_buf: [256]pgoutput.ColumnData,
    tuple_buf2: [256]pgoutput.ColumnData,

    // Current transaction state
    current_txn_lsn: u64,
    current_txn_xid: u32,
    current_txn_timestamp: i64,
    in_transaction: bool,

    // Streaming context (proto v2+)
    current_stream_xid: ?u32,

    // Batch accumulation for partial (split) batches
    pending_data: std.ArrayListUnmanaged(u8),
    pending_batch_ids: std.ArrayListUnmanaged([]u8),

    // Stop flag for graceful shutdown
    stop_flag: std.atomic.Value(bool),

    const OwnedRelation = struct {
        oid: u32,
        namespace: []u8,
        name: []u8,
        replica_identity: u8,
        columns: []OwnedColumn,

        fn deinit(self: *OwnedRelation, allocator: std.mem.Allocator) void {
            allocator.free(self.namespace);
            allocator.free(self.name);
            for (self.columns) |col| {
                allocator.free(col.name);
            }
            allocator.free(self.columns);
        }
    };

    const OwnedColumn = struct {
        flags: u8,
        name: []u8,
        type_oid: u32,
        type_modifier: i32,
    };

    pub const InitError = Connection.ConnectError || Connection.QueryError || std.mem.Allocator.Error;

    pub fn init(allocator: std.mem.Allocator, config: types.ProcessorConfig) InitError!Processor {
        var dest_config = config.dest;
        dest_config.replication = false;
        var dest = try Connection.connect(allocator, dest_config);
        errdefer dest.close();

        try schema_mod.ensureSchema(&dest);

        return Processor{
            .dest = dest,
            .allocator = allocator,
            .config = config,
            .relations = std.AutoHashMap(u32, OwnedRelation).init(allocator),
            .col_buf = undefined,
            .tuple_buf = undefined,
            .tuple_buf2 = undefined,
            .current_txn_lsn = 0,
            .current_txn_xid = 0,
            .current_txn_timestamp = 0,
            .in_transaction = false,
            .current_stream_xid = null,
            .pending_data = .{},
            .pending_batch_ids = .{},
            .stop_flag = std.atomic.Value(bool).init(false),
        };
    }

    pub const ProcessError = Connection.QueryError || std.mem.Allocator.Error || pgoutput.DecodeError;

    /// Process one pending batch. Returns true if a batch was claimed,
    /// false if no pending batches were found.
    ///
    /// Partial batches (complete=false) are accumulated in memory until
    /// a complete batch arrives, then all accumulated data is processed
    /// as one logical unit.
    pub fn processOne(self: *Processor) ProcessError!bool {
        // Claim a batch
        const source_id = try query_mod.escapeUuid(self.allocator, self.config.source_id);
        defer self.allocator.free(source_id);

        var claim_buf: [512]u8 = undefined;
        const claim_sql = std.fmt.bufPrint(&claim_buf,
            \\UPDATE wal_batches SET state='processing'
            \\ WHERE id = (
            \\   SELECT id FROM wal_batches
            \\   WHERE source_id={s} AND state='pending'
            \\   ORDER BY start_lsn LIMIT 1
            \\   FOR UPDATE SKIP LOCKED
            \\ ) RETURNING id, data, complete
        , .{source_id}) catch unreachable;

        const result = try self.dest.simpleQuery(claim_sql);
        if (result.column_count == 0) return false;

        const batch_id = result.columns[0].data;
        const raw_data = result.columns[1].data;
        const complete_str = result.columns[2].data;
        const is_complete = complete_str.len > 0 and complete_str[0] == 't';

        // Copy batch_id (borrowed from recv_buf)
        const batch_id_copy = try self.allocator.alloc(u8, batch_id.len);
        @memcpy(batch_id_copy, batch_id);

        // Decode hex bytea to binary
        const batch_data = try self.decodeBytea(raw_data);

        // Accumulate data and batch ID
        try self.pending_data.appendSlice(self.allocator, batch_data);
        self.allocator.free(batch_data);
        try self.pending_batch_ids.append(self.allocator, batch_id_copy);

        if (!is_complete) {
            // Partial batch — wait for the complete one
            return true;
        }

        // Complete batch — process all accumulated data
        const all_data = self.pending_data.items;
        self.processBatch(all_data) catch |err| {
            // On error, mark all accumulated batches as error
            self.markBatchesError();
            self.clearPending();
            return err;
        };

        // On success, delete all accumulated batches
        self.deleteBatches() catch {};
        self.clearPending();

        return true;
    }

    fn markBatchesError(self: *Processor) void {
        for (self.pending_batch_ids.items) |bid| {
            var err_buf: [256]u8 = undefined;
            const err_sql = std.fmt.bufPrint(&err_buf,
                "UPDATE wal_batches SET state='error' WHERE id={s}",
                .{bid},
            ) catch continue;
            _ = self.dest.simpleQuery(err_sql) catch {};
        }
    }

    fn deleteBatches(self: *Processor) !void {
        for (self.pending_batch_ids.items) |bid| {
            var del_buf: [128]u8 = undefined;
            const del_sql = std.fmt.bufPrint(&del_buf,
                "DELETE FROM wal_batches WHERE id={s}",
                .{bid},
            ) catch continue;
            _ = try self.dest.simpleQuery(del_sql);
        }
    }

    fn clearPending(self: *Processor) void {
        for (self.pending_batch_ids.items) |bid| {
            self.allocator.free(bid);
        }
        self.pending_batch_ids.clearRetainingCapacity();
        self.pending_data.clearRetainingCapacity();
    }

    fn processBatch(self: *Processor, data: []const u8) !void {
        var pos: usize = 0;

        while (pos + 4 <= data.len) {
            const msg_len = std.mem.readInt(u32, data[pos..][0..4], .big);
            pos += 4;
            if (pos + msg_len > data.len) break;

            const msg_data = data[pos .. pos + msg_len];
            pos += msg_len;

            const msg = try pgoutput.decode(
                msg_data,
                &self.col_buf,
                &self.tuple_buf,
                &self.tuple_buf2,
                self.current_stream_xid,
            );

            switch (msg) {
                .begin => |begin| {
                    self.current_txn_lsn = begin.final_lsn;
                    self.current_txn_xid = begin.xid;
                    self.current_txn_timestamp = begin.timestamp;
                    self.in_transaction = true;
                    try self.insertTransaction(begin.final_lsn);
                },
                .commit => {
                    self.in_transaction = false;
                },
                .relation => |rel| {
                    try self.updateRelationCache(rel);
                },
                .insert => |ins| {
                    try self.insertEvent(ins.relation_oid, 0, ins.new_tuple, null);
                },
                .update => |upd| {
                    try self.insertEvent(upd.relation_oid, 1, upd.new_tuple, upd.old_tuple);
                },
                .delete => |del| {
                    try self.insertEvent(del.relation_oid, 2, del.old_tuple, null);
                },
                .truncate => {
                    try self.insertTruncateEvent();
                },
                // Proto v2: streaming
                .stream_start => |ss| {
                    self.current_stream_xid = ss.xid;
                },
                .stream_stop => {
                    self.current_stream_xid = null;
                },
                .stream_commit => {
                    self.current_stream_xid = null;
                    self.in_transaction = false;
                },
                .stream_abort => {
                    self.current_stream_xid = null;
                    self.in_transaction = false;
                },
                // Proto v3: two-phase commit
                .begin_prepare => |bp| {
                    self.current_txn_lsn = bp.lsn;
                    self.current_txn_xid = bp.xid;
                    self.current_txn_timestamp = bp.timestamp;
                    self.in_transaction = true;
                    try self.insertTransaction(bp.lsn);
                },
                .prepare => {
                    // Prepared but not yet committed — keep txn state
                },
                .commit_prepared => {
                    self.in_transaction = false;
                },
                .rollback_prepared => {
                    self.in_transaction = false;
                },
                .stream_prepare => {
                    // Prepared within streaming — keep txn state
                },
                else => {},
            }
        }
    }

    fn insertTransaction(self: *Processor, lsn: u64) !void {
        const source_id = try query_mod.escapeUuid(self.allocator, self.config.source_id);
        defer self.allocator.free(source_id);

        const committed_at = try query_mod.formatTimestamp(self.allocator, self.current_txn_timestamp);
        defer self.allocator.free(committed_at);

        var sql_buf: [512]u8 = undefined;
        const sql = std.fmt.bufPrint(&sql_buf,
            \\INSERT INTO transactions (source_id, lsn, xid, committed_at)
            \\ VALUES ({s}, {d}, {d}, {s})
            \\ ON CONFLICT (source_id, lsn) DO NOTHING
        , .{
            source_id,
            lsn,
            self.current_txn_xid,
            committed_at,
        }) catch unreachable;

        _ = try self.dest.simpleQuery(sql);
    }

    fn insertEvent(
        self: *Processor,
        relation_oid: u32,
        event_type: u2,
        tuple: []const pgoutput.ColumnData,
        old_tuple: ?[]const pgoutput.ColumnData,
    ) !void {
        const rel = self.relations.get(relation_oid) orelse return;

        const source_id = try query_mod.escapeUuid(self.allocator, self.config.source_id);
        defer self.allocator.free(source_id);

        const committed_at = try query_mod.formatTimestamp(self.allocator, self.current_txn_timestamp);
        defer self.allocator.free(committed_at);

        // Compute identity digests
        const identity_digest = try self.computeIdentityDigest(rel, tuple);
        defer if (identity_digest) |d| self.allocator.free(d);

        const prev_identity_digest = if (old_tuple) |ot|
            try self.computeIdentityDigest(rel, ot)
        else
            null;
        defer if (prev_identity_digest) |d| self.allocator.free(d);

        const id_hex = if (identity_digest) |d|
            try query_mod.escapeBytea(self.allocator, d)
        else
            try query_mod.formatNull(self.allocator);
        defer self.allocator.free(id_hex);

        const prev_id_hex = if (prev_identity_digest) |d|
            try query_mod.escapeBytea(self.allocator, d)
        else
            try query_mod.formatNull(self.allocator);
        defer self.allocator.free(prev_id_hex);

        const schema_name = try query_mod.escapeString(self.allocator, rel.namespace);
        defer self.allocator.free(schema_name);

        const table_name = try query_mod.escapeString(self.allocator, rel.name);
        defer self.allocator.free(table_name);

        // INSERT event and get back the event ID
        var sql: std.ArrayListUnmanaged(u8) = .{};
        defer sql.deinit(self.allocator);

        try sql.appendSlice(self.allocator,
            "INSERT INTO events (source_id, lsn, type, schema_name, table_name, committed_at, " ++
                "identity_digest, previous_identity_digest, transaction_id) " ++
                "VALUES (",
        );
        try sql.appendSlice(self.allocator, source_id);
        try sql.appendSlice(self.allocator, ", ");
        var lsn_buf: [20]u8 = undefined;
        const lsn_str = std.fmt.bufPrint(&lsn_buf, "{d}", .{self.current_txn_lsn}) catch unreachable;
        try sql.appendSlice(self.allocator, lsn_str);
        try sql.appendSlice(self.allocator, ", ");
        var type_buf: [1]u8 = undefined;
        const type_str = std.fmt.bufPrint(&type_buf, "{d}", .{event_type}) catch unreachable;
        try sql.appendSlice(self.allocator, type_str);
        try sql.appendSlice(self.allocator, ", ");
        try sql.appendSlice(self.allocator, schema_name);
        try sql.appendSlice(self.allocator, ", ");
        try sql.appendSlice(self.allocator, table_name);
        try sql.appendSlice(self.allocator, ", ");
        try sql.appendSlice(self.allocator, committed_at);
        try sql.appendSlice(self.allocator, ", ");
        try sql.appendSlice(self.allocator, id_hex);
        try sql.appendSlice(self.allocator, ", ");
        try sql.appendSlice(self.allocator, prev_id_hex);
        try sql.appendSlice(self.allocator, ", (SELECT id FROM transactions WHERE source_id=");
        try sql.appendSlice(self.allocator, source_id);
        try sql.appendSlice(self.allocator, " AND lsn=");
        try sql.appendSlice(self.allocator, lsn_str);
        try sql.appendSlice(self.allocator, ")) RETURNING id");

        const event_result = try self.dest.execLargeWithResult(self.allocator, sql.items);
        if (event_result.column_count == 0) return;

        const event_id = try self.allocator.alloc(u8, event_result.columns[0].data.len);
        defer self.allocator.free(event_id);
        @memcpy(event_id, event_result.columns[0].data);

        // Insert columns
        try self.insertColumns(event_id, rel, tuple, old_tuple);
    }

    fn insertTruncateEvent(self: *Processor) !void {
        const source_id = try query_mod.escapeUuid(self.allocator, self.config.source_id);
        defer self.allocator.free(source_id);

        const committed_at = try query_mod.formatTimestamp(self.allocator, self.current_txn_timestamp);
        defer self.allocator.free(committed_at);

        var sql_buf: [512]u8 = undefined;
        const sql = std.fmt.bufPrint(&sql_buf,
            \\INSERT INTO events (source_id, lsn, type, schema_name, table_name, committed_at,
            \\ transaction_id)
            \\ VALUES ({s}, {d}, 3, '', '',
            \\ {s}, (SELECT id FROM transactions WHERE source_id={s} AND lsn={d}))
        , .{
            source_id,
            self.current_txn_lsn,
            committed_at,
            source_id,
            self.current_txn_lsn,
        }) catch unreachable;

        _ = try self.dest.simpleQuery(sql);
    }

    fn insertColumns(
        self: *Processor,
        event_id: []const u8,
        rel: OwnedRelation,
        tuple: []const pgoutput.ColumnData,
        old_tuple: ?[]const pgoutput.ColumnData,
    ) !void {
        const source_id = try query_mod.escapeUuid(self.allocator, self.config.source_id);
        defer self.allocator.free(source_id);

        const event_id_escaped = try query_mod.escapeUuid(self.allocator, event_id);
        defer self.allocator.free(event_id_escaped);

        for (rel.columns, 0..) |col, i| {
            const is_identity = (col.flags & 1) != 0;

            // Get current value
            const value_data: ?[]const u8 = if (i < tuple.len)
                switch (tuple[i]) {
                    .text => |t| t,
                    .binary => |b| b,
                    .null_value => null,
                    .unchanged => null,
                }
            else
                null;

            // Get previous value (for updates)
            const prev_value_data: ?[]const u8 = if (old_tuple) |ot| blk: {
                if (i < ot.len) {
                    break :blk switch (ot[i]) {
                        .text => |t| t,
                        .binary => |b| b,
                        .null_value => null,
                        .unchanged => null,
                    };
                }
                break :blk null;
            } else null;

            const value_sql = if (value_data) |v|
                try query_mod.escapeBytea(self.allocator, v)
            else
                try query_mod.formatNull(self.allocator);
            defer self.allocator.free(value_sql);

            const prev_value_sql = if (prev_value_data) |v|
                try query_mod.escapeBytea(self.allocator, v)
            else
                try query_mod.formatNull(self.allocator);
            defer self.allocator.free(prev_value_sql);

            const col_name = try query_mod.escapeString(self.allocator, col.name);
            defer self.allocator.free(col_name);

            var type_name_buf: [32]u8 = undefined;
            const type_name = pg_types.oidToName(col.type_oid, &type_name_buf);
            const type_name_escaped = try query_mod.escapeString(self.allocator, type_name);
            defer self.allocator.free(type_name_escaped);

            var sql: std.ArrayListUnmanaged(u8) = .{};
            defer sql.deinit(self.allocator);

            try sql.appendSlice(self.allocator,
                "INSERT INTO columns (event_id, source_id, name, type_oid, type_name, " ++
                    "value, previous_value, identity, ordinal) VALUES (",
            );
            try sql.appendSlice(self.allocator, event_id_escaped);
            try sql.appendSlice(self.allocator, ", ");
            try sql.appendSlice(self.allocator, source_id);
            try sql.appendSlice(self.allocator, ", ");
            try sql.appendSlice(self.allocator, col_name);
            try sql.appendSlice(self.allocator, ", ");

            var oid_buf: [10]u8 = undefined;
            const oid_str = std.fmt.bufPrint(&oid_buf, "{d}", .{col.type_oid}) catch unreachable;
            try sql.appendSlice(self.allocator, oid_str);
            try sql.appendSlice(self.allocator, ", ");

            try sql.appendSlice(self.allocator, type_name_escaped);
            try sql.appendSlice(self.allocator, ", ");

            try sql.appendSlice(self.allocator, value_sql);
            try sql.appendSlice(self.allocator, ", ");

            try sql.appendSlice(self.allocator, prev_value_sql);
            try sql.appendSlice(self.allocator, ", ");

            if (is_identity) {
                try sql.appendSlice(self.allocator, "true");
            } else {
                try sql.appendSlice(self.allocator, "false");
            }
            try sql.appendSlice(self.allocator, ", ");

            var ord_buf: [5]u8 = undefined;
            const ord_str = std.fmt.bufPrint(&ord_buf, "{d}", .{i}) catch unreachable;
            try sql.appendSlice(self.allocator, ord_str);
            try sql.append(self.allocator, ')');

            try self.dest.execLarge(self.allocator, sql.items);
        }
    }

    fn computeIdentityDigest(self: *Processor, rel: OwnedRelation, tuple: []const pgoutput.ColumnData) !?[]u8 {
        // Find identity columns
        var has_identity = false;
        for (rel.columns) |col| {
            if ((col.flags & 1) != 0) {
                has_identity = true;
                break;
            }
        }
        if (!has_identity) return null;

        var hasher = Sha256.init(.{});

        for (rel.columns, 0..) |col, i| {
            if ((col.flags & 1) == 0) continue;

            if (i < tuple.len) {
                switch (tuple[i]) {
                    .text => |t| {
                        var len_buf: [4]u8 = undefined;
                        std.mem.writeInt(u32, &len_buf, @intCast(t.len), .big);
                        hasher.update(&len_buf);
                        hasher.update(t);
                    },
                    .binary => |b| {
                        var len_buf: [4]u8 = undefined;
                        std.mem.writeInt(u32, &len_buf, @intCast(b.len), .big);
                        hasher.update(&len_buf);
                        hasher.update(b);
                    },
                    .null_value => {
                        hasher.update(&[_]u8{ 0xFF, 0xFF, 0xFF, 0xFF });
                    },
                    .unchanged => {},
                }
            }
        }

        const digest = try self.allocator.alloc(u8, Sha256.digest_length);
        const result = hasher.finalResult();
        @memcpy(digest, &result);
        return digest;
    }

    fn updateRelationCache(self: *Processor, rel: pgoutput.Relation) !void {
        if (self.relations.fetchRemove(rel.oid)) |entry| {
            var old = entry.value;
            old.deinit(self.allocator);
        }

        const namespace = try self.allocator.alloc(u8, rel.namespace.len);
        @memcpy(namespace, rel.namespace);

        const name = try self.allocator.alloc(u8, rel.name.len);
        @memcpy(name, rel.name);

        const columns = try self.allocator.alloc(OwnedColumn, rel.columns.len);
        for (rel.columns, 0..) |col, i| {
            const col_name = try self.allocator.alloc(u8, col.name.len);
            @memcpy(col_name, col.name);
            columns[i] = .{
                .flags = col.flags,
                .name = col_name,
                .type_oid = col.type_oid,
                .type_modifier = col.type_modifier,
            };
        }

        try self.relations.put(rel.oid, .{
            .oid = rel.oid,
            .namespace = namespace,
            .name = name,
            .replica_identity = rel.replica_identity,
            .columns = columns,
        });
    }

    fn decodeBytea(self: *Processor, hex_data: []const u8) ![]u8 {
        if (hex_data.len < 2 or hex_data[0] != '\\' or hex_data[1] != 'x') {
            const copy = try self.allocator.alloc(u8, hex_data.len);
            @memcpy(copy, hex_data);
            return copy;
        }

        const hex = hex_data[2..];
        if (hex.len % 2 != 0) {
            return try self.allocator.alloc(u8, 0);
        }

        const out_len = hex.len / 2;
        const out = try self.allocator.alloc(u8, out_len);

        for (0..out_len) |i| {
            const hi = hexDigit(hex[i * 2]) orelse {
                self.allocator.free(out);
                return error.OutOfMemory;
            };
            const lo = hexDigit(hex[i * 2 + 1]) orelse {
                self.allocator.free(out);
                return error.OutOfMemory;
            };
            out[i] = (hi << 4) | lo;
        }

        return out;
    }

    fn hexDigit(c: u8) ?u8 {
        return switch (c) {
            '0'...'9' => c - '0',
            'a'...'f' => c - 'a' + 10,
            'A'...'F' => c - 'A' + 10,
            else => null,
        };
    }

    /// Main processing loop. Processes pending batches until stopped.
    /// When no batches are pending, sleeps for poll_interval_ms.
    pub fn run(self: *Processor) ProcessError!void {
        while (!self.stop_flag.load(.acquire)) {
            const processed = try self.processOne();
            if (!processed) {
                std.Thread.sleep(self.config.poll_interval_ms * std.time.ns_per_ms);
            }
        }
    }

    /// Request the processor to stop. Safe to call from another thread.
    pub fn stop(self: *Processor) void {
        self.stop_flag.store(true, .release);
    }

    pub fn deinit(self: *Processor) void {
        var it = self.relations.iterator();
        while (it.next()) |entry| {
            var rel = entry.value_ptr;
            rel.deinit(self.allocator);
        }
        self.relations.deinit();
        self.clearPending();
        self.pending_data.deinit(self.allocator);
        self.pending_batch_ids.deinit(self.allocator);
        self.dest.close();
    }
};
