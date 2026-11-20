const std = @import("std");
const Replicator = @import("replicator.zig").Replicator;
const Connection = @import("connection.zig").Connection;
const pgoutput = @import("pgoutput.zig");
const types = @import("types.zig");
const Lsn = @import("lsn.zig").Lsn;
const query = @import("query.zig");
const schema_mod = @import("schema.zig");

pub const Ingestor = struct {
    replicator: Replicator,
    dest: Connection,
    allocator: std.mem.Allocator,
    config: types.IngestConfig,

    // Batch accumulation
    batch_buf: std.ArrayListUnmanaged(u8),
    batch_start_lsn: Lsn,
    batch_end_lsn: Lsn,
    batch_msg_count: usize,

    // Open-transaction tracking. A batch group (a complete batch plus the
    // partial batches that precede it) covers a contiguous window of the
    // WAL stream; the group's begin_lsn is fixed when the first transaction
    // opens and the group closes only when a commit/abort leaves zero
    // transactions open. Streamed transactions (proto v2+) may interleave,
    // so open streamed xids are tracked as a set; non-streamed transactions
    // are contiguous, so one flag suffices.
    nonstream_open: bool,
    open_stream_xids: std.AutoHashMap(u32, void),
    group_begin_lsn: Lsn,

    // Streaming context (proto v2+): xid of the stream segment currently
    // open (between StreamStart and StreamStop). DML and Relation messages
    // inside a segment carry a leading Int32 xid the decoder must skip.
    stream_xid: ?u32,

    // Relation cache (owned copies)
    relations: std.AutoHashMap(u32, OwnedRelation),

    // pgoutput decode scratch buffers
    col_buf: [256]pgoutput.Column,
    tuple_buf: [256]pgoutput.ColumnData,
    tuple_buf2: [256]pgoutput.ColumnData,

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

    pub const InitError = Replicator.InitError || Connection.ConnectError || Connection.QueryError || std.mem.Allocator.Error;

    pub fn init(allocator: std.mem.Allocator, io: std.Io, config: types.IngestConfig) InitError!Ingestor {
        var dest_config = config.dest;
        dest_config.replication = false;
        var dest = try Connection.connect(allocator, io, dest_config);
        errdefer dest.close();

        try schema_mod.ensureSchema(&dest);

        var replicator = try Replicator.init(allocator, io, config.source);
        errdefer replicator.deinit();

        return Ingestor{
            .replicator = replicator,
            .dest = dest,
            .allocator = allocator,
            .config = config,
            .batch_buf = .empty,
            .batch_start_lsn = Lsn.zero,
            .batch_end_lsn = Lsn.zero,
            .batch_msg_count = 0,
            .nonstream_open = false,
            .open_stream_xids = std.AutoHashMap(u32, void).init(allocator),
            .group_begin_lsn = Lsn.zero,
            .stream_xid = null,
            .relations = std.AutoHashMap(u32, OwnedRelation).init(allocator),
            .col_buf = undefined,
            .tuple_buf = undefined,
            .tuple_buf2 = undefined,
        };
    }

    pub const RunError = Replicator.NextError || Connection.QueryError || std.mem.Allocator.Error || error{InvalidUuid};

    /// Main ingest loop. Streams WAL from source and stores packed batches
    /// in the destination database. Blocks until the replicator is stopped
    /// or end_position is reached.
    ///
    /// Batches are flushed on COMMIT boundaries. If a transaction exceeds
    /// `max_batch_size`, the batch is split mid-transaction with
    /// `complete=false`. The processor reassembles partial batches.
    pub fn run(self: *Ingestor) RunError!void {
        std.log.info("Ingestor started: source_id={s}", .{self.config.source_id});
        var total_msgs: u64 = 0;
        while (try self.replicator.next()) |wal_msg| {
            const data = wal_msg.data;
            if (data.len == 0) continue;

            total_msgs += 1;
            const msg_type: u8 = data[0];
            std.log.debug("ingest: msg #{d} type='{c}' lsn={f} len={d} open_txns={d} batch_msgs={d} batch_bytes={d}", .{
                total_msgs,
                msg_type,
                wal_msg.wal_start,
                data.len,
                self.openTxnCount(),
                self.batch_msg_count,
                self.batch_buf.items.len,
            });

            // Track batch LSN boundaries
            if (self.batch_msg_count == 0) {
                self.batch_start_lsn = wal_msg.wal_start;
            }
            self.batch_end_lsn = wal_msg.wal_start;

            // Track transaction opens; the first open fixes the group's
            // begin_lsn. 'S' also opens/continues the stream segment so
            // streamed messages (which carry a leading xid) decode
            // correctly; 'E' (StreamStop) ends the segment.
            if (msg_type == 'B') {
                self.nonstream_open = true;
                if (self.group_begin_lsn.value == 0) self.group_begin_lsn = wal_msg.wal_start;
            } else if (msg_type == 'S') {
                if (data.len >= 5) {
                    const xid = std.mem.readInt(u32, data[1..5], .big);
                    self.stream_xid = xid;
                    try self.open_stream_xids.put(xid, {});
                } else {
                    self.stream_xid = null;
                }
                if (self.group_begin_lsn.value == 0) self.group_begin_lsn = wal_msg.wal_start;
            } else if (msg_type == 'E') {
                self.stream_xid = null;
            }

            // Parse relation messages to maintain the cache
            if (msg_type == 'R') {
                try self.updateRelationCache(data);
            }

            // Append message to batch: [4-byte big-endian length][message bytes]
            const len: u32 = @intCast(data.len);
            try self.batch_buf.appendSlice(self.allocator, &std.mem.toBytes(std.mem.nativeTo(u32, len, .big)));
            try self.batch_buf.appendSlice(self.allocator, data);
            self.batch_msg_count += 1;

            // Track transaction closes. StreamAbort closes the top-level
            // transaction only when sub_xid == xid; a subtransaction abort
            // leaves the stream open.
            var closed = false;
            if (msg_type == 'C') {
                self.nonstream_open = false;
                closed = true;
            } else if (msg_type == 'c') {
                self.stream_xid = null;
                if (data.len >= 5) {
                    _ = self.open_stream_xids.remove(std.mem.readInt(u32, data[1..5], .big));
                }
                closed = true;
            } else if (msg_type == 'A') {
                self.stream_xid = null;
                if (data.len >= 9) {
                    const xid = std.mem.readInt(u32, data[1..5], .big);
                    const sub_xid = std.mem.readInt(u32, data[5..9], .big);
                    if (sub_xid == xid) {
                        _ = self.open_stream_xids.remove(xid);
                        closed = true;
                    }
                }
            }

            // Flush complete when a close leaves no transaction open; the
            // group (and its begin_lsn) ends here. Commits with other
            // streamed transactions still open stay in the buffer so every
            // batch of the window shares one group.
            if (closed and self.openTxnCount() == 0) {
                std.log.debug("ingest: flushing batch on group close, msgs={d} bytes={d}", .{
                    self.batch_msg_count,
                    self.batch_buf.items.len,
                });
                try self.flushBatch(true);
                self.group_begin_lsn = Lsn.zero;
                self.replicator.ack(wal_msg.wal_start);
                continue;
            }

            // Flush mid-group when batch size limit is exceeded
            if (self.batch_buf.items.len >= self.config.max_batch_size) {
                std.log.debug("ingest: batch size limit reached ({d} >= {d})", .{
                    self.batch_buf.items.len,
                    self.config.max_batch_size,
                });
                if (self.openTxnCount() > 0) {
                    // Partial batch — processor will reassemble
                    try self.flushBatch(false);
                } else {
                    // Between transactions — safe to flush as complete
                    try self.flushBatch(true);
                    self.group_begin_lsn = Lsn.zero;
                    self.replicator.ack(wal_msg.wal_start);
                }
            }
        }

        // Flush any remaining batch data
        if (self.batch_msg_count > 0) {
            try self.flushBatch(self.openTxnCount() == 0);
        }

        std.log.info("Ingestor stopped", .{});
    }

    fn openTxnCount(self: *const Ingestor) usize {
        return self.open_stream_xids.count() + @intFromBool(self.nonstream_open);
    }

    fn updateRelationCache(self: *Ingestor, data: []const u8) !void {
        const msg = pgoutput.decode(data, &self.col_buf, &self.tuple_buf, &self.tuple_buf2, self.stream_xid) catch return;
        const rel = switch (msg) {
            .relation => |r| r,
            else => return,
        };

        // Remove old entry if it exists
        if (self.relations.fetchRemove(rel.oid)) |entry| {
            var old = entry.value;
            old.deinit(self.allocator);
        }

        // Copy all borrowed strings into owned memory
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

    fn flushBatch(self: *Ingestor, complete: bool) !void {
        if (self.batch_msg_count == 0) return;

        if (!query.isValidUuid(self.config.source_id)) return error.InvalidUuid;

        const relations_json = try self.serializeRelations();
        defer self.allocator.free(relations_json);

        // Build one COPY text-format row. COPY skips SQL
        // lexing/parsing/planning on the server — for megabyte-scale
        // batch rows that dominates the insert cost.
        var row: std.ArrayListUnmanaged(u8) = .empty;
        defer row.deinit(self.allocator);

        const begin_lsn = if (self.group_begin_lsn.value != 0) self.group_begin_lsn else self.batch_start_lsn;

        try row.appendSlice(self.allocator, self.config.source_id);
        try row.append(self.allocator, '\t');
        try query.appendIntValue(&row, self.allocator, begin_lsn.value);
        try row.append(self.allocator, '\t');
        try query.appendIntValue(&row, self.allocator, self.batch_start_lsn.value);
        try row.append(self.allocator, '\t');
        try query.appendIntValue(&row, self.allocator, self.batch_end_lsn.value);
        try row.append(self.allocator, '\t');
        try query.appendCopyBytea(&row, self.allocator, self.batch_buf.items);
        try row.append(self.allocator, '\t');
        try query.appendCopyText(&row, self.allocator, relations_json);
        try row.append(self.allocator, '\t');
        try row.append(self.allocator, if (complete) 't' else 'f');
        try row.append(self.allocator, '\n');

        std.log.debug("flushBatch: sending COPY row_len={d} data_bytes={d}", .{
            row.items.len,
            self.batch_buf.items.len,
        });
        self.dest.copyIn(
            self.allocator,
            "COPY wal_batches (source_id, begin_lsn, start_lsn, end_lsn, data, relations, complete) FROM STDIN",
            row.items,
        ) catch |err| switch (err) {
            error.ServerError => {
                // A unique violation on (source_id, start_lsn) means this
                // batch was already stored by a previous run (unacked WAL
                // replayed after restart) — equivalent to the old
                // ON CONFLICT DO NOTHING.
                const code = self.dest.lastServerErrorCode() orelse return err;
                if (!std.mem.eql(u8, code, "23505")) return err;
                std.log.debug("flushBatch: duplicate batch start_lsn={f}, already stored", .{self.batch_start_lsn});
            },
            else => return err,
        };

        std.log.info("batch flushed: start_lsn={f} end_lsn={f} msgs={d} bytes={d} complete={}", .{
            self.batch_start_lsn,
            self.batch_end_lsn,
            self.batch_msg_count,
            self.batch_buf.items.len,
            complete,
        });

        // Notify caller of successful flush
        if (self.config.on_flush) |cb| {
            cb(self.config.on_flush_context, self.batch_start_lsn, self.batch_end_lsn, self.batch_msg_count, complete);
        }

        // Reset batch
        self.batch_buf.clearRetainingCapacity();
        self.batch_msg_count = 0;
    }

    fn serializeRelations(self: *Ingestor) ![]u8 {
        var json: std.ArrayListUnmanaged(u8) = .empty;
        errdefer json.deinit(self.allocator);

        try json.append(self.allocator, '{');

        var first_rel = true;
        var it = self.relations.iterator();
        while (it.next()) |entry| {
            const rel = entry.value_ptr;
            if (!first_rel) try json.append(self.allocator, ',');
            first_rel = false;

            // Key: OID as string
            try json.append(self.allocator, '"');
            var oid_buf: [10]u8 = undefined;
            const oid_str = std.fmt.bufPrint(&oid_buf, "{d}", .{rel.oid}) catch unreachable;
            try json.appendSlice(self.allocator, oid_str);
            try json.appendSlice(self.allocator, "\":{");

            // schema
            try json.appendSlice(self.allocator, "\"schema\":\"");
            try appendJsonEscaped(&json, self.allocator, rel.namespace);
            try json.appendSlice(self.allocator, "\",");

            // table
            try json.appendSlice(self.allocator, "\"table\":\"");
            try appendJsonEscaped(&json, self.allocator, rel.name);
            try json.appendSlice(self.allocator, "\",");

            // relreplident
            try json.appendSlice(self.allocator, "\"relreplident\":");
            var ri_buf: [3]u8 = undefined;
            const ri_str = std.fmt.bufPrint(&ri_buf, "{d}", .{rel.replica_identity}) catch unreachable;
            try json.appendSlice(self.allocator, ri_str);
            try json.append(self.allocator, ',');

            // columns
            try json.appendSlice(self.allocator, "\"columns\":[");
            for (rel.columns, 0..) |col, i| {
                if (i > 0) try json.append(self.allocator, ',');
                try json.append(self.allocator, '{');

                try json.appendSlice(self.allocator, "\"flags\":");
                var flags_buf: [3]u8 = undefined;
                const flags_str = std.fmt.bufPrint(&flags_buf, "{d}", .{col.flags}) catch unreachable;
                try json.appendSlice(self.allocator, flags_str);
                try json.append(self.allocator, ',');

                try json.appendSlice(self.allocator, "\"name\":\"");
                try appendJsonEscaped(&json, self.allocator, col.name);
                try json.appendSlice(self.allocator, "\",");

                try json.appendSlice(self.allocator, "\"oid\":");
                var col_oid_buf: [10]u8 = undefined;
                const col_oid_str = std.fmt.bufPrint(&col_oid_buf, "{d}", .{col.type_oid}) catch unreachable;
                try json.appendSlice(self.allocator, col_oid_str);
                try json.append(self.allocator, ',');

                try json.appendSlice(self.allocator, "\"typmod\":");
                var typmod_buf: [11]u8 = undefined;
                const typmod_str = std.fmt.bufPrint(&typmod_buf, "{d}", .{col.type_modifier}) catch unreachable;
                try json.appendSlice(self.allocator, typmod_str);

                try json.append(self.allocator, '}');
            }
            try json.appendSlice(self.allocator, "]}");
        }

        try json.append(self.allocator, '}');

        return json.toOwnedSlice(self.allocator);
    }

    fn appendJsonEscaped(list: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, s: []const u8) !void {
        for (s) |c| {
            switch (c) {
                '"' => try list.appendSlice(allocator, "\\\""),
                '\\' => try list.appendSlice(allocator, "\\\\"),
                '\n' => try list.appendSlice(allocator, "\\n"),
                '\r' => try list.appendSlice(allocator, "\\r"),
                '\t' => try list.appendSlice(allocator, "\\t"),
                0x00...0x08, 0x0b, 0x0c, 0x0e...0x1f => {
                    var buf: [6]u8 = undefined;
                    _ = std.fmt.bufPrint(&buf, "\\u{x:0>4}", .{c}) catch unreachable;
                    try list.appendSlice(allocator, &buf);
                },
                else => try list.append(allocator, c),
            }
        }
    }

    pub fn stop(self: *Ingestor) void {
        self.replicator.stop();
    }

    pub fn isStopRequested(self: *Ingestor) bool {
        return self.replicator.isStopRequested();
    }

    pub fn deinit(self: *Ingestor) void {
        // Free all owned relation data
        var it = self.relations.iterator();
        while (it.next()) |entry| {
            var rel = entry.value_ptr;
            rel.deinit(self.allocator);
        }
        self.relations.deinit();
        self.open_stream_xids.deinit();
        self.batch_buf.deinit(self.allocator);
        self.replicator.deinit();
        self.dest.close();
    }
};
