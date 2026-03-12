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
    in_transaction: bool,
    txn_begin_lsn: Lsn,

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

    pub fn init(allocator: std.mem.Allocator, config: types.IngestConfig) InitError!Ingestor {
        var dest_config = config.dest;
        dest_config.replication = false;
        var dest = try Connection.connect(allocator, dest_config);
        errdefer dest.close();

        try schema_mod.ensureSchema(&dest);

        var replicator = try Replicator.init(allocator, config.source);
        errdefer replicator.deinit();

        return Ingestor{
            .replicator = replicator,
            .dest = dest,
            .allocator = allocator,
            .config = config,
            .batch_buf = .{},
            .batch_start_lsn = Lsn.zero,
            .batch_end_lsn = Lsn.zero,
            .batch_msg_count = 0,
            .in_transaction = false,
            .txn_begin_lsn = Lsn.zero,
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
            std.log.debug("ingest: msg #{d} type='{c}' lsn={f} len={d} in_tx={} batch_msgs={d} batch_bytes={d}", .{
                total_msgs,
                msg_type,
                wal_msg.wal_start,
                data.len,
                self.in_transaction,
                self.batch_msg_count,
                self.batch_buf.items.len,
            });

            // Track batch LSN boundaries
            if (self.batch_msg_count == 0) {
                self.batch_start_lsn = wal_msg.wal_start;
            }
            self.batch_end_lsn = wal_msg.wal_start;

            // Track transaction boundaries (Begin or StreamStart)
            if (msg_type == 'B' or msg_type == 'S') {
                self.in_transaction = true;
                self.txn_begin_lsn = wal_msg.wal_start;
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

            // Flush on COMMIT or StreamCommit boundaries
            if (msg_type == 'C' or msg_type == 'c') {
                self.in_transaction = false;
                self.txn_begin_lsn = Lsn.zero;
                std.log.debug("ingest: flushing batch on commit, msgs={d} bytes={d}", .{
                    self.batch_msg_count,
                    self.batch_buf.items.len,
                });
                try self.flushBatch(true);
                std.log.debug("ingest: flush complete, calling ack lsn={f}", .{wal_msg.wal_start});
                self.replicator.ack(wal_msg.wal_start);
                std.log.debug("ingest: ack done, continuing to next message", .{});
                continue;
            }

            // Flush mid-transaction when batch size limit is exceeded
            if (self.batch_buf.items.len >= self.config.max_batch_size) {
                std.log.debug("ingest: batch size limit reached ({d} >= {d})", .{
                    self.batch_buf.items.len,
                    self.config.max_batch_size,
                });
                if (self.in_transaction) {
                    // Partial batch — processor will reassemble
                    try self.flushBatch(false);
                } else {
                    // Between transactions — safe to flush as complete
                    try self.flushBatch(true);
                    self.replicator.ack(wal_msg.wal_start);
                }
            }
        }

        // Flush any remaining batch data
        if (self.batch_msg_count > 0) {
            try self.flushBatch(!self.in_transaction);
        }

        std.log.info("Ingestor stopped", .{});
    }

    fn updateRelationCache(self: *Ingestor, data: []const u8) !void {
        const msg = pgoutput.decode(data, &self.col_buf, &self.tuple_buf, &self.tuple_buf2, null) catch return;
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

        const relations_json = try self.serializeRelations();
        defer self.allocator.free(relations_json);

        // Build INSERT query with inline hex encoding (no intermediate data_hex allocation)
        var sql: std.ArrayListUnmanaged(u8) = .{};
        defer sql.deinit(self.allocator);

        const begin_lsn = if (self.txn_begin_lsn.value != 0) self.txn_begin_lsn else self.batch_start_lsn;

        try sql.appendSlice(self.allocator, "INSERT INTO wal_batches (source_id, begin_lsn, start_lsn, end_lsn, data, relations, complete) VALUES (");
        try query.appendEscapedUuid(&sql, self.allocator, self.config.source_id);
        try sql.appendSlice(self.allocator, ", ");
        try query.appendIntValue(&sql, self.allocator, begin_lsn.value);
        try sql.appendSlice(self.allocator, ", ");
        try query.appendIntValue(&sql, self.allocator, self.batch_start_lsn.value);
        try sql.appendSlice(self.allocator, ", ");
        try query.appendIntValue(&sql, self.allocator, self.batch_end_lsn.value);
        try sql.appendSlice(self.allocator, ", ");
        try query.appendEscapedBytea(&sql, self.allocator, self.batch_buf.items);
        try sql.appendSlice(self.allocator, ", ");
        try sql.appendSlice(self.allocator, relations_json);
        try sql.appendSlice(self.allocator, ", ");

        if (complete) {
            try sql.appendSlice(self.allocator, "true");
        } else {
            try sql.appendSlice(self.allocator, "false");
        }
        try sql.appendSlice(self.allocator, ") ON CONFLICT (source_id, start_lsn) DO NOTHING");

        std.log.debug("flushBatch: sending INSERT sql_len={d} data_bytes={d}", .{
            sql.items.len,
            self.batch_buf.items.len,
        });
        try self.dest.execLarge(self.allocator, sql.items);
        std.log.debug("flushBatch: execLarge complete", .{});

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
        var json: std.ArrayListUnmanaged(u8) = .{};
        errdefer json.deinit(self.allocator);

        try json.append(self.allocator, '\'');
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

        try json.appendSlice(self.allocator, "}'::jsonb");

        return json.toOwnedSlice(self.allocator);
    }

    fn appendJsonEscaped(list: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, s: []const u8) !void {
        for (s) |c| {
            switch (c) {
                '"' => try list.appendSlice(allocator, "\\\""),
                '\'' => try list.appendSlice(allocator, "''"),
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
        self.batch_buf.deinit(self.allocator);
        self.replicator.deinit();
        self.dest.close();
    }
};
