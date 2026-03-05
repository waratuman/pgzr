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
    current_txn_id: ?i64,
    in_transaction: bool,

    // Streaming context (proto v2+)
    current_stream_xid: ?u32,

    // Batch accumulation for partial (split) batches
    pending_data: std.ArrayListUnmanaged(u8),
    pending_batch_ids: std.ArrayListUnmanaged([]u8),

    // Metadata accumulation (per-transaction)
    metadata_chunks: std.ArrayListUnmanaged([]u8),
    metadata_table_oid: ?u32,

    // Stop flag for graceful shutdown
    stop_flag: std.atomic.Value(bool),

    const OwnedRelation = struct {
        oid: u32,
        namespace: []u8,
        name: []u8,
        replica_identity: u8,
        columns: []OwnedColumn,
        snapshot_lsn: ?u64,

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
            .current_txn_id = null,
            .in_transaction = false,
            .current_stream_xid = null,
            .pending_data = .{},
            .pending_batch_ids = .{},
            .metadata_chunks = .{},
            .metadata_table_oid = null,
            .stop_flag = std.atomic.Value(bool).init(false),
        };
    }

    pub const ProcessError = Connection.QueryError || std.mem.Allocator.Error || pgoutput.DecodeError || error{InvalidUuid};

    /// Process one pending batch. Returns true if a batch was claimed,
    /// false if no pending batches were found.
    ///
    /// Partial batches (complete=false) are accumulated in memory until
    /// a complete batch arrives, then all accumulated data is processed
    /// as one logical unit.
    pub fn processOne(self: *Processor) ProcessError!bool {
        // Claim a batch
        var claim_sql: std.ArrayListUnmanaged(u8) = .{};
        defer claim_sql.deinit(self.allocator);

        try claim_sql.appendSlice(
            self.allocator,
            "UPDATE wal_batches SET state='processing'" ++
                " WHERE id = (" ++
                " SELECT id FROM wal_batches" ++
                " WHERE source_id=",
        );
        try query_mod.appendEscapedUuid(&claim_sql, self.allocator, self.config.source_id);
        try claim_sql.appendSlice(
            self.allocator,
            " AND state='pending'" ++
                " ORDER BY start_lsn LIMIT 1" ++
                " FOR UPDATE SKIP LOCKED" ++
                ") RETURNING id, data, complete",
        );

        const result = try self.dest.simpleQuery(claim_sql.items);
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
            const err_sql = std.fmt.bufPrint(
                &err_buf,
                "UPDATE wal_batches SET state='error' WHERE id={s}",
                .{bid},
            ) catch continue;
            _ = self.dest.simpleQuery(err_sql) catch {};
        }
    }

    fn deleteBatches(self: *Processor) !void {
        for (self.pending_batch_ids.items) |bid| {
            var del_buf: [128]u8 = undefined;
            const del_sql = std.fmt.bufPrint(
                &del_buf,
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
                    self.clearMetadataChunks();
                    self.current_txn_id = try self.insertTransaction(begin.final_lsn);
                },
                .commit => {
                    try self.flushMetadata();
                    self.in_transaction = false;
                    self.current_txn_id = null;
                },
                .relation => |rel| {
                    try self.updateRelationCache(rel);
                    try self.insertRelationSnapshot(rel);
                    if (self.config.metadata_table) |mt| {
                        if (std.mem.eql(u8, rel.name, mt)) {
                            self.metadata_table_oid = rel.oid;
                        }
                    }
                },
                .insert => |ins| {
                    if (self.isMetadataTable(ins.relation_oid)) {
                        try self.extractMetadataFromTuple(ins.relation_oid, ins.new_tuple);
                    } else {
                        try self.insertEvent(ins.relation_oid, 'I', ins.new_tuple, null);
                    }
                },
                .update => |upd| {
                    if (self.isMetadataTable(upd.relation_oid)) {
                        try self.extractMetadataFromTuple(upd.relation_oid, upd.new_tuple);
                    } else {
                        try self.insertEvent(upd.relation_oid, 'U', upd.new_tuple, upd.old_tuple);
                    }
                },
                .delete => |del| {
                    if (!self.isMetadataTable(del.relation_oid)) {
                        try self.insertEvent(del.relation_oid, 'D', null, del.old_tuple);
                    }
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
                    try self.flushMetadata();
                    self.current_stream_xid = null;
                    self.in_transaction = false;
                    self.current_txn_id = null;
                },
                .stream_abort => {
                    self.current_stream_xid = null;
                    self.in_transaction = false;
                    self.current_txn_id = null;
                },
                // Proto v3: two-phase commit
                .begin_prepare => |bp| {
                    self.current_txn_lsn = bp.lsn;
                    self.current_txn_xid = bp.xid;
                    self.current_txn_timestamp = bp.timestamp;
                    self.in_transaction = true;
                    self.clearMetadataChunks();
                    self.current_txn_id = try self.insertTransaction(bp.lsn);
                },
                .prepare => {
                    // Prepared but not yet committed — keep txn state
                },
                .commit_prepared => {
                    try self.flushMetadata();
                    self.in_transaction = false;
                    self.current_txn_id = null;
                },
                .rollback_prepared => {
                    self.in_transaction = false;
                    self.current_txn_id = null;
                },
                .stream_prepare => {
                    // Prepared within streaming — keep txn state
                },
                .message => |logical_msg| {
                    if (self.config.metadata_message_prefix) |prefix| {
                        if (std.mem.eql(u8, logical_msg.prefix, prefix)) {
                            try self.addMetadataChunk(logical_msg.content);
                        }
                    }
                },
                else => {},
            }
        }
    }

    fn insertTransaction(self: *Processor, lsn: u64) !?i64 {
        var sql: std.ArrayListUnmanaged(u8) = .{};
        defer sql.deinit(self.allocator);

        try sql.appendSlice(self.allocator, "INSERT INTO transactions (source_id, lsn, xid, committed_at) VALUES (");
        try query_mod.appendEscapedUuid(&sql, self.allocator, self.config.source_id);
        try sql.appendSlice(self.allocator, ", ");
        try query_mod.appendIntValue(&sql, self.allocator, lsn);
        try sql.appendSlice(self.allocator, ", ");
        try query_mod.appendIntValue(&sql, self.allocator, self.current_txn_xid);
        try sql.appendSlice(self.allocator, ", ");
        try query_mod.appendTimestamp(&sql, self.allocator, self.current_txn_timestamp);
        try sql.appendSlice(self.allocator, ") ON CONFLICT (source_id, lsn, committed_at) DO NOTHING RETURNING id");

        const result = try self.dest.execLargeWithResult(self.allocator, sql.items);
        if (result.column_count > 0 and result.columns[0].data.len > 0) {
            return std.fmt.parseInt(i64, result.columns[0].data, 10) catch null;
        }
        // ON CONFLICT hit — look up existing id
        var lookup_sql: std.ArrayListUnmanaged(u8) = .{};
        defer lookup_sql.deinit(self.allocator);

        try lookup_sql.appendSlice(self.allocator, "SELECT id FROM transactions WHERE source_id=");
        try query_mod.appendEscapedUuid(&lookup_sql, self.allocator, self.config.source_id);
        try lookup_sql.appendSlice(self.allocator, " AND lsn=");
        try query_mod.appendIntValue(&lookup_sql, self.allocator, lsn);
        try lookup_sql.appendSlice(self.allocator, " AND committed_at=");
        try query_mod.appendTimestamp(&lookup_sql, self.allocator, self.current_txn_timestamp);

        const lookup_result = try self.dest.simpleQuery(lookup_sql.items);
        if (lookup_result.column_count > 0 and lookup_result.columns[0].data.len > 0) {
            return std.fmt.parseInt(i64, lookup_result.columns[0].data, 10) catch null;
        }
        return null;
    }

    fn insertRelationSnapshot(self: *Processor, rel: pgoutput.Relation) !void {
        // Check if we already have a snapshot for this OID at this LSN
        if (self.relations.get(rel.oid)) |owned| {
            if (owned.snapshot_lsn != null and owned.snapshot_lsn.? == self.current_txn_lsn) {
                return; // Already inserted for this LSN
            }
        }

        var sql: std.ArrayListUnmanaged(u8) = .{};
        defer sql.deinit(self.allocator);

        try sql.appendSlice(
            self.allocator,
            "INSERT INTO relation_snapshots (source_id, lsn, rel_oid, schema_name, table_name, replica_identity, columns) VALUES (",
        );
        try query_mod.appendEscapedUuid(&sql, self.allocator, self.config.source_id);
        try sql.appendSlice(self.allocator, ", ");
        try query_mod.appendIntValue(&sql, self.allocator, self.current_txn_lsn);
        try sql.appendSlice(self.allocator, ", ");
        try query_mod.appendIntValue(&sql, self.allocator, rel.oid);
        try sql.appendSlice(self.allocator, ", ");
        try query_mod.appendEscapedString(&sql, self.allocator, rel.namespace);
        try sql.appendSlice(self.allocator, ", ");
        try query_mod.appendEscapedString(&sql, self.allocator, rel.name);
        try sql.appendSlice(self.allocator, ", ");
        try query_mod.appendIntValue(&sql, self.allocator, rel.replica_identity);
        try sql.appendSlice(self.allocator, ", '");

        // Build JSONB columns array
        try sql.appendSlice(self.allocator, "[");
        for (rel.columns, 0..) |col, i| {
            if (i > 0) try sql.appendSlice(self.allocator, ",");
            try sql.appendSlice(self.allocator, "{\"name\":");
            try appendJsonString(&sql, self.allocator, col.name);
            try sql.appendSlice(self.allocator, ",\"oid\":");
            try query_mod.appendIntValue(&sql, self.allocator, col.type_oid);
            try sql.appendSlice(self.allocator, ",\"type\":");
            var type_name_buf: [32]u8 = undefined;
            const type_name = pg_types.oidToName(col.type_oid, &type_name_buf);
            try appendJsonString(&sql, self.allocator, type_name);
            try sql.appendSlice(self.allocator, ",\"identity\":");
            if ((col.flags & 1) != 0) {
                try sql.appendSlice(self.allocator, "true");
            } else {
                try sql.appendSlice(self.allocator, "false");
            }
            try sql.appendSlice(self.allocator, ",\"ordinal\":");
            try query_mod.appendIntValue(&sql, self.allocator, i);
            try sql.append(self.allocator, '}');
        }
        try sql.appendSlice(self.allocator, "]");

        try sql.appendSlice(self.allocator, "') ON CONFLICT (source_id, rel_oid, lsn) DO NOTHING");

        try self.dest.execLarge(self.allocator, sql.items);

        // Update snapshot_lsn in cache
        if (self.relations.getPtr(rel.oid)) |owned| {
            owned.snapshot_lsn = self.current_txn_lsn;
        }
    }

    fn insertEvent(
        self: *Processor,
        relation_oid: u32,
        event_type: u8,
        tuple: ?[]const pgoutput.ColumnData,
        old_tuple: ?[]const pgoutput.ColumnData,
    ) !void {
        const rel = self.relations.get(relation_oid) orelse return;

        // Compute identity digests
        const identity_digest = if (tuple) |t|
            try self.computeIdentityDigest(rel, t)
        else if (old_tuple) |ot|
            try self.computeIdentityDigest(rel, ot)
        else
            null;
        defer if (identity_digest) |d| self.allocator.free(d);

        const prev_identity_digest = if (old_tuple) |ot|
            try self.computeIdentityDigest(rel, ot)
        else
            null;
        defer if (prev_identity_digest) |d| self.allocator.free(d);

        var sql: std.ArrayListUnmanaged(u8) = .{};
        defer sql.deinit(self.allocator);

        try sql.appendSlice(
            self.allocator,
            "INSERT INTO events (transaction_id, committed_at, rel_oid, type, " ++
                "identity_digest, previous_identity_digest, data, old_data) VALUES (",
        );

        // transaction_id
        if (self.current_txn_id) |txn_id| {
            try query_mod.appendIntValue(&sql, self.allocator, txn_id);
        } else {
            try sql.appendSlice(self.allocator, "(SELECT id FROM transactions WHERE source_id=");
            try query_mod.appendEscapedUuid(&sql, self.allocator, self.config.source_id);
            try sql.appendSlice(self.allocator, " AND lsn=");
            try query_mod.appendIntValue(&sql, self.allocator, self.current_txn_lsn);
            try sql.appendSlice(self.allocator, " AND committed_at=");
            try query_mod.appendTimestamp(&sql, self.allocator, self.current_txn_timestamp);
            try sql.append(self.allocator, ')');
        }
        try sql.appendSlice(self.allocator, ", ");

        // committed_at
        try query_mod.appendTimestamp(&sql, self.allocator, self.current_txn_timestamp);
        try sql.appendSlice(self.allocator, ", ");

        // rel_oid
        try query_mod.appendIntValue(&sql, self.allocator, relation_oid);
        try sql.appendSlice(self.allocator, ", '");
        try sql.append(self.allocator, event_type);
        try sql.appendSlice(self.allocator, "', ");

        // identity_digest
        try query_mod.appendByteaOrNull(&sql, self.allocator, identity_digest);
        try sql.appendSlice(self.allocator, ", ");

        // previous_identity_digest
        try query_mod.appendByteaOrNull(&sql, self.allocator, prev_identity_digest);
        try sql.appendSlice(self.allocator, ", ");

        // data JSONB — build from tuple
        if (tuple) |t| {
            try self.appendTupleAsJsonb(&sql, rel, t);
        } else {
            try sql.appendSlice(self.allocator, "NULL");
        }
        try sql.appendSlice(self.allocator, ", ");

        // old_data JSONB — build from old_tuple
        if (old_tuple) |ot| {
            try self.appendTupleAsJsonb(&sql, rel, ot);
        } else {
            try sql.appendSlice(self.allocator, "NULL");
        }

        try sql.append(self.allocator, ')');

        try self.dest.execLarge(self.allocator, sql.items);
    }

    fn insertTruncateEvent(self: *Processor) !void {
        var sql: std.ArrayListUnmanaged(u8) = .{};
        defer sql.deinit(self.allocator);

        try sql.appendSlice(
            self.allocator,
            "INSERT INTO events (transaction_id, committed_at, rel_oid, type, data, old_data) VALUES (",
        );

        // transaction_id
        if (self.current_txn_id) |txn_id| {
            try query_mod.appendIntValue(&sql, self.allocator, txn_id);
        } else {
            try sql.appendSlice(self.allocator, "(SELECT id FROM transactions WHERE source_id=");
            try query_mod.appendEscapedUuid(&sql, self.allocator, self.config.source_id);
            try sql.appendSlice(self.allocator, " AND lsn=");
            try query_mod.appendIntValue(&sql, self.allocator, self.current_txn_lsn);
            try sql.appendSlice(self.allocator, " AND committed_at=");
            try query_mod.appendTimestamp(&sql, self.allocator, self.current_txn_timestamp);
            try sql.append(self.allocator, ')');
        }

        try sql.appendSlice(self.allocator, ", ");
        try query_mod.appendTimestamp(&sql, self.allocator, self.current_txn_timestamp);
        try sql.appendSlice(self.allocator, ", 0, 'T', NULL, NULL)");

        try self.dest.execLarge(self.allocator, sql.items);
    }

    /// Append a pgoutput tuple as a JSONB literal ('{"col":"val",...}')
    /// or NULL if the tuple has no data.
    fn appendTupleAsJsonb(
        self: *Processor,
        sql: *std.ArrayListUnmanaged(u8),
        rel: OwnedRelation,
        tuple: []const pgoutput.ColumnData,
    ) !void {
        var has_any = false;
        for (rel.columns, 0..) |_, i| {
            if (i < tuple.len) {
                switch (tuple[i]) {
                    .text, .binary, .null_value => {
                        has_any = true;
                        break;
                    },
                    .unchanged => {},
                }
            }
        }

        if (!has_any) {
            try sql.appendSlice(self.allocator, "NULL");
            return;
        }

        try sql.appendSlice(self.allocator, "'{");
        var first = true;
        for (rel.columns, 0..) |col, i| {
            if (i >= tuple.len) continue;

            switch (tuple[i]) {
                .text => |t| {
                    if (!first) try sql.appendSlice(self.allocator, ",");
                    first = false;
                    try appendJsonString(sql, self.allocator, col.name);
                    try sql.append(self.allocator, ':');
                    try appendJsonString(sql, self.allocator, t);
                },
                .binary => |b| {
                    if (!first) try sql.appendSlice(self.allocator, ",");
                    first = false;
                    try appendJsonString(sql, self.allocator, col.name);
                    try sql.append(self.allocator, ':');
                    try appendJsonString(sql, self.allocator, b);
                },
                .null_value => {
                    if (!first) try sql.appendSlice(self.allocator, ",");
                    first = false;
                    try appendJsonString(sql, self.allocator, col.name);
                    try sql.appendSlice(self.allocator, ":null");
                },
                .unchanged => {},
            }
        }
        try sql.appendSlice(self.allocator, "}'::jsonb");
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
        // Preserve snapshot_lsn if updating an existing entry
        var prev_snapshot_lsn: ?u64 = null;
        if (self.relations.fetchRemove(rel.oid)) |entry| {
            prev_snapshot_lsn = entry.value.snapshot_lsn;
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
            .snapshot_lsn = prev_snapshot_lsn,
        });
    }

    fn isMetadataTable(self: *Processor, relation_oid: u32) bool {
        const mt_oid = self.metadata_table_oid orelse return false;
        return relation_oid == mt_oid;
    }

    fn addMetadataChunk(self: *Processor, content: []const u8) !void {
        const copy = try self.allocator.alloc(u8, content.len);
        @memcpy(copy, content);
        try self.metadata_chunks.append(self.allocator, copy);
    }

    fn extractMetadataFromTuple(self: *Processor, relation_oid: u32, tuple: ?[]const pgoutput.ColumnData) !void {
        const t = tuple orelse return;
        const rel = self.relations.get(relation_oid) orelse return;

        // Find the "data" column and extract its value
        for (rel.columns, 0..) |col, i| {
            if (std.mem.eql(u8, col.name, "data") and i < t.len) {
                switch (t[i]) {
                    .text => |v| try self.addMetadataChunk(v),
                    .binary => |v| try self.addMetadataChunk(v),
                    else => {},
                }
                return;
            }
        }
    }

    fn clearMetadataChunks(self: *Processor) void {
        for (self.metadata_chunks.items) |chunk| {
            self.allocator.free(chunk);
        }
        self.metadata_chunks.clearRetainingCapacity();
    }

    fn flushMetadata(self: *Processor) !void {
        if (self.metadata_chunks.items.len == 0) return;

        var sql: std.ArrayListUnmanaged(u8) = .{};
        defer sql.deinit(self.allocator);

        try sql.appendSlice(self.allocator, "UPDATE transactions SET metadata = ");

        // Build merged JSONB expression: 'chunk1'::jsonb || 'chunk2'::jsonb
        for (self.metadata_chunks.items, 0..) |chunk, i| {
            if (i > 0) try sql.appendSlice(self.allocator, " || ");
            try query_mod.appendEscapedString(&sql, self.allocator, chunk);
            try sql.appendSlice(self.allocator, "::jsonb");
        }

        try sql.appendSlice(self.allocator, " WHERE id = ");
        if (self.current_txn_id) |txn_id| {
            try query_mod.appendIntValue(&sql, self.allocator, txn_id);
        } else {
            try sql.appendSlice(self.allocator, "(SELECT id FROM transactions WHERE source_id=");
            try query_mod.appendEscapedUuid(&sql, self.allocator, self.config.source_id);
            try sql.appendSlice(self.allocator, " AND lsn=");
            try query_mod.appendIntValue(&sql, self.allocator, self.current_txn_lsn);
            try sql.appendSlice(self.allocator, " AND committed_at=");
            try query_mod.appendTimestamp(&sql, self.allocator, self.current_txn_timestamp);
            try sql.append(self.allocator, ')');
        }
        try sql.appendSlice(self.allocator, " AND committed_at=");
        try query_mod.appendTimestamp(&sql, self.allocator, self.current_txn_timestamp);

        try self.dest.execLarge(self.allocator, sql.items);
        self.clearMetadataChunks();
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
        self.clearMetadataChunks();
        self.metadata_chunks.deinit(self.allocator);
        self.dest.close();
    }
};

/// Append a JSON-escaped string (with surrounding double quotes) to the list.
fn appendJsonString(list: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, value: []const u8) !void {
    try list.append(allocator, '"');
    for (value) |c| {
        switch (c) {
            '"' => try list.appendSlice(allocator, "\\\""),
            '\\' => try list.appendSlice(allocator, "\\\\"),
            '\n' => try list.appendSlice(allocator, "\\n"),
            '\r' => try list.appendSlice(allocator, "\\r"),
            '\t' => try list.appendSlice(allocator, "\\t"),
            // Control characters
            0x00...0x08, 0x0b, 0x0c, 0x0e...0x1f => {
                var buf: [6]u8 = undefined;
                _ = std.fmt.bufPrint(&buf, "\\u{x:0>4}", .{c}) catch unreachable;
                try list.appendSlice(allocator, &buf);
            },
            else => try list.append(allocator, c),
        }
    }
    try list.append(allocator, '"');
}
