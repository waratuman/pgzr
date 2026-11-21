const std = @import("std");
const json = std.json;
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
    io: std.Io,
    config: types.ProcessorConfig,

    // Relation cache (rebuilt from binary messages in each batch)
    relations: std.AutoHashMap(u32, OwnedRelation),

    // pgoutput decode scratch buffers
    col_buf: [256]pgoutput.Column,
    tuple_buf: [256]pgoutput.ColumnData,
    tuple_buf2: [256]pgoutput.ColumnData,

    // Current batch context
    current_source_id: ?[]u8,

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

    // Buffered events: accumulated during a transaction and flushed as a
    // single COPY at commit time (or in chunks when the buffer exceeds
    // flush_threshold to bound memory for large transactions).
    pending_events: std.ArrayListUnmanaged([]u8),

    // Per-transaction buffers for streamed (proto v2+) transactions,
    // keyed by top-level xid. Streamed transactions may interleave, so
    // their events and metadata cannot share the non-streamed buffers;
    // they are adopted into the shared flush path at stream_commit. Each
    // item is tagged with the (sub)xid that produced it so a
    // subtransaction abort can discard exactly its changes.
    stream_events: std.AutoHashMap(u32, std.ArrayListUnmanaged(StreamItem)),
    stream_metadata: std.AutoHashMap(u32, std.ArrayListUnmanaged(StreamItem)),

    // Reusable COPY stream buffer for flushPendingEvents.
    copy_buf: std.ArrayListUnmanaged(u8),

    // Stop flag for graceful shutdown
    stop_flag: std.atomic.Value(bool),

    // Maximum number of buffered events before an intermediate flush.
    const flush_threshold: usize = 1000;

    // Named server-side prepared statements (created in init). Extended
    // query protocol: plan caching across calls, parameter binding
    // instead of SQL escaping on the hot path.
    const stmt_probe = "pgzr_probe";
    const stmt_claim = "pgzr_claim";
    const stmt_insert_txn = "pgzr_insert_txn";
    const stmt_insert_rel_snapshot = "pgzr_insert_rel_snapshot";
    const stmt_delete_batches = "pgzr_delete_batches";

    // Claim the whole batch group (one complete batch + its partials) in a
    // single statement. The group selector uses SKIP LOCKED so concurrent
    // workers pick different groups; member rows use plain FOR UPDATE so
    // an external reaper's UPDATE blocks on rows this worker holds
    // (issue #14).
    const claim_group_sql =
        "WITH grp AS (" ++
        " SELECT id, source_id, begin_lsn FROM wal_batches" ++
        " WHERE state='pending' AND complete=true" ++
        " ORDER BY created_at LIMIT 1" ++
        " FOR UPDATE SKIP LOCKED" ++
        "), members AS (" ++
        " SELECT w.id FROM wal_batches w, grp" ++
        " WHERE w.source_id=grp.source_id AND w.begin_lsn=grp.begin_lsn" ++
        " AND w.state='pending' AND w.complete=false" ++
        " FOR UPDATE OF w" ++
        ")" ++
        " UPDATE wal_batches SET state='processing'" ++
        " WHERE id IN (SELECT id FROM grp UNION ALL SELECT id FROM members)" ++
        " RETURNING id, source_id, start_lsn, complete, data, relations";

    const StreamItem = struct {
        sub_xid: u32,
        data: []u8,
    };

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

    pub fn init(allocator: std.mem.Allocator, io: std.Io, config: types.ProcessorConfig) InitError!Processor {
        var dest_config = config.dest;
        dest_config.replication = false;
        var dest = try Connection.connect(allocator, io, dest_config);
        errdefer dest.close();

        try schema_mod.ensureSchema(&dest);

        // All five statements are prepared in one pipelined round trip.
        var prep = dest.pipeline();
        try prep.prepare(stmt_probe, "SELECT 1 FROM wal_batches WHERE state='pending' AND complete=true LIMIT 1");
        try prep.prepare(stmt_claim, claim_group_sql);
        try prep.prepare(stmt_insert_txn, "INSERT INTO transactions (source_id, lsn, xid, committed_at)" ++
            " VALUES ($1::uuid, $2::bigint, $3::integer, $4::timestamptz)" ++
            // DO UPDATE (a no-op overwrite of xid) instead of DO NOTHING so
            // RETURNING always yields the row's id — on conflict included —
            // avoiding a fallback SELECT round trip.
            " ON CONFLICT (source_id, lsn, committed_at) DO UPDATE SET xid = EXCLUDED.xid RETURNING id");
        try prep.prepare(stmt_insert_rel_snapshot, "INSERT INTO relation_snapshots" ++
            " (source_id, lsn, rel_oid, schema_name, table_name, replica_identity, columns)" ++
            " VALUES ($1::uuid, $2::bigint, $3::integer, $4, $5, $6::smallint, $7::jsonb)" ++
            " ON CONFLICT (source_id, rel_oid, lsn) DO NOTHING");
        try prep.prepare(stmt_delete_batches, "DELETE FROM wal_batches WHERE id = ANY($1::bigint[])");
        try prep.syncDiscard();

        return Processor{
            .dest = dest,
            .allocator = allocator,
            .io = io,
            .config = config,
            .relations = std.AutoHashMap(u32, OwnedRelation).init(allocator),
            .col_buf = undefined,
            .tuple_buf = undefined,
            .tuple_buf2 = undefined,
            .current_source_id = null,
            .current_txn_lsn = 0,
            .current_txn_xid = 0,
            .current_txn_timestamp = 0,
            .current_txn_id = null,
            .in_transaction = false,
            .current_stream_xid = null,
            .pending_data = .empty,
            .pending_batch_ids = .empty,
            .metadata_chunks = .empty,
            .metadata_table_oid = null,
            .pending_events = .empty,
            .stream_events = std.AutoHashMap(u32, std.ArrayListUnmanaged(StreamItem)).init(allocator),
            .stream_metadata = std.AutoHashMap(u32, std.ArrayListUnmanaged(StreamItem)).init(allocator),
            .copy_buf = .empty,
            .stop_flag = std.atomic.Value(bool).init(false),
        };
    }

    pub const ProcessError = Connection.QueryError || std.mem.Allocator.Error || pgoutput.DecodeError || error{InvalidUuid};

    /// Process one pending batch group. Returns true if a batch was claimed,
    /// false if no pending batches were found.
    ///
    /// Claims all batches belonging to a single transaction (grouped by
    /// source_id + begin_lsn). Partial batches are fetched alongside their
    /// completing batch and processed as one logical unit.
    pub fn processOne(self: *Processor) ProcessError!bool {
        // Cheap existence probe first: an idle poll costs one round trip
        // instead of three (BEGIN + claim + COMMIT). The claim re-checks
        // under lock, so a stale probe result is harmless — a miss is
        // caught next poll, a false hit yields an empty claim.
        const probe = try self.dest.execPreparedWithResult(stmt_probe, &.{});
        if (probe.column_count == 0) return false;

        // The whole claim/decode/insert/delete cycle runs inside one
        // transaction (BEGIN is pipelined with the claim by claimAndProcess,
        // COMMIT with the batch delete below) so the claim's row lock
        // (FOR UPDATE) is held until the work commits.
        //
        // This makes the processor idempotent under concurrent zombie recovery
        // (issue #14): an external reaper that requeues stuck 'processing'
        // batches cannot reclaim a row this worker still holds — its UPDATE
        // blocks on the lock. If this worker crashes mid-batch the transaction
        // rolls back, reverting state='processing' to 'pending' and releasing
        // the lock, so the batch is retried automatically with no duplicate
        // events and no reaper required.
        const claimed = self.claimAndProcess() catch |err| {
            // Roll back the claim and all inserts. Park the claimed
            // batch(es) in 'error' state only for poison failures (decode
            // errors, non-retryable SQLSTATEs) that would repeat forever;
            // transient failures (connection loss, deadlock, timeout,
            // resource exhaustion) leave the rows 'pending' for retry.
            // Classify before ROLLBACK: it clears the stored SQLSTATE.
            // Both statements are best-effort: a dead connection makes
            // them no-ops. markBatchesError runs post-rollback
            // (autocommit) so it is not undone by the rollback.
            const park = self.shouldParkBatches(err);
            _ = self.dest.simpleQuery("ROLLBACK") catch {};
            if (park) self.markBatchesError();
            self.resetBatchState();
            return err;
        };

        if (!claimed) {
            // End the (empty) claim transaction.
            _ = self.dest.simpleQuery("COMMIT") catch {};
            return false;
        }

        self.deleteBatchesAndCommit() catch |err| {
            // The transaction rolled back server-side (or the connection
            // died), reverting the claimed rows to 'pending'. The batch
            // data is intact, so never park it — discard all local state
            // so the next cycle starts clean instead of replaying stale
            // pending_data/pending_events against a new claim. The
            // ROLLBACK is a no-op if the transaction already ended.
            _ = self.dest.simpleQuery("ROLLBACK") catch {};
            self.resetBatchState();
            return err;
        };
        self.clearPending();
        return true;
    }

    /// Decide whether a claimAndProcess failure should park the claimed
    /// batches in 'error' state (poison: retrying would fail forever) or
    /// leave them 'pending' for retry (transient). Nothing requeues
    /// 'error' rows automatically, so parking must be reserved for
    /// failures caused by the batch data itself.
    fn shouldParkBatches(self: *Processor, err: ProcessError) bool {
        return switch (err) {
            // Client-side or connection trouble: the batch data is intact.
            error.OutOfMemory,
            error.ConnectionClosed,
            error.ReadFailed,
            error.WriteFailed,
            error.Canceled,
            error.ProtocolError,
            => false,
            // Server rejected a statement: consult the SQLSTATE.
            error.ServerError => blk: {
                const code = self.dest.lastServerErrorCode() orelse break :blk true;
                break :blk !isRetryableSqlState(code);
            },
            // Decode/validation failures are inherent to the batch data.
            else => true,
        };
    }

    /// Discard all per-batch state after a failed cycle: claimed ids,
    /// assembled batch data, buffered events, metadata chunks, and any
    /// mid-transaction decode state.
    fn resetBatchState(self: *Processor) void {
        self.clearPending();
        self.clearPendingEvents();
        self.clearMetadataChunks();
        self.clearStreamBuffers();
        self.in_transaction = false;
        self.current_txn_id = null;
        self.current_stream_xid = null;
    }

    /// One row captured by the group claim, owned copies of the data the
    /// callback borrows from recv_buf.
    const ClaimedRow = struct {
        start_lsn: u64,
        complete: bool,
        data: []u8,

        /// Partials (in start_lsn order) sort before the complete batch:
        /// partials contain the beginning of the transaction, the complete
        /// batch contains the end.
        fn lessThan(_: void, a: ClaimedRow, b: ClaimedRow) bool {
            if (a.complete != b.complete) return b.complete;
            return a.start_lsn < b.start_lsn;
        }
    };

    /// Callback context for the group claim query. Pipeline row callbacks
    /// cannot return errors, so failures (allocation, corrupt stored bytea)
    /// are recorded in `err` and re-raised after the query completes.
    const ClaimContext = struct {
        proc: *Processor,
        rows: std.ArrayListUnmanaged(ClaimedRow) = .empty,
        source_id: ?[]u8 = null,
        relations_json: ?[]u8 = null,
        err: ?Processor.ProcessError = null,

        fn onRow(self: *ClaimContext, stmt_index: usize, cols: []const Connection.QueryColumn) void {
            // Rows come from the claim statement (pipelined after BEGIN).
            if (stmt_index != 1) return;
            if (self.err != null) return;
            self.onRowInner(cols) catch |e| {
                self.err = e;
            };
        }

        fn onRowInner(self: *ClaimContext, cols: []const Connection.QueryColumn) !void {
            const proc = self.proc;
            if (cols.len < 6) return;

            // Track the claimed id for deleteBatchesAndCommit/markBatchesError.
            // The errdefer is scoped to the block: once append succeeds the
            // list owns id_copy, and clearPending must be the only freer.
            const id_copy = try proc.allocator.alloc(u8, cols[0].data.len);
            @memcpy(id_copy, cols[0].data);
            {
                errdefer proc.allocator.free(id_copy);
                try proc.pending_batch_ids.append(proc.allocator, id_copy);
            }

            const start_lsn = std.fmt.parseInt(u64, cols[2].data, 10) catch 0;
            const complete = cols[3].data.len > 0 and cols[3].data[0] == 't';

            // Same scoping: after append, rows (freed via ctx.deinit) owns data.
            const data = try proc.decodeBytea(cols[4].data);
            {
                errdefer proc.allocator.free(data);
                try self.rows.append(proc.allocator, .{
                    .start_lsn = start_lsn,
                    .complete = complete,
                    .data = data,
                });
            }

            // source_id and relations snapshot come from the complete batch
            if (complete) {
                if (self.source_id == null) {
                    const sid = try proc.allocator.alloc(u8, cols[1].data.len);
                    @memcpy(sid, cols[1].data);
                    self.source_id = sid;
                }
                if (self.relations_json == null and !cols[5].is_null) {
                    const rj = try proc.allocator.alloc(u8, cols[5].data.len);
                    @memcpy(rj, cols[5].data);
                    self.relations_json = rj;
                }
            }
        }

        fn deinit(self: *ClaimContext, allocator: std.mem.Allocator) void {
            for (self.rows.items) |row| {
                allocator.free(row.data);
            }
            self.rows.deinit(allocator);
            if (self.source_id) |s| allocator.free(s);
            if (self.relations_json) |r| allocator.free(r);
        }
    };

    /// Claim a pending batch group and process it, inserting all decoded
    /// rows. Opens the claim transaction itself (BEGIN pipelined with the
    /// claim) so the claim's row lock is held throughout; the caller
    /// (processOne) deletes the batches and commits on success, or rolls
    /// back on error.
    /// Returns true if a batch was claimed, false if none were pending.
    fn claimAndProcess(self: *Processor) ProcessError!bool {
        var ctx = ClaimContext{ .proc = self };
        defer ctx.deinit(self.allocator);

        // BEGIN and the claim share one round trip.
        var claim_pipe = self.dest.pipeline();
        try claim_pipe.exec("BEGIN", &.{});
        try claim_pipe.execPrepared(stmt_claim, &.{});
        try claim_pipe.sync(&ctx, ClaimContext.onRow);
        if (ctx.err) |e| return e;
        if (ctx.rows.items.len == 0) return false;

        // Set current source_id from the complete batch (ownership transfer)
        if (ctx.source_id) |sid| {
            if (self.current_source_id) |old| self.allocator.free(old);
            self.current_source_id = sid;
            ctx.source_id = null;
        }

        // Load relations from the complete batch's JSONB snapshot
        if (ctx.relations_json) |rj| {
            try self.loadRelationsFromJson(rj);
        }

        // Assemble data: partials in start_lsn order, complete batch last.
        // Each row's copy is freed as soon as it is appended so peak memory
        // stays ~1x the group size instead of holding a second full copy
        // until ctx.deinit.
        std.mem.sort(ClaimedRow, ctx.rows.items, {}, ClaimedRow.lessThan);
        for (ctx.rows.items) |*row| {
            try self.pending_data.appendSlice(self.allocator, row.data);
            self.allocator.free(row.data);
            row.data = &.{};
        }

        // Step 3: Process all accumulated data. Errors propagate to processOne,
        // which rolls back the transaction (reverting the claim) and parks the
        // batches in 'error' state.
        try self.processBatch(self.pending_data.items);

        // The caller deletes the claimed batches and commits (pipelined
        // into one round trip). Still inside the claim's transaction, so
        // a delete failure rolls back the inserts too — the batch returns
        // to 'pending' rather than being processed without being removed.
        return true;
    }

    /// Append "id IN (id1, id2, ...)" for all pending batch ids. The ids come
    /// from RETURNING id on a BIGSERIAL column, so they are numeric text.
    fn appendBatchIdList(self: *Processor, sql: *std.ArrayListUnmanaged(u8)) !void {
        try sql.appendSlice(self.allocator, "id IN (");
        for (self.pending_batch_ids.items, 0..) |bid, i| {
            if (i > 0) try sql.append(self.allocator, ',');
            try sql.appendSlice(self.allocator, bid);
        }
        try sql.append(self.allocator, ')');
    }

    fn markBatchesError(self: *Processor) void {
        if (self.pending_batch_ids.items.len == 0) return;

        var sql: std.ArrayListUnmanaged(u8) = .empty;
        defer sql.deinit(self.allocator);

        sql.appendSlice(self.allocator, "UPDATE wal_batches SET state='error' WHERE ") catch return;
        self.appendBatchIdList(&sql) catch return;
        self.dest.execLarge(self.allocator, sql.items) catch {};
    }

    /// Delete all claimed batches and COMMIT the claim transaction in a
    /// single pipelined round trip. The ids come from RETURNING id on a
    /// BIGSERIAL column (numeric text), so they are safe to embed in a
    /// bigint[] array literal unquoted.
    fn deleteBatchesAndCommit(self: *Processor) !void {
        var ids: std.ArrayListUnmanaged(u8) = .empty;
        defer ids.deinit(self.allocator);

        try ids.append(self.allocator, '{');
        for (self.pending_batch_ids.items, 0..) |bid, i| {
            if (i > 0) try ids.append(self.allocator, ',');
            try ids.appendSlice(self.allocator, bid);
        }
        try ids.append(self.allocator, '}');

        var p = self.dest.pipeline();
        try p.execPrepared(stmt_delete_batches, &.{ids.items});
        try p.exec("COMMIT", &.{});
        try p.syncDiscard();
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
                    self.clearPendingEvents();
                    self.current_txn_id = try self.insertTransaction(begin.final_lsn);
                },
                .commit => {
                    try self.flushPendingEvents();
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
                        try self.extractMetadataFromTuple(ins.relation_oid, ins.new_tuple, ins.xid);
                    } else {
                        try self.bufferEvent(ins.relation_oid, 'I', ins.new_tuple, null, ins.xid);
                    }
                },
                .update => |upd| {
                    if (self.isMetadataTable(upd.relation_oid)) {
                        try self.extractMetadataFromTuple(upd.relation_oid, upd.new_tuple, upd.xid);
                    } else {
                        try self.bufferEvent(upd.relation_oid, 'U', upd.new_tuple, upd.old_tuple, upd.xid);
                    }
                },
                .delete => |del| {
                    if (!self.isMetadataTable(del.relation_oid)) {
                        try self.bufferEvent(del.relation_oid, 'D', null, del.old_tuple, del.xid);
                    }
                },
                .truncate => |trunc| {
                    // One event per truncated relation so the audit trail
                    // records which tables were truncated.
                    var i: u32 = 0;
                    while (i < trunc.relation_count) : (i += 1) {
                        const oid = try trunc.relationOid(i);
                        if (!self.isMetadataTable(oid)) {
                            try self.bufferEvent(oid, 'T', null, null, trunc.xid);
                        }
                    }
                },
                // Proto v2: streaming — events and metadata are buffered
                // during stream chunks and flushed at commit time when
                // the commit LSN and timestamp become available.
                .stream_start => |ss| {
                    self.current_stream_xid = ss.xid;
                    if (ss.first_segment) {
                        // Fresh start of this transaction's stream: drop
                        // any stale buffered state for this xid (e.g. a
                        // batch replayed after a crash).
                        self.dropStreamTxn(ss.xid);
                    }
                },
                .stream_stop => {
                    self.current_stream_xid = null;
                },
                .stream_commit => |sc| {
                    self.current_txn_lsn = sc.lsn;
                    self.current_txn_xid = sc.xid;
                    self.current_txn_timestamp = sc.timestamp;
                    self.current_txn_id = try self.insertTransaction(sc.lsn);
                    try self.adoptStreamBuffers(sc.xid);
                    try self.flushPendingEvents();
                    try self.flushMetadata();
                    self.current_stream_xid = null;
                    self.current_txn_id = null;
                },
                .stream_abort => |sa| {
                    // A top-level abort discards the whole transaction; a
                    // subtransaction abort discards only the changes tagged
                    // with that sub_xid. Other interleaved transactions are
                    // untouched.
                    if (sa.sub_xid == sa.xid) {
                        self.dropStreamTxn(sa.xid);
                    } else {
                        self.dropStreamSubxact(sa.xid, sa.sub_xid);
                    }
                    self.current_stream_xid = null;
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
                    try self.flushPendingEvents();
                    try self.flushMetadata();
                    self.in_transaction = false;
                    self.current_txn_id = null;
                },
                .rollback_prepared => {
                    self.clearPendingEvents();
                    self.in_transaction = false;
                    self.current_txn_id = null;
                },
                .stream_prepare => {
                    // Prepared within streaming — keep txn state
                },
                .message => |logical_msg| {
                    // Only transactional messages (flags bit 0) carry
                    // metadata for the surrounding transaction; a
                    // non-transactional message has no transaction and
                    // would otherwise be mis-attributed to the next one.
                    if (self.config.metadata_message_prefix) |prefix| {
                        if ((logical_msg.flags & 1) != 0 and
                            std.mem.eql(u8, logical_msg.prefix, prefix))
                        {
                            if (self.current_stream_xid) |top| {
                                try self.addStreamMetadata(top, logical_msg.xid orelse top, logical_msg.content);
                            } else {
                                try self.addMetadataChunk(logical_msg.content);
                            }
                        }
                    }
                },
                else => {},
            }
        }
    }

    fn insertTransaction(self: *Processor, lsn: u64) !?i64 {
        var lsn_buf: [20]u8 = undefined;
        var xid_buf: [10]u8 = undefined;
        var ts_buf: [40]u8 = undefined;
        const params = [_]?[]const u8{
            self.sourceId(),
            std.fmt.bufPrint(&lsn_buf, "{d}", .{lsn}) catch unreachable,
            std.fmt.bufPrint(&xid_buf, "{d}", .{self.current_txn_xid}) catch unreachable,
            query_mod.writeTimestampText(&ts_buf, self.current_txn_timestamp),
        };

        const result = try self.dest.execPreparedWithResult(stmt_insert_txn, &params);
        if (result.column_count > 0 and result.columns[0].data.len > 0) {
            return std.fmt.parseInt(i64, result.columns[0].data, 10) catch null;
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

        // Build the JSONB columns array as plain JSON (bound as a
        // parameter, so no SQL-literal escaping layer).
        var cols_json: std.ArrayListUnmanaged(u8) = .empty;
        defer cols_json.deinit(self.allocator);

        try cols_json.append(self.allocator, '[');
        for (rel.columns, 0..) |col, i| {
            if (i > 0) try cols_json.append(self.allocator, ',');
            try cols_json.appendSlice(self.allocator, "{\"name\":");
            try appendJsonStringPlain(&cols_json, self.allocator, col.name);
            try cols_json.appendSlice(self.allocator, ",\"oid\":");
            try query_mod.appendIntValue(&cols_json, self.allocator, col.type_oid);
            try cols_json.appendSlice(self.allocator, ",\"type\":");
            var type_name_buf: [32]u8 = undefined;
            const type_name = pg_types.oidToName(col.type_oid, &type_name_buf);
            try appendJsonStringPlain(&cols_json, self.allocator, type_name);
            try cols_json.appendSlice(self.allocator, ",\"identity\":");
            if ((col.flags & 1) != 0) {
                try cols_json.appendSlice(self.allocator, "true");
            } else {
                try cols_json.appendSlice(self.allocator, "false");
            }
            try cols_json.appendSlice(self.allocator, ",\"ordinal\":");
            try query_mod.appendIntValue(&cols_json, self.allocator, i);
            try cols_json.append(self.allocator, '}');
        }
        try cols_json.append(self.allocator, ']');

        var lsn_buf: [20]u8 = undefined;
        var oid_buf: [10]u8 = undefined;
        var ri_buf: [3]u8 = undefined;
        const params = [_]?[]const u8{
            self.sourceId(),
            std.fmt.bufPrint(&lsn_buf, "{d}", .{self.current_txn_lsn}) catch unreachable,
            std.fmt.bufPrint(&oid_buf, "{d}", .{rel.oid}) catch unreachable,
            rel.namespace,
            rel.name,
            std.fmt.bufPrint(&ri_buf, "{d}", .{rel.replica_identity}) catch unreachable,
            cols_json.items,
        };

        try self.dest.execPrepared(stmt_insert_rel_snapshot, &params);

        // Update snapshot_lsn in cache
        if (self.relations.getPtr(rel.oid)) |owned| {
            owned.snapshot_lsn = self.current_txn_lsn;
        }
    }

    /// Append a pgoutput tuple as a COPY binary JSONB field
    /// (length + version byte + {"col":"val",...}) or a NULL field if the
    /// tuple has no data.
    fn appendTupleAsJsonbField(
        self: *Processor,
        frag: *std.ArrayListUnmanaged(u8),
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
            try frag.appendSlice(self.allocator, query_mod.copy_binary_null);
            return;
        }

        // Field length is patched once the JSON is built.
        const len_offset = frag.items.len;
        try query_mod.appendBinU32(frag, self.allocator, 0);
        try frag.append(self.allocator, 1); // JSONB version
        try frag.append(self.allocator, '{');
        var first = true;
        for (rel.columns, 0..) |col, i| {
            if (i >= tuple.len) continue;

            switch (tuple[i]) {
                .text => |t| {
                    if (!first) try frag.append(self.allocator, ',');
                    first = false;
                    try appendJsonStringPlain(frag, self.allocator, col.name);
                    try frag.append(self.allocator, ':');
                    try appendJsonStringPlain(frag, self.allocator, t);
                },
                .binary => |b| {
                    if (!first) try frag.append(self.allocator, ',');
                    first = false;
                    try appendJsonStringPlain(frag, self.allocator, col.name);
                    try frag.append(self.allocator, ':');
                    try appendJsonStringPlain(frag, self.allocator, b);
                },
                .null_value => {
                    if (!first) try frag.append(self.allocator, ',');
                    first = false;
                    try appendJsonStringPlain(frag, self.allocator, col.name);
                    try frag.appendSlice(self.allocator, ":null");
                },
                .unchanged => {},
            }
        }
        try frag.append(self.allocator, '}');

        const field_len: u32 = @intCast(frag.items.len - len_offset - 4);
        std.mem.writeInt(u32, frag.items[len_offset..][0..4], field_len, .big);
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

    fn sourceId(self: *Processor) []const u8 {
        return self.current_source_id orelse "";
    }

    /// Load the relation cache from a JSONB relations snapshot stored in
    /// the wal_batches row.  This makes each batch self-contained so that
    /// batches can be processed in any order.
    fn loadRelationsFromJson(self: *Processor, raw: []const u8) !void {
        if (raw.len == 0) return;

        const parsed = json.parseFromSlice(json.Value, self.allocator, raw, .{}) catch return;
        defer parsed.deinit();

        const root = switch (parsed.value) {
            .object => |o| o,
            else => return,
        };

        var it = root.iterator();
        while (it.next()) |entry| {
            const oid = std.fmt.parseInt(u32, entry.key_ptr.*, 10) catch continue;
            const obj = switch (entry.value_ptr.*) {
                .object => |o| o,
                else => continue,
            };

            const schema_val = obj.get("schema") orelse continue;
            const table_val = obj.get("table") orelse continue;
            const ri_val = obj.get("relreplident") orelse continue;
            const cols_val = obj.get("columns") orelse continue;

            const namespace_str = switch (schema_val) {
                .string => |s| s,
                else => continue,
            };
            const table_str = switch (table_val) {
                .string => |s| s,
                else => continue,
            };
            const replica_identity: u8 = switch (ri_val) {
                .integer => |i| std.math.cast(u8, i) orelse continue,
                else => continue,
            };
            const cols_arr = switch (cols_val) {
                .array => |a| a,
                else => continue,
            };

            // Skip if we already have this relation cached (from a prior batch
            // in the same processor lifetime) — the JSONB snapshot from the
            // current batch is authoritative, so replace it.
            if (self.relations.fetchRemove(oid)) |removed| {
                var old = removed.value;
                old.deinit(self.allocator);
            }

            const namespace = try self.allocator.alloc(u8, namespace_str.len);
            @memcpy(namespace, namespace_str);

            const name = try self.allocator.alloc(u8, table_str.len);
            @memcpy(name, table_str);

            const columns = try self.allocator.alloc(OwnedColumn, cols_arr.items.len);
            for (cols_arr.items, 0..) |col_val, ci| {
                const col_obj = switch (col_val) {
                    .object => |o| o,
                    else => {
                        columns[ci] = .{ .flags = 0, .name = &.{}, .type_oid = 0, .type_modifier = -1 };
                        continue;
                    },
                };

                const col_name_val = col_obj.get("name") orelse {
                    columns[ci] = .{ .flags = 0, .name = &.{}, .type_oid = 0, .type_modifier = -1 };
                    continue;
                };
                const col_name_str = switch (col_name_val) {
                    .string => |s| s,
                    else => {
                        columns[ci] = .{ .flags = 0, .name = &.{}, .type_oid = 0, .type_modifier = -1 };
                        continue;
                    },
                };
                const col_name = try self.allocator.alloc(u8, col_name_str.len);
                @memcpy(col_name, col_name_str);

                const flags: u8 = if (col_obj.get("flags")) |f| switch (f) {
                    .integer => |i| std.math.cast(u8, i) orelse 0,
                    else => 0,
                } else 0;

                const type_oid: u32 = if (col_obj.get("oid")) |o| switch (o) {
                    .integer => |i| std.math.cast(u32, i) orelse 0,
                    else => 0,
                } else 0;

                const type_modifier: i32 = if (col_obj.get("typmod")) |t| switch (t) {
                    .integer => |i| std.math.cast(i32, i) orelse -1,
                    else => -1,
                } else -1;

                columns[ci] = .{
                    .flags = flags,
                    .name = col_name,
                    .type_oid = type_oid,
                    .type_modifier = type_modifier,
                };
            }

            try self.relations.put(oid, .{
                .oid = oid,
                .namespace = namespace,
                .name = name,
                .replica_identity = replica_identity,
                .columns = columns,
                .snapshot_lsn = null,
            });
        }
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

    fn addStreamMetadata(self: *Processor, xid: u32, sub_xid: u32, content: []const u8) !void {
        const copy = try self.allocator.dupe(u8, content);
        errdefer self.allocator.free(copy);
        const gop = try self.stream_metadata.getOrPut(xid);
        if (!gop.found_existing) gop.value_ptr.* = .empty;
        try gop.value_ptr.append(self.allocator, .{ .sub_xid = sub_xid, .data = copy });
    }

    fn extractMetadataFromTuple(self: *Processor, relation_oid: u32, tuple: ?[]const pgoutput.ColumnData, sub_xid: ?u32) !void {
        const t = tuple orelse return;
        const rel = self.relations.get(relation_oid) orelse return;

        // Find the "data" column and extract its value
        for (rel.columns, 0..) |col, i| {
            if (std.mem.eql(u8, col.name, "data") and i < t.len) {
                const value: []const u8 = switch (t[i]) {
                    .text => |v| v,
                    .binary => |v| v,
                    else => return,
                };
                if (self.current_stream_xid) |top| {
                    try self.addStreamMetadata(top, sub_xid orelse top, value);
                } else {
                    try self.addMetadataChunk(value);
                }
                return;
            }
        }
    }

    /// Move a committed streamed transaction's buffered events and
    /// metadata into the shared (non-streamed) buffers for flushing.
    /// Ownership of each item's bytes transfers to the shared lists.
    fn adoptStreamBuffers(self: *Processor, xid: u32) !void {
        if (self.stream_events.fetchRemove(xid)) |entry| {
            var list = entry.value;
            defer list.deinit(self.allocator);
            for (list.items, 0..) |item, i| {
                self.pending_events.append(self.allocator, item.data) catch |err| {
                    for (list.items[i..]) |rest| self.allocator.free(rest.data);
                    return err;
                };
            }
        }
        if (self.stream_metadata.fetchRemove(xid)) |entry| {
            var list = entry.value;
            defer list.deinit(self.allocator);
            for (list.items, 0..) |item, i| {
                self.metadata_chunks.append(self.allocator, item.data) catch |err| {
                    for (list.items[i..]) |rest| self.allocator.free(rest.data);
                    return err;
                };
            }
        }
    }

    /// Discard all buffered state for a streamed transaction.
    fn dropStreamTxn(self: *Processor, xid: u32) void {
        if (self.stream_events.fetchRemove(xid)) |entry| {
            var list = entry.value;
            for (list.items) |item| self.allocator.free(item.data);
            list.deinit(self.allocator);
        }
        if (self.stream_metadata.fetchRemove(xid)) |entry| {
            var list = entry.value;
            for (list.items) |item| self.allocator.free(item.data);
            list.deinit(self.allocator);
        }
    }

    /// Discard the buffered changes of one aborted subtransaction,
    /// keeping the rest of the transaction's buffer intact.
    fn dropStreamSubxact(self: *Processor, xid: u32, sub_xid: u32) void {
        if (self.stream_events.getPtr(xid)) |list| {
            var i: usize = 0;
            while (i < list.items.len) {
                if (list.items[i].sub_xid == sub_xid) {
                    self.allocator.free(list.items[i].data);
                    _ = list.orderedRemove(i);
                } else {
                    i += 1;
                }
            }
        }
        if (self.stream_metadata.getPtr(xid)) |list| {
            var i: usize = 0;
            while (i < list.items.len) {
                if (list.items[i].sub_xid == sub_xid) {
                    self.allocator.free(list.items[i].data);
                    _ = list.orderedRemove(i);
                } else {
                    i += 1;
                }
            }
        }
    }

    /// Discard all per-xid streamed transaction buffers.
    fn clearStreamBuffers(self: *Processor) void {
        var ev_it = self.stream_events.iterator();
        while (ev_it.next()) |entry| {
            for (entry.value_ptr.items) |item| self.allocator.free(item.data);
            entry.value_ptr.deinit(self.allocator);
        }
        self.stream_events.clearRetainingCapacity();
        var md_it = self.stream_metadata.iterator();
        while (md_it.next()) |entry| {
            for (entry.value_ptr.items) |item| self.allocator.free(item.data);
            entry.value_ptr.deinit(self.allocator);
        }
        self.stream_metadata.clearRetainingCapacity();
    }

    fn clearMetadataChunks(self: *Processor) void {
        for (self.metadata_chunks.items) |chunk| {
            self.allocator.free(chunk);
        }
        self.metadata_chunks.clearRetainingCapacity();
    }

    fn flushMetadata(self: *Processor) !void {
        if (self.metadata_chunks.items.len == 0) return;

        var sql: std.ArrayListUnmanaged(u8) = .empty;
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
            try query_mod.appendEscapedUuid(&sql, self.allocator, self.sourceId());
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

    /// Buffer an event's value fragment for later batch insertion.
    /// The fragment contains everything except transaction_id and committed_at,
    /// which are filled in at flush time by flushPendingEvents.
    fn bufferEvent(
        self: *Processor,
        relation_oid: u32,
        event_type: u8,
        tuple: ?[]const pgoutput.ColumnData,
        old_tuple: ?[]const pgoutput.ColumnData,
        sub_xid: ?u32,
    ) !void {
        const rel = if (relation_oid != 0) self.relations.get(relation_oid) else null;

        // Compute identity digests
        const identity_digest = if (rel) |r| blk: {
            break :blk if (tuple) |t|
                try self.computeIdentityDigest(r, t)
            else if (old_tuple) |ot|
                try self.computeIdentityDigest(r, ot)
            else
                null;
        } else null;
        defer if (identity_digest) |d| self.allocator.free(d);

        const prev_identity_digest = if (rel) |r| blk: {
            break :blk if (old_tuple) |ot|
                try self.computeIdentityDigest(r, ot)
            else
                null;
        } else null;
        defer if (prev_identity_digest) |d| self.allocator.free(d);

        // Build the COPY binary fields for rel_oid, type, identity_digest,
        // previous_identity_digest, data, old_data. transaction_id and
        // committed_at are prepended at flush.
        var frag: std.ArrayListUnmanaged(u8) = .empty;
        errdefer frag.deinit(self.allocator);

        // rel_oid (int4; OID bits carried through as-is)
        try query_mod.appendBinU32(&frag, self.allocator, 4);
        try query_mod.appendBinU32(&frag, self.allocator, relation_oid);

        // type (CHAR(1): the character's byte)
        try query_mod.appendBinU32(&frag, self.allocator, 1);
        try frag.append(self.allocator, event_type);

        // identity digests (bytea: raw bytes, no hex)
        try query_mod.appendBinByteaField(&frag, self.allocator, identity_digest);
        try query_mod.appendBinByteaField(&frag, self.allocator, prev_identity_digest);

        // data JSONB
        if (rel != null and tuple != null) {
            try self.appendTupleAsJsonbField(&frag, rel.?, tuple.?);
        } else {
            try frag.appendSlice(self.allocator, query_mod.copy_binary_null);
        }

        // old_data JSONB
        if (rel != null and old_tuple != null) {
            try self.appendTupleAsJsonbField(&frag, rel.?, old_tuple.?);
        } else {
            try frag.appendSlice(self.allocator, query_mod.copy_binary_null);
        }

        const owned = try frag.toOwnedSlice(self.allocator);

        if (self.current_stream_xid) |top| {
            // Streamed transaction: buffer per-xid until its stream_commit.
            errdefer self.allocator.free(owned);
            const gop = try self.stream_events.getOrPut(top);
            if (!gop.found_existing) gop.value_ptr.* = .empty;
            try gop.value_ptr.append(self.allocator, .{ .sub_xid = sub_xid orelse top, .data = owned });
            return;
        }

        {
            errdefer self.allocator.free(owned);
            try self.pending_events.append(self.allocator, owned);
        }

        // Flush in chunks to bound memory for large transactions.
        // Only possible when current_txn_id is known (non-streaming path).
        if (self.pending_events.items.len >= flush_threshold and self.current_txn_id != null) {
            try self.flushPendingEvents();
        }
    }

    /// Insert all buffered events via COPY with the transaction_id and
    /// committed_at timestamp. COPY skips SQL lexing/parsing/planning on
    /// the server, which dominates the cost of large multi-row INSERTs.
    /// Rows are streamed through copy-in mode in ~64 KiB chunks, so peak
    /// memory stays at one chunk instead of a full concatenated copy of
    /// the batch.
    fn flushPendingEvents(self: *Processor) !void {
        if (self.pending_events.items.len == 0) return;

        const txn_id = self.current_txn_id orelse try self.lookupTransactionId();

        // Binary timestamptz is microseconds since 2000-01-01 UTC — exactly
        // the pgoutput commit timestamp. Clamp to the Unix epoch to match
        // the text path used by insertTransaction (writeTimestampText), so
        // events.committed_at always equals the parent transaction row's.
        const min_pg_usec: i64 = -946_684_800 * 1_000_000;
        const ts_usec = @max(self.current_txn_timestamp, min_pg_usec);

        try self.dest.copyInBegin(
            self.allocator,
            "COPY events (transaction_id, committed_at, rel_oid, type, " ++
                "identity_digest, previous_identity_digest, data, old_data) " ++
                "FROM STDIN WITH (FORMAT binary)",
        );
        self.streamEventRows(txn_id, ts_usec) catch |err| {
            // Leave copy mode so the connection is usable for ROLLBACK.
            self.dest.copyInFail("pgzr: client-side failure while streaming events");
            return err;
        };
        try self.dest.copyInEnd();
        self.clearPendingEvents();
    }

    /// Accumulate event rows into copy_buf and ship each ~64 KiB chunk.
    const copy_chunk_target: usize = 64 * 1024;

    fn streamEventRows(self: *Processor, txn_id: i64, ts_usec: i64) !void {
        self.copy_buf.clearRetainingCapacity();
        try self.copy_buf.appendSlice(self.allocator, query_mod.copy_binary_header);
        for (self.pending_events.items) |frag| {
            // Tuple: field count, transaction_id, committed_at, then the
            // pre-encoded fields 3..8 from the fragment.
            try query_mod.appendBinU16(&self.copy_buf, self.allocator, 8);
            try query_mod.appendBinI64Field(&self.copy_buf, self.allocator, txn_id);
            try query_mod.appendBinI64Field(&self.copy_buf, self.allocator, ts_usec);
            try self.copy_buf.appendSlice(self.allocator, frag);
            if (self.copy_buf.items.len >= copy_chunk_target) {
                try self.dest.copyInWrite(self.copy_buf.items);
                self.copy_buf.clearRetainingCapacity();
            }
        }
        try self.copy_buf.appendSlice(self.allocator, query_mod.copy_binary_trailer);
        try self.dest.copyInWrite(self.copy_buf.items);
        self.copy_buf.clearRetainingCapacity();
    }

    /// Resolve the current transaction's id when insertTransaction did not
    /// return one. Preserves the pre-COPY fallback (a per-row subquery in
    /// the INSERT); with COPY the id must be resolved client-side.
    fn lookupTransactionId(self: *Processor) !i64 {
        var sql: std.ArrayListUnmanaged(u8) = .empty;
        defer sql.deinit(self.allocator);

        try sql.appendSlice(self.allocator, "SELECT id FROM transactions WHERE source_id=");
        try query_mod.appendEscapedUuid(&sql, self.allocator, self.sourceId());
        try sql.appendSlice(self.allocator, " AND lsn=");
        try query_mod.appendIntValue(&sql, self.allocator, self.current_txn_lsn);
        try sql.appendSlice(self.allocator, " AND committed_at=");
        try query_mod.appendTimestamp(&sql, self.allocator, self.current_txn_timestamp);

        const result = try self.dest.simpleQuery(sql.items);
        if (result.column_count > 0 and result.columns[0].data.len > 0) {
            return std.fmt.parseInt(i64, result.columns[0].data, 10) catch return error.ServerError;
        }
        return error.ServerError;
    }

    fn clearPendingEvents(self: *Processor) void {
        for (self.pending_events.items) |frag| {
            self.allocator.free(frag);
        }
        self.pending_events.clearRetainingCapacity();
    }

    fn decodeBytea(self: *Processor, hex_data: []const u8) ![]u8 {
        return decodeByteaAlloc(self.allocator, hex_data);
    }

    /// Main processing loop. Processes pending batches until stopped.
    /// When no batches are pending, sleeps for poll_interval_ms.
    pub fn run(self: *Processor) ProcessError!void {
        while (!self.stop_flag.load(.acquire)) {
            const processed = try self.processOne();
            if (!processed) {
                self.io.sleep(.fromMilliseconds(@intCast(self.config.poll_interval_ms)), .awake) catch {};
            }
        }
    }

    /// Request the processor to stop. Safe to call from another thread.
    pub fn stop(self: *Processor) void {
        self.stop_flag.store(true, .release);
    }

    pub fn deinit(self: *Processor) void {
        if (self.current_source_id) |sid| self.allocator.free(sid);
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
        self.clearPendingEvents();
        self.pending_events.deinit(self.allocator);
        self.clearStreamBuffers();
        self.stream_events.deinit();
        self.stream_metadata.deinit();
        self.copy_buf.deinit(self.allocator);
        self.dest.close();
    }
};

/// Decode a bytea value from PostgreSQL text output. Hex format
/// (\x-prefixed) is decoded; anything else is returned as-is (escape
/// format is not produced by servers since 9.0 defaults, but raw
/// pass-through keeps older data readable). Corrupt hex (odd length or
/// a non-hex digit) is an error — truncating or dropping WAL data here
/// would silently lose events downstream.
fn decodeByteaAlloc(allocator: std.mem.Allocator, hex_data: []const u8) ![]u8 {
    if (hex_data.len < 2 or hex_data[0] != '\\' or hex_data[1] != 'x') {
        return allocator.dupe(u8, hex_data);
    }

    const hex = hex_data[2..];
    if (hex.len % 2 != 0) return error.InvalidMessage;

    const out_len = hex.len / 2;
    const out = try allocator.alloc(u8, out_len);
    errdefer allocator.free(out);

    for (0..out_len) |i| {
        const hi = hex_decode_table[hex[i * 2]];
        const lo = hex_decode_table[hex[i * 2 + 1]];
        // 0xff marks an invalid digit; surviving values are 0..15.
        if ((hi | lo) == 0xff) return error.InvalidMessage;
        out[i] = (hi << 4) | lo;
    }

    return out;
}

/// Table-driven hex digit decode (0xff = invalid). The branchy
/// switch-based decode ran at ~0.2 GB/s and dominated client CPU when
/// decoding megabyte-scale batch rows; the table runs at multi-GB/s.
const hex_decode_table = blk: {
    var t: [256]u8 = @splat(0xff);
    for ('0'..'9' + 1) |c| t[c] = @intCast(c - '0');
    for ('a'..'f' + 1) |c| t[c] = @intCast(c - 'a' + 10);
    for ('A'..'F' + 1) |c| t[c] = @intCast(c - 'A' + 10);
    break :blk t;
};

test "decodeByteaAlloc: valid hex" {
    const out = try decodeByteaAlloc(std.testing.allocator, "\\xdeadBEEF");
    defer std.testing.allocator.free(out);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0xde, 0xad, 0xbe, 0xef }, out);
}

test "decodeByteaAlloc: empty hex" {
    const out = try decodeByteaAlloc(std.testing.allocator, "\\x");
    defer std.testing.allocator.free(out);
    try std.testing.expectEqual(@as(usize, 0), out.len);
}

test "decodeByteaAlloc: odd-length hex is an error, not empty data" {
    try std.testing.expectError(error.InvalidMessage, decodeByteaAlloc(std.testing.allocator, "\\xabc"));
}

test "decodeByteaAlloc: invalid hex digit is an error" {
    try std.testing.expectError(error.InvalidMessage, decodeByteaAlloc(std.testing.allocator, "\\xzz"));
}

test "decodeByteaAlloc: non-hex input passes through" {
    const out = try decodeByteaAlloc(std.testing.allocator, "raw");
    defer std.testing.allocator.free(out);
    try std.testing.expectEqualStrings("raw", out);
}

/// SQLSTATE classes whose errors are expected to succeed on retry:
///   08 — connection exception
///   40 — transaction rollback (deadlock_detected, serialization_failure)
///   53 — insufficient resources (memory, disk, connection slots)
///   55 — object not in prerequisite state (lock_not_available)
///   57 — operator intervention (query_canceled, shutdown)
/// Everything else (22 data, 23 integrity, 42 syntax/access, ...) is
/// treated as a property of the statement and therefore of the batch.
fn isRetryableSqlState(code: []const u8) bool {
    if (code.len < 2) return false;
    const class = code[0..2];
    return std.mem.eql(u8, class, "08") or
        std.mem.eql(u8, class, "40") or
        std.mem.eql(u8, class, "53") or
        std.mem.eql(u8, class, "55") or
        std.mem.eql(u8, class, "57");
}

test "isRetryableSqlState: transient classes retry" {
    try std.testing.expect(isRetryableSqlState("40P01")); // deadlock_detected
    try std.testing.expect(isRetryableSqlState("40001")); // serialization_failure
    try std.testing.expect(isRetryableSqlState("57014")); // query_canceled
    try std.testing.expect(isRetryableSqlState("53200")); // out_of_memory
    try std.testing.expect(isRetryableSqlState("55P03")); // lock_not_available
    try std.testing.expect(isRetryableSqlState("08006")); // connection_failure
}

test "isRetryableSqlState: data/constraint/syntax errors park" {
    try std.testing.expect(!isRetryableSqlState("22021")); // invalid byte sequence
    try std.testing.expect(!isRetryableSqlState("23502")); // not_null_violation
    try std.testing.expect(!isRetryableSqlState("42601")); // syntax_error
    try std.testing.expect(!isRetryableSqlState("22P02")); // invalid_text_representation
    try std.testing.expect(!isRetryableSqlState(""));
    try std.testing.expect(!isRetryableSqlState("4"));
}

/// Append a plain JSON-escaped string (with surrounding double quotes).
/// Used inside COPY binary JSONB fields, which carry raw JSON text — no
/// COPY text-format escaping layer. Runs of characters that need no
/// escaping are skipped 16 bytes at a time.
fn appendJsonStringPlain(list: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, value: []const u8) !void {
    try list.append(allocator, '"');
    var start: usize = 0;
    var i: usize = 0;
    while (i < value.len) : (i += 1) {
        // Vectorized fast path: skip 16-byte blocks containing no byte
        // that needs escaping (control chars, '"', '\\'). Typical column
        // values are clean text, so this is the common case.
        const V = @Vector(16, u8);
        while (i + 16 <= value.len) {
            const v: V = value[i..][0..16].*;
            const needs = (v < @as(V, @splat(0x20))) |
                (v == @as(V, @splat('"'))) |
                (v == @as(V, @splat('\\')));
            if (@reduce(.Or, needs)) break;
            i += 16;
        }
        if (i >= value.len) break;
        const c = value[i];
        const esc: []const u8 = switch (c) {
            '"' => "\\\"",
            '\\' => "\\\\",
            '\n' => "\\n",
            '\r' => "\\r",
            '\t' => "\\t",
            // Control characters: \u-escaped below
            0x00...0x08, 0x0b, 0x0c, 0x0e...0x1f => "",
            else => continue,
        };
        try list.appendSlice(allocator, value[start..i]);
        if (esc.len > 0) {
            try list.appendSlice(allocator, esc);
        } else {
            var buf: [8]u8 = undefined;
            const u = std.fmt.bufPrint(&buf, "\\u{x:0>4}", .{c}) catch unreachable;
            try list.appendSlice(allocator, u);
        }
        start = i + 1;
    }
    try list.appendSlice(allocator, value[start..]);
    try list.append(allocator, '"');
}

test "appendJsonStringPlain: plain" {
    var list: std.ArrayListUnmanaged(u8) = .empty;
    defer list.deinit(std.testing.allocator);
    try appendJsonStringPlain(&list, std.testing.allocator, "hello");
    try std.testing.expectEqualStrings("\"hello\"", list.items);
}

test "appendJsonStringPlain: quote and backslash" {
    var list: std.ArrayListUnmanaged(u8) = .empty;
    defer list.deinit(std.testing.allocator);
    try appendJsonStringPlain(&list, std.testing.allocator, "a\"b\\c");
    try std.testing.expectEqualStrings("\"a\\\"b\\\\c\"", list.items);
}

test "appendJsonStringPlain: single quote passes through" {
    var list: std.ArrayListUnmanaged(u8) = .empty;
    defer list.deinit(std.testing.allocator);
    try appendJsonStringPlain(&list, std.testing.allocator, "it's");
    try std.testing.expectEqualStrings("\"it's\"", list.items);
}

test "appendJsonStringPlain: newline tab control" {
    var list: std.ArrayListUnmanaged(u8) = .empty;
    defer list.deinit(std.testing.allocator);
    try appendJsonStringPlain(&list, std.testing.allocator, "a\nb\tc\x07d");
    try std.testing.expectEqualStrings("\"a\\nb\\tc\\u0007d\"", list.items);
}

test "appendJsonStringPlain: long clean value uses vector fast path" {
    var list: std.ArrayListUnmanaged(u8) = .empty;
    defer list.deinit(std.testing.allocator);
    const unit = "abcdefghijklmnopqrstuvwxyz0123456789";
    const value = unit ++ unit ++ unit ++ unit;
    try appendJsonStringPlain(&list, std.testing.allocator, value);
    try std.testing.expectEqualStrings("\"" ++ value ++ "\"", list.items);
}
