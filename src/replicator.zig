const std = @import("std");
const Connection = @import("connection.zig").Connection;
const protocol = @import("protocol.zig");
const Lsn = @import("lsn.zig").Lsn;
const types = @import("types.zig");
const Transport = @import("transport.zig").Transport;

/// Microseconds between Unix epoch (1970-01-01) and PG epoch (2000-01-01).
const PG_EPOCH_OFFSET_US: i64 = 946_684_800 * 1_000_000;

pub const Replicator = struct {
    conn: Connection,
    config: types.ReplicatorConfig,
    allocator: std.mem.Allocator,

    last_server_lsn: Lsn = Lsn.zero,
    last_received_lsn: Lsn = Lsn.zero,
    last_processed_lsn: Lsn = Lsn.zero,

    /// Timestamp (ms) of last status update sent.
    last_status_time_ms: i64 = 0,

    /// Atomic flag for cross-thread stop requests.
    stop_flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    system_id: ?[]const u8 = null,
    timeline: ?[]const u8 = null,
    xlogpos: ?[]const u8 = null,
    db_name: ?[]const u8 = null,

    pub const InitError = Connection.ConnectError || Connection.QueryError || error{
        DatabaseMismatch,
        TimelineMismatch,
        SystemIdMismatch,
    };

    pub fn init(allocator: std.mem.Allocator, config: types.ReplicatorConfig) InitError!Replicator {
        var conn = try Connection.connect(allocator, config.conn);
        errdefer conn.close();

        var repl = Replicator{
            .conn = conn,
            .config = config,
            .allocator = allocator,
        };

        try repl.identifySystem();
        try repl.startReplication();

        return repl;
    }

    fn identifySystem(self: *Replicator) InitError!void {
        const result = try self.conn.simpleQuery("IDENTIFY_SYSTEM");
        if (result.column_count < 3) return error.ProtocolError;

        self.system_id = result.columns[0].data;
        self.timeline = result.columns[1].data;
        self.xlogpos = result.columns[2].data;
        if (result.column_count > 3 and !result.columns[3].is_null) {
            self.db_name = result.columns[3].data;
        }

        // Verify database name matches if server reported one
        if (self.db_name) |server_db| {
            if (!std.mem.eql(u8, server_db, self.config.conn.database)) {
                std.log.err("Database mismatch: expected '{s}', server reports '{s}'", .{
                    self.config.conn.database,
                    server_db,
                });
                return error.DatabaseMismatch;
            }
        }

        // Verify timeline if caller specified an expected value
        if (self.config.expected_timeline) |expected| {
            if (self.timeline) |server_timeline| {
                const server_val = std.fmt.parseUnsigned(u32, server_timeline, 10) catch 0;
                if (server_val != expected) {
                    return error.TimelineMismatch;
                }
            }
        }

        // Verify system identifier if caller specified an expected value
        if (self.config.expected_systemid) |expected| {
            if (self.system_id) |server_id| {
                if (!std.mem.eql(u8, server_id, expected)) {
                    return error.SystemIdMismatch;
                }
            }
        }
    }

    fn startReplication(self: *Replicator) InitError!void {
        var query_buf: [4096]u8 = undefined;
        const start_lsn = self.config.start_position orelse Lsn.zero;

        const query = std.fmt.bufPrint(&query_buf, "START_REPLICATION SLOT {s} LOGICAL {f}", .{
            self.config.slot_name,
            start_lsn,
        }) catch return error.ProtocolError;

        var pos = query.len;

        if (self.config.options.len > 0) {
            query_buf[pos] = ' ';
            pos += 1;
            query_buf[pos] = '(';
            pos += 1;
            for (self.config.options, 0..) |opt, i| {
                if (i > 0) {
                    @memcpy(query_buf[pos..][0..2], ", ");
                    pos += 2;
                }
                query_buf[pos] = '"';
                pos += 1;
                @memcpy(query_buf[pos..][0..opt[0].len], opt[0]);
                pos += opt[0].len;
                query_buf[pos] = '"';
                pos += 1;
                query_buf[pos] = ' ';
                pos += 1;
                query_buf[pos] = '\'';
                pos += 1;
                @memcpy(query_buf[pos..][0..opt[1].len], opt[1]);
                pos += opt[1].len;
                query_buf[pos] = '\'';
                pos += 1;
            }
            query_buf[pos] = ')';
            pos += 1;
        }

        const result = try self.conn.simpleQuery(query_buf[0..pos]);
        if (!result.in_copy_mode) return error.ProtocolError;
    }

    pub const NextError = protocol.ReadError || protocol.ReadBodyError || Transport.WriteError || error{
        ServerError,
        ReconnectFailed,
    };

    /// Get the next WAL message. Returns null when end_position is reached
    /// or the server sends CopyDone.
    ///
    /// When `auto_reconnect` is enabled, connection errors trigger automatic
    /// reconnection with exponential backoff. Replication resumes from the
    /// last acknowledged LSN.
    pub fn next(self: *Replicator) NextError!?types.WalMessage {
        if (!self.config.auto_reconnect) {
            return self.nextInner();
        }

        while (true) {
            if (self.nextInner()) |result| {
                return result;
            } else |err| {
                if (isConnectionError(err)) {
                    self.reconnect() catch return error.ReconnectFailed;
                    continue;
                }
                return err;
            }
        }
    }

    fn nextInner(self: *Replicator) NextError!?types.WalMessage {
        while (true) {
            if (self.stop_flag.load(.acquire)) return null;
            if (self.endPositionReached()) return null;

            try self.maybeSendStatus();

            const header = try protocol.readHeader(self.conn.transport);

            switch (header.msg_type) {
                protocol.MSG_COPY_DATA => {
                    const body = try self.readCopyBody(header);
                    if (body.len == 0) return error.ProtocolError;

                    switch (body[0]) {
                        protocol.XLOG_DATA => {
                            return self.handleXLogData(body);
                        },
                        protocol.KEEPALIVE => {
                            try self.handleKeepalive(body);
                            continue;
                        },
                        else => return error.ProtocolError,
                    }
                },
                protocol.MSG_COPY_DONE => {
                    _ = try protocol.readBody(self.conn.transport, header, self.conn.recv_buf);
                    return null;
                },
                protocol.MSG_ERROR => {
                    const body = try protocol.readBody(self.conn.transport, header, self.conn.recv_buf);
                    const err = protocol.parseError(body);
                    std.log.err("Replication error: {s}: {s}", .{ err.code, err.message });
                    return error.ServerError;
                },
                protocol.MSG_NOTICE => {
                    _ = try protocol.readBody(self.conn.transport, header, self.conn.recv_buf);
                    continue;
                },
                else => {
                    _ = try protocol.readBody(self.conn.transport, header, self.conn.recv_buf);
                    continue;
                },
            }
        }
    }

    fn isConnectionError(err: NextError) bool {
        return switch (err) {
            error.ConnectionClosed,
            error.ConnectionResetByPeer,
            error.BrokenPipe,
            error.WriteFailed,
            error.TlsConnectionTruncated,
            => true,
            else => false,
        };
    }

    fn reconnect(self: *Replicator) InitError!void {
        // Close the old connection (ignore errors)
        self.conn.close();

        var delay_ms: u64 = 100;
        var attempts: u32 = 0;

        while (true) {
            attempts += 1;

            if (self.config.max_reconnect_attempts > 0 and
                attempts > self.config.max_reconnect_attempts)
            {
                std.log.err("Reconnection failed after {d} attempts", .{attempts - 1});
                return error.ServerError;
            }

            std.log.info("Reconnecting (attempt {d})...", .{attempts});
            std.Thread.sleep(delay_ms * std.time.ns_per_ms);

            // Try to establish a new connection
            var conn = Connection.connect(self.allocator, self.config.conn) catch {
                delay_ms = @min(delay_ms * 2, self.config.max_reconnect_delay_ms);
                continue;
            };
            errdefer conn.close();

            self.conn = conn;

            // Re-identify and start replication from last processed LSN
            self.identifySystem() catch {
                self.conn.close();
                delay_ms = @min(delay_ms * 2, self.config.max_reconnect_delay_ms);
                continue;
            };

            // Override start_position with last_processed_lsn for resume
            const saved_start = self.config.start_position;
            if (self.last_processed_lsn.value != 0) {
                self.config.start_position = self.last_processed_lsn;
            }
            self.startReplication() catch {
                self.config.start_position = saved_start;
                self.conn.close();
                delay_ms = @min(delay_ms * 2, self.config.max_reconnect_delay_ms);
                continue;
            };
            self.config.start_position = saved_start;

            // Reset status timer so we send a fresh status soon
            self.last_status_time_ms = 0;

            std.log.info("Reconnected successfully", .{});
            return;
        }
    }

    fn readCopyBody(self: *Replicator, header: protocol.MessageHeader) NextError![]u8 {
        const body_len = header.bodyLen();

        // Grow buffer if needed
        if (body_len > self.conn.recv_buf.len) {
            const new_buf = self.allocator.realloc(self.conn.recv_buf, body_len) catch {
                return error.ProtocolError;
            };
            self.conn.recv_buf = new_buf;
        }

        try protocol.readExact(self.conn.transport, self.conn.recv_buf[0..body_len]);
        return self.conn.recv_buf[0..body_len];
    }

    fn handleXLogData(self: *Replicator, body: []const u8) ?types.WalMessage {
        if (body.len < 25) return null;

        const wal_start = Lsn.readBig(body[1..9]);
        const wal_end = Lsn.readBig(body[9..17]);
        const send_time: i64 = @bitCast(std.mem.readInt(u64, body[17..25], .big));
        const data = body[25..];

        if (wal_end.value != 0) self.last_server_lsn = wal_end;
        if (wal_start.value != 0) self.last_received_lsn = wal_start;

        // Check end_position
        if (self.config.end_position) |end_pos| {
            if (wal_start.value > end_pos.value) return null;
        }

        return types.WalMessage{
            .wal_start = wal_start,
            .wal_end = wal_end,
            .send_time = send_time,
            .data = data,
        };
    }

    fn handleKeepalive(self: *Replicator, body: []const u8) NextError!void {
        if (body.len < 18) return;

        const server_lsn = Lsn.readBig(body[1..9]);
        // body[9..17] = send_time (skip)
        const reply_requested = body[17];

        if (server_lsn.value != 0) self.last_server_lsn = server_lsn;

        if (reply_requested == 1) {
            try self.sendStatus();
        }
    }

    /// Check if the end_position has been reached based on received LSN.
    fn endPositionReached(self: *Replicator) bool {
        if (self.config.end_position) |end_pos| {
            if (self.last_received_lsn.value != 0 and self.last_received_lsn.value >= end_pos.value) {
                return true;
            }
            if (self.last_server_lsn.value != 0 and self.last_server_lsn.value >= end_pos.value) {
                return true;
            }
        }
        return false;
    }

    /// Advance the processed LSN. Call this after successfully handling a message.
    pub fn ack(self: *Replicator, lsn: Lsn) void {
        self.last_processed_lsn = lsn;
    }

    /// Request the replicator to stop. Safe to call from another thread.
    /// The next call to `next()` will return `null`.
    pub fn stop(self: *Replicator) void {
        self.stop_flag.store(true, .release);
    }

    /// Check whether a stop has been requested. Safe to call from any thread.
    pub fn isStopRequested(self: *Replicator) bool {
        return self.stop_flag.load(.acquire);
    }

    fn maybeSendStatus(self: *Replicator) NextError!void {
        const now_ms = std.time.milliTimestamp();
        const interval: i64 = @intCast(self.config.status_interval_ms);
        if (now_ms - self.last_status_time_ms >= interval) {
            try self.sendStatus();
        }
    }

    /// Send a standby status update to the server.
    pub fn sendStatus(self: *Replicator) NextError!void {
        self.last_status_time_ms = std.time.milliTimestamp();

        const lsn_location = if (self.last_processed_lsn.value == 0)
            Lsn.zero
        else
            self.last_processed_lsn.inc();

        // Convert current time to PG epoch (microseconds since 2000-01-01)
        const now_us = std.time.microTimestamp();
        const pg_timestamp = now_us - PG_EPOCH_OFFSET_US;

        var buf: [128]u8 = undefined;
        const msg = protocol.encodeStandbyStatus(
            &buf,
            lsn_location,
            lsn_location,
            lsn_location,
            pg_timestamp,
            false,
        );
        try self.conn.transport.writeAll(msg);
    }

    pub fn deinit(self: *Replicator) void {
        self.conn.close();
    }
};
