const std = @import("std");
const Io = std.Io;
const net = std.Io.net;
const protocol = @import("protocol.zig");
const auth_mod = @import("auth.zig");
const Lsn = @import("lsn.zig").Lsn;
const types = @import("types.zig");
const transport_mod = @import("transport.zig");
const Transport = transport_mod.Transport;
const PlainState = transport_mod.PlainState;
const TlsState = transport_mod.TlsState;

pub const default_recv_buf_size: usize = 1024 * 1024;

pub const Connection = struct {
    transport: Transport,
    allocator: std.mem.Allocator,
    io: Io,
    backend_pid: u32 = 0,
    backend_key: u32 = 0,
    recv_buf: []u8,
    /// Reusable send buffer for simple queries, COPY commands, and
    /// extended-protocol pipelines.
    send_buf: std.ArrayListUnmanaged(u8) = .empty,
    /// Side buffer holding the row returned by execPreparedWithResult.
    /// Separate from recv_buf so later protocol reads (which may
    /// overwrite or realloc recv_buf) cannot corrupt or dangle the
    /// returned column slices.
    result_buf: [4096]u8 = undefined,
    /// Heap-allocated transport state (PlainState or TlsState).
    plain_state: ?*PlainState = null,
    tls_state: ?*TlsState = null,
    /// SQLSTATE of the most recent server ErrorResponse within the current
    /// command (cleared at the start of each query). Lets callers classify
    /// error.ServerError (e.g. retryable deadlock vs. poison data).
    last_error_code: [5]u8 = undefined,
    last_error_code_len: u8 = 0,

    pub const ConnectError = auth_mod.AuthError || std.mem.Allocator.Error ||
        Transport.ReadError || Transport.WriteError || TlsState.UpgradeError ||
        net.IpAddress.ConnectError || net.HostName.ConnectError ||
        net.UnixAddress.ConnectError || net.UnixAddress.InitError ||
        net.HostName.ValidateError || error{
        ServerError,
        TlsNotSupported,
    };

    pub fn connect(allocator: std.mem.Allocator, io: Io, config: types.ConnConfig) ConnectError!Connection {
        const stream = if (config.socket_path) |path| blk: {
            // Build socket path: if it doesn't end with a port suffix,
            // append "/.s.PGSQL.<port>"
            if (std.mem.endsWith(u8, path, ".s.PGSQL.5432") or
                std.mem.indexOf(u8, path, ".s.PGSQL.") != null)
            {
                const ua = try net.UnixAddress.init(path);
                break :blk try ua.connect(io);
            } else {
                var path_buf: [256]u8 = undefined;
                const full_path = std.fmt.bufPrint(&path_buf, "{s}/.s.PGSQL.{d}", .{
                    path,
                    config.port,
                }) catch return error.NameTooLong;
                const ua = try net.UnixAddress.init(full_path);
                break :blk try ua.connect(io);
            }
        } else blk: {
            // IP literal first, then DNS via HostName.
            if (net.IpAddress.parse(config.host, config.port)) |addr| {
                break :blk try addr.connect(io, .{ .mode = .stream });
            } else |_| {
                const hn = try net.HostName.init(config.host);
                break :blk try hn.connect(io, config.port, .{ .mode = .stream });
            }
        };
        errdefer stream.close(io);

        // Set up transport (plain initially, may upgrade to TLS)
        var plain_state: ?*PlainState = try allocator.create(PlainState);
        errdefer if (plain_state) |ps| allocator.destroy(ps);
        plain_state.?.* = .{ .io = io, .stream = stream };
        var transport = Transport.plain(plain_state.?);

        var tls_state: ?*TlsState = null;

        // TLS negotiation (only for TCP connections, not Unix sockets)
        if (config.tls != .disable and config.socket_path == null) {
            // Send SSLRequest
            var ssl_buf: [8]u8 = undefined;
            std.mem.writeInt(u32, ssl_buf[0..4], 8, .big);
            std.mem.writeInt(u32, ssl_buf[4..8], 80877103, .big);
            try transport.writeAll(&ssl_buf);

            // Read 1-byte response. Safe with the buffered plain transport:
            // the server sends exactly one byte and then waits for the
            // client (TLS ClientHello), so the refill cannot slurp bytes
            // that belong to the TLS stream.
            var resp: [1]u8 = undefined;
            const n = try transport.read(&resp);
            if (n == 0) return error.ConnectionClosed;

            if (resp[0] == 'S') {
                // Server accepts TLS — perform handshake
                const ts = try TlsState.upgrade(allocator, io, stream, config.host, config.tls == .verify_full);
                tls_state = ts;
                errdefer if (tls_state) |ts2| allocator.destroy(ts2);
                transport = Transport.tlsClient(ts);
                // Free the plain state since we're now using TLS
                allocator.destroy(plain_state.?);
                plain_state = null;
            } else if (config.tls == .require or config.tls == .verify_full) {
                return error.TlsNotSupported;
            }
            // else: .prefer mode, server said 'N', continue with plain
        }

        var send_buf: [4096]u8 = undefined;

        // Send StartupMessage

        const startup = protocol.encodeStartup(&send_buf, config.user, config.database, config.replication);
        try transport.writeAll(startup);

        // Authenticate
        try auth_mod.authenticate(transport, io, config.user, config.password);

        const recv_buf = try allocator.alloc(u8, default_recv_buf_size);
        errdefer allocator.free(recv_buf);

        var conn = Connection{
            .transport = transport,
            .allocator = allocator,
            .io = io,
            .recv_buf = recv_buf,
            .plain_state = if (tls_state == null) plain_state else null,
            .tls_state = tls_state,
        };

        // Process post-auth messages until ReadyForQuery
        try conn.processPostAuth();

        return conn;
    }

    /// Read a message body, growing recv_buf if the body exceeds current capacity.
    /// Allocates 2x the body size to preserve the split-buffer strategy used by
    /// simpleQuery (first half for reads, second half for stable column
    /// data copies).
    pub fn readBodyGrowing(self: *Connection, header: protocol.MessageHeader) ![]u8 {
        const body_len = header.bodyLen();
        if (body_len > self.recv_buf.len) {
            const new_size = @max(body_len * 2, default_recv_buf_size);
            self.recv_buf = self.allocator.realloc(self.recv_buf, new_size) catch
                return error.OutOfMemory;
        }
        return try protocol.readBody(self.transport, header, self.recv_buf);
    }

    /// Shrink recv_buf back to default size if it was grown.
    fn resetRecvBuf(self: *Connection) void {
        if (self.recv_buf.len <= default_recv_buf_size) return;
        if (self.allocator.resize(self.recv_buf, default_recv_buf_size)) {
            self.recv_buf.len = default_recv_buf_size;
        }
    }

    /// Per-command setup: shrink recv_buf and clear the stored SQLSTATE.
    fn beginCommand(self: *Connection) void {
        self.resetRecvBuf();
        self.last_error_code_len = 0;
    }

    /// Record the SQLSTATE from a server ErrorResponse so callers can
    /// classify error.ServerError after the call returns.
    fn noteServerError(self: *Connection, info: protocol.ErrorInfo) void {
        const n: u8 = @intCast(@min(info.code.len, self.last_error_code.len));
        @memcpy(self.last_error_code[0..n], info.code[0..n]);
        self.last_error_code_len = n;
    }

    /// SQLSTATE of the server error raised by the most recent command, or
    /// null if the command did not produce a server error.
    pub fn lastServerErrorCode(self: *const Connection) ?[]const u8 {
        if (self.last_error_code_len == 0) return null;
        return self.last_error_code[0..self.last_error_code_len];
    }

    fn processPostAuth(self: *Connection) ConnectError!void {
        while (true) {
            const header = try protocol.readHeader(self.transport);
            switch (header.msg_type) {
                protocol.MSG_PARAM_STATUS => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf);
                },
                protocol.MSG_BACKEND_KEY => {
                    var buf: [8]u8 = undefined;
                    const body = try protocol.readBody(self.transport, header, &buf);
                    self.backend_pid = std.mem.readInt(u32, body[0..4], .big);
                    self.backend_key = std.mem.readInt(u32, body[4..8], .big);
                },
                protocol.MSG_READY => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf);
                    return;
                },
                protocol.MSG_ERROR => {
                    const body = try protocol.readBody(self.transport, header, self.recv_buf);
                    const err = protocol.parseError(body);
                    std.log.err("Server error: {s}: {s}", .{ err.code, err.message });
                    return error.ServerError;
                },
                protocol.MSG_NOTICE => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf);
                },
                else => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf);
                },
            }
        }
    }

    pub const QueryColumn = struct {
        data: []const u8,
        is_null: bool,
    };

    pub const QueryResult = struct {
        columns: [16]QueryColumn,
        column_count: usize,
        /// True when CopyBothResponse was received (START_REPLICATION).
        in_copy_mode: bool,
    };

    pub const QueryError = protocol.ReadError || protocol.ReadBodyError || Transport.WriteError || error{
        ServerError,
        OutOfMemory,
    };

    /// Execute a simple query and return the first DataRow result.
    /// Column data is copied into the second half of recv_buf so it
    /// survives subsequent protocol reads within this call.
    /// For CopyBothResponse (START_REPLICATION), returns with in_copy_mode=true.
    pub fn simpleQuery(self: *Connection, query: []const u8) QueryError!QueryResult {
        self.beginCommand();

        // Query message: 'Q' (1) + int32 len (4) + query + '\0' (1).
        // Uses the reusable growable send buffer: queries can exceed any
        // fixed stack buffer (e.g. START_REPLICATION with many options).
        const msg_len = 1 + 4 + query.len + 1;
        self.send_buf.clearRetainingCapacity();
        self.send_buf.ensureTotalCapacity(self.allocator, msg_len) catch return error.OutOfMemory;
        const msg = protocol.encodeQuery(self.send_buf.allocatedSlice()[0..msg_len], query);
        try self.transport.writeAll(msg);

        var result = QueryResult{
            .columns = undefined,
            .column_count = 0,
            .in_copy_mode = false,
        };

        // Reserve second half of recv_buf for stable column data copies.
        // Recomputed after readBodyGrowing since the buffer may grow.
        var stable_base = self.recv_buf.len / 2;
        var stable_pos: usize = 0;
        // On ErrorResponse the server still sends a trailing ReadyForQuery; we
        // must read through it before returning so the connection stays in sync
        // for subsequent queries (e.g. a ROLLBACK after a failed statement).
        var server_error = false;

        while (true) {
            const header = try protocol.readHeader(self.transport);

            switch (header.msg_type) {
                protocol.MSG_ROW_DESC => {
                    _ = try self.readBodyGrowing(header);
                },
                protocol.MSG_DATA_ROW => {
                    const body = try self.readBodyGrowing(header);
                    // Recompute stable_base in case the buffer grew
                    stable_base = self.recv_buf.len / 2;
                    if (result.column_count == 0) {
                        const raw_col_count = std.mem.readInt(u16, body[0..2], .big);
                        const col_count = @min(raw_col_count, result.columns.len);
                        var pos: usize = 2;
                        for (0..col_count) |i| {
                            const col_len_raw = std.mem.readInt(i32, body[pos..][0..4], .big);
                            pos += 4;
                            if (col_len_raw < 0) {
                                result.columns[i] = .{ .data = "", .is_null = true };
                            } else {
                                const col_len: usize = @intCast(col_len_raw);
                                if (stable_pos + col_len > stable_base) {
                                    // Column data exceeds stable region — skip remaining columns
                                    result.column_count = i;
                                    break;
                                }
                                const src = body[pos .. pos + col_len];
                                const dest = self.recv_buf[stable_base + stable_pos ..][0..col_len];
                                @memcpy(dest, src);
                                result.columns[i] = .{ .data = dest, .is_null = false };
                                stable_pos += col_len;
                                pos += col_len;
                            }
                        }
                        if (result.column_count == 0) result.column_count = col_count;
                    }
                },
                protocol.MSG_CMD_COMPLETE => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf[0..stable_base]);
                },
                protocol.MSG_READY => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf[0..stable_base]);
                    if (server_error) return error.ServerError;
                    return result;
                },
                protocol.MSG_ERROR => {
                    const body = try protocol.readBody(self.transport, header, self.recv_buf[0..stable_base]);
                    const err = protocol.parseError(body);
                    std.log.err("Query error: {s}: {s}", .{ err.code, err.message });
                    self.noteServerError(err);
                    server_error = true;
                },
                protocol.MSG_COPY_BOTH => {
                    _ = try self.readBodyGrowing(header);
                    result.in_copy_mode = true;
                    return result;
                },
                protocol.MSG_NOTICE => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf[0..stable_base]);
                },
                else => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf[0..stable_base]);
                },
            }
        }
    }

    /// Execute a simple query that may be larger than the stack buffer.
    /// Uses a reusable send buffer to avoid per-call allocations.
    /// Does not return result rows — use for INSERT/UPDATE/DELETE/DDL.
    pub fn execLarge(self: *Connection, allocator: std.mem.Allocator, sql: []const u8) QueryError!void {
        self.beginCommand();

        // Query message: 'Q' (1) + int32 len (4) + query + '\0' (1)
        const msg_len = 1 + 4 + sql.len + 1;
        self.send_buf.clearRetainingCapacity();
        self.send_buf.ensureTotalCapacity(allocator, msg_len) catch return error.OutOfMemory;
        const buf = self.send_buf.allocatedSlice()[0..msg_len];

        const msg = protocol.encodeQuery(buf, sql);
        std.log.debug("execLarge: sending query msg_len={d}", .{msg.len});
        try self.transport.writeAll(msg);
        std.log.debug("execLarge: query sent, waiting for response", .{});

        // On ErrorResponse the server still sends a trailing ReadyForQuery; read
        // through it before returning so the connection stays in sync.
        var server_error = false;

        while (true) {
            std.log.debug("execLarge: reading response header...", .{});
            const header = try protocol.readHeader(self.transport);
            std.log.debug("execLarge: response type='{c}' len={d}", .{ header.msg_type, header.length });

            switch (header.msg_type) {
                protocol.MSG_ROW_DESC,
                protocol.MSG_DATA_ROW,
                protocol.MSG_CMD_COMPLETE,
                protocol.MSG_NOTICE,
                => {
                    _ = try self.readBodyGrowing(header);
                },
                protocol.MSG_READY => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf);
                    if (server_error) return error.ServerError;
                    return;
                },
                protocol.MSG_ERROR => {
                    const body = try self.readBodyGrowing(header);
                    const err = protocol.parseError(body);
                    std.log.err("Query error: {s}: {s}", .{ err.code, err.message });
                    self.noteServerError(err);
                    server_error = true;
                },
                else => {
                    _ = try self.readBodyGrowing(header);
                },
            }
        }
    }

    /// Execute a COPY ... FROM STDIN command, sending `data` as the copy
    /// stream (pre-encoded in the format the command expects, e.g. text
    /// format rows). The server parses the stream directly, skipping SQL
    /// lexing/parsing/planning — much faster than multi-row INSERT for bulk
    /// loads. Returns error.ServerError if the command is rejected or any
    /// row fails; the connection is drained to ReadyForQuery either way.
    pub fn copyIn(self: *Connection, allocator: std.mem.Allocator, command: []const u8, data: []const u8) QueryError!void {
        try self.copyInBegin(allocator, command);
        try self.copyInWrite(data);
        try self.copyInEnd();
    }

    /// Start a COPY ... FROM STDIN command. On success the connection is
    /// in copy-in mode: send rows with copyInWrite, then finish with
    /// copyInEnd — or, if row production fails mid-stream, abort with
    /// copyInFail so the connection leaves copy mode. Returns
    /// error.ServerError (connection drained) if the command is rejected.
    pub fn copyInBegin(self: *Connection, allocator: std.mem.Allocator, command: []const u8) QueryError!void {
        self.beginCommand();

        const msg_len = 1 + 4 + command.len + 1;
        self.send_buf.clearRetainingCapacity();
        self.send_buf.ensureTotalCapacity(allocator, msg_len) catch return error.OutOfMemory;
        const msg = protocol.encodeQuery(self.send_buf.allocatedSlice()[0..msg_len], command);
        try self.transport.writeAll(msg);

        // Wait for CopyInResponse. If the command is rejected the server
        // sends ErrorResponse + ReadyForQuery without entering copy mode.
        while (true) {
            const header = try protocol.readHeader(self.transport);
            switch (header.msg_type) {
                protocol.MSG_COPY_IN => {
                    _ = try self.readBodyGrowing(header);
                    return;
                },
                protocol.MSG_READY => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf);
                    return error.ServerError;
                },
                protocol.MSG_ERROR => {
                    const body = try self.readBodyGrowing(header);
                    const err = protocol.parseError(body);
                    std.log.err("Copy error: {s}: {s}", .{ err.code, err.message });
                    self.noteServerError(err);
                },
                else => {
                    _ = try self.readBodyGrowing(header);
                },
            }
        }
    }

    /// Send one chunk of copy data while in copy-in mode. If the server
    /// errors mid-copy it discards further CopyData until CopyDone, so
    /// writing without reading cannot deadlock.
    pub fn copyInWrite(self: *Connection, data: []const u8) QueryError!void {
        var hdr: [5]u8 = undefined;
        const chunk_size: usize = 64 * 1024;
        var pos: usize = 0;
        while (pos < data.len) {
            const n = @min(chunk_size, data.len - pos);
            hdr[0] = protocol.MSG_COPY_DATA;
            std.mem.writeInt(u32, hdr[1..5], @intCast(4 + n), .big);
            try self.transport.writeAll(&hdr);
            try self.transport.writeAll(data[pos .. pos + n]);
            pos += n;
        }
    }

    /// Finish copy-in mode: send CopyDone and drain to ReadyForQuery.
    pub fn copyInEnd(self: *Connection) QueryError!void {
        var hdr: [5]u8 = undefined;
        hdr[0] = protocol.MSG_COPY_DONE;
        std.mem.writeInt(u32, hdr[1..5], 4, .big);
        try self.transport.writeAll(&hdr);
        try self.drainCopyResult();
    }

    /// Abort copy-in mode: send CopyFail and drain to ReadyForQuery. The
    /// server responds with an ErrorResponse, so errors are swallowed —
    /// this is a cleanup path for client-side failures mid-stream.
    pub fn copyInFail(self: *Connection, message: []const u8) void {
        var buf: [128]u8 = undefined;
        var w = protocol.MsgWriter{ .buf = &buf };
        w.writeByte(protocol.MSG_COPY_FAIL);
        const len_offset = w.pos;
        w.writeInt32(0);
        w.writeString(message[0..@min(message.len, buf.len - 7)]);
        w.patchLength(len_offset);
        self.transport.writeAll(w.getWritten()) catch return;
        self.drainCopyResult() catch {};
    }

    fn drainCopyResult(self: *Connection) QueryError!void {
        var server_error = false;
        while (true) {
            const header = try protocol.readHeader(self.transport);
            switch (header.msg_type) {
                protocol.MSG_READY => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf);
                    if (server_error) return error.ServerError;
                    return;
                },
                protocol.MSG_ERROR => {
                    const body = try self.readBodyGrowing(header);
                    const err = protocol.parseError(body);
                    std.log.err("Copy error: {s}: {s}", .{ err.code, err.message });
                    self.noteServerError(err);
                    server_error = true;
                },
                else => {
                    _ = try self.readBodyGrowing(header);
                },
            }
        }
    }

    // ── Extended query protocol (Parse/Bind/Execute/Sync) ─────────────

    /// Parse and save a named server-side prepared statement (Parse +
    /// Sync, one round trip). The statement lives for the session and is
    /// executed with execPrepared/execPreparedWithResult or queued in a
    /// Pipeline, giving plan caching and parameter binding without SQL
    /// escaping. To prepare several statements in one round trip, use
    /// Pipeline.prepare instead.
    pub fn prepare(self: *Connection, name: []const u8, sql: []const u8) QueryError!void {
        var p = self.pipeline();
        try p.prepare(name, sql);
        try p.syncDiscard();
    }

    /// Extended-protocol pipeline: queue several statements, send them in
    /// one network flush, and read all their responses after a single
    /// Sync — one round trip for the whole batch. Between pipeline() and
    /// sync() no other command may run on the connection.
    pub const Pipeline = struct {
        conn: *Connection,

        /// Queue one parameterized statement via the unnamed prepared
        /// statement (parsed fresh server-side). Params are text format;
        /// null entries bind SQL NULL.
        pub fn exec(self: *Pipeline, sql: []const u8, params: []const ?[]const u8) QueryError!void {
            protocol.appendParse(&self.conn.send_buf, self.conn.allocator, "", sql) catch
                return error.OutOfMemory;
            try self.bindExec("", params);
        }

        /// Queue execution of a statement previously created with
        /// Connection.prepare or Pipeline.prepare.
        pub fn execPrepared(self: *Pipeline, name: []const u8, params: []const ?[]const u8) QueryError!void {
            try self.bindExec(name, params);
        }

        /// Queue a Parse for a named prepared statement without executing
        /// it — several statements can be prepared in one round trip.
        /// Parse produces no CommandComplete, so prepares do not count
        /// toward the statement indexes passed to sync's callback.
        pub fn prepare(self: *Pipeline, name: []const u8, sql: []const u8) QueryError!void {
            protocol.appendParse(&self.conn.send_buf, self.conn.allocator, name, sql) catch
                return error.OutOfMemory;
        }

        fn bindExec(self: *Pipeline, stmt_name: []const u8, params: []const ?[]const u8) QueryError!void {
            const c = self.conn;
            protocol.appendBind(&c.send_buf, c.allocator, "", stmt_name, params) catch
                return error.OutOfMemory;
            protocol.appendExecute(&c.send_buf, c.allocator, "", 0) catch
                return error.OutOfMemory;
        }

        /// Send all queued messages plus Sync and read responses through
        /// ReadyForQuery. Each DataRow is passed to `callback` with the
        /// 0-based index of the queued statement that produced it; column
        /// data is borrowed from recv_buf and must be copied if kept.
        /// If any statement fails, the server skips the pipeline's
        /// remaining statements and error.ServerError is returned after
        /// draining (the SQLSTATE of the first failure is stored).
        pub fn sync(
            self: *Pipeline,
            context: anytype,
            callback: fn (@TypeOf(context), usize, []const QueryColumn) void,
        ) QueryError!void {
            const c = self.conn;
            protocol.appendSync(&c.send_buf, c.allocator) catch return error.OutOfMemory;
            try c.transport.writeAll(c.send_buf.items);
            c.send_buf.clearRetainingCapacity();
            try c.drainExtended(context, callback);
        }

        /// sync() for pipelines whose statements return no rows.
        pub fn syncDiscard(self: *Pipeline) QueryError!void {
            try self.sync({}, discardExtendedRow);
        }
    };

    /// Start an extended-protocol pipeline. Queue statements with exec /
    /// execPrepared, then call sync (or syncDiscard) to flush and collect
    /// the results.
    pub fn pipeline(self: *Connection) Pipeline {
        self.beginCommand();
        self.send_buf.clearRetainingCapacity();
        return .{ .conn = self };
    }

    /// Execute a prepared statement in one round trip, discarding any
    /// result rows.
    pub fn execPrepared(self: *Connection, name: []const u8, params: []const ?[]const u8) QueryError!void {
        var p = self.pipeline();
        try p.execPrepared(name, params);
        try p.syncDiscard();
    }

    /// Execute a prepared statement in one round trip and return the
    /// first result row. Column data is copied into result_buf — a side
    /// buffer separate from recv_buf — so it cannot be overwritten or
    /// left dangling by later protocol reads (which may overwrite or
    /// realloc recv_buf). Columns that don't fit in result_buf are
    /// dropped. The result is valid until the next
    /// execPreparedWithResult call on this connection.
    pub fn execPreparedWithResult(self: *Connection, name: []const u8, params: []const ?[]const u8) QueryError!QueryResult {
        var p = self.pipeline();
        try p.execPrepared(name, params);
        var cap = FirstRowCapture{ .conn = self };
        try p.sync(&cap, FirstRowCapture.onRow);
        return cap.result;
    }

    fn discardExtendedRow(_: void, _: usize, _: []const QueryColumn) void {}

    /// Parse a DataRow body into caller-provided column slots. Column
    /// data is borrowed from `body`; a raw column count above the slot
    /// array's size is clamped (extra columns are dropped).
    fn parseDataRow(body: []const u8, cols: *[16]QueryColumn) []const QueryColumn {
        const raw_col_count = std.mem.readInt(u16, body[0..2], .big);
        const col_count = @min(raw_col_count, cols.len);
        var pos: usize = 2;
        for (0..col_count) |i| {
            const col_len_raw = std.mem.readInt(i32, body[pos..][0..4], .big);
            pos += 4;
            if (col_len_raw < 0) {
                cols[i] = .{ .data = "", .is_null = true };
            } else {
                const col_len: usize = @intCast(col_len_raw);
                cols[i] = .{ .data = body[pos .. pos + col_len], .is_null = false };
                pos += col_len;
            }
        }
        return cols[0..col_count];
    }

    /// Captures the first DataRow of an extended-protocol exchange into
    /// the connection's result_buf side buffer.
    const FirstRowCapture = struct {
        conn: *Connection,
        pos: usize = 0,
        captured: bool = false,
        result: QueryResult = .{
            .columns = undefined,
            .column_count = 0,
            .in_copy_mode = false,
        },

        fn onRow(self: *FirstRowCapture, stmt_index: usize, cols: []const QueryColumn) void {
            _ = stmt_index;
            if (self.captured) return;
            self.captured = true;
            const buf = &self.conn.result_buf;
            const col_count = @min(cols.len, self.result.columns.len);
            for (cols[0..col_count], 0..) |col, i| {
                if (col.is_null) {
                    self.result.columns[i] = .{ .data = "", .is_null = true };
                    continue;
                }
                if (self.pos + col.data.len > buf.len) {
                    // Column data exceeds result_buf — drop remaining columns
                    self.result.column_count = i;
                    return;
                }
                const dest = buf[self.pos..][0..col.data.len];
                @memcpy(dest, col.data);
                self.result.columns[i] = .{ .data = dest, .is_null = false };
                self.pos += col.data.len;
            }
            self.result.column_count = col_count;
        }
    };

    /// Read extended-protocol responses until ReadyForQuery, dispatching
    /// DataRows to `callback` tagged with the index of the statement that
    /// produced them (statements are delimited by CommandComplete /
    /// EmptyQueryResponse / PortalSuspended).
    fn drainExtended(
        self: *Connection,
        context: anytype,
        callback: fn (@TypeOf(context), usize, []const QueryColumn) void,
    ) QueryError!void {
        var stmt_index: usize = 0;
        var server_error = false;

        while (true) {
            const header = try protocol.readHeader(self.transport);

            switch (header.msg_type) {
                protocol.MSG_DATA_ROW => {
                    const body = try self.readBodyGrowing(header);
                    var cols: [16]QueryColumn = undefined;
                    callback(context, stmt_index, parseDataRow(body, &cols));
                },
                protocol.MSG_CMD_COMPLETE,
                protocol.MSG_EMPTY_QUERY,
                protocol.MSG_PORTAL_SUSPENDED,
                => {
                    _ = try self.readBodyGrowing(header);
                    stmt_index += 1;
                },
                protocol.MSG_READY => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf[0..1]);
                    if (server_error) return error.ServerError;
                    return;
                },
                protocol.MSG_ERROR => {
                    const body = try self.readBodyGrowing(header);
                    const err = protocol.parseError(body);
                    std.log.err("Pipeline error: {s}: {s}", .{ err.code, err.message });
                    // Keep the first failure's SQLSTATE: it's the one that
                    // aborted the pipeline.
                    if (!server_error) self.noteServerError(err);
                    server_error = true;
                },
                else => {
                    // ParseComplete, BindComplete, RowDescription, NoData,
                    // ParameterDescription, ParameterStatus, NoticeResponse
                    _ = try self.readBodyGrowing(header);
                },
            }
        }
    }

    /// Return the underlying socket file descriptor.
    pub fn getStreamHandle(self: *const Connection) std.posix.fd_t {
        if (self.tls_state) |ts| return ts.stream.socket.handle;
        if (self.plain_state) |ps| return ps.stream.socket.handle;
        unreachable;
    }

    pub fn close(self: *Connection) void {
        self.transport.close();
        self.send_buf.deinit(self.allocator);
        self.allocator.free(self.recv_buf);
        if (self.tls_state) |ts| self.allocator.destroy(ts);
        if (self.plain_state) |ps| self.allocator.destroy(ps);
    }
};
