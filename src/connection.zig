const std = @import("std");
const protocol = @import("protocol.zig");
const auth_mod = @import("auth.zig");
const Lsn = @import("lsn.zig").Lsn;
const types = @import("types.zig");
const transport_mod = @import("transport.zig");
const Transport = transport_mod.Transport;
const PlainState = transport_mod.PlainState;
const TlsState = transport_mod.TlsState;

pub const Connection = struct {
    transport: Transport,
    allocator: std.mem.Allocator,
    backend_pid: u32 = 0,
    backend_key: u32 = 0,
    recv_buf: []u8,
    /// Reusable send buffer for execLarge/execLargeWithResult.
    send_buf: std.ArrayListUnmanaged(u8) = .{},
    /// Heap-allocated transport state (PlainState or TlsState).
    plain_state: ?*PlainState = null,
    tls_state: ?*TlsState = null,

    pub const TcpConnectError = @typeInfo(@typeInfo(@TypeOf(std.net.tcpConnectToHost)).@"fn".return_type.?).error_union.error_set;
    pub const UnixConnectError = @typeInfo(@typeInfo(@TypeOf(std.net.connectUnixSocket)).@"fn".return_type.?).error_union.error_set;
    pub const ConnectError = auth_mod.AuthError || std.mem.Allocator.Error || TcpConnectError || UnixConnectError || Transport.WriteError || TlsState.UpgradeError || error{
        ServerError,
        TlsNotSupported,
    };

    pub fn connect(allocator: std.mem.Allocator, config: types.ConnConfig) ConnectError!Connection {
        const stream = if (config.socket_path) |path| blk: {
            // Build socket path: if it doesn't end with a port suffix,
            // append "/.s.PGSQL.<port>"
            if (std.mem.endsWith(u8, path, ".s.PGSQL.5432") or
                std.mem.indexOf(u8, path, ".s.PGSQL.") != null)
            {
                break :blk try std.net.connectUnixSocket(path);
            } else {
                var path_buf: [256]u8 = undefined;
                const full_path = std.fmt.bufPrint(&path_buf, "{s}/.s.PGSQL.{d}", .{
                    path,
                    config.port,
                }) catch return error.OutOfMemory;
                break :blk try std.net.connectUnixSocket(full_path);
            }
        } else try std.net.tcpConnectToHost(allocator, config.host, config.port);
        errdefer stream.close();

        // Set up transport (plain initially, may upgrade to TLS)
        var plain_state = try allocator.create(PlainState);
        errdefer allocator.destroy(plain_state);
        plain_state.* = .{ .stream = stream };
        var transport = Transport.plain(plain_state);

        var tls_state: ?*TlsState = null;

        // TLS negotiation (only for TCP connections, not Unix sockets)
        if (config.tls != .disable and config.socket_path == null) {
            // Send SSLRequest
            var ssl_buf: [8]u8 = undefined;
            std.mem.writeInt(u32, ssl_buf[0..4], 8, .big);
            std.mem.writeInt(u32, ssl_buf[4..8], 80877103, .big);
            stream.writeAll(&ssl_buf) catch return error.WriteFailed;

            // Read 1-byte response
            var resp: [1]u8 = undefined;
            _ = stream.read(&resp) catch return error.ConnectionClosed;

            if (resp[0] == 'S') {
                // Server accepts TLS — perform handshake
                const ts = try TlsState.upgrade(allocator, stream, config.host, config.tls == .verify_full);
                tls_state = ts;
                transport = Transport.tlsClient(ts);
                // Free the plain state since we're now using TLS
                allocator.destroy(plain_state);
                plain_state = undefined;
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
        try auth_mod.authenticate(transport, config.user, config.password);

        // Allocate receive buffer (1 MiB)
        const recv_buf = try allocator.alloc(u8, 1024 * 1024);
        errdefer allocator.free(recv_buf);

        var conn = Connection{
            .transport = transport,
            .allocator = allocator,
            .recv_buf = recv_buf,
            .plain_state = if (tls_state == null) plain_state else null,
            .tls_state = tls_state,
        };

        // Process post-auth messages until ReadyForQuery
        try conn.processPostAuth();

        return conn;
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
        var send_buf: [4096]u8 = undefined;
        const msg = protocol.encodeQuery(&send_buf, query);
        try self.transport.writeAll(msg);

        var result = QueryResult{
            .columns = undefined,
            .column_count = 0,
            .in_copy_mode = false,
        };

        // Reserve second half of recv_buf for stable column data copies
        const stable_base = self.recv_buf.len / 2;
        var stable_pos: usize = 0;

        while (true) {
            const header = try protocol.readHeader(self.transport);

            switch (header.msg_type) {
                protocol.MSG_ROW_DESC => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf);
                },
                protocol.MSG_DATA_ROW => {
                    const body = try protocol.readBody(self.transport, header, self.recv_buf);
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
                    return result;
                },
                protocol.MSG_ERROR => {
                    const body = try protocol.readBody(self.transport, header, self.recv_buf[0..stable_base]);
                    const err = protocol.parseError(body);
                    std.log.err("Query error: {s}: {s}", .{ err.code, err.message });
                    return error.ServerError;
                },
                protocol.MSG_COPY_BOTH => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf);
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
    pub fn execLarge(self: *Connection, allocator: std.mem.Allocator, query: []const u8) QueryError!void {
        // Query message: 'Q' (1) + int32 len (4) + query + '\0' (1)
        const msg_len = 1 + 4 + query.len + 1;
        self.send_buf.clearRetainingCapacity();
        self.send_buf.ensureTotalCapacity(allocator, msg_len) catch return error.OutOfMemory;
        const buf = self.send_buf.allocatedSlice()[0..msg_len];

        const msg = protocol.encodeQuery(buf, query);
        try self.transport.writeAll(msg);

        while (true) {
            const header = try protocol.readHeader(self.transport);

            switch (header.msg_type) {
                protocol.MSG_ROW_DESC,
                protocol.MSG_DATA_ROW,
                protocol.MSG_CMD_COMPLETE,
                protocol.MSG_NOTICE,
                => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf);
                },
                protocol.MSG_READY => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf);
                    return;
                },
                protocol.MSG_ERROR => {
                    const body = try protocol.readBody(self.transport, header, self.recv_buf);
                    const err = protocol.parseError(body);
                    std.log.err("Query error: {s}: {s}", .{ err.code, err.message });
                    return error.ServerError;
                },
                else => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf);
                },
            }
        }
    }

    /// Execute a large query and return the first DataRow result.
    /// Like execLarge but captures the first row (for RETURNING clauses).
    /// Column data is copied into the second half of recv_buf so it
    /// survives subsequent protocol reads.
    pub fn execLargeWithResult(self: *Connection, allocator: std.mem.Allocator, query: []const u8) QueryError!QueryResult {
        const msg_len = 1 + 4 + query.len + 1;
        self.send_buf.clearRetainingCapacity();
        self.send_buf.ensureTotalCapacity(allocator, msg_len) catch return error.OutOfMemory;
        const buf = self.send_buf.allocatedSlice()[0..msg_len];

        const msg = protocol.encodeQuery(buf, query);
        try self.transport.writeAll(msg);

        var result = QueryResult{
            .columns = undefined,
            .column_count = 0,
            .in_copy_mode = false,
        };

        // Reserve second half of recv_buf for stable column data copies
        const stable_base = self.recv_buf.len / 2;
        var stable_pos: usize = 0;

        while (true) {
            const header = try protocol.readHeader(self.transport);

            switch (header.msg_type) {
                protocol.MSG_ROW_DESC => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf);
                },
                protocol.MSG_DATA_ROW => {
                    const body = try protocol.readBody(self.transport, header, self.recv_buf);
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
                    return result;
                },
                protocol.MSG_ERROR => {
                    const body = try protocol.readBody(self.transport, header, self.recv_buf[0..stable_base]);
                    const err = protocol.parseError(body);
                    std.log.err("Query error: {s}: {s}", .{ err.code, err.message });
                    return error.ServerError;
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

    /// Execute a simple query and invoke a callback for each DataRow.
    /// The callback receives the column data slice (borrowed from recv_buf)
    /// and must copy any data it needs before returning.
    /// Returns the number of rows yielded.
    pub fn queryRows(
        self: *Connection,
        query: []const u8,
        context: anytype,
        callback: fn (@TypeOf(context), []const QueryColumn) void,
    ) QueryError!usize {
        var send_buf: [4096]u8 = undefined;
        const msg = protocol.encodeQuery(&send_buf, query);
        try self.transport.writeAll(msg);

        var row_count: usize = 0;

        while (true) {
            const header = try protocol.readHeader(self.transport);

            switch (header.msg_type) {
                protocol.MSG_ROW_DESC => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf);
                },
                protocol.MSG_DATA_ROW => {
                    const body = try protocol.readBody(self.transport, header, self.recv_buf);
                    const col_count = std.mem.readInt(u16, body[0..2], .big);
                    var cols: [16]QueryColumn = undefined;
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
                    callback(context, cols[0..col_count]);
                    row_count += 1;
                },
                protocol.MSG_CMD_COMPLETE => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf);
                },
                protocol.MSG_READY => {
                    _ = try protocol.readBody(self.transport, header, self.recv_buf);
                    return row_count;
                },
                protocol.MSG_ERROR => {
                    const body = try protocol.readBody(self.transport, header, self.recv_buf);
                    const err = protocol.parseError(body);
                    std.log.err("Query error: {s}: {s}", .{ err.code, err.message });
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

    pub fn close(self: *Connection) void {
        self.transport.close();
        self.send_buf.deinit(self.allocator);
        self.allocator.free(self.recv_buf);
        if (self.tls_state) |ts| self.allocator.destroy(ts);
        if (self.plain_state) |ps| self.allocator.destroy(ps);
    }
};
