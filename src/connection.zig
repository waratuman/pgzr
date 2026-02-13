const std = @import("std");
const protocol = @import("protocol.zig");
const auth_mod = @import("auth.zig");
const Lsn = @import("lsn.zig").Lsn;
const types = @import("types.zig");

pub const Connection = struct {
    stream: std.net.Stream,
    allocator: std.mem.Allocator,
    backend_pid: u32 = 0,
    backend_key: u32 = 0,
    recv_buf: []u8,

    pub const TcpConnectError = @typeInfo(@typeInfo(@TypeOf(std.net.tcpConnectToHost)).@"fn".return_type.?).error_union.error_set;
    pub const ConnectError = auth_mod.AuthError || std.mem.Allocator.Error || TcpConnectError || std.net.Stream.WriteError || error{
        ServerError,
    };

    pub fn connect(allocator: std.mem.Allocator, config: types.ConnConfig) ConnectError!Connection {
        const stream = try std.net.tcpConnectToHost(allocator, config.host, config.port);
        errdefer stream.close();

        var send_buf: [4096]u8 = undefined;

        // Send StartupMessage
        const startup = protocol.encodeStartup(&send_buf, config.user, config.database);
        try stream.writeAll(startup);

        // Authenticate
        try auth_mod.authenticate(stream, config.user, config.password);

        // Allocate receive buffer (1 MiB)
        const recv_buf = try allocator.alloc(u8, 1024 * 1024);
        errdefer allocator.free(recv_buf);

        var conn = Connection{
            .stream = stream,
            .allocator = allocator,
            .recv_buf = recv_buf,
        };

        // Process post-auth messages until ReadyForQuery
        try conn.processPostAuth();

        return conn;
    }

    fn processPostAuth(self: *Connection) ConnectError!void {
        while (true) {
            const header = try protocol.readHeader(self.stream);
            switch (header.msg_type) {
                protocol.MSG_PARAM_STATUS => {
                    _ = try protocol.readBody(self.stream, header, self.recv_buf);
                },
                protocol.MSG_BACKEND_KEY => {
                    var buf: [8]u8 = undefined;
                    const body = try protocol.readBody(self.stream, header, &buf);
                    self.backend_pid = std.mem.readInt(u32, body[0..4], .big);
                    self.backend_key = std.mem.readInt(u32, body[4..8], .big);
                },
                protocol.MSG_READY => {
                    _ = try protocol.readBody(self.stream, header, self.recv_buf);
                    return;
                },
                protocol.MSG_ERROR => {
                    const body = try protocol.readBody(self.stream, header, self.recv_buf);
                    const err = protocol.parseError(body);
                    std.log.err("Server error: {s}: {s}", .{ err.code, err.message });
                    return error.ServerError;
                },
                protocol.MSG_NOTICE => {
                    _ = try protocol.readBody(self.stream, header, self.recv_buf);
                },
                else => {
                    _ = try protocol.readBody(self.stream, header, self.recv_buf);
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

    pub const QueryError = protocol.ReadError || protocol.ReadBodyError || std.net.Stream.WriteError || error{
        ServerError,
    };

    /// Execute a simple query and return the first DataRow result.
    /// For CopyBothResponse (START_REPLICATION), returns with in_copy_mode=true.
    pub fn simpleQuery(self: *Connection, query: []const u8) QueryError!QueryResult {
        var send_buf: [4096]u8 = undefined;
        const msg = protocol.encodeQuery(&send_buf, query);
        try self.stream.writeAll(msg);

        var result = QueryResult{
            .columns = undefined,
            .column_count = 0,
            .in_copy_mode = false,
        };

        while (true) {
            const header = try protocol.readHeader(self.stream);

            switch (header.msg_type) {
                protocol.MSG_ROW_DESC => {
                    _ = try protocol.readBody(self.stream, header, self.recv_buf);
                },
                protocol.MSG_DATA_ROW => {
                    const body = try protocol.readBody(self.stream, header, self.recv_buf);
                    const col_count = std.mem.readInt(u16, body[0..2], .big);
                    var pos: usize = 2;
                    for (0..col_count) |i| {
                        const col_len_raw = std.mem.readInt(i32, body[pos..][0..4], .big);
                        pos += 4;
                        if (col_len_raw < 0) {
                            result.columns[i] = .{ .data = "", .is_null = true };
                        } else {
                            const col_len: usize = @intCast(col_len_raw);
                            result.columns[i] = .{ .data = body[pos .. pos + col_len], .is_null = false };
                            pos += col_len;
                        }
                    }
                    result.column_count = col_count;
                },
                protocol.MSG_CMD_COMPLETE => {
                    _ = try protocol.readBody(self.stream, header, self.recv_buf);
                },
                protocol.MSG_READY => {
                    _ = try protocol.readBody(self.stream, header, self.recv_buf);
                    return result;
                },
                protocol.MSG_ERROR => {
                    const body = try protocol.readBody(self.stream, header, self.recv_buf);
                    const err = protocol.parseError(body);
                    std.log.err("Query error: {s}: {s}", .{ err.code, err.message });
                    return error.ServerError;
                },
                protocol.MSG_COPY_BOTH => {
                    _ = try protocol.readBody(self.stream, header, self.recv_buf);
                    result.in_copy_mode = true;
                    return result;
                },
                protocol.MSG_NOTICE => {
                    _ = try protocol.readBody(self.stream, header, self.recv_buf);
                },
                else => {
                    _ = try protocol.readBody(self.stream, header, self.recv_buf);
                },
            }
        }
    }

    pub fn close(self: *Connection) void {
        self.stream.close();
        self.allocator.free(self.recv_buf);
    }
};
