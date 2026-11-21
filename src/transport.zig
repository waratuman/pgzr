const std = @import("std");
const Io = std.Io;
const net = std.Io.net;
const tls = std.crypto.tls;
const Certificate = std.crypto.Certificate;

fn tlsDebugEnabled() bool {
    const ptr = std.c.getenv("PGZR_TLS_DEBUG") orelse return false;
    const value = std.mem.span(ptr);
    return value.len > 0 and !std.mem.eql(u8, value, "0");
}

fn tlsDebug(comptime fmt: []const u8, args: anytype) void {
    if (!tlsDebugEnabled()) return;
    std.debug.print("[pgzr tls/transport] " ++ fmt ++ "\n", args);
}

/// A transport abstraction over plain TCP/Unix sockets and TLS connections.
/// Provides a unified read/write interface used by the protocol layer.
pub const Transport = struct {
    context: *anyopaque,
    read_fn: *const fn (ctx: *anyopaque, buf: []u8) ReadError!usize,
    write_fn: *const fn (ctx: *anyopaque, data: []const u8) WriteError!void,
    close_fn: *const fn (ctx: *anyopaque) void,
    has_pending_fn: *const fn (ctx: *anyopaque) bool,

    pub const ReadError = error{ ConnectionClosed, ReadFailed, Canceled };
    pub const WriteError = error{ WriteFailed, Canceled };

    pub fn read(self: Transport, buf: []u8) ReadError!usize {
        return self.read_fn(self.context, buf);
    }

    pub fn writeAll(self: Transport, data: []const u8) WriteError!void {
        return self.write_fn(self.context, data);
    }

    pub fn close(self: Transport) void {
        self.close_fn(self.context);
    }

    /// Returns true if the transport has buffered data ready to read
    /// without needing to wait on the underlying socket.
    pub fn hasPending(self: Transport) bool {
        return self.has_pending_fn(self.context);
    }

    /// Create a Transport backed by a plain TCP or Unix socket stream.
    pub fn plain(state: *PlainState) Transport {
        return .{
            .context = @ptrCast(state),
            .read_fn = PlainState.readFn,
            .write_fn = PlainState.writeFn,
            .close_fn = PlainState.closeFn,
            .has_pending_fn = PlainState.hasPendingFn,
        };
    }

    /// Create a Transport backed by a TLS-wrapped connection.
    pub fn tlsClient(state: *TlsState) Transport {
        return .{
            .context = @ptrCast(state),
            .read_fn = TlsState.readFn,
            .write_fn = TlsState.writeFn,
            .close_fn = TlsState.closeFn,
            .has_pending_fn = TlsState.hasPendingFn,
        };
    }
};

pub const PlainState = struct {
    io: Io,
    stream: net.Stream,
    /// Buffered reads: the protocol layer reads a 5-byte header then a
    /// body per message, which unbuffered costs 2 syscalls per message
    /// (plus a poll). One refill syscall serves many messages, and
    /// hasPending lets the replicator skip poll entirely while data is
    /// buffered.
    read_buf: [read_buf_len]u8 = undefined,
    read_start: usize = 0,
    read_end: usize = 0,

    const read_buf_len = 64 * 1024;

    fn readFn(ctx: *anyopaque, buf: []u8) Transport.ReadError!usize {
        const self: *PlainState = @ptrCast(@alignCast(ctx));
        if (self.read_start == self.read_end) {
            // Large destination with nothing buffered: read directly into
            // it, skipping the intermediate copy (big message bodies).
            if (buf.len >= read_buf_len) {
                var bufs: [1][]u8 = .{buf};
                return self.stream.read(self.io, &bufs) catch |err| switch (err) {
                    error.Canceled => error.Canceled,
                    else => error.ReadFailed,
                };
            }
            var bufs: [1][]u8 = .{&self.read_buf};
            const n = self.stream.read(self.io, &bufs) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => return error.ReadFailed,
            };
            if (n == 0) return 0;
            self.read_start = 0;
            self.read_end = n;
        }
        const n = @min(buf.len, self.read_end - self.read_start);
        @memcpy(buf[0..n], self.read_buf[self.read_start..][0..n]);
        self.read_start += n;
        return n;
    }

    fn writeFn(ctx: *anyopaque, data: []const u8) Transport.WriteError!void {
        const self: *PlainState = @ptrCast(@alignCast(ctx));
        // Unbuffered writer: writeAll drains directly to the socket.
        var w = self.stream.writer(self.io, &.{});
        w.interface.writeAll(data) catch {
            if (w.err) |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => return error.WriteFailed,
            };
            return error.WriteFailed;
        };
    }

    fn closeFn(ctx: *anyopaque) void {
        const self: *PlainState = @ptrCast(@alignCast(ctx));
        self.stream.close(self.io);
    }

    fn hasPendingFn(ctx: *anyopaque) bool {
        const self: *PlainState = @ptrCast(@alignCast(ctx));
        return self.read_start != self.read_end;
    }
};

pub const TlsState = struct {
    io: Io,
    allocator: std.mem.Allocator,
    stream: net.Stream,
    tls_client: tls.Client,
    stream_reader: net.Stream.Reader,
    stream_writer: net.Stream.Writer,
    stream_read_buf: [tls.Client.min_buffer_len]u8,
    stream_write_buf: [tls.Client.min_buffer_len]u8,
    tls_read_buf: [tls.Client.min_buffer_len]u8,
    tls_write_buf: [tls.Client.min_buffer_len]u8,
    /// CA bundle for verify-full mode. Populated by tls.Client.init during
    /// the handshake; owned by this state and freed on close.
    ca_bundle: Certificate.Bundle,
    ca_lock: Io.RwLock,

    pub const UpgradeError = std.mem.Allocator.Error || tls.Client.InitError;

    /// Upgrade an existing plain TCP connection to TLS.
    /// Performs the TLS handshake (the SSLRequest/response exchange has
    /// already happened on the plain stream).
    /// When `verify` is true, the server certificate and hostname are
    /// validated against system CA certificates (equivalent to PostgreSQL
    /// sslmode=verify-full); verification fails closed if no CA bundle can
    /// be loaded (error.TlsCertificateNotVerified).
    /// When `verify` is false, the connection is encrypted but the
    /// certificate is not checked (equivalent to PostgreSQL sslmode=require).
    pub fn upgrade(
        allocator: std.mem.Allocator,
        io: Io,
        stream: net.Stream,
        host: []const u8,
        verify: bool,
    ) UpgradeError!*TlsState {
        const state = try allocator.create(TlsState);
        errdefer allocator.destroy(state);

        state.io = io;
        state.allocator = allocator;
        state.stream = stream;
        state.stream_read_buf = undefined;
        state.stream_write_buf = undefined;
        state.tls_read_buf = undefined;
        state.tls_write_buf = undefined;
        state.ca_bundle = .empty;
        state.ca_lock = .init;
        state.stream_reader = stream.reader(io, &state.stream_read_buf);
        state.stream_writer = stream.writer(io, &state.stream_write_buf);
        errdefer state.ca_bundle.deinit(allocator);

        var entropy: [tls.Client.Options.entropy_len]u8 = undefined;
        io.random(&entropy);

        state.tls_client = try tls.Client.init(
            &state.stream_reader.interface,
            &state.stream_writer.interface,
            .{
                .host = if (verify) .{ .explicit = host } else .no_verification,
                .ca = if (verify) .{ .bundle = .{
                    .gpa = allocator,
                    .io = io,
                    .lock = &state.ca_lock,
                    .bundle = &state.ca_bundle,
                } } else .no_verification,
                .read_buffer = &state.tls_read_buf,
                .write_buffer = &state.tls_write_buf,
                .entropy = &entropy,
                .realtime_now = Io.Clock.real.now(io),
            },
        );

        return state;
    }

    fn readFn(ctx: *anyopaque, buf: []u8) Transport.ReadError!usize {
        const self: *TlsState = @ptrCast(@alignCast(ctx));
        if (buf.len == 0) return 0;

        // Use peek/toss to correctly interact with the TLS reader's internal
        // buffer: peekGreedy decrypts into the reader's buffer, we copy out
        // as much as fits and toss it.
        const available = self.tls_client.reader.peekGreedy(1) catch |err| switch (err) {
            error.EndOfStream => {
                tlsDebug("tls read EndOfStream", .{});
                return 0;
            },
            error.ReadFailed => {
                if (self.tls_client.read_err) |tls_err| {
                    tlsDebug("tls read failed tls_err={}", .{tls_err});
                } else {
                    tlsDebug("tls read failed", .{});
                }
                return error.ReadFailed;
            },
        };
        const n = @min(buf.len, available.len);
        @memcpy(buf[0..n], available[0..n]);
        self.tls_client.reader.toss(n);
        tlsDebug("tls read bytes={d}", .{n});
        return n;
    }

    fn writeFn(ctx: *anyopaque, data: []const u8) Transport.WriteError!void {
        const self: *TlsState = @ptrCast(@alignCast(ctx));
        // Write in chunks that fit within a single TLS record so every
        // flush fully drains the plaintext into encrypted records.
        const max_chunk = tls.max_ciphertext_inner_record_len;
        var offset: usize = 0;
        while (offset < data.len) {
            const end = @min(offset + max_chunk, data.len);
            const chunk = data[offset..end];
            self.tls_client.writer.writeAll(chunk) catch return error.WriteFailed;
            self.tls_client.writer.flush() catch return error.WriteFailed;
            self.tls_client.output.flush() catch return error.WriteFailed;
            offset = end;
        }
    }

    fn closeFn(ctx: *anyopaque) void {
        const self: *TlsState = @ptrCast(@alignCast(ctx));
        self.tls_client.end() catch {};
        self.stream.close(self.io);
        self.ca_bundle.deinit(self.allocator);
    }

    fn hasPendingFn(ctx: *anyopaque) bool {
        const self: *TlsState = @ptrCast(@alignCast(ctx));
        // Only check the decrypted reader buffer. Buffered encrypted data
        // in the input stream may not form a complete TLS record — if we
        // skip poll() based on partial encrypted data, readHeader will
        // block indefinitely waiting for more socket data, preventing
        // status updates and causing connection timeouts.
        return self.tls_client.reader.seek < self.tls_client.reader.end;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "TlsState buffers do not alias" {
    // The stream reader/writer and TLS client must use separate buffers.
    // Previously they shared the same buffers, causing data corruption
    // during the TLS handshake.
    const stream_read_off = @offsetOf(TlsState, "stream_read_buf");
    const stream_write_off = @offsetOf(TlsState, "stream_write_buf");
    const tls_read_off = @offsetOf(TlsState, "tls_read_buf");
    const tls_write_off = @offsetOf(TlsState, "tls_write_buf");

    const stream_read_end = stream_read_off + @sizeOf(@TypeOf(@as(TlsState, undefined).stream_read_buf));
    const stream_write_end = stream_write_off + @sizeOf(@TypeOf(@as(TlsState, undefined).stream_write_buf));
    const tls_read_end = tls_read_off + @sizeOf(@TypeOf(@as(TlsState, undefined).tls_read_buf));
    const tls_write_end = tls_write_off + @sizeOf(@TypeOf(@as(TlsState, undefined).tls_write_buf));

    // No pair of buffers should overlap
    try std.testing.expect(stream_read_end <= stream_write_off or stream_write_end <= stream_read_off);
    try std.testing.expect(stream_read_end <= tls_read_off or tls_read_end <= stream_read_off);
    try std.testing.expect(stream_read_end <= tls_write_off or tls_write_end <= stream_read_off);
    try std.testing.expect(stream_write_end <= tls_read_off or tls_read_end <= stream_write_off);
    try std.testing.expect(stream_write_end <= tls_write_off or tls_write_end <= stream_write_off);
    try std.testing.expect(tls_read_end <= tls_write_off or tls_write_end <= tls_read_off);
}
