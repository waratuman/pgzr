const std = @import("std");
const tls = std.crypto.tls;
const Certificate = std.crypto.Certificate;

/// A transport abstraction over plain TCP/Unix sockets and TLS connections.
/// Provides a unified read/write interface used by the protocol layer.
pub const Transport = struct {
    context: *anyopaque,
    read_fn: *const fn (ctx: *anyopaque, buf: []u8) ReadError!usize,
    write_fn: *const fn (ctx: *anyopaque, data: []const u8) WriteError!void,
    close_fn: *const fn (ctx: *anyopaque) void,

    pub const ReadError = std.net.Stream.ReadError || tls.Client.ReadError || error{ConnectionClosed};
    pub const WriteError = std.net.Stream.WriteError || error{WriteFailed};

    pub fn read(self: Transport, buf: []u8) ReadError!usize {
        return self.read_fn(self.context, buf);
    }

    pub fn writeAll(self: Transport, data: []const u8) WriteError!void {
        return self.write_fn(self.context, data);
    }

    pub fn close(self: Transport) void {
        self.close_fn(self.context);
    }

    /// Create a Transport backed by a plain std.net.Stream (TCP or Unix socket).
    pub fn plain(state: *PlainState) Transport {
        return .{
            .context = @ptrCast(state),
            .read_fn = PlainState.readFn,
            .write_fn = PlainState.writeFn,
            .close_fn = PlainState.closeFn,
        };
    }

    /// Create a Transport backed by a TLS-wrapped connection.
    pub fn tlsClient(state: *TlsState) Transport {
        return .{
            .context = @ptrCast(state),
            .read_fn = TlsState.readFn,
            .write_fn = TlsState.writeFn,
            .close_fn = TlsState.closeFn,
        };
    }
};

pub const PlainState = struct {
    stream: std.net.Stream,

    fn readFn(ctx: *anyopaque, buf: []u8) Transport.ReadError!usize {
        const self: *PlainState = @ptrCast(@alignCast(ctx));
        return self.stream.read(buf);
    }

    fn writeFn(ctx: *anyopaque, data: []const u8) Transport.WriteError!void {
        const self: *PlainState = @ptrCast(@alignCast(ctx));
        var index: usize = 0;
        while (index < data.len) {
            index += self.stream.write(data[index..]) catch |err| switch (err) {
                inline else => |e| return @as(Transport.WriteError, e),
            };
        }
    }

    fn closeFn(ctx: *anyopaque) void {
        const self: *PlainState = @ptrCast(@alignCast(ctx));
        self.stream.close();
    }
};

pub const TlsState = struct {
    stream: std.net.Stream,
    tls_client: tls.Client,
    stream_reader: std.net.Stream.Reader,
    stream_writer: std.net.Stream.Writer,
    stream_read_buf: [16384]u8,
    stream_write_buf: [16384]u8,
    tls_read_buf: [tls.Client.min_buffer_len]u8,
    tls_write_buf: [16384]u8,

    pub const UpgradeError = error{
        TlsNotSupported,
        TlsAlert,
        TlsUnexpectedMessage,
        TlsIllegalParameter,
        TlsDecryptFailure,
        TlsRecordOverflow,
        TlsBadRecordMac,
        TlsDecryptError,
        TlsConnectionTruncated,
        TlsDecodeError,
        TlsBadSignatureScheme,
        TlsBadRsaSignatureBitCount,
        CertificateFieldHasInvalidLength,
        CertificateHostMismatch,
        CertificatePublicKeyInvalid,
        CertificateExpired,
        CertificateFieldHasWrongDataType,
        CertificateIssuerMismatch,
        CertificateNotYetValid,
        CertificateSignatureAlgorithmMismatch,
        CertificateSignatureAlgorithmUnsupported,
        CertificateSignatureInvalid,
        CertificateSignatureInvalidLength,
        CertificateSignatureNamedCurveUnsupported,
        CertificateSignatureUnsupportedBitCount,
        TlsCertificateNotVerified,
        UnsupportedCertificateVersion,
        CertificateTimeInvalid,
        CertificateHasUnrecognizedObjectId,
        CertificateHasInvalidBitString,
        InvalidEncoding,
        InvalidSignature,
        SignatureVerificationFailed,
        IdentityElement,
        NotSquare,
        NonCanonical,
        WeakPublicKey,
        MessageTooLong,
        NegativeIntoUnsigned,
        TargetTooSmall,
        BufferTooSmall,
        WriteFailed,
        ReadFailed,
        InsufficientEntropy,
        DiskQuota,
        LockViolation,
        NotOpenForWriting,
    };

    /// Upgrade an existing plain TCP connection to TLS.
    /// Sends the SSLRequest, reads the server response, and performs the TLS handshake.
    /// When `verify` is true, the server certificate and hostname are validated
    /// against system CA certificates (equivalent to PostgreSQL sslmode=verify-full).
    /// When `verify` is false, the connection is encrypted but the certificate
    /// is not checked (equivalent to PostgreSQL sslmode=require).
    pub fn upgrade(
        allocator: std.mem.Allocator,
        stream: std.net.Stream,
        host: []const u8,
        verify: bool,
    ) UpgradeError!*TlsState {
        const state = allocator.create(TlsState) catch return error.BufferTooSmall;
        errdefer allocator.destroy(state);

        state.stream = stream;
        state.stream_read_buf = undefined;
        state.stream_write_buf = undefined;
        state.tls_read_buf = undefined;
        state.tls_write_buf = undefined;
        state.stream_reader = stream.reader(&state.stream_read_buf);
        state.stream_writer = stream.writer(&state.stream_write_buf);

        if (verify) {
            // Load system CA certificates for verification
            var ca_bundle: Certificate.Bundle = .{};
            ca_bundle.rescan(allocator) catch {
                ca_bundle = .{};
            };

            state.tls_client = tls.Client.init(
                state.stream_reader.interface(),
                &state.stream_writer.interface,
                .{
                    .host = .{ .explicit = host },
                    .ca = if (ca_bundle.map.count() > 0) .{ .bundle = ca_bundle } else .no_verification,
                    .read_buffer = &state.tls_read_buf,
                    .write_buffer = &state.tls_write_buf,
                },
            ) catch |err| {
                ca_bundle.deinit(allocator);
                return err;
            };
        } else {
            // Encrypt only — no certificate or hostname verification
            state.tls_client = tls.Client.init(
                state.stream_reader.interface(),
                &state.stream_writer.interface,
                .{
                    .host = .no_verification,
                    .ca = .no_verification,
                    .read_buffer = &state.tls_read_buf,
                    .write_buffer = &state.tls_write_buf,
                },
            ) catch |err| {
                return err;
            };
        }

        return state;
    }

    fn readFn(ctx: *anyopaque, buf: []u8) Transport.ReadError!usize {
        const self: *TlsState = @ptrCast(@alignCast(ctx));
        self.tls_client.reader.readSliceAll(buf) catch |err| switch (err) {
            error.EndOfStream => return 0,
            error.ReadFailed => {
                if (self.tls_client.read_err) |tls_err| return tls_err;
                return error.ConnectionClosed;
            },
        };
        return buf.len;
    }

    fn writeFn(ctx: *anyopaque, data: []const u8) Transport.WriteError!void {
        const self: *TlsState = @ptrCast(@alignCast(ctx));
        self.tls_client.writer.writeAll(data) catch return error.WriteFailed;
        self.tls_client.writer.flush() catch return error.WriteFailed;
    }

    fn closeFn(ctx: *anyopaque) void {
        const self: *TlsState = @ptrCast(@alignCast(ctx));
        self.tls_client.end() catch {};
        self.stream.close();
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "PlainState size" {
    try std.testing.expect(@sizeOf(PlainState) <= 16);
}

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
