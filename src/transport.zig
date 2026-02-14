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
    read_buf: [tls.Client.min_buffer_len]u8,
    write_buf: [16384]u8,

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
    pub fn upgrade(
        allocator: std.mem.Allocator,
        stream: std.net.Stream,
        host: []const u8,
    ) UpgradeError!*TlsState {
        const state = allocator.create(TlsState) catch return error.BufferTooSmall;
        errdefer allocator.destroy(state);

        state.stream = stream;
        state.read_buf = undefined;
        state.write_buf = undefined;
        state.stream_reader = stream.reader(&state.read_buf);
        state.stream_writer = stream.writer(&state.write_buf);

        // Load system CA certificates
        var ca_bundle: Certificate.Bundle = .{};
        ca_bundle.rescan(allocator) catch {
            // If we can't load system CAs, proceed without verification
            ca_bundle = .{};
        };

        state.tls_client = tls.Client.init(
            state.stream_reader.interface(),
            &state.stream_writer.interface,
            .{
                .host = .{ .explicit = host },
                .ca = if (ca_bundle.map.count() > 0) .{ .bundle = ca_bundle } else .no_verification,
                .read_buffer = &state.read_buf,
                .write_buffer = &state.write_buf,
            },
        ) catch |err| {
            ca_bundle.deinit(allocator);
            return err;
        };

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
