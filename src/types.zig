const Lsn = @import("lsn.zig").Lsn;

pub const TlsMode = enum {
    /// Do not use TLS.
    disable,
    /// Try TLS, fall back to plaintext if server doesn't support it.
    prefer,
    /// Require TLS; fail if server doesn't support it.
    require,
};

pub const ConnConfig = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 5432,
    user: []const u8,
    password: []const u8 = "",
    database: []const u8,
    /// Connect via Unix domain socket instead of TCP.
    /// When set, `host` and `port` are ignored for the connection itself,
    /// but `port` is still used to construct the socket filename if the
    /// path is a directory (e.g. "/tmp" -> "/tmp/.s.PGSQL.5432").
    socket_path: ?[]const u8 = null,
    /// TLS connection mode.
    tls: TlsMode = .disable,
};

pub const ReplicatorConfig = struct {
    conn: ConnConfig,
    slot_name: []const u8,
    /// Plugin options passed to START_REPLICATION, e.g.
    /// &.{ .{ "proto_version", "1" }, .{ "publication_names", "my_pub" } }
    options: []const [2][]const u8 = &.{},
    /// Where to start reading. null = use slot's confirmed_flush_lsn (0/0).
    start_position: ?Lsn = null,
    /// Where to stop. null = stream forever.
    end_position: ?Lsn = null,
    /// Interval in milliseconds between automatic status updates.
    /// Default 10 seconds.
    status_interval_ms: u64 = 10_000,
    /// Automatically reconnect on connection loss.
    auto_reconnect: bool = false,
    /// Maximum delay between reconnection attempts (milliseconds).
    /// Uses exponential backoff starting at 100ms up to this value.
    max_reconnect_delay_ms: u64 = 30_000,
    /// Maximum number of reconnection attempts. 0 = unlimited.
    max_reconnect_attempts: u32 = 0,
    /// Expected timeline ID. If set and the server reports a different
    /// timeline, `Replicator.init` returns `error.TimelineMismatch`.
    expected_timeline: ?u32 = null,
    /// Expected system identifier. If set and the server reports a different
    /// system ID, `Replicator.init` returns `error.SystemIdMismatch`.
    expected_systemid: ?[]const u8 = null,
};

pub const WalMessage = struct {
    /// WAL position where this record starts.
    wal_start: Lsn,
    /// Current end-of-WAL on the server.
    wal_end: Lsn,
    /// Server's send timestamp (microseconds since 2000-01-01 00:00:00 UTC).
    send_time: i64,
    /// The WAL data payload. This slice is borrowed from the receive buffer
    /// and is valid only until the next call to `next()`.
    data: []const u8,
};
