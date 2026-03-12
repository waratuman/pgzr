const Lsn = @import("lsn.zig").Lsn;

pub const TlsMode = enum {
    /// Do not use TLS.
    disable,
    /// Try TLS, fall back to plaintext if server doesn't support it.
    prefer,
    /// Require TLS; fail if server doesn't support it.
    /// Encrypts the connection but does NOT verify the server certificate
    /// or hostname (matches PostgreSQL sslmode=require).
    require,
    /// Require TLS with full certificate and hostname verification
    /// (matches PostgreSQL sslmode=verify-full).
    verify_full,
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
    /// Whether to open a replication connection. Set to false for normal
    /// connections (e.g. the destination database for WAL storage).
    replication: bool = true,
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

pub const OnFlushFn = *const fn (
    context: ?*anyopaque,
    batch_start_lsn: Lsn,
    batch_end_lsn: Lsn,
    msg_count: usize,
    is_complete: bool,
) void;

pub const IngestConfig = struct {
    source: ReplicatorConfig,
    dest: ConnConfig,
    source_id: []const u8,
    /// Maximum batch size in bytes before flushing (default 4 MiB).
    max_batch_size: usize = 4 * 1024 * 1024,
    /// Called after each successful batch flush. null = disabled.
    on_flush: ?OnFlushFn = null,
    /// Opaque context pointer passed to `on_flush`.
    on_flush_context: ?*anyopaque = null,
};

pub const ProcessorConfig = struct {
    dest: ConnConfig,
    /// Maximum number of batches to claim per processing cycle.
    batch_limit: u32 = 10,
    /// Polling interval in milliseconds when no pending batches are found.
    poll_interval_ms: u64 = 1_000,
    /// Prefix for pg_logical_emit_message metadata. null = disabled.
    metadata_message_prefix: ?[]const u8 = null,
    /// Source table name for metadata upserts. null = disabled.
    metadata_table: ?[]const u8 = null,
};

pub const RelationColumnInfo = struct {
    flags: u8,
    name: []const u8,
    type_oid: u32,
    type_modifier: i32,
};

pub const RelationInfo = struct {
    oid: u32,
    namespace: []const u8,
    name: []const u8,
    replica_identity: u8,
    columns: []const RelationColumnInfo,
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
