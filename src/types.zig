const Lsn = @import("lsn.zig").Lsn;

pub const ConnConfig = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 5432,
    user: []const u8,
    password: []const u8 = "",
    database: []const u8,
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
