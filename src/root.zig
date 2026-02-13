pub const Replicator = @import("replicator.zig").Replicator;
pub const Lsn = @import("lsn.zig").Lsn;
pub const ConnConfig = @import("types.zig").ConnConfig;
pub const ReplicatorConfig = @import("types.zig").ReplicatorConfig;
pub const WalMessage = @import("types.zig").WalMessage;

test {
    _ = @import("lsn.zig");
    _ = @import("protocol.zig");
    _ = @import("auth.zig");
    _ = @import("connection.zig");
    _ = @import("replicator.zig");
    _ = @import("types.zig");
}
