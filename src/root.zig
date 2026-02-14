pub const Replicator = @import("replicator.zig").Replicator;
pub const Lsn = @import("lsn.zig").Lsn;
pub const ConnConfig = @import("types.zig").ConnConfig;
pub const ReplicatorConfig = @import("types.zig").ReplicatorConfig;
pub const WalMessage = @import("types.zig").WalMessage;
pub const TlsMode = @import("types.zig").TlsMode;
pub const pgoutput = @import("pgoutput.zig");

test {
    _ = @import("lsn.zig");
    _ = @import("protocol.zig");
    _ = @import("auth.zig");
    _ = @import("scram.zig");
    _ = @import("transport.zig");
    _ = @import("connection.zig");
    _ = @import("replicator.zig");
    _ = @import("pgoutput.zig");
    _ = @import("types.zig");
}
