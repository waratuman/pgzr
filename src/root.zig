pub const Replicator = @import("replicator.zig").Replicator;
pub const Lsn = @import("lsn.zig").Lsn;
pub const ConnConfig = @import("types.zig").ConnConfig;
pub const ReplicatorConfig = @import("types.zig").ReplicatorConfig;
pub const WalMessage = @import("types.zig").WalMessage;
pub const TlsMode = @import("types.zig").TlsMode;
pub const pgoutput = @import("pgoutput.zig");
pub const Connection = @import("connection.zig").Connection;
pub const IngestConfig = @import("types.zig").IngestConfig;
pub const ProcessorConfig = @import("types.zig").ProcessorConfig;
pub const RelationInfo = @import("types.zig").RelationInfo;
pub const RelationColumnInfo = @import("types.zig").RelationColumnInfo;
pub const query = @import("query.zig");
pub const pg_types = @import("pg_types.zig");
pub const schema = @import("schema.zig");
pub const Ingestor = @import("ingest.zig").Ingestor;
pub const Processor = @import("processor.zig").Processor;

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
    _ = @import("query.zig");
    _ = @import("pg_types.zig");
    _ = @import("schema.zig");
    _ = @import("ingest.zig");
}
