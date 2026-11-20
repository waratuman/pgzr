const std = @import("std");
const Ingestor = @import("ingest.zig").Ingestor;
const Processor = @import("processor.zig").Processor;
const types = @import("types.zig");
const Lsn = @import("lsn.zig").Lsn;
const query_mod = @import("query.zig");

// Randomness note: pre-0.17 this file set `crypto_always_getrandom` to
// avoid a dlopen+threads crash in std's threadlocal CSPRNG. Since the
// 0.17 upgrade all randomness goes through the per-handle `Io.Threaded`
// instance instead of a global threadlocal CSPRNG, so no std.Options
// override is needed.

// ── ABI version ───────────────────────────────────────────────────────
//
// Bumped whenever the layout or semantics of any exported config struct
// or function changes in an incompatible way. Callers (e.g. the Ruby
// gem) should resolve this symbol at load time and refuse to proceed
// when it does not match the value they were built against. A missing
// symbol indicates a pre-0.4.0 library, which is also incompatible.
const PGZR_ABI_VERSION: u32 = 0x00040000;

export fn pgzr_abi_version() u32 {
    return PGZR_ABI_VERSION;
}

// ── Error handling ────────────────────────────────────────────────────

threadlocal var last_error_buf: [1024]u8 = undefined;
threadlocal var last_error_len: usize = 0;

fn setError(comptime fmt: []const u8, args: anytype) void {
    const result = std.fmt.bufPrint(&last_error_buf, fmt, args) catch {
        const msg = "error message too long";
        @memcpy(last_error_buf[0..msg.len], msg);
        last_error_len = msg.len;
        return;
    };
    last_error_len = result.len;
}

fn setErrorStr(msg: []const u8) void {
    const len = @min(msg.len, last_error_buf.len);
    @memcpy(last_error_buf[0..len], msg[0..len]);
    last_error_len = len;
}

export fn pgzr_last_error(out_len: *usize) [*]const u8 {
    out_len.* = last_error_len;
    return &last_error_buf;
}

// ── Signal handling ───────────────────────────────────────────────────
//
// When pgzr is called via FFI (Ruby, Python), the host language's signal
// handlers can't execute during a blocking C call. We install our own
// SIGINT/SIGTERM handler that calls stop() on the active ingestor and/or
// processor, allowing the blocking run() call to return cleanly.

const posix = std.posix;

var active_ingestor = std.atomic.Value(?*Ingestor).init(null);
var active_processor = std.atomic.Value(?*Processor).init(null);

fn signalHandler(_: posix.SIG) callconv(.c) void {
    if (active_ingestor.load(.monotonic)) |ing| ing.stop();
    if (active_processor.load(.monotonic)) |proc| proc.stop();
}

fn installSignalHandlers(
    prev_int: *posix.Sigaction,
    prev_term: *posix.Sigaction,
) void {
    const sa = posix.Sigaction{
        .handler = .{ .handler = signalHandler },
        .mask = posix.sigemptyset(),
        .flags = 0,
    };
    posix.sigaction(.INT, &sa, prev_int);
    posix.sigaction(.TERM, &sa, prev_term);
}

fn restoreSignalHandlers(
    prev_int: *const posix.Sigaction,
    prev_term: *const posix.Sigaction,
) void {
    posix.sigaction(.INT, prev_int, null);
    posix.sigaction(.TERM, prev_term, null);
}

// ── Helpers ───────────────────────────────────────────────────────────

/// Copy a caller-provided C string into `aa`-owned memory (empty slice for
/// null). Everything handed to Ingestor/Processor configs must be copied:
/// the FFI host (Ruby, Python) is free to garbage-collect its config struct
/// and strings as soon as pgzr_*_new returns.
fn dupeCStr(aa: std.mem.Allocator, ptr: ?[*:0]const u8) std.mem.Allocator.Error![]const u8 {
    const p = ptr orelse return "";
    return aa.dupe(u8, std.mem.span(p));
}

fn buildConnConfig(
    aa: std.mem.Allocator,
    host: ?[*:0]const u8,
    port: u16,
    user: ?[*:0]const u8,
    password: ?[*:0]const u8,
    database: ?[*:0]const u8,
    socket_path: ?[*:0]const u8,
    tls_mode: u8,
) std.mem.Allocator.Error!types.ConnConfig {
    return .{
        .host = try dupeCStr(aa, host),
        .port = port,
        .user = try dupeCStr(aa, user),
        .password = try dupeCStr(aa, password),
        .database = try dupeCStr(aa, database),
        .socket_path = if (socket_path != null) try dupeCStr(aa, socket_path) else null,
        .tls = switch (tls_mode) {
            1 => .prefer,
            2 => .require,
            3 => .verify_full,
            else => .disable,
        },
        .replication = true,
    };
}

// ── Ingestor ──────────────────────────────────────────────────────────

/// C-compatible on_flush callback type.
/// LSN values are passed as raw u64 (Lsn.value).
pub const PgzrOnFlushFn = *const fn (
    context: ?*anyopaque,
    batch_start_lsn: u64,
    batch_end_lsn: u64,
    msg_count: usize,
    is_complete: bool,
) callconv(.c) void;

/// Bridge between C-calling-convention callbacks and Zig's OnFlushFn.
const CFlushBridge = struct {
    c_callback: PgzrOnFlushFn,
    c_context: ?*anyopaque,

    fn zigCallback(context: ?*anyopaque, batch_start_lsn: Lsn, batch_end_lsn: Lsn, msg_count: usize, is_complete: bool) void {
        const self: *CFlushBridge = @ptrCast(@alignCast(context));
        self.c_callback(self.c_context, batch_start_lsn.value, batch_end_lsn.value, msg_count, is_complete);
    }
};

pub const PgzrIngestConfig = extern struct {
    source_host: ?[*:0]const u8,
    source_port: u16,
    source_user: ?[*:0]const u8,
    source_password: ?[*:0]const u8,
    source_database: ?[*:0]const u8,
    source_socket_path: ?[*:0]const u8,
    source_tls_mode: u8,
    slot_name: ?[*:0]const u8,
    publication_names: ?[*:0]const u8,
    proto_version: ?[*:0]const u8,
    dest_host: ?[*:0]const u8,
    dest_port: u16,
    dest_user: ?[*:0]const u8,
    dest_password: ?[*:0]const u8,
    dest_database: ?[*:0]const u8,
    dest_socket_path: ?[*:0]const u8,
    dest_tls_mode: u8,
    source_id: ?[*:0]const u8,
    max_batch_size: u32,
    on_flush: ?PgzrOnFlushFn,
    on_flush_context: ?*anyopaque,
};

/// Opaque handle returned to C callers. The arena owns copies of every
/// caller-provided string, the replication options array, and the flush
/// bridge, so their lifetime is tied to the handle — the caller's config
/// struct and strings may be freed as soon as pgzr_ingestor_new returns.
const IngestorHandle = struct {
    ingestor: Ingestor,
    arena: std.heap.ArenaAllocator,
    io_threaded: std.Io.Threaded,
};

export fn pgzr_ingestor_new(config: *const PgzrIngestConfig) ?*IngestorHandle {
    return ingestorNew(config) catch |err| {
        setError("ingestor init failed: {s}", .{@errorName(err)});
        return null;
    };
}

fn ingestorNew(config: *const PgzrIngestConfig) !*IngestorHandle {
    const allocator = std.heap.page_allocator;

    const handle = try allocator.create(IngestorHandle);
    errdefer allocator.destroy(handle);
    handle.arena = std.heap.ArenaAllocator.init(allocator);
    errdefer handle.arena.deinit();
    handle.io_threaded = .init(allocator, .{});
    errdefer handle.io_threaded.deinit();
    const io = handle.io_threaded.io();
    const aa = handle.arena.allocator();

    const source_conn = try buildConnConfig(
        aa,
        config.source_host,
        config.source_port,
        config.source_user,
        config.source_password,
        config.source_database,
        config.source_socket_path,
        config.source_tls_mode,
    );

    const dest_conn = try buildConnConfig(
        aa,
        config.dest_host,
        config.dest_port,
        config.dest_user,
        config.dest_password,
        config.dest_database,
        config.dest_socket_path,
        config.dest_tls_mode,
    );

    const proto_ver = try dupeCStr(aa, config.proto_version);
    const pub_names = try dupeCStr(aa, config.publication_names);

    // Build options array (up to 3 entries). Arena-allocated: the slice is
    // retained by the Replicator config for the life of the handle (e.g.
    // startReplication on reconnect), so it must not live on this stack frame.
    const options = try aa.alloc([2][]const u8, 3);
    var opt_count: usize = 0;

    if (proto_ver.len > 0) {
        options[opt_count] = .{ "proto_version", proto_ver };
        opt_count += 1;
    }
    if (pub_names.len > 0) {
        options[opt_count] = .{ "publication_names", pub_names };
        opt_count += 1;
    }
    // Always enable logical decoding messages so pg_logical_emit_message
    // metadata reaches the processor.
    options[opt_count] = .{ "messages", "true" };
    opt_count += 1;

    // Set up on_flush callback bridge if provided
    var on_flush: ?types.OnFlushFn = null;
    var on_flush_context: ?*anyopaque = null;

    if (config.on_flush) |c_cb| {
        const bridge = try aa.create(CFlushBridge);
        bridge.* = .{ .c_callback = c_cb, .c_context = config.on_flush_context };
        on_flush = CFlushBridge.zigCallback;
        on_flush_context = @ptrCast(bridge);
    }

    const ingest_config = types.IngestConfig{
        .source = .{
            .conn = source_conn,
            .slot_name = try dupeCStr(aa, config.slot_name),
            .options = options[0..opt_count],
        },
        .dest = dest_conn,
        .source_id = try dupeCStr(aa, config.source_id),
        .max_batch_size = if (config.max_batch_size > 0) config.max_batch_size else 4 * 1024 * 1024,
        .on_flush = on_flush,
        .on_flush_context = on_flush_context,
    };

    handle.ingestor = try Ingestor.init(allocator, io, ingest_config);
    return handle;
}

export fn pgzr_ingestor_run(handle: *IngestorHandle) c_int {
    const ingestor = &handle.ingestor;
    active_ingestor.store(ingestor, .release);
    var prev_int: posix.Sigaction = undefined;
    var prev_term: posix.Sigaction = undefined;
    installSignalHandlers(&prev_int, &prev_term);

    defer {
        active_ingestor.store(null, .release);
        restoreSignalHandlers(&prev_int, &prev_term);
    }

    ingestor.run() catch |err| {
        if (ingestor.isStopRequested()) return 0;
        setError("ingestor run failed: {s}", .{@errorName(err)});
        return -1;
    };
    return 0;
}

export fn pgzr_ingestor_stop(handle: *IngestorHandle) void {
    handle.ingestor.stop();
}

export fn pgzr_ingestor_free(handle: *IngestorHandle) void {
    handle.ingestor.deinit();
    handle.io_threaded.deinit();
    handle.arena.deinit();
    std.heap.page_allocator.destroy(handle);
}

// ── Processor ─────────────────────────────────────────────────────────

pub const PgzrProcessorConfig = extern struct {
    dest_host: ?[*:0]const u8,
    dest_port: u16,
    dest_user: ?[*:0]const u8,
    dest_password: ?[*:0]const u8,
    dest_database: ?[*:0]const u8,
    dest_socket_path: ?[*:0]const u8,
    dest_tls_mode: u8,
    poll_interval_ms: u32,
    metadata_message_prefix: ?[*:0]const u8,
    metadata_table: ?[*:0]const u8,
};

/// Opaque handle returned to C callers. The arena owns copies of every
/// caller-provided string (notably metadata_message_prefix/metadata_table,
/// which are dereferenced on every batch during run), so the caller's
/// config struct and strings may be freed once pgzr_processor_new returns.
const ProcessorHandle = struct {
    processor: Processor,
    arena: std.heap.ArenaAllocator,
    io_threaded: std.Io.Threaded,
};

export fn pgzr_processor_new(config: *const PgzrProcessorConfig) ?*ProcessorHandle {
    return processorNew(config) catch |err| {
        setError("processor init failed: {s}", .{@errorName(err)});
        return null;
    };
}

fn processorNew(config: *const PgzrProcessorConfig) !*ProcessorHandle {
    const allocator = std.heap.page_allocator;

    const handle = try allocator.create(ProcessorHandle);
    errdefer allocator.destroy(handle);
    handle.arena = std.heap.ArenaAllocator.init(allocator);
    errdefer handle.arena.deinit();
    handle.io_threaded = .init(allocator, .{});
    errdefer handle.io_threaded.deinit();
    const io = handle.io_threaded.io();
    const aa = handle.arena.allocator();

    var dest_conn = try buildConnConfig(
        aa,
        config.dest_host,
        config.dest_port,
        config.dest_user,
        config.dest_password,
        config.dest_database,
        config.dest_socket_path,
        config.dest_tls_mode,
    );
    dest_conn.replication = false;

    const msg_prefix = try dupeCStr(aa, config.metadata_message_prefix);
    const meta_table = try dupeCStr(aa, config.metadata_table);

    const processor_config = types.ProcessorConfig{
        .dest = dest_conn,
        .poll_interval_ms = if (config.poll_interval_ms > 0) config.poll_interval_ms else 1_000,
        .metadata_message_prefix = if (msg_prefix.len > 0) msg_prefix else null,
        .metadata_table = if (meta_table.len > 0) meta_table else null,
    };

    handle.processor = try Processor.init(allocator, io, processor_config);
    return handle;
}

export fn pgzr_processor_run(handle: *ProcessorHandle) c_int {
    const processor = &handle.processor;
    active_processor.store(processor, .release);
    var prev_int: posix.Sigaction = undefined;
    var prev_term: posix.Sigaction = undefined;
    installSignalHandlers(&prev_int, &prev_term);

    defer {
        active_processor.store(null, .release);
        restoreSignalHandlers(&prev_int, &prev_term);
    }

    processor.run() catch |err| {
        if (processor.stop_flag.load(.acquire)) return 0;
        setError("processor run failed: {s}", .{@errorName(err)});
        return -1;
    };
    return 0;
}

export fn pgzr_processor_process_one(handle: *ProcessorHandle) c_int {
    const processed = handle.processor.processOne() catch |err| {
        setError("processor process_one failed: {s}", .{@errorName(err)});
        return -1;
    };
    return if (processed) 1 else 0;
}

export fn pgzr_processor_stop(handle: *ProcessorHandle) void {
    handle.processor.stop();
}

export fn pgzr_processor_free(handle: *ProcessorHandle) void {
    handle.processor.deinit();
    handle.io_threaded.deinit();
    handle.arena.deinit();
    std.heap.page_allocator.destroy(handle);
}
