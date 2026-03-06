const std = @import("std");
const Ingestor = @import("ingest.zig").Ingestor;
const Processor = @import("processor.zig").Processor;
const types = @import("types.zig");
const Lsn = @import("lsn.zig").Lsn;
const query_mod = @import("query.zig");

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

fn signalHandler(_: c_int) callconv(.c) void {
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
    posix.sigaction(posix.SIG.INT, &sa, prev_int);
    posix.sigaction(posix.SIG.TERM, &sa, prev_term);
}

fn restoreSignalHandlers(
    prev_int: *const posix.Sigaction,
    prev_term: *const posix.Sigaction,
) void {
    posix.sigaction(posix.SIG.INT, prev_int, null);
    posix.sigaction(posix.SIG.TERM, prev_term, null);
}

// ── Helpers ───────────────────────────────────────────────────────────

fn sliceFromCStr(ptr: ?[*:0]const u8) []const u8 {
    const p = ptr orelse return "";
    return std.mem.span(p);
}

fn buildConnConfig(
    host: ?[*:0]const u8,
    port: u16,
    user: ?[*:0]const u8,
    password: ?[*:0]const u8,
    database: ?[*:0]const u8,
    socket_path: ?[*:0]const u8,
    tls_mode: u8,
) types.ConnConfig {
    return .{
        .host = sliceFromCStr(host),
        .port = port,
        .user = sliceFromCStr(user),
        .password = sliceFromCStr(password),
        .database = sliceFromCStr(database),
        .socket_path = if (socket_path) |p| std.mem.span(p) else null,
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

export fn pgzr_ingestor_new(config: *const PgzrIngestConfig) ?*Ingestor {
    const allocator = std.heap.page_allocator;

    const source_conn = buildConnConfig(
        config.source_host,
        config.source_port,
        config.source_user,
        config.source_password,
        config.source_database,
        config.source_socket_path,
        config.source_tls_mode,
    );

    const dest_conn = buildConnConfig(
        config.dest_host,
        config.dest_port,
        config.dest_user,
        config.dest_password,
        config.dest_database,
        config.dest_socket_path,
        config.dest_tls_mode,
    );

    const proto_ver = sliceFromCStr(config.proto_version);
    const pub_names = sliceFromCStr(config.publication_names);

    // Build options array (up to 3 entries)
    var options: [3][2][]const u8 = undefined;
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
    var flush_bridge: ?*CFlushBridge = null;

    if (config.on_flush) |c_cb| {
        const bridge = allocator.create(CFlushBridge) catch {
            setErrorStr("out of memory");
            return null;
        };
        bridge.* = .{ .c_callback = c_cb, .c_context = config.on_flush_context };
        on_flush = CFlushBridge.zigCallback;
        on_flush_context = @ptrCast(bridge);
        flush_bridge = bridge;
    }

    const ingest_config = types.IngestConfig{
        .source = .{
            .conn = source_conn,
            .slot_name = sliceFromCStr(config.slot_name),
            .options = options[0..opt_count],
        },
        .dest = dest_conn,
        .source_id = sliceFromCStr(config.source_id),
        .max_batch_size = if (config.max_batch_size > 0) config.max_batch_size else 4 * 1024 * 1024,
        .on_flush = on_flush,
        .on_flush_context = on_flush_context,
    };

    const ingestor = allocator.create(Ingestor) catch {
        setErrorStr("out of memory");
        return null;
    };

    ingestor.* = Ingestor.init(allocator, ingest_config) catch |err| {
        setError("ingestor init failed: {s}", .{@errorName(err)});
        allocator.destroy(ingestor);
        if (flush_bridge) |b| allocator.destroy(b);
        return null;
    };

    return ingestor;
}

export fn pgzr_ingestor_run(ingestor: *Ingestor) c_int {
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

export fn pgzr_ingestor_stop(ingestor: *Ingestor) void {
    ingestor.stop();
}

export fn pgzr_ingestor_free(ingestor: *Ingestor) void {
    // Free the C flush bridge if one was allocated
    if (ingestor.config.on_flush != null) {
        if (ingestor.config.on_flush_context) |ctx| {
            const bridge: *CFlushBridge = @ptrCast(@alignCast(ctx));
            std.heap.page_allocator.destroy(bridge);
        }
    }
    ingestor.deinit();
    std.heap.page_allocator.destroy(ingestor);
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
    source_id: ?[*:0]const u8,
    poll_interval_ms: u32,
    metadata_message_prefix: ?[*:0]const u8,
    metadata_table: ?[*:0]const u8,
};

export fn pgzr_processor_new(config: *const PgzrProcessorConfig) ?*Processor {
    const allocator = std.heap.page_allocator;

    var dest_conn = buildConnConfig(
        config.dest_host,
        config.dest_port,
        config.dest_user,
        config.dest_password,
        config.dest_database,
        config.dest_socket_path,
        config.dest_tls_mode,
    );
    dest_conn.replication = false;

    const msg_prefix = sliceFromCStr(config.metadata_message_prefix);
    const meta_table = sliceFromCStr(config.metadata_table);

    const processor_config = types.ProcessorConfig{
        .dest = dest_conn,
        .source_id = sliceFromCStr(config.source_id),
        .poll_interval_ms = if (config.poll_interval_ms > 0) config.poll_interval_ms else 1_000,
        .metadata_message_prefix = if (msg_prefix.len > 0) msg_prefix else null,
        .metadata_table = if (meta_table.len > 0) meta_table else null,
    };

    const processor = allocator.create(Processor) catch {
        setErrorStr("out of memory");
        return null;
    };

    processor.* = Processor.init(allocator, processor_config) catch |err| {
        setError("processor init failed: {s}", .{@errorName(err)});
        allocator.destroy(processor);
        return null;
    };

    return processor;
}

export fn pgzr_processor_run(processor: *Processor) c_int {
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

export fn pgzr_processor_process_one(processor: *Processor) c_int {
    const processed = processor.processOne() catch |err| {
        setError("processor process_one failed: {s}", .{@errorName(err)});
        return -1;
    };
    return if (processed) 1 else 0;
}

export fn pgzr_processor_stop(processor: *Processor) void {
    processor.stop();
}

export fn pgzr_processor_free(processor: *Processor) void {
    processor.deinit();
    std.heap.page_allocator.destroy(processor);
}
