const std = @import("std");
const zon = @import("build.zig.zon");

pub fn build(b: *std.Build) void {
    const version = std.SemanticVersion.parse(zon.version) catch @panic("bad version in build.zig.zon");
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("pgzr", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
    });

    // Example executable
    const example = b.addExecutable(.{
        .name = "basic-example",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/basic.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "pgzr", .module = mod },
            },
        }),
    });

    const run_example = b.addRunArtifact(example);
    if (b.args) |args| {
        run_example.addArgs(args);
    }
    const example_step = b.step("example", "Run the basic example");
    example_step.dependOn(&run_example.step);

    // Ingest example
    const ingest_example = b.addExecutable(.{
        .name = "ingest-example",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/ingest.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "pgzr", .module = mod },
            },
        }),
    });

    const run_ingest_example = b.addRunArtifact(ingest_example);
    if (b.args) |args| {
        run_ingest_example.addArgs(args);
    }
    const ingest_example_step = b.step("ingest-example", "Run the ingest pipeline example");
    ingest_example_step.dependOn(&run_ingest_example.step);

    // Tests
    const mod_tests = b.addTest(.{
        .root_module = mod,
    });
    const run_mod_tests = b.addRunArtifact(mod_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_mod_tests.step);

    // Benchmark
    const bench = b.addExecutable(.{
        .name = "benchmark",
        .root_module = b.createModule(.{
            .root_source_file = b.path("benchmark/bench.zig"),
            .target = target,
            .optimize = .ReleaseFast,
            .imports = &.{
                .{ .name = "pgzr", .module = mod },
            },
        }),
    });

    const run_bench = b.addRunArtifact(bench);
    if (b.args) |args| {
        run_bench.addArgs(args);
    }
    const bench_step = b.step("benchmark", "Run the benchmark");
    bench_step.dependOn(&run_bench.step);

    // Integration tests (requires a running PostgreSQL instance)
    const integration_tests = b.addExecutable(.{
        .name = "integration-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/integration.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "pgzr", .module = mod },
            },
        }),
    });

    const run_integration = b.addRunArtifact(integration_tests);
    const integration_step = b.step("integration-test", "Run integration tests (requires PostgreSQL)");
    integration_step.dependOn(&run_integration.step);

    // Shared library (C ABI for Ruby FFI)
    const lib = b.addLibrary(.{
        .linkage = .dynamic,
        .name = "pgzr",
        .version = version,
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/cabi.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    b.installArtifact(lib);
    const lib_step = b.step("lib", "Build shared library (libpgzr.dylib/so)");
    lib_step.dependOn(b.getInstallStep());

    // Pipeline benchmark (requires a running PostgreSQL instance)
    const pipeline_bench = b.addExecutable(.{
        .name = "pipeline-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("benchmark/pipeline_bench.zig"),
            .target = target,
            .optimize = .ReleaseFast,
            .imports = &.{
                .{ .name = "pgzr", .module = mod },
            },
        }),
    });

    const run_pipeline_bench = b.addRunArtifact(pipeline_bench);
    if (b.args) |args| {
        run_pipeline_bench.addArgs(args);
    }
    const pipeline_bench_step = b.step("pipeline-bench", "Run pipeline benchmark (requires PostgreSQL)");
    pipeline_bench_step.dependOn(&run_pipeline_bench.step);

    // SSL ingest test
    const ssl_ingest_test = b.addExecutable(.{
        .name = "ssl-ingest-test",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/ssl_ingest_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "pgzr", .module = mod },
            },
        }),
    });

    const run_ssl_ingest = b.addRunArtifact(ssl_ingest_test);
    const ssl_ingest_step = b.step("ssl-ingest-test", "Run SSL ingest test (requires PostgreSQL with SSL)");
    ssl_ingest_step.dependOn(&run_ssl_ingest.step);

    // Pipeline integration tests (requires a running PostgreSQL instance)
    const pipeline_tests = b.addExecutable(.{
        .name = "pipeline-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/pipeline.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "pgzr", .module = mod },
            },
        }),
    });

    const run_pipeline = b.addRunArtifact(pipeline_tests);
    const pipeline_step = b.step("pipeline-test", "Run pipeline integration tests (requires PostgreSQL)");
    pipeline_step.dependOn(&run_pipeline.step);
}
