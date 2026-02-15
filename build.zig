const std = @import("std");

pub fn build(b: *std.Build) void {
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
    b.installArtifact(example);

    const run_example = b.addRunArtifact(example);
    run_example.step.dependOn(b.getInstallStep());
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
    b.installArtifact(ingest_example);

    const run_ingest_example = b.addRunArtifact(ingest_example);
    run_ingest_example.step.dependOn(b.getInstallStep());
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
    b.installArtifact(bench);

    const run_bench = b.addRunArtifact(bench);
    run_bench.step.dependOn(b.getInstallStep());
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
    b.installArtifact(integration_tests);

    const run_integration = b.addRunArtifact(integration_tests);
    run_integration.step.dependOn(b.getInstallStep());
    const integration_step = b.step("integration-test", "Run integration tests (requires PostgreSQL)");
    integration_step.dependOn(&run_integration.step);

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
    b.installArtifact(pipeline_tests);

    const run_pipeline = b.addRunArtifact(pipeline_tests);
    run_pipeline.step.dependOn(b.getInstallStep());
    const pipeline_step = b.step("pipeline-test", "Run pipeline integration tests (requires PostgreSQL)");
    pipeline_step.dependOn(&run_pipeline.step);
}
