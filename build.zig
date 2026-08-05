const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const websocket_dep = b.dependency("websocket", .{
        .target = target,
        .optimize = optimize,
    });
    const websocket_module = websocket_dep.module("websocket");

    const channel_module = b.addModule("channel", .{
        .root_source_file = b.path("src/channel.zig"),
        .target = target,
        .optimize = optimize,
    });
    channel_module.addImport("websocket", websocket_module);

    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "channel_zig",
        .root_module = channel_module,
    });
    b.installArtifact(lib);

    const unit_test_module = b.createModule(.{
        .root_source_file = b.path("tests/unit/process_test.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "channel", .module = channel_module },
        },
    });
    const unit_tests = b.addTest(.{
        .name = "test-unit",
        .root_module = unit_test_module,
    });
    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_unit_step = b.step("test-unit", "Run unit tests");
    test_unit_step.dependOn(&run_unit_tests.step);

    const integration_test_module = b.createModule(.{
        .root_source_file = b.path("tests/integration/ts_server_test.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "channel", .module = channel_module },
        },
    });
    const integration_tests = b.addTest(.{
        .name = "test-integration",
        .root_module = integration_test_module,
    });
    const run_integration_tests = b.addRunArtifact(integration_tests);
    const test_integration_step = b.step("test-integration", "Run integration tests (requires TS fixture server)");
    test_integration_step.dependOn(&run_integration_tests.step);

    const bench_module = b.createModule(.{
        .root_source_file = b.path("bench/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "channel", .module = channel_module },
            .{ .name = "websocket", .module = websocket_module },
        },
    });
    const bench_exe = b.addExecutable(.{
        .name = "channel-zig-benchmark",
        .root_module = bench_module,
    });
    b.installArtifact(bench_exe);

    const run_bench = b.addRunArtifact(bench_exe);
    if (b.args) |args| run_bench.addArgs(args);
    const benchmark_step = b.step("benchmark", "Run benchmarks");
    benchmark_step.dependOn(&run_bench.step);

    const test_step = b.step("test", "Run local tests");
    test_step.dependOn(&run_unit_tests.step);
}
