const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("seb", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
    });

    const exe_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "seb", .module = mod },
        },
    });

    const exe = b.addExecutable(.{
        .name = "seb",
        .root_module = exe_mod,
    });

    b.installArtifact(exe);

    const run_step = b.step("run", "Run the app");

    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);

    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const test_filter = b.option(
        []const u8,
        "test-filter",
        "Only run tests matching this filter",
    );
    const test_filters: []const []const u8 = if (test_filter) |filter|
        &.{filter}
    else
        &.{};

    const mod_tests = b.addTest(.{
        .root_module = mod,
        .filters = test_filters,
    });

    const run_mod_tests = b.addRunArtifact(mod_tests);

    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
        .filters = test_filters,
    });

    const run_exe_tests = b.addRunArtifact(exe_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_exe_tests.step);

    const exe_check = b.addExecutable(.{
        .name = "seb",
        .root_module = exe_mod,
    });

    const check = b.step("check", "Check if seb compiles");
    check.dependOn(&exe_check.step);
}
