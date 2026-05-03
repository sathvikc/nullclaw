const std = @import("std");
const std_compat = @import("compat");
const builtin = @import("builtin");

/// Run a probe command with stdio suppressed and treat exit code 0 as success.
pub fn runQuietCommand(argv: []const []const u8) bool {
    if (argv.len == 0) return false;
    if (!canResolveExecutable(argv[0])) return false;

    var child = std_compat.process.Child.init(argv, std.heap.page_allocator);
    child.stderr_behavior = .Ignore;
    child.stdout_behavior = .Ignore;
    child.stdin_behavior = .Ignore;
    child.spawn() catch return false;
    const term = child.wait() catch return false;
    return switch (term) {
        .exited => |code| code == 0,
        else => false,
    };
}

/// Check whether an executable name can be found — either as an absolute
/// path or by searching each PATH directory for an executable file.
fn canResolveExecutable(name: []const u8) bool {
    if (name.len == 0) return false;

    // Absolute or relative path — check directly.
    if (name[0] == '/' or std.mem.indexOfScalar(u8, name, '/') != null) {
        return fileIsExecutable(name);
    }

    // Search PATH.
    const path_env = std_compat.process.getEnvVarOwned(std.heap.page_allocator, "PATH") catch return false;
    defer std.heap.page_allocator.free(path_env);

    var it = std.mem.splitScalar(u8, path_env, if (comptime builtin.os.tag == .windows) ';' else ':');
    while (it.next()) |dir| {
        if (dir.len == 0) continue;
        // Build "dir/name" on the stack when possible.
        var buf: [std.fs.max_path_bytes]u8 = undefined;
        if (dir.len + 1 + name.len >= buf.len) continue;
        @memcpy(buf[0..dir.len], dir);
        buf[dir.len] = '/';
        @memcpy(buf[dir.len + 1 ..][0..name.len], name);
        const full = buf[0 .. dir.len + 1 + name.len];
        if (fileIsExecutable(full)) return true;
    }
    return false;
}

fn fileIsExecutable(path: []const u8) bool {
    std_compat.fs.accessAbsolute(path, .{ .execute = true }) catch return false;
    return true;
}

test "runQuietCommand reports child exit status" {
    const platform = @import("../platform.zig");
    try std.testing.expect(runQuietCommand(&.{ platform.getShell(), platform.getShellFlag(), "exit 0" }));
    try std.testing.expect(!runQuietCommand(&.{ platform.getShell(), platform.getShellFlag(), "exit 9" }));
}

test "runQuietCommand rejects empty argv" {
    try std.testing.expect(!runQuietCommand(&.{}));
}

test "canResolveExecutable finds absolute path" {
    const platform = @import("../platform.zig");
    try std.testing.expect(canResolveExecutable(platform.getShell()));
}

test "canResolveExecutable finds bare name on PATH" {
    const name = if (comptime builtin.os.tag == .windows) "cmd.exe" else "sh";
    try std.testing.expect(canResolveExecutable(name));
}

test "canResolveExecutable rejects empty and nonexistent" {
    try std.testing.expect(!canResolveExecutable(""));
    try std.testing.expect(!canResolveExecutable("__nonexistent_binary_nullclaw_test__"));
}