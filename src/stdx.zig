const std = @import("std");
const Io = std.Io;

pub fn openFile(io: Io, path: []const u8) !std.Io.File {
    return if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
}

pub const Clock = struct {
    io: std.Io = undefined,

    pub fn now(self: Clock) u64 {
        return @intCast(std.Io.Timestamp.now(self.io, .real).toNanoseconds());
    }
};
