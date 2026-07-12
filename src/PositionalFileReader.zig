const std = @import("std");

const Self = @This();

io: std.Io,
file: std.Io.File,

pub fn readAt(self: Self, dest: []u8, offset: u64) !usize {
    return self.file.readPositionalAll(self.io, dest, offset);
}

pub fn length(self: Self) !usize {
    return self.file.length(self.io);
}

pub fn close(self: Self) void {
    return self.file.close(self.io);
}
