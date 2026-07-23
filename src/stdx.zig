const std = @import("std");
const Io = std.Io;
const assert = std.debug.assert;

pub const KiB = 1 << 10;
pub const MiB = 1 << 20;
pub const GiB = 1 << 30;
pub const TiB = 1 << 40;
pub const PiB = 1 << 50;

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

pub fn ClockNow(comptime T: type) type {
    return struct {
        now: fn (T) u64,
    };
}

pub const BufferReader = struct {
    const Self = @This();

    buf: []const u8,

    pub fn readAt(self: Self, dest: []u8, offset: usize) !usize {
        if (offset > self.buf.len) return error.OutOfBounds;

        const read_size = @min(dest.len, self.buf.len - offset);
        for (0..read_size) |i| {
            dest[i] = self.buf[offset + i];
        }

        return read_size;
    }

    pub fn close(_: Self) void {}
};

test "BufferReader can read entire buffer" {
    const input = "0123456789";
    const rdr = BufferReader{ .buf = input };
    var dst: [input.len]u8 = undefined;

    const read_size = try rdr.readAt(dst[0..], 0);
    assert(std.mem.eql(u8, dst[0..read_size], input));
}

test "BufferReader can read one byte at the time" {
    const input = "0123456789";
    const rdr = BufferReader{ .buf = input };
    var dst: [input.len]u8 = undefined;

    for (0..input.len) |offset| {
        assert(try rdr.readAt(dst[offset .. offset + 1], offset) == 1);
    }
    assert(std.mem.eql(u8, dst[0..], input[0..]));
}

test "BufferReader can't read beyond buffer" {
    const input = "0123456789";
    const rdr = BufferReader{ .buf = input };
    var dst: [32]u8 = undefined;

    const read_size = try rdr.readAt(dst[0..], 0);
    assert(std.mem.eql(u8, dst[0..read_size], input));
}

pub const BufferWriter = struct {
    const Self = @This();

    buf: []u8,
    index: usize = 0,

    pub fn write(self: *Self, src: []const u8) anyerror!usize {
        const write_size = @min(src.len, self.buf.len - self.index);
        for (0..write_size) |i| {
            self.buf[self.index + i] = src[i];
        }

        self.index += write_size;

        return write_size;
    }

    pub fn close(_: *Self) void {}
};

test "BufferWriter can write full buffer at once" {
    var buf: [32]u8 = undefined;
    var wtr = BufferWriter{ .buf = buf[0..] };

    const write_size = try wtr.write("01234567890123456789012345678901");
    assert(write_size == buf.len);
}

test "BufferWriter can write one byte at the time" {
    var buf: [32]u8 = undefined;
    var wtr = BufferWriter{ .buf = buf[0..] };

    for (0..buf.len) |_| {
        const write_size = try wtr.write("1");
        assert(write_size == 1);
    }
}

test "BufferWriter can't write beyond buffer" {
    var buf: [1]u8 = undefined;
    var wtr = BufferWriter{ .buf = buf[0..] };

    assert(try wtr.write("0") == 1);
    assert(try wtr.write("1") == 0);
}
