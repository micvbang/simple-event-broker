const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("stdx.zig");
const testing = @import("testing.zig");
const storage = @import("storage.zig");

pub fn Parse(reader: storage.Reader, buf: []u8, offsets: []u64) ![]u64 {
    const header = try Header.parse(reader);

    if (buf.len < header.num_offsets * Header.record_size - Header.size) return error.BufferTooSmall;
    if (!std.mem.eql(u8, header.magic_bytes[0..], Header.expected_magic_bytes[0..])) return error.InvalidMagicBytes;
    if (header.version != 1) return error.InvalidVersion;

    const read_size = try reader.readAt(buf, Header.size);
    if (read_size < header.num_offsets * Header.record_size) return error.EndOfStream;

    for (0..header.num_offsets) |i| {
        offsets[i] = std.mem.readInt(u64, buf[i * Header.record_size ..][0..8], .little);
    }

    return offsets[0..header.num_offsets];
}

const Header = struct {
    const size = 32;
    const record_size = 8;
    const expected_magic_bytes = "seb@";
    const expected_version = 1;

    magic_bytes: [4]u8,
    version: i16,
    unix_epoch_us: i64,
    num_offsets: u32,
    reserved: [14]u8,

    fn parse(reader: storage.Reader) !Header {
        var header_buf: [Header.size]u8 = undefined;
        const read_size = try reader.readAt(&header_buf, 0);
        if (read_size < Header.size) return error.EndOfStream;

        const magic_bytes = header_buf[0..4].*;
        const version = std.mem.readInt(i16, header_buf[4..6], .little);
        const unix_epoch_us = std.mem.readInt(i64, header_buf[6..14], .little);
        const num_offsets = std.mem.readInt(u32, header_buf[14..18], .little);
        const reserved = header_buf[18..32].*;

        return Header{
            .magic_bytes = magic_bytes,
            .version = version,
            .unix_epoch_us = unix_epoch_us,
            .num_offsets = num_offsets,
            .reserved = reserved,
        };
    }
};

fn ClockNow(comptime T: type) type {
    return struct {
        now: fn (T) u64,
    };
}

pub fn offsets_file_size(offsets_num: usize) usize {
    return Header.size + offsets_num * Header.record_size;
}

pub fn Write(buf: []u8, output: *std.Io.Writer, offsets: []u64, now: anytype, fns: ClockNow(@TypeOf(now))) !u64 {
    assert(buf.len >= offsets_file_size(offsets.len));

    const file = buf[0..offsets_file_size(offsets.len)];

    // Write correct format into buf, making as few writes to output as we can
    var mem_writer = std.Io.Writer.fixed(file);
    try mem_writer.writeSliceEndian(u8, Header.expected_magic_bytes[0..], .little);
    try mem_writer.writeInt(u16, Header.expected_version, .little);
    try mem_writer.writeInt(u64, fns.now(now), .little);
    try mem_writer.writeInt(u32, @as(u32, @intCast(offsets.len)), .little);
    try mem_writer.writeSliceEndian(u8, &([_]u8{0} ** 14), .little);
    try mem_writer.writeSliceEndian(u64, offsets, .little);

    assert(mem_writer.unusedCapacityLen() == 0);

    // Write buf and data to output in a single write
    try output.writeAll(file);
    return file.len;
}

test "can read and write offsets file" {
    const io = std.testing.io;

    var offsets_in_buf = [_]u64{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    var offsets_out_buf: [128]u64 = undefined;

    const clock = stdx.Clock{ .io = io };

    var buf_file: [offsets_file_size(offsets_in_buf.len)]u8 = undefined;
    var memory_writer = std.Io.Writer.fixed(buf_file[0..]);

    var buf_scratch: [offsets_file_size(offsets_in_buf.len)]u8 = undefined;
    const write_size = try Write(buf_scratch[0..], &memory_writer, offsets_in_buf[0..], clock, .{ .now = stdx.Clock.now });

    var rdr = stdx.ReaderAdapter(stdx.BufferReader, false){
        .allocator = {},
        .reader = .{
            .buf = buf_file[0..write_size],
        },
    };
    const reader = storage.Reader{
        .context = &rdr,
        .vtable = &.{
            .readAt = @TypeOf(rdr).readAtAdapter,
            .close = @TypeOf(rdr).closeAdapter,
        },
    };

    var buf_output: [offsets_file_size(offsets_in_buf.len)]u8 = undefined;
    const offsets_out = try Parse(reader, buf_output[0..], offsets_out_buf[0..]);
    assert(std.mem.eql(u64, offsets_in_buf[0..], offsets_out));
}
