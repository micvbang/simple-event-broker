const std = @import("std");
const assert = std.debug.assert;

const stdx = @import("stdx.zig");
const testing = @import("testing.zig");
const storage = @import("storage.zig");

pub fn Parse(reader: storage.Reader, scratch: []u8, offsets: []u64) ![]u64 {
    const header = try Header.parse(reader);

    if (scratch.len < header.num_offsets * Header.record_size) return error.BufferTooSmall;
    if (!std.mem.eql(u8, header.magic_bytes[0..], Header.expected_magic_bytes[0..])) return error.InvalidMagicBytes;
    if (header.version != Header.expected_version) return error.InvalidVersion;

    const read_size = try reader.readAt(scratch, Header.size);
    if (read_size < header.num_offsets * Header.record_size) return error.EndOfStream;

    for (0..header.num_offsets) |i| {
        offsets[i] = std.mem.readInt(u64, scratch[i * Header.record_size ..][0..8], .little);
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

pub fn Write(scratch: []u8, output: storage.Writer, offsets: []const u64, now: anytype, fns: ClockNow(@TypeOf(now))) !u64 {
    assert(scratch.len >= offsets_file_size(offsets.len));

    // Write correct format into output_buf, making as few writes to output as we can
    const output_buf = scratch[0..offsets_file_size(offsets.len)];

    var mem_writer = std.Io.Writer.fixed(output_buf);
    try mem_writer.writeSliceEndian(u8, Header.expected_magic_bytes[0..], .little);
    try mem_writer.writeInt(u16, Header.expected_version, .little);
    try mem_writer.writeInt(u64, fns.now(now), .little);
    try mem_writer.writeInt(u32, @as(u32, @intCast(offsets.len)), .little);
    try mem_writer.writeSliceEndian(u8, &([_]u8{0} ** 14), .little);
    try mem_writer.writeSliceEndian(u64, offsets, .little);

    assert(mem_writer.unusedCapacityLen() == 0);

    // Write buf and data to output in a single write
    if (try output.write(output_buf) < output_buf.len) return error.ShortWrite;

    return output_buf.len;
}

test "can read and write offsets file" {
    const io = std.testing.io;
    const gpa = std.testing.allocator;
    const clock = stdx.Clock{ .io = io };

    var offsets_in = [_]u64{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

    const strg_helper = try testing.MemoryStorageHelper.init(gpa, io, stdx.MiB);
    defer strg_helper.deinit();

    var wtr = try strg_helper.offsets_file_writer("topic", 0);
    defer wtr.close();

    var write_buf_scratch: [offsets_file_size(offsets_in.len)]u8 = undefined;
    const write_size = try Write(&write_buf_scratch, wtr, &offsets_in, clock, .{ .now = stdx.Clock.now });
    assert(write_size == offsets_file_size(offsets_in.len));

    const rdr = try strg_helper.offsets_file_reader("topic", 0);
    defer rdr.close();

    var output_buf: [offsets_file_size(offsets_in.len)]u8 = undefined;

    var offsets_out_buf: [128]u64 = undefined;
    const offsets_out = try Parse(rdr, &output_buf, &offsets_out_buf);
    assert(std.mem.eql(u64, &offsets_in, offsets_out));
}
