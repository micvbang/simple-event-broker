const std = @import("std");
const assert = std.debug.assert;

const PositionalFileReader = struct {
    io: std.Io,
    file: std.Io.File,

    pub fn readAt(self: PositionalFileReader, dest: []u8, offset: u64) !usize {
        return self.file.readPositionalAll(self.io, dest, offset);
    }
};

pub const RecordBatch = struct {
    header: Header,

    pub fn init(input: anytype, gpa: std.mem.Allocator) !RecordBatch {
        const header = try Header.parse(input, gpa);

        return RecordBatch{
            .header = header,
        };
    }

    pub fn deinit(self: RecordBatch) void {
        self.header.deinit();
    }

    pub fn size(self: RecordBatch, record_id: u32) !usize {
        const record_offset = self.header.record_index[record_id];
        return @as(usize, @intCast(self.header.record_index[record_id + 1] - record_offset));
    }

    pub fn read(self: RecordBatch, input: anytype, record_id: u32, buf: []u8) !usize {
        if (record_id >= self.header.num_records) return error.RecordNotFound;
        if (buf.len < try self.size(record_id)) return error.BufferTooSmall;

        const record_offset = self.header.record_index[record_id];
        // BUG: this goes one beyond recordIndex when reading the last element; with Seb's
        // current format it needs the file length to compute the length of the last element.
        const record_size = self.header.record_index[record_id + 1] - record_offset;
        const read_size = try input.readAt(buf[0..record_size], self.header.size() + record_offset);

        if (@as(usize, @intCast(record_size)) != read_size) {
            return error.ShortRead;
        }

        return read_size;
    }
};

const Header = struct {
    const header_bytes = 32;
    const header_index_entry_bytes = 4;
    const record_index_max_entries = 1024 * 1024;

    // static header
    magic_bytes: [4]u8,
    version: i16,
    unix_epoch_us: i64,
    num_records: u32,
    reserved: [14]u8,

    // dynamic header
    record_index: []u32,
    allocator: std.mem.Allocator,

    fn parse(input: anytype, allocator: std.mem.Allocator) !Header {
        var header_buf: [header_bytes]u8 = undefined;
        const read = try input.readAt(&header_buf, 0);
        if (read < header_bytes) return error.EndOfStream;

        // parse static part
        const magic_bytes = header_buf[0..4].*;
        const version = std.mem.readInt(i16, header_buf[4..6], .little);
        const unix_epoch_us = std.mem.readInt(i64, header_buf[6..14], .little);
        const num_records = std.mem.readInt(u32, header_buf[14..18], .little);
        const reserved = header_buf[18..32].*;

        // parse dynamic part (record index)
        var index_buf = try allocator.alloc(u8, num_records * 4);
        defer allocator.free(index_buf);

        const read_size = try input.readAt(index_buf, header_bytes);
        if (read_size < @as(usize, @intCast(num_records * 4))) {
            return error.EndOfStream;
        }

        var record_index: []u32 = try allocator.alloc(u32, num_records);
        for (0..num_records) |i| {
            record_index[i] = std.mem.readInt(u32, index_buf[i * 4 ..][0..4], .little);
        }

        return .{
            .magic_bytes = magic_bytes,
            .version = version,
            .unix_epoch_us = unix_epoch_us,
            .num_records = num_records,
            .reserved = reserved,
            .record_index = record_index,
            .allocator = allocator,
        };
    }

    fn deinit(self: Header) void {
        self.allocator.free(self.record_index);
    }

    fn size(self: Header) usize {
        return header_bytes + self.num_records * header_index_entry_bytes;
    }

    pub fn format(self: Header, writer: *std.Io.Writer) !void {
        try writer.print("Header{{.magic_bytes=\"{s}\", .version={d}, .unix_epoch_us={d}, .num_records={d}, .reserved={any}}}", .{
            &self.magic_bytes,
            self.version,
            self.unix_epoch_us,
            self.num_records,
            self.reserved,
        });
    }
};

pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.iterateAllocator(init.minimal.args, init.gpa);
    defer args.deinit();

    _ = args.next(); // program path

    const records_path = args.next() orelse "/Users/micvbang/projects/simple-event-broker/zig/src/000000000000.record_batch";

    const f = try std.Io.Dir.openFileAbsolute(init.io, records_path, .{});
    defer f.close(init.io);
    const f_positional = PositionalFileReader{ .file = f, .io = init.io };

    const file_length = try f.length(init.io);
    _ = file_length;
    const records = try RecordBatch.init(f_positional, init.gpa);
    defer records.deinit();

    const buf = try init.gpa.alloc(u8, try records.size(10));
    defer init.gpa.free(buf);

    const size = try records.read(f_positional, 10, buf[0..]);

    std.debug.print("record: {s}", .{buf[0..size]});
}
