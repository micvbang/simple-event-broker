const std = @import("std");
const stdx = @import("stdx.zig");
const PositionalFileReader = @import("PositionalFileReader.zig");
const Batch = @import("Batch.zig");
const assert = std.debug.assert;
const Pool = @import("Pool.zig");
const testing = @import("testing.zig");

pub const ParserError = error{
    RecordNotFound,
    FileHasNoRecords,
    EndOfStream,
    FileTooSmall,
    OffsetsNotMonotonicallyIncreasing,
    OffsetOutOfBounds,
    StartOffsetOutOfBounds,
    EndOffsetOutOfBounds,
    StartIndexLargerThanEndIndex,
    OffsetsMustStartAtZero,
    BatchSizesTooSmall,
    BatchDataTooSmall,
    ShortRead,
    InvalidMagicBytes,
    InvalidVersion,
};

pub fn Parser(comptime Input: type) type {
    return struct {
        const Self = @This();

        header: Header,
        fileSize: usize,
        maxSize: usize,
        input: Input,

        pub fn init(gpa: std.mem.Allocator, input: Input, file_size: usize) !Self {
            const header = try Header.parse(input, gpa, file_size);

            var parser = Self{
                .header = header,
                .fileSize = file_size,
                .maxSize = 0,
                .input = input,
            };

            var max_size: usize = 0;
            for (0..header.num_records) |record_id| {
                const rid: u32 = @intCast(record_id);
                max_size = @max(parser.sizeOf(rid, rid + 1), max_size);
            }
            parser.maxSize = max_size;

            return parser;
        }

        pub fn deinit(self: Self) void {
            self.header.deinit();
        }

        pub fn sizeOf(self: Self, start_index: u32, end_index: u32) usize {
            // TODO: verify end_index isn't beyond num_records
            // if (start_index >= self.header.num_records) return ParserError.StartOffsetOutOfBounds;
            // if (end_index > self.header.num_records) return ParserError.EndOffsetOutOfBounds;
            // if (start_index > end_index) return ParserError.StartIndexLargerThanEndIndex;

            const start_offset = self.header.record_offsets[start_index];
            const end_offset = if (end_index < self.header.num_records)
                @as(usize, self.header.record_offsets[end_index])
            else
                self.fileSize - self.header.size();

            return end_offset - start_offset;
        }

        pub fn record(self: Self, batch: *Batch, record_id: u32) !void {
            if (record_id >= self.header.num_records) return ParserError.RecordNotFound;
            if (batch.sizes.len < 1) return ParserError.BatchSizesTooSmall;

            const record_size = self.sizeOf(record_id, record_id + 1);
            if (record_size > batch.data.len) return ParserError.BatchDataTooSmall;

            const record_offset = self.header.record_offsets[record_id];

            const read_size = try self.input.readAt(batch.data[0..record_size], self.header.size() + record_offset);
            if (read_size != record_size) {
                return ParserError.ShortRead;
            }
            batch.sizes[0] = @intCast(read_size);

            batch.sizes = batch.sizes[0..1];
            batch.data = batch.data[0..read_size];
        }

        // NOTE: reads records in the range [index_start; index_end[
        pub fn records(self: Self, batch: *Batch, index_start: u32, index_end: u32) !void {
            if (index_start >= self.header.num_records) return ParserError.StartOffsetOutOfBounds;
            if (index_end > self.header.num_records) return ParserError.EndOffsetOutOfBounds;
            if (index_start > index_end) return ParserError.StartIndexLargerThanEndIndex;

            const records_num = index_end - index_start;
            if (records_num > batch.sizes.len) return ParserError.BatchSizesTooSmall;

            const data_size = self.sizeOf(index_start, index_end);
            if (data_size > batch.data.len) return ParserError.BatchDataTooSmall;

            const offset_start = self.header.record_offsets[index_start];

            const read_size = try self.input.readAt(batch.data[0..data_size], self.header.size() + offset_start);
            if (read_size != data_size) {
                return ParserError.ShortRead;
            }

            for (0..records_num) |i| {
                // TODO: compute size instead of calling sizeOf() records_num times
                batch.sizes[i] = @intCast(self.sizeOf(
                    index_start + @as(u32, @intCast(i)),
                    index_start + @as(u32, @intCast(i)) + 1,
                ));
            }
            batch.data = batch.data[0..read_size];
            batch.sizes = batch.sizes[0..records_num];
        }
    };
}

pub const Header = struct {
    pub const header_bytes = 32;
    pub const record_offset_size = 4;
    const expected_magic_bytes = "seb!";
    const expected_version = 1;

    // static header
    magic_bytes: [4]u8,
    version: i16,
    unix_epoch_us: i64,
    num_records: u32,
    reserved: [14]u8,

    // dynamic header
    record_offsets: []u32,
    allocator: std.mem.Allocator,

    fn parse(input: anytype, allocator: std.mem.Allocator, file_size: usize) !Header {
        var header_buf: [header_bytes]u8 = undefined;
        const read = try input.readAt(&header_buf, 0);
        if (read < header_bytes) return ParserError.EndOfStream;

        // parse static fields
        const magic_bytes = header_buf[0..4].*;
        const version = std.mem.readInt(i16, header_buf[4..6], .little);
        const unix_epoch_us = std.mem.readInt(i64, header_buf[6..14], .little);
        const num_records = std.mem.readInt(u32, header_buf[14..18], .little);
        const reserved = header_buf[18..32].*;

        if (num_records == 0) return ParserError.FileHasNoRecords;
        if (!std.mem.eql(u8, magic_bytes[0..], Header.expected_magic_bytes)) return ParserError.InvalidMagicBytes;
        if (Header.expected_version != version) return ParserError.InvalidVersion;

        // parse record offsets
        const record_offsets_size = @as(usize, num_records) * record_offset_size;
        const header_end_offset = header_bytes + record_offsets_size;
        const file_size_min = header_end_offset + @as(usize, num_records);
        if (file_size < file_size_min) return ParserError.FileTooSmall; // assumes at least 1 byte per record

        var record_offsets_buf = try allocator.alloc(u8, record_offsets_size);
        defer allocator.free(record_offsets_buf);

        const read_size = try input.readAt(record_offsets_buf, header_bytes);
        if (read_size < record_offsets_size) return ParserError.EndOfStream;

        var record_offsets: []u32 = try allocator.alloc(u32, num_records);
        for (0..num_records) |i| {
            record_offsets[i] = std.mem.readInt(u32, record_offsets_buf[i * 4 ..][0..4], .little);
        }

        // validate record_offsets
        {
            // offset must start at 0
            if (record_offsets[0] != 0) return ParserError.OffsetsMustStartAtZero;

            const record_offsets_max_size = file_size - header_end_offset;

            var previous_offset: u32 = undefined;
            for (0.., record_offsets) |i, offset| {
                // Validate that offset doesn't point beyond file length
                if (offset >= record_offsets_max_size) return ParserError.OffsetOutOfBounds;

                // Verify that offset is monotonically increasing
                if (i > 0 and previous_offset >= offset) return ParserError.OffsetsNotMonotonicallyIncreasing;
                previous_offset = offset;
            }
        }

        return .{
            .magic_bytes = magic_bytes,
            .version = version,
            .unix_epoch_us = unix_epoch_us,
            .num_records = num_records,
            .reserved = reserved,
            .record_offsets = record_offsets,
            .allocator = allocator,
        };
    }

    fn deinit(self: Header) void {
        self.allocator.free(self.record_offsets);
    }

    fn size(self: Header) usize {
        return header_bytes + self.num_records * record_offset_size;
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

fn unix_epoch_ns_static_nonsense_value() u64 {
    return 41231;
}

// TODO: declare now to be a type with now() u64 on it.
pub fn Write(allocator: std.mem.Allocator, output: *std.Io.Writer, batch: Batch, now: anytype) !void {
    const header_size = Header.header_bytes + batch.sizes.len * Header.record_offset_size;
    const buf: []u8 = try allocator.alloc(u8, header_size);
    defer allocator.free(buf);

    var index: u32 = 0;
    for (0..batch.sizes.len) |i| {
        // NOTE: reusing buffer here; safe?
        const index_cur = batch.sizes[i];
        batch.sizes[i] = index;
        index += index_cur;
    }

    // Write header to buf, making as few writes to output as we can
    var headerWriter = std.Io.Writer.fixed(buf);
    try headerWriter.writeSliceEndian(u8, Header.expected_magic_bytes[0..], .little);
    try headerWriter.writeInt(u16, Header.expected_version, .little);
    try headerWriter.writeInt(u64, now.now(), .little);
    try headerWriter.writeInt(u32, @as(u32, @intCast(batch.sizes.len)), .little);
    try headerWriter.writeSliceEndian(u8, &([_]u8{0} ** 14), .little);
    try headerWriter.writeSliceEndian(u32, batch.sizes, .little);

    // Write buf and data to output
    try output.writeAll(buf);
    try output.writeSliceEndian(u8, batch.data, .little);
}

test "can write and read record batch" {
    const records_num = 8;
    const records_bytes = 32;
    const file_size = Header.header_bytes + records_num * (Header.record_offset_size + records_bytes);

    var buf: [file_size]u8 = undefined;
    var memory_writer = std.Io.Writer.fixed(&buf);

    const batch_write = try testing.randomBatch(std.testing.allocator, records_num, records_bytes);
    defer batch_write.deinit();

    var batch_read = try Batch.init(std.testing.allocator, 1024 * 1024 * 10, 32 * 1024);
    defer batch_read.deinit();

    try Write(std.testing.allocator, &memory_writer, batch_write, testing.NowFactory(std.testing.io));

    const memory_reader = testing.PositionalBufferReader{ .buf = &buf };
    const parser = try Parser(@TypeOf(memory_reader)).init(std.testing.allocator, memory_reader, file_size);
    defer parser.deinit();

    try parser.records(&batch_read, 0, records_num);

    assert(std.mem.eql(u8, batch_write.data, batch_read.data));
}

pub fn openPositionalFile(io: std.Io, path: []const u8) !PositionalFileReader {
    const f = try stdx.openFile(io, path);
    return PositionalFileReader{ .io = io, .file = f };
}
