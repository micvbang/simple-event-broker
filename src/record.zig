const std = @import("std");
const stdx = @import("stdx.zig");
const PositionalFileReader = @import("PositionalFileReader.zig");
const Batch = @import("Batch.zig");
const assert = std.debug.assert;
const pool = @import("pool.zig");
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
        file_size: usize,
        input: Input,

        // NOTE: Parser borrows bufs during its entire lifetime
        pub fn init(bufs: *Buffers, input: Input, file_size: usize) !Self {
            const header = try Header.parse(bufs, input, file_size);

            return Self{
                .header = header,
                .file_size = file_size,
                .input = input,
            };
        }

        pub fn deinit(_: Self) void {}

        pub fn sizeOf(self: Self, index_start: u32, index_end: u32) !usize {
            if (index_start >= self.header.num_records) return ParserError.StartOffsetOutOfBounds;
            if (index_end > self.header.num_records) return ParserError.EndOffsetOutOfBounds;
            if (index_start >= index_end) return ParserError.StartIndexLargerThanEndIndex;

            const offset_start = self.header.record_offsets[index_start];
            const offset_end = if (index_end < self.header.num_records)
                @as(usize, self.header.record_offsets[index_end])
            else
                self.file_size - self.header.size();

            return offset_end - offset_start;
        }

        pub fn record(self: Self, batch: *Batch, record_id: u32) !void {
            if (record_id >= self.header.num_records) return ParserError.RecordNotFound;
            if (batch.offsets_full.len < 1) return ParserError.BatchSizesTooSmall;

            const record_size = try self.sizeOf(record_id, record_id + 1);
            if (record_size > batch.data_full.len) return ParserError.BatchDataTooSmall;

            const record_offset = self.header.record_offsets[record_id];

            const read_size = try self.input.readAt(batch.data_full[0..record_size], self.header.size() + record_offset);
            if (read_size != record_size) {
                return ParserError.ShortRead;
            }
            batch.offsets_full[0] = 0;

            // reslice to expose data to caller
            batch.offsets = batch.offsets_full[0..1];
            batch.data = batch.data_full[0..read_size];
        }

        // NOTE: reads records in the range [index_start; index_end[
        pub fn records(self: Self, batch: *Batch, index_start: u32, index_end: u32) !void {
            if (index_start >= self.header.num_records) return ParserError.StartOffsetOutOfBounds;
            if (index_end > self.header.num_records) return ParserError.EndOffsetOutOfBounds;
            if (index_start >= index_end) return ParserError.StartIndexLargerThanEndIndex;

            const records_num = index_end - index_start;
            if (records_num > batch.offsets_full.len) return ParserError.BatchSizesTooSmall;

            const data_size = try self.sizeOf(index_start, index_end);
            if (data_size > batch.data_full.len) return ParserError.BatchDataTooSmall;

            const offset_start = self.header.record_offsets[index_start];

            const read_size = try self.input.readAt(batch.data_full[0..data_size], self.header.size() + offset_start);
            if (read_size != data_size) {
                return ParserError.ShortRead;
            }

            var offset: u32 = 0;
            for (0..records_num) |i| {
                const index = @as(u32, @intCast(index_start + i));
                batch.offsets_full[i] = offset;
                offset += @intCast(try self.sizeOf(index, index + 1));
            }

            // point user-facing slices into backing storage
            batch.data = batch.data_full[0..read_size];
            batch.offsets = batch.offsets_full[0..records_num];
        }
    };
}

pub const Header = struct {
    pub const header_size = 32;
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

    // NOTE: parse borrows bufs through its lifetime
    fn parse(bufs: *Buffers, input: anytype, file_size: usize) !Header {
        assert(bufs.data.len >= file_size);

        var header_buf: [header_size]u8 = undefined;
        const read = try input.readAt(&header_buf, 0);
        if (read < header_size) return ParserError.EndOfStream;

        // parse static fields
        const magic_bytes = header_buf[0..4].*;
        const version = std.mem.readInt(i16, header_buf[4..6], .little);
        const unix_epoch_us = std.mem.readInt(i64, header_buf[6..14], .little);
        const num_records = std.mem.readInt(u32, header_buf[14..18], .little);
        const reserved = header_buf[18..32].*;

        if (num_records == 0) return ParserError.FileHasNoRecords;
        if (!std.mem.eql(u8, magic_bytes[0..], Header.expected_magic_bytes)) return ParserError.InvalidMagicBytes;
        if (Header.expected_version != version) return ParserError.InvalidVersion;
        assert(bufs.offsets.len >= num_records);

        // parse record offsets
        const record_offsets_size = num_records * record_offset_size;
        const header_end_offset = header_size + record_offsets_size;
        const file_size_min = header_end_offset + num_records;
        if (file_size < file_size_min) return ParserError.FileTooSmall; // assumes at least 1 byte per record

        const data = bufs.data[0..record_offsets_size];
        const read_size = try input.readAt(data, header_size);
        if (read_size < record_offsets_size) return ParserError.EndOfStream;

        const offsets = bufs.offsets[0..num_records];
        for (0..num_records) |i| {
            offsets[i] = std.mem.readInt(u32, data[i * 4 ..][0..4], .little);
        }

        // validate offsets
        {
            // offset must start at 0
            if (offsets[0] != 0) return ParserError.OffsetsMustStartAtZero;

            const record_offsets_max_size = file_size - header_end_offset;

            var previous_offset: u32 = undefined;
            for (0.., offsets) |i, offset| {
                // offset must not point beyond file length
                if (offset >= record_offsets_max_size) return ParserError.OffsetOutOfBounds;

                // offset is monotonically increasing
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
            .record_offsets = offsets,
        };
    }

    fn size(self: Header) usize {
        return header_size + self.num_records * record_offset_size;
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

// batch_file_size returns the exact file size of a batch with the given data
// size and number of records
pub fn batch_file_size(data_size: usize, offsets_size: usize) usize {
    // static header + dynamic header + data
    const header_static_size = Header.header_size;
    const header_dynamic_size = offsets_size * Header.record_offset_size;
    return header_static_size + header_dynamic_size + data_size;
}

pub const Buffers = struct {
    allocator: std.mem.Allocator,
    offsets: []u32,
    data: []u8,

    pub fn init(allocator: std.mem.Allocator, data_size: usize, offsets_size: usize) !Buffers {
        const offsets = try allocator.alloc(u32, offsets_size);
        errdefer allocator.free(offsets);

        return Buffers{
            .allocator = allocator,
            .offsets = offsets,
            .data = try allocator.alloc(u8, batch_file_size(data_size, offsets_size)),
        };
    }

    pub fn deinit(self: Buffers) void {
        self.allocator.free(self.offsets);
        self.allocator.free(self.data);
    }
};

pub fn Write(bufs: Buffers, output: *std.Io.Writer, batch: Batch, now: anytype, fns: stdx.ClockNow(@TypeOf(now))) !void {
    assert(bufs.offsets.len >= batch.offsets.len);
    assert(bufs.data.len >= batch_file_size(batch.data.len, batch.offsets.len));

    const file = bufs.data[0..batch_file_size(batch.data.len, batch.offsets.len)];

    // Write correct format into buf, making as few writes to output as we can
    var mem_writer = std.Io.Writer.fixed(file);
    try mem_writer.writeSliceEndian(u8, Header.expected_magic_bytes[0..], .little);
    try mem_writer.writeInt(u16, Header.expected_version, .little);
    try mem_writer.writeInt(u64, fns.now(now), .little);
    try mem_writer.writeInt(u32, @as(u32, @intCast(batch.offsets.len)), .little);
    try mem_writer.writeSliceEndian(u8, &([_]u8{0} ** 14), .little);
    try mem_writer.writeSliceEndian(u32, batch.offsets, .little);
    try mem_writer.writeSliceEndian(u8, batch.data, .little);

    assert(mem_writer.unusedCapacityLen() == 0);

    // Write buf and data to output in a single write
    try output.writeAll(file);
}

test "can write and read record batch" {
    const io = std.testing.io;
    const gpa = std.testing.allocator;

    const records_num = 8;
    const records_size = 32;
    const batch_write = try testing.randomBatch(gpa, records_num, records_size);
    defer batch_write.deinit();

    const file_size = Header.header_size + records_num * (Header.record_offset_size + records_size);

    var buf: [file_size]u8 = undefined;
    var memory_writer = std.Io.Writer.fixed(&buf);

    const write_buffers = try Buffers.init(std.testing.allocator, batch_write.data.len, batch_write.offsets.len);
    defer write_buffers.deinit();

    var parser_buffers = try Buffers.init(std.testing.allocator, batch_write.data.len, batch_write.offsets.len);
    defer parser_buffers.deinit();

    const clock = stdx.Clock{ .io = io };
    try Write(write_buffers, &memory_writer, batch_write, clock, .{ .now = stdx.Clock.now });

    const memory_reader = stdx.BufferReader{ .buf = &buf };
    const parser = try Parser(@TypeOf(memory_reader)).init(&parser_buffers, memory_reader, file_size);
    defer parser.deinit();

    var batch_read = try Batch.init(gpa, 10 * stdx.MiB, 32 * 1024);
    defer batch_read.deinit();
    try parser.records(&batch_read, 0, records_num);

    assert(std.mem.eql(u8, batch_write.data, batch_read.data));
}

test "record and records reads the same" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const batch_size = 10 * stdx.MiB;
    const batch_num_records = 32 * 1024;
    var batch_pool = try pool.BatchPool.init(allocator, 3, batch_size, batch_num_records);
    defer batch_pool.deinit();

    const records_num = 8;
    const records_size = 32;
    var mem_batch = try testing.MemWriteBatch(allocator, io, records_num, records_size);
    defer mem_batch.deinit();

    const parser = try mem_batch.parser();
    defer parser.deinit();

    const batch_multiple_records = try batch_pool.get();
    defer batch_pool.put(batch_multiple_records);

    const batch_single_record = try batch_pool.get();
    defer batch_pool.put(batch_single_record);

    try parser.records(batch_multiple_records, 0, parser.header.num_records);
    for (0..parser.header.num_records) |i| {
        const index: u32 = @intCast(i);
        try parser.record(batch_single_record, index);

        const b1_offset_start = batch_multiple_records.offsets[i];
        const b1_offset_end: u32 = @intCast(b1_offset_start + try parser.sizeOf(index, index + 1));

        const b1_data = batch_multiple_records.data[b1_offset_start..b1_offset_end];
        const b2_data = batch_single_record.data;
        assert(std.mem.eql(u8, b1_data, b2_data));
    }
}

test "records fails when batch input is too small" {
    const io = std.testing.io;
    const allocator = std.testing.allocator;

    const records_num = 8;
    const records_size = 32;

    var mem_batch = try testing.MemWriteBatch(allocator, io, records_num, records_size);
    defer mem_batch.deinit();
    const batch = mem_batch.batch;

    const parser = try mem_batch.parser();
    defer parser.deinit();

    {
        var batch_offsets_too_small = try Batch.init(allocator, batch.data.len, batch.offsets.len - 1);
        defer batch_offsets_too_small.deinit();

        const err = parser.records(&batch_offsets_too_small, 0, parser.header.num_records);
        assert(err == ParserError.BatchSizesTooSmall);
    }

    {
        var batch_data_too_small = try Batch.init(allocator, batch.data.len - 1, batch.offsets.len);
        defer batch_data_too_small.deinit();

        const err = parser.records(&batch_data_too_small, 0, parser.header.num_records);
        assert(err == ParserError.BatchDataTooSmall);
    }
}

pub fn openPositionalFile(io: std.Io, path: []const u8) !PositionalFileReader {
    const f = try stdx.openFile(io, path);
    return PositionalFileReader{ .io = io, .file = f };
}
