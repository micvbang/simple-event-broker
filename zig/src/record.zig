const std = @import("std");
const assert = std.debug.assert;

const PositionalFileReader = struct {
    io: std.Io,
    file: std.Io.File,

    pub fn readAt(self: PositionalFileReader, dest: []u8, offset: u64) !usize {
        return self.file.readPositionalAll(self.io, dest, offset);
    }

    pub fn length(self: PositionalFileReader) !usize {
        return self.file.length(self.io);
    }

    pub fn close(self: PositionalFileReader) void {
        return self.file.close(self.io);
    }
};

pub const RecordBatch = struct {
    header: Header,
    fileSize: usize,
    maxSize: usize,

    pub fn init(input: anytype, gpa: std.mem.Allocator, file_size: usize) !RecordBatch {
        const header = try Header.parse(input, gpa, file_size);

        var rb = RecordBatch{
            .header = header,
            .fileSize = file_size,
            .maxSize = 0,
        };

        var max_size: usize = 0;
        for (0..header.num_records) |record_id| {
            max_size = @max(rb.size(@intCast(record_id)), max_size);
        }
        rb.maxSize = max_size;

        return rb;
    }

    pub fn deinit(self: RecordBatch) void {
        self.header.deinit();
    }

    pub fn size(self: RecordBatch, record_id: u32) usize {
        const record_offset = self.header.record_index[record_id];
        const next_record_offset = if (record_id + 1 < self.header.num_records)
            @as(usize, self.header.record_index[record_id + 1])
        else
            self.fileSize - self.header.size();

        return next_record_offset - record_offset;
    }

    pub fn read(self: RecordBatch, input: anytype, record_id: u32, buf: []u8) !usize {
        if (record_id >= self.header.num_records) return error.RecordNotFound;
        if (buf.len < self.size(record_id)) return error.BufferTooSmall;

        const record_offset = self.header.record_index[record_id];
        const record_size = self.size(record_id);

        const read_size = try input.readAt(buf[0..record_size], self.header.size() + record_offset);
        if (record_size != read_size) {
            return error.ShortRead;
        }

        return read_size;
    }
};

const Header = struct {
    const header_bytes = 32;
    const header_index_entry_bytes = 4;

    // static header
    magic_bytes: [4]u8,
    version: i16,
    unix_epoch_us: i64,
    num_records: u32,
    reserved: [14]u8,

    // dynamic header
    record_index: []u32,
    allocator: std.mem.Allocator,

    const ParseError = error{
        NoRecords,
        EndOfStream,
        FileTooSmall,
        IndexNotMonotonicallyIncreasing,
        IndexPointsOutOfBounds,
        IndexMustStartAtZero,
    };

    fn parse(input: anytype, allocator: std.mem.Allocator, file_size: usize) !Header {
        var header_buf: [header_bytes]u8 = undefined;
        const read = try input.readAt(&header_buf, 0);
        if (read < header_bytes) return ParseError.EndOfStream;

        // parse static part
        const magic_bytes = header_buf[0..4].*;
        const version = std.mem.readInt(i16, header_buf[4..6], .little);
        const unix_epoch_us = std.mem.readInt(i64, header_buf[6..14], .little);
        const num_records = std.mem.readInt(u32, header_buf[14..18], .little);
        const reserved = header_buf[18..32].*;

        if (num_records == 0) return ParseError.NoRecords;

        // parse dynamic part (record index)
        const index_size = @as(usize, num_records) * header_index_entry_bytes;
        const header_end_offset = header_bytes + index_size;
        const file_size_min = header_end_offset + @as(usize, num_records);
        if (file_size < file_size_min) return ParseError.FileTooSmall; // assumes at least 1 byte per record

        var index_buf = try allocator.alloc(u8, index_size);
        defer allocator.free(index_buf);

        const index_read_size = try input.readAt(index_buf, header_bytes);
        if (index_read_size < index_size) return ParseError.EndOfStream;

        var record_index: []u32 = try allocator.alloc(u32, num_records);
        for (0..num_records) |i| {
            record_index[i] = std.mem.readInt(u32, index_buf[i * 4 ..][0..4], .little);
        }

        try Header.validateRecordIndex(record_index, file_size - header_end_offset);

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

    fn validateRecordIndex(record_index: []u32, index_max: usize) !void {
        // Index must start at 0
        if (record_index[0] != 0) return ParseError.IndexMustStartAtZero;

        var last_index: u32 = undefined;
        for (0.., record_index) |i, index| {
            // Validate that index doesn't point beyond file length
            if (index >= index_max) return ParseError.IndexPointsOutOfBounds;

            // Verify that index is monotonically increasing
            if (i > 0 and last_index >= index) return ParseError.IndexNotMonotonicallyIncreasing;
            last_index = index;
        }
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

fn openFile(io: std.Io, path: []const u8) !std.Io.File {
    return if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
}

fn openPositionalFile(io: std.Io, path: []const u8) !PositionalFileReader {
    const f = try openFile(io, path);
    return PositionalFileReader{ .io = io, .file = f };
}

pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.iterateAllocator(init.minimal.args, init.gpa);
    defer args.deinit();

    _ = args.next(); // program path

    const records_path = args.next() orelse "/Users/micvbang/projects/simple-event-broker/zig/src/000000000000.record_batch";

    const f = try openPositionalFile(init.io, records_path);
    defer f.close();

    const file_length = try f.length();
    const records = try RecordBatch.init(f, init.gpa, file_length);
    defer records.deinit();

    const buf = try init.gpa.alloc(u8, records.maxSize);
    defer init.gpa.free(buf);

    for (0..records.header.num_records) |i| {
        const size = try records.read(f, @intCast(i), buf[0..]);
        std.debug.print("{d}: record: {s}\n", .{ i, buf[0..@min(80, size)] });
    }
}
