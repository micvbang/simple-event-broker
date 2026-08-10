const std = @import("std");
const stdx = @import("stdx.zig");

const Allocator = std.mem.Allocator;
const Writer = std.Io.Writer;
const Io = std.Io;
const Clock = stdx.Clock;

const Batch = @import("Batch.zig");
const record = @import("record.zig");

var random = std.Random.DefaultPrng.init(0);

pub fn randomBatch(allocator: std.mem.Allocator, num_records: usize, record_size: u32) !Batch {
    var batch = try Batch.init(allocator, num_records * record_size, num_records);
    randomizeBatch(&batch, num_records, record_size);
    return batch;
}

pub fn randomizeBatch(batch: *Batch, num_records: usize, record_size: usize) void {
    batch.data = batch.data[0 .. num_records * record_size];
    batch.offsets = batch.offsets[0..num_records];

    random.fill(batch.data);
    var offset: u32 = 0;
    for (0..num_records) |i| {
        batch.offsets[i] = offset;
        offset += @intCast(record_size);
    }
}

const MemBatch = struct {
    allocator: Allocator,
    buf: []u8,
    batch: Batch,

    parser_buffers: ?record.Buffers,

    pub fn parser(self: *MemBatch) !record.Parser(stdx.BufferReader) {
        std.debug.assert(self.parser_buffers == null); // don't leak parser_buffers

        self.parser_buffers = try record.Buffers.init(self.allocator, self.buf.len, self.batch.offsets.len);
        const reader = stdx.BufferReader{ .buf = self.buf };
        return try record.Parser(@TypeOf(reader)).init(&self.parser_buffers.?, reader, self.buf.len);
    }

    pub fn deinit(self: MemBatch) void {
        self.allocator.free(self.buf);
        self.batch.deinit();
        if (self.parser_buffers) |buffers| buffers.deinit();
    }
};

pub fn MemWriteBatch(allocator: Allocator, io: Io, records_num: usize, records_size: usize) !MemBatch {
    const file_size = record.Header.header_size + records_num * (records_size + record.Header.record_offset_size);
    const buf: []u8 = try allocator.alloc(u8, file_size);
    var memory_writer = Writer.fixed(buf);

    var batch = try Batch.init(allocator, 10 * 1024 * 1024, 32 * 1024);
    randomizeBatch(&batch, records_num, records_size);

    const clock = Clock{ .io = io };
    const write_buffers = try record.Buffers.init(std.testing.allocator, batch.data.len, batch.offsets.len);
    defer write_buffers.deinit();
    try record.Write(write_buffers, &memory_writer, batch, clock, .{ .now = stdx.Clock.now });

    return MemBatch{
        .allocator = allocator,
        .buf = buf,
        .batch = batch,
        .parser_buffers = null,
    };
}

pub fn WriteBatch(allocator: Allocator, io: Io, records_num: usize, records_size: usize) !MemBatch {
    const file_size = record.Header.header_size + records_num * (records_size + record.Header.record_offset_size);
    const buf: []u8 = try allocator.alloc(u8, file_size);
    var memory_writer = Writer.fixed(buf);

    var batch = try Batch.init(allocator, 10 * 1024 * 1024, 32 * 1024);
    randomizeBatch(&batch, records_num, records_size);

    const clock = Clock{ .io = io };
    const write_buffers = try record.Buffers.init(std.testing.allocator, batch.data.len, batch.offsets.len);
    defer write_buffers.deinit();
    try record.Write(write_buffers, &memory_writer, batch, clock, .{ .now = stdx.Clock.now });

    return MemBatch{
        .allocator = allocator,
        .buf = buf,
        .batch = batch,
        .parser_buffers = null,
    };
}
