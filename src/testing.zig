const std = @import("std");

const Batch = @import("Batch.zig");

var random = std.Random.DefaultPrng.init(0);

pub fn randomBatch(allocator: std.mem.Allocator, num_records: usize, record_size: u32) !Batch {
    var batch = try Batch.init(allocator, num_records * record_size, num_records);
    randomizeBatch(&batch, num_records, record_size);
    return batch;
}

pub fn randomizeBatch(batch: *Batch, num_records: usize, record_size: u32) void {
    batch.data = batch.data[0 .. num_records * record_size];
    batch.offsets = batch.offsets[0..num_records];

    random.fill(batch.data);
    var offset: u32 = 0;
    for (0..num_records) |i| {
        batch.offsets[i] = offset;
        offset += record_size;
    }
}

pub const PositionalBufferReader = struct {
    const Self = @This();

    buf: []u8,

    pub fn readAt(self: Self, dest: []u8, offset: u64) !usize {
        for (0..dest.len) |i| {
            dest[i] = self.buf[offset + i];
        }

        return dest.len;
    }

    pub fn length(self: Self) !usize {
        return self.buf.len;
    }

    pub fn close(_: Self) void {}
};
