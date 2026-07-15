const std = @import("std");

const Self = @This();

allocator: std.mem.Allocator,
data: []u8,
offsets: []u32,

// for keeping track of the originally allocated sizes of data and sizes
data_full: []u8,
offsets_full: []u32,

pub fn init(allocator: std.mem.Allocator, data_size: usize, offsets_size: usize) !Self {
    const data_full = try allocator.alloc(u8, data_size);
    errdefer allocator.free(data_full);

    const offsets_full = try allocator.alloc(u32, offsets_size);

    return Self{
        .allocator = allocator,
        .data = data_full[0..data_size],
        .data_full = data_full[0..data_size],

        .offsets = offsets_full[0..offsets_size],
        .offsets_full = offsets_full[0..offsets_size],
    };
}

pub fn deinit(self: Self) void {
    self.allocator.free(self.data_full);
    self.allocator.free(self.offsets_full);
}

// reset() reslices data and sizes to their full allocations
pub fn reset(self: *Self) void {
    // TODO: do we need to zero out the memory? I don't think so..
    self.data = self.data_full;
    self.offsets = self.offsets_full;
}
