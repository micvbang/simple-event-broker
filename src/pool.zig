const std = @import("std");
const Batch = @import("Batch.zig");

pub const PoolError = error{
    PoolInUse,
    PoolFull,
    PoolEmpty,
    InvalidPoolSize,
};

pub fn Pool(comptime T: type) type {
    return struct {
        const Self = @This();

        elements: []*T,
        unused_index: usize,

        pub fn init(elements: []*T) !Self {
            if (elements.len == 0) return PoolError.InvalidPoolSize;

            return Self{
                .elements = elements,
                .unused_index = elements.len,
            };
        }

        pub fn deinit(self: Self) void {
            std.debug.assert(self.unused_index == self.elements.len);
        }

        pub fn get(self: *Self) !*T {
            if (self.unused_index == 0) return PoolError.PoolEmpty;

            const element = self.elements[self.unused_index - 1];
            self.unused_index -= 1;
            return element;
        }

        pub fn put(self: *Self, element: *T) void {
            std.debug.assert(self.unused_index < self.elements.len);
            // if (self.unused_index == self.elements.len) return PoolError.PoolFull;

            self.elements[self.unused_index] = element;
            self.unused_index += 1;
        }
    };
}

test "can get and return elements from/to small pool" {
    var element: u32 = 1;
    var elements: [1]*u32 = .{&element};
    var pool = try Pool(u32).init(&elements);
    defer pool.deinit();

    const v = try pool.get();
    pool.put(v);
}

test "can get and return elements from/to medium pool" {
    var pool_elements: [10]*u32 = undefined;

    var pool = try Pool(u32).init(&pool_elements);
    defer pool.deinit();

    var elements: [10]*u32 = undefined;
    for (0..elements.len) |i| {
        elements[i] = try pool.get();
    }

    for (elements) |element| {
        pool.put(element);
    }
}

test "get returns PoolEmpty error" {
    var pool_elements: [1]*u32 = undefined;
    var pool = try Pool(u32).init(&pool_elements);
    defer pool.deinit();

    const v = try pool.get();
    try std.testing.expectError(PoolError.PoolEmpty, pool.get());

    pool.put(v); // avoid leaking
}

pub const BatchPool = struct {
    allocator: std.mem.Allocator,
    batches: []Batch,
    pool_elements: []*Batch,
    pool: Pool(Batch),

    pub fn init(allocator: std.mem.Allocator, pool_size: usize, data_size: usize, sizes_size: usize) !BatchPool {
        const batches = try allocator.alloc(Batch, pool_size);
        errdefer allocator.free(batches);

        var initialized: usize = 0;
        for (batches) |*batch| {
            batch.* = try Batch.init(allocator, data_size, sizes_size);
            initialized += 1;
        }
        errdefer {
            for (batches[0..initialized]) |batch| {
                batch.deinit();
            }
        }

        const pool_elements = try allocator.alloc(*Batch, pool_size);
        errdefer allocator.free(pool_elements);

        for (batches, pool_elements) |*batch, *pool_element| {
            pool_element.* = batch;
        }

        const pool = try Pool(Batch).init(pool_elements);

        return BatchPool{
            .allocator = allocator,
            .batches = batches,
            .pool_elements = pool_elements,
            .pool = pool,
        };
    }

    pub fn deinit(self: *BatchPool) void {
        self.pool.deinit();

        for (self.batches) |batch| {
            batch.deinit();
        }
        self.allocator.free(self.pool_elements);
        self.allocator.free(self.batches);
    }

    pub fn get(self: *BatchPool) !*Batch {
        const batch = try self.pool.get();
        batch.reset();
        return batch;
    }

    pub fn put(self: *BatchPool, batch: *Batch) void {
        return self.pool.put(batch);
    }
};
