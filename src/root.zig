//! By convention, root.zig is the root source file when making a package.
const std = @import("std");

pub const record = @import("record.zig");
pub const stdx = @import("stdx.zig");

pub const Batch = @import("Batch.zig");
pub const pool = @import("pool.zig");
pub const BatchPool = pool.BatchPool;

pub const testing = @import("testing.zig");

test {
    std.testing.refAllDecls(@This());
}
