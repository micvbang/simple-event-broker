const std = @import("std");
const storage = @import("storage/storage.zig");

pub const Storage = storage.Storage;
pub const Reader = storage.Reader;
pub const Writer = storage.Writer;

pub const memory = @import("storage/memory.zig");

test {
    std.testing.refAllDecls(@This());
}
