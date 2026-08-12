const std = @import("std");
const storage = @import("storage/storage.zig");

pub const key_len_max = storage.key_len_max;
pub const offset_files_prefix = storage.offset_files_prefix;
pub const offset_files_extension = storage.offset_files_extension;
pub const record_batch_extension = storage.record_batch_extension;

pub const recordBatchKey = storage.recordBatchKey;
pub const recordBatchBaseName = storage.recordBatchBaseName;
pub const offsetsFileKey = storage.offsetsFileKey;

pub const Storage = storage.Storage;
pub const File = storage.File;
pub const Reader = storage.Reader;
pub const Writer = storage.Writer;

pub const memory = @import("storage/memory.zig");

test {
    std.testing.refAllDecls(@This());
}
