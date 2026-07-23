const std = @import("std");
const s3 = @import("s3.zig");
const Storage = @import("storage.zig").Storage;
const record_offsets = @import("offsets.zig");
const assert = std.debug.assert;
const ArrayList = std.ArrayList;
const Allocator = std.mem.Allocator;

const Self = @This();

const offsetFilesPrefix = "offsets_";
const offsetFilesExtension = ".offsets";
const recordBatchExtension = ".record_batch";

allocator: Allocator,
name: []u8,

// TODO: we should probably write (parts of) this to disk, so that we are not
// limited by memory in how many offsets a topic can contain.
storage_files_offsets: ArrayList(u64),
next_offset: u64,

// TODO: stop using allocator, likely writing offsets to disk when they get larger than memory
pub fn init(allocator: Allocator, storage: Storage, bufs: *Buffers, name: []u8) !Self {
    var offsets = try listBatchRecordOffsets(storage, bufs, name);
    errdefer offsets.deinit(allocator);
    std.debug.print("got {d} offsets: {any}", offsets.items.len, offsets.items);

    const next_offset = try if (offsets.len > 0)
        // TODO: read offsets[offsets.len-1] to get number of records, then return
        // offsets[offsets.len-1] + parsed_record_batch.header.num_records
        error.NotImplemented
    else
        0;

    return Self{
        .allocator = allocator,
        .name = name,
        .bufs = bufs,
        .storage_files_offsets = &offsets,
        .next_offset = next_offset,
    };
}

fn listBatchRecordOffsets(allocator: Allocator, storage: Storage, bufs: *Buffers, topic_name: []u8) !ArrayList(u64) {
    var files_buf: [10 * 1024]storage.File = undefined;

    const offset_files = try storage.listFiles(topic_name, offsetFilesExtension, null, &files_buf);

    const offsetsBuf = bufs.offset_file_offsets;
    if (offset_files.len > 0) {
        const most_recent_file = offset_files[offset_files.len - 1];
        const reader = try storage.reader(most_recent_file);
        defer reader.close();

        try record_offsets.Parse(reader, &bufs.offset_file_buf, &offsetsBuf);
    }

    const offsets = try ArrayList(u64).initCapacity(allocator, @max(offset_files.len * 1.25, 10 * 1024));
    try offsets.appendSlice(allocator, offsetsBuf);

    const record_name_buf: [1024]u8 = undefined;
    const offset_most_recent = if (offsets.len > 0)
        offsets[offsets.len - 1]
    else
        0;

    const list_files_start_after = try recordBatchKey(record_name_buf, topic_name, offset_most_recent);
    const files = try storage.listFiles(topic_name, recordBatchExtension, list_files_start_after, files_buf);

    var file_offsets: []u64 = allocator.alloc(u64, files.len);
    for (0.., files) |i, file| {
        const file_name: []u8 = std.fs.path.basename(file.path);
        const offset_str = file_name[0 .. file_name.len - recordBatchExtension.len];

        file_offsets[i] = try std.fmt.parseInt(u64, offset_str, 10);
    }

    try offsets.appendSlice(allocator, file_offsets);

    assert(std.sort.isSorted(u64, offsets.items, .{}, std.sort.asc(u64)));

    return offsets;
}

fn recordBatchKey(buf: []u8, topic_name: []u8, record_batch_id: u64) ![]u8 {
    return try std.fmt.bufPrint(buf, "{s}/{d:0>12}{s}", .{ topic_name, record_batch_id, recordBatchExtension });
}

const Buffers = struct {
    offset_file_offsets: []u64,
    offset_file_buf: []u8,
};
