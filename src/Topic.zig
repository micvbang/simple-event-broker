const std = @import("std");
const testing = @import("testing.zig");
const s3 = @import("s3.zig");
const storage = @import("storage.zig");
const Storage = storage.Storage;
const record_offsets = @import("offsets.zig");
const record = @import("record.zig");
const assert = std.debug.assert;
const ArrayList = std.ArrayList;
const Allocator = std.mem.Allocator;

const Self = @This();

const offsetFilesPrefix = "offsets_";
const offsetFilesExtension = ".offsets";
const recordBatchExtension = ".record_batch";
const key_len_max = 512;

allocator: Allocator,
name: []const u8,

// TODO: we should probably write (parts of) this to disk, so that we are not
// limited by memory in how many offsets a topic can contain.
storage_files_offsets: ArrayList(u64),
next_offset: u64,

// TODO: stop using allocator, likely writing offsets to disk when they get larger than memory
pub fn init(allocator: Allocator, strg: Storage, bufs: *Buffers, name: []const u8) !Self {
    var offsets = try listBatchRecordOffsets(allocator, strg, bufs, name);
    errdefer offsets.deinit(allocator);
    std.debug.print("got {d} offsets: {any}", .{ offsets.items.len, offsets.items });

    const next_offset = if (offsets.items.len == 0)
        0
    else blk: {
        var buf: [key_len_max]u8 = undefined;
        const key = try recordBatchKey(buf[0..], name, offsets.getLast());
        const rdr = try strg.reader(key);
        defer rdr.close();

        // TODO: make it possible to only parse the header so we don't need to
        // allocate buffers and know the exact file size
        var buffers = try record.Buffers.init(allocator, 512, 512);
        defer buffers.deinit();
        // BUG: the exact file size is not 99999 and we don't want to have to specify it here
        const parser = try record.Parser(@TypeOf(rdr)).init(&buffers, rdr, 99999);
        defer parser.deinit();
        break :blk offsets.items[offsets.items.len - 1] + parser.header.num_records;
    };

    return Self{
        .allocator = allocator,
        .name = name,
        .storage_files_offsets = offsets,
        .next_offset = next_offset,
    };
}

fn listBatchRecordOffsets(allocator: Allocator, strg: Storage, bufs: *Buffers, topic_name: []const u8) !ArrayList(u64) {
    var files_buf: [10 * 1024]storage.File = undefined;
    var files_: []storage.File = files_buf[0..];

    const offset_files = try strg.listFiles(topic_name, offsetFilesExtension, null, &files_);

    var offsets = try ArrayList(u64).initCapacity(allocator, @max(offset_files.len * 2, 10 * 1024));
    errdefer offsets.deinit(allocator);

    if (offset_files.len > 0) {
        const most_recent_file = offset_files[offset_files.len - 1];
        const reader = try strg.reader(most_recent_file.path);
        defer reader.close();

        const offsetsBuf = try record_offsets.Parse(reader, bufs.offset_file_buf, bufs.offset_file_offsets);
        try offsets.appendSlice(allocator, offsetsBuf);
    }

    var record_name_buf: [1024]u8 = undefined;
    const start_after = if (offsets.items.len > 0)
        try recordBatchBaseName(record_name_buf[0..], offsets.getLast())
    else
        null;

    const files = try strg.listFiles(topic_name, recordBatchExtension, start_after, &files_);

    // TODO: use stack-allocated buffer and do in batches instead of allocating.
    // var file_offsets: [1024]u64 = undefined;
    var file_offsets: []u64 = try allocator.alloc(u64, files.len);
    defer allocator.free(file_offsets);
    for (0.., files) |i, file| {
        const file_name: []const u8 = std.fs.path.basename(file.path);
        const offset_str = file_name[0 .. file_name.len - recordBatchExtension.len];

        file_offsets[i] = try std.fmt.parseInt(u64, offset_str, 10);
    }

    try offsets.appendSlice(allocator, file_offsets);

    assert(std.sort.isSorted(u64, offsets.items, {}, std.sort.asc(u64)));

    return offsets;
}

fn recordBatchKey(buf: []u8, topic_name: []const u8, record_batch_id: u64) ![]u8 {
    return try std.fmt.bufPrint(buf, "{s}/{d:0>12}{s}", .{ topic_name, record_batch_id, recordBatchExtension });
}

fn recordBatchBaseName(buf: []u8, record_batch_id: u64) ![]u8 {
    return try std.fmt.bufPrint(buf, "{d:0>12}{s}", .{ record_batch_id, recordBatchExtension });
}

const Buffers = struct {
    offset_file_offsets: []u64,
    offset_file_buf: []u8,
};

test "listBatchRecordOffsets lists all files" {
    const allocator = std.testing.allocator;

    var mem_strg = storage.memory.Storage.init(allocator, 512);
    const strg = mem_strg.interface();
    defer strg.deinit();

    const topic_name = "topic";

    const offsets_expected = [_]u64{ 0, 1, 5, 9, 13, 17, 300, 342, 1337 };
    for (offsets_expected) |offset| {
        var buf: [key_len_max]u8 = undefined;
        const key = try recordBatchKey(buf[0..], topic_name, offset);
        const wtr = try strg.writer(key);
        defer wtr.close();
    }

    var offset_file_buf: [512]u8 = undefined;
    var offset_file_offsets: [512]u64 = undefined;

    var bufs = Buffers{
        .offset_file_buf = offset_file_buf[0..],
        .offset_file_offsets = offset_file_offsets[0..],
    };

    var offsets = try listBatchRecordOffsets(allocator, strg, &bufs, topic_name);
    defer offsets.deinit(allocator);

    try std.testing.expectEqualSlices(u64, offsets_expected[0..], offsets.items);
}

test "listBatchRecordOffsets list empty topic" {
    const allocator = std.testing.allocator;

    var mem_strg = storage.memory.Storage.init(allocator, 512);
    var strg = mem_strg.interface();
    defer strg.deinit();

    var offset_file_buf: [512]u8 = undefined;
    var offset_file_offsets: [512]u64 = undefined;

    var bufs = Buffers{
        .offset_file_buf = offset_file_buf[0..],
        .offset_file_offsets = offset_file_offsets[0..],
    };

    var offsets = try listBatchRecordOffsets(allocator, strg, &bufs, "topic");
    defer offsets.deinit(allocator);

    assert(offsets.items.len == 0);
}

test "list existing files, no offsets file" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var mem_strg = storage.memory.Storage.init(allocator, 512);
    const strg = mem_strg.interface();
    defer strg.deinit();

    const topic_name = "topic";

    const batch_size = 32;
    const offsets_expected = comptime [_]u64{ batch_size * 0, batch_size * 1, batch_size * 2 };
    for (offsets_expected) |offset| {
        var buf: [key_len_max]u8 = undefined;
        const key = try recordBatchKey(buf[0..], topic_name, offset);
        const wtr = try strg.writer(key);
        defer wtr.close();

        const mem_batch = try testing.MemWriteBatch(allocator, io, batch_size, 32);
        defer mem_batch.deinit();
        _ = try wtr.write(mem_batch.buf);
    }

    var offset_file_buf: [512]u8 = undefined;
    var offset_file_offsets: [512]u64 = undefined;

    var bufs = Buffers{
        .offset_file_buf = offset_file_buf[0..],
        .offset_file_offsets = offset_file_offsets[0..],
    };

    const topic = try Self.init(allocator, strg, &bufs, "topic");
    assert(topic.next_offset == batch_size * 3);
}
