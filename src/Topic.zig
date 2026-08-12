const std = @import("std");
const stdx = @import("stdx.zig");
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

allocator: Allocator,
name: []const u8,

// TODO: we should probably write (parts of) this to disk, so that we are not
// limited by memory in how many offsets a topic can contain.
storage_files_offsets: ArrayList(u64),
next_offset: u64,

pub fn init(allocator: Allocator, strg: Storage, bufs: *Buffers, name: []const u8) !Self {
    var offsets = try listBatchRecordOffsets(allocator, strg, bufs, name);
    errdefer offsets.deinit(allocator);
    std.debug.print("got {d} offsets: {any}\n", .{ offsets.items.len, offsets.items });

    const next_offset = if (offsets.items.len == 0)
        0
    else blk: {
        var buf: [storage.key_len_max]u8 = undefined;
        const key = try storage.recordBatchKey(buf[0..], name, offsets.getLast());
        const rdr = try strg.reader(key);
        defer rdr.close();

        // TODO: we don't want to allocate here
        var buffers = try record.Buffers.init(allocator, 512, 512);
        defer buffers.deinit();
        const header = try record.Header.parse(&buffers, rdr);
        break :blk offsets.items[offsets.items.len - 1] + header.num_records;
    };

    return Self{
        .allocator = allocator,
        .name = name,
        .storage_files_offsets = offsets,
        .next_offset = next_offset,
    };
}

fn deinit(self: *Self) void {
    self.storage_files_offsets.deinit(self.allocator);
}

fn listBatchRecordOffsets(allocator: Allocator, strg: Storage, bufs: *const Buffers, topic_name: []const u8) !ArrayList(u64) {
    var files_buf: [10 * 1024]storage.File = undefined;
    var files_: []storage.File = files_buf[0..];

    const offset_files = try strg.listFiles(topic_name, storage.offset_files_extension, null, &files_);

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
        try storage.recordBatchBaseName(record_name_buf[0..], offsets.getLast())
    else
        null;

    const files = try strg.listFiles(topic_name, storage.record_batch_extension, start_after, &files_);

    // TODO: use stack-allocated buffer and do in batches instead of allocating.
    // var file_offsets: [1024]u64 = undefined;
    var file_offsets: []u64 = try allocator.alloc(u64, files.len);
    defer allocator.free(file_offsets);
    for (0.., files) |i, file| {
        const file_name: []const u8 = std.fs.path.basename(file.path);
        const offset_str = file_name[0 .. file_name.len - storage.record_batch_extension.len];

        file_offsets[i] = try std.fmt.parseInt(u64, offset_str, 10);
    }

    try offsets.appendSlice(allocator, file_offsets);

    assert(std.sort.isSorted(u64, offsets.items, {}, std.sort.asc(u64)));

    return offsets;
}

const Buffers = struct {
    allocator: ?Allocator,
    offset_file_offsets: []u64,
    offset_file_buf: []u8,

    fn init(offset_file_offsets: []u64, offset_file_buf: []u8) Buffers {
        return Buffers{
            .allocator = null,
            .offset_file_offsets = offset_file_offsets,
            .offset_file_buf = offset_file_buf,
        };
    }

    fn init_alloc(allocator: Allocator, offset_file_offsets_num: usize, file_buf_num: usize) !Buffers {
        return Buffers{
            .allocator = allocator,
            .offset_file_offsets = try allocator.alloc(u64, offset_file_offsets_num),
            .offset_file_buf = try allocator.alloc(u8, file_buf_num),
        };
    }

    fn deinit(self: Buffers) void {
        if (self.allocator) |allocator| {
            allocator.free(self.offset_file_offsets);
            allocator.free(self.offset_file_buf);
        }
    }
};

test "listBatchRecordOffsets lists all files" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var strg_helper = try testing.MemoryStorageHelper.init(gpa, io, 512);
    defer strg_helper.deinit();

    const topic_name = "topic";

    const offsets_expected = [_]u64{ 0, 1, 5, 9, 13, 17, 300, 342, 1337 };
    for (offsets_expected) |offset| {
        const batch = try strg_helper.write_record_batch(topic_name, offset, 1, 1);
        defer batch.deinit();
    }

    var bufs = try Buffers.init_alloc(gpa, 512, 512);
    defer bufs.deinit();
    var offsets = try listBatchRecordOffsets(gpa, strg_helper.storage, &bufs, topic_name);
    defer offsets.deinit(gpa);

    try std.testing.expectEqualSlices(u64, &offsets_expected, offsets.items);
}

test "listBatchRecordOffsets list empty topic" {
    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var strg_helper = try testing.MemoryStorageHelper.init(gpa, io, 512);
    defer strg_helper.deinit();

    const bufs = try Buffers.init_alloc(gpa, 512, 512);
    defer bufs.deinit();

    var offsets = try listBatchRecordOffsets(gpa, strg_helper.storage, &bufs, "topic");
    defer offsets.deinit(gpa);

    assert(offsets.items.len == 0);
}

test "Topic init, no offsets file" {
    // Verifies that Topic.init() computes the correct next_offset when there's
    // no offsets file.

    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var strg_helper = try testing.MemoryStorageHelper.init(gpa, io, 512);
    defer strg_helper.deinit();

    const topic_name = "topic";
    const batch_size = 32;
    const offsets_expected = comptime [_]u64{ batch_size * 0, batch_size * 1, batch_size * 2 };

    for (offsets_expected) |offset| {
        const batch = try strg_helper.write_record_batch(topic_name, offset, batch_size, 0);
        defer batch.deinit();
    }

    var bufs = try Buffers.init_alloc(gpa, 512, 512);
    defer bufs.deinit();

    var topic = try Self.init(gpa, strg_helper.storage, &bufs, "topic");
    defer topic.deinit();
    assert(topic.next_offset == batch_size * 3);
}

test "Topic init, only offsets file" {
    // Verifies that Topic.init() computes the correct next_offset when the
    // offsets file covers all offsets.

    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var strg_helper = try testing.MemoryStorageHelper.init(gpa, io, 512);
    defer strg_helper.deinit();

    const topic_name = "topic";
    const batch_records = 32;
    const record_size = 1;
    const offsets_expected = [_]u64{ 0, 1, 5, 9, 12, 31 };

    for (offsets_expected) |offset| {
        const batch = try strg_helper.write_record_batch(topic_name, offset, batch_records, record_size);
        defer batch.deinit();
    }
    try strg_helper.write_offsets_file(topic_name, 0, &offsets_expected);

    var bufs = try Buffers.init_alloc(gpa, 512, 512);
    defer bufs.deinit();

    var topic = try Self.init(gpa, strg_helper.storage, &bufs, "topic");
    defer topic.deinit();
    assert(topic.next_offset == 31 + batch_records);
}

test "Topic init, offsets file not covering all offsets" {
    // Verifies that Topic.init() computes the correct next_offset when the
    // offset file does not cover all existing offsets.

    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var strg_helper = try testing.MemoryStorageHelper.init(gpa, io, 512);
    defer strg_helper.deinit();

    const topic_name = "topic";
    const batch_records = 32;
    const record_size = 1;
    const offsets_expected = [_]u64{ 0, 1, 5, 9, 12, 31 };
    const offsets_file_offsets = offsets_expected[0 .. offsets_expected.len - 2];

    for (offsets_expected) |offset| {
        const batch = try strg_helper.write_record_batch(topic_name, offset, batch_records, record_size);
        defer batch.deinit();
    }
    try strg_helper.write_offsets_file(topic_name, 0, offsets_file_offsets);

    var bufs = try Buffers.init_alloc(gpa, 512, 512);
    defer bufs.deinit();

    var topic = try Self.init(gpa, strg_helper.storage, &bufs, "topic");
    defer topic.deinit();
    assert(topic.next_offset == 31 + batch_records);
}

// test "topic short write" {
//     const gpa = std.testing.allocator;
//     const io = std.testing.io;

//     var mem_storage = storage.memory.Storage.init(gpa, 512);
//     const strg = mem_storage.interface();
//     const strg_helper = testing.StorageHelper.init(gpa, io, strg);
//     defer strg_helper.deinit();

//     const topic_name = "topic";
//     const batch_records = 32;
//     const record_size = 32;
//     const offsets_expected = [_]u64{0};

//     for (offsets_expected) |offset| {
//         try strg_helper.write_record_batch(topic_name, offset, batch_records, record_size);
//     }
// }
