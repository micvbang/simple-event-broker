const std = @import("std");
const stdx = @import("stdx.zig");
const assert = std.debug.assert;

const Allocator = std.mem.Allocator;
const Writer = std.Io.Writer;
const Io = std.Io;
const Clock = stdx.Clock;

const Batch = @import("Batch.zig");
const record = @import("record.zig");
const storage = @import("storage.zig");
const Topic = @import("Topic.zig");
const record_offsets = @import("offsets.zig");

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

const TestBatch = struct {
    allocator: Allocator,
    batch: Batch,

    parser_buffers: ?record.Buffers,

    pub fn parser(self: *TestBatch, rdr: storage.Reader) !record.Parser {
        std.debug.assert(self.parser_buffers == null); // don't leak parser_buffers

        const file_size = record.batch_file_size(self.batch.data.len, self.batch.offsets.len);
        self.parser_buffers = try record.Buffers.init(self.allocator, file_size, self.batch.offsets.len);
        return try record.Parser.init(&self.parser_buffers.?, rdr);
    }

    pub fn deinit(self: TestBatch) void {
        self.batch.deinit();
        if (self.parser_buffers) |buffers| buffers.deinit();
    }
};

pub const MemoryStorageHelper = struct {
    gpa: Allocator,
    mem_storage: storage.memory.Storage,
    storage: storage.Storage,
    helper: StorageHelper,
    clock: stdx.Clock,

    pub fn init(allocator: Allocator, io: std.Io, file_buf_size: usize) !*MemoryStorageHelper {
        const mem_helper = try allocator.create(MemoryStorageHelper);
        mem_helper.gpa = allocator;
        mem_helper.mem_storage = storage.memory.Storage.init(allocator, file_buf_size);
        mem_helper.storage = mem_helper.mem_storage.interface();
        mem_helper.helper = StorageHelper.init(allocator, io, mem_helper.storage);
        mem_helper.clock = stdx.Clock{ .io = io };
        return mem_helper;
    }

    pub fn deinit(self: *MemoryStorageHelper) void {
        self.helper.deinit();
        const gpa = self.gpa;
        gpa.destroy(self);
    }

    pub fn write_offsets_file(self: *const MemoryStorageHelper, topic_name: []const u8, offset_file_id: u64, offsets: []const u64) !void {
        return self.helper.write_offsets_file(topic_name, offset_file_id, offsets);
    }

    pub fn write_record_batch(self: *const MemoryStorageHelper, topic_name: []const u8, file_offset_id: u64, records_num: usize, record_size: usize) !TestBatch {
        return self.helper.write_record_batch(topic_name, file_offset_id, records_num, record_size);
    }

    pub fn record_batch_reader(self: *const MemoryStorageHelper, topic_name: []const u8, offset: u64) !storage.Reader {
        return try self.helper.record_batch_reader(topic_name, offset);
    }

    pub fn record_batch_writer(self: *const MemoryStorageHelper, topic_name: []const u8, offset: u64) !storage.Writer {
        return try self.helper.record_batch_writer(topic_name, offset);
    }

    pub fn offsets_file_reader(self: *const MemoryStorageHelper, topic_name: []const u8, file_offset_id: u64) !storage.Reader {
        return try self.helper.offsets_file_reader(topic_name, file_offset_id);
    }

    pub fn offsets_file_writer(self: *const MemoryStorageHelper, topic_name: []const u8, file_offset_id: u64) !storage.Writer {
        return try self.helper.offsets_file_writer(topic_name, file_offset_id);
    }
};

pub const StorageHelper = struct {
    allocator: Allocator,
    io: std.Io,
    strg: storage.Storage,

    pub fn init(allocator: Allocator, io: std.Io, strg: storage.Storage) StorageHelper {
        return StorageHelper{
            .allocator = allocator,
            .io = io,
            .strg = strg,
        };
    }

    pub fn deinit(self: StorageHelper) void {
        self.strg.deinit();
    }

    pub fn write_offsets_file(self: StorageHelper, topic_name: []const u8, offset_file_id: u64, offsets: []const u64) !void {
        var scratch: [512]u8 = undefined;
        const offsets_wtr = try self.offsets_file_writer(topic_name, offset_file_id);
        defer offsets_wtr.close();

        const clock = stdx.Clock{ .io = self.io };
        const write_size = try record_offsets.Write(&scratch, offsets_wtr, offsets, clock, .{ .now = stdx.Clock.now });
        assert(write_size == record_offsets.offsets_file_size(offsets.len));
    }

    pub fn write_record_batch(self: StorageHelper, topic_name: []const u8, file_offset_id: u64, records_num: usize, record_size: usize) !TestBatch {
        const wtr = try self.record_batch_writer(topic_name, file_offset_id);
        defer wtr.close();

        var batch = try Batch.init(self.allocator, records_num * record_size, records_num);
        randomizeBatch(&batch, records_num, record_size);

        const clock = Clock{ .io = self.io };
        const write_buffers = try record.Buffers.init(std.testing.allocator, batch.data.len, batch.offsets.len);
        defer write_buffers.deinit();
        try record.Write(write_buffers, wtr, batch, clock, .{ .now = stdx.Clock.now });

        return TestBatch{
            .allocator = self.allocator,
            .batch = batch,
            .parser_buffers = null,
        };
    }

    pub fn record_batch_reader(self: StorageHelper, topic_name: []const u8, file_offset_id: u64) !storage.Reader {
        var key_buf: [storage.key_len_max]u8 = undefined;
        const key = try storage.recordBatchKey(&key_buf, topic_name, file_offset_id);
        return try self.strg.reader(key);
    }

    pub fn record_batch_writer(self: StorageHelper, topic_name: []const u8, file_offset_id: u64) !storage.Writer {
        var key_buf: [storage.key_len_max]u8 = undefined;
        const key = try storage.recordBatchKey(&key_buf, topic_name, file_offset_id);
        return try self.strg.writer(key);
    }

    pub fn offsets_file_reader(self: StorageHelper, topic_name: []const u8, file_offset_id: u64) !storage.Reader {
        var key_buf: [storage.key_len_max]u8 = undefined;
        const key = try storage.offsetsFileKey(&key_buf, topic_name, file_offset_id);
        return try self.strg.reader(key);
    }

    pub fn offsets_file_writer(self: StorageHelper, topic_name: []const u8, file_offset_id: u64) !storage.Writer {
        var key_buf: [storage.key_len_max]u8 = undefined;
        const key = try storage.offsetsFileKey(&key_buf, topic_name, file_offset_id);
        return try self.strg.writer(key);
    }
};
