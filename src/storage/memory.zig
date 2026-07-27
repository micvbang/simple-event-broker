const std = @import("std");
const assert = std.debug.assert;
const ArrayList = std.ArrayList;
const Allocator = std.mem.Allocator;

const storage = @import("storage.zig");
const stdx = @import("../stdx.zig");
const BufferReader = stdx.BufferReader;
const BufferWriter = stdx.BufferWriter;

pub const Storage = struct {
    const Self = @This();

    allocator: Allocator,
    topics: std.StringHashMap([]u8),
    file_buf_size: usize,

    pub fn init(allocator: Allocator, file_buf_size: usize) Self {
        return Self{
            .allocator = allocator,
            .topics = std.StringHashMap([]u8).init(allocator),
            .file_buf_size = file_buf_size,
        };
    }

    pub fn interface(self: *Self) storage.Storage {
        return storage.Storage{
            .context = self,
            .vtable = &.{
                .listFiles = Storage.listFilesAdapter,
                .reader = Storage.readerAdapter,
                .writer = Storage.writerAdapter,
                .deinit = Storage.deinitAdapter,
            },
        };
    }

    pub fn deinit(self: *Self) void {
        var iter = self.topics.iterator();
        while (iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }

        self.topics.deinit();
    }

    pub fn listFiles(self: *Self, topic_name: []const u8, ext: []const u8, start_after: ?[]const u8, files: *[]storage.File) ![]storage.File {
        var iter = self.topics.keyIterator();

        var i: usize = 0;
        while (iter.next()) |key| {
            if (key.*.len <= topic_name.len) {
                continue;
            }

            // if key starts with "topic name/"
            if (key.*[topic_name.len] != '/' or !std.mem.startsWith(u8, key.*, topic_name)) {
                continue;
            }

            if (!std.mem.endsWith(u8, key.*, ext)) {
                continue;
            }

            const file_name = std.fs.path.basename(key.*);
            if (start_after != null and std.mem.order(u8, file_name, start_after.?) != .gt) {
                continue;
            }

            if (i == files.len) return error.BufferTooSmall;

            files.*[i] = storage.File{
                .size = self.topics.get(key.*).?.len,
                .path = key.*,
            };

            i += 1;
        }

        const output = files.*[0..i];
        std.mem.sort(storage.File, output, {}, sortFiles);
        return output;
    }

    pub fn sortFiles(_: void, a: storage.File, b: storage.File) bool {
        return std.mem.order(u8, a.path, b.path) == .lt;
    }

    pub fn reader(self: *Self, key: []const u8) anyerror!storage.Reader {
        const buf = self.topics.get(key) orelse return error.KeyNotFound;

        const rdr = try self.allocator.create(stdx.ReaderAdapter(BufferReader, true));
        rdr.* = .{
            .allocator = self.allocator,
            .reader = .{ .buf = buf },
        };

        return storage.Reader{
            .context = rdr,
            .vtable = &.{
                .readAt = @TypeOf(rdr.*).readAtAdapter,
                .close = @TypeOf(rdr.*).closeAdapter,
            },
        };
    }

    pub fn writer(self: *Self, key: []const u8) anyerror!storage.Writer {
        const buf: []u8 = try self.allocator.alloc(u8, self.file_buf_size);
        errdefer self.allocator.free(buf);

        const wtr = try self.allocator.create(stdx.WriterAdapter(BufferWriter));
        errdefer self.allocator.destroy(wtr);

        wtr.* = .{
            .allocator = self.allocator,
            .writer = .{ .buf = buf },
        };

        // TODO: we would like to now have to allocate here
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);

        // TODO: this shouldn't be done until wtr.close() is called, otherwise
        // we expose a file that hasn't been written yet.
        try self.topics.put(owned_key, buf);

        return storage.Writer{
            .context = wtr,
            .vtable = &.{
                .write = @TypeOf(wtr.*).writeAdapter,
                .close = @TypeOf(wtr.*).closeAdapter,
            },
        };
    }

    pub fn deinitAdapter(context: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(context));
        self.deinit();
    }

    pub fn listFilesAdapter(context: *anyopaque, topic_name: []const u8, ext: []const u8, start_after: ?[]const u8, files: *[]storage.File) anyerror![]storage.File {
        const self: *Self = @ptrCast(@alignCast(context));
        return self.listFiles(topic_name, ext, start_after, files);
    }

    pub fn readerAdapter(context: *anyopaque, key: []const u8) anyerror!storage.Reader {
        const self: *Self = @ptrCast(@alignCast(context));
        return self.reader(key);
    }

    pub fn writerAdapter(context: *anyopaque, key: []const u8) anyerror!storage.Writer {
        const self: *Self = @ptrCast(@alignCast(context));
        return self.writer(key);
    }
};

fn assertFilesEqual(as: []storage.File, bs: []storage.File) void {
    assert(as.len == bs.len);
    for (as, bs) |a, b| {
        assert(a.size == b.size);
        assert(std.mem.eql(u8, a.path, b.path));
    }
}

test "can list zero files" {
    var memory_storage = Storage.init(std.testing.allocator, 0);
    defer memory_storage.deinit();

    var files_in: [0]storage.File = undefined;
    var files_buf: []storage.File = files_in[0..];
    const files_out = try memory_storage.listFiles("topic1", ".file", null, &files_buf);
    assertFilesEqual(files_buf, files_out);
}

test "can list multiple files" {
    var memory_storage = Storage.init(std.testing.allocator, 0);
    defer memory_storage.deinit();

    const files_num = 10;
    const key_length = 15;

    var files_expected: [files_num]storage.File = undefined;
    var file_name_bufs: [files_num][key_length]u8 = undefined;
    for (0..files_expected.len) |i| {
        var key = file_name_bufs[i][0..];
        files_expected[i].path = try std.fmt.bufPrint(key[0..], "topic/key{d}.file", .{i});

        var wtr = try memory_storage.writer(key);
        defer wtr.close();

        files_expected[i].size = try wtr.write(key);
    }

    var files_in: [files_num]storage.File = undefined;
    var files_buf: []storage.File = files_in[0..];
    const files_out = try memory_storage.listFiles("topic", ".file", null, &files_buf);

    assert(files_expected.len == files_out.len);
    assertFilesEqual(files_expected[0..], files_out);
}

test "file buffer not large enough" {
    var memory_storage = Storage.init(std.testing.allocator, 0);
    defer memory_storage.deinit();

    var wtr = try memory_storage.writer("topic/name.file");
    defer wtr.close();

    _ = try wtr.write("contents");

    var files_in: [0]storage.File = undefined;
    var files_buf: []storage.File = files_in[0..];
    try std.testing.expectError(error.BufferTooSmall, memory_storage.listFiles("topic", ".file", null, &files_buf));
}

test "can read and write files" {
    var memory_storage = Storage.init(std.testing.allocator, 128);
    defer memory_storage.deinit();

    var wtr = try memory_storage.writer("topic/key");
    defer wtr.close();

    const input = "hello buf!";
    const write_size = try wtr.write(input);
    assert(write_size == input.len);

    const rdr = try memory_storage.reader("topic/key");
    defer rdr.close();

    var buf: [input.len]u8 = undefined;
    const read_size = try rdr.readAt(buf[0..], 0);
    assert(read_size == buf.len);
    assert(std.mem.eql(u8, buf[0..], input));
}

test "returns KeyNotFound" {
    var memory_storage = Storage.init(std.testing.allocator, 128);
    defer memory_storage.deinit();

    try std.testing.expectError(error.KeyNotFound, memory_storage.reader("topic/key"));
}
