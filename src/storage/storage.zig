pub const File = struct {
    size: usize,
    path: []const u8,
};

pub const VTable = struct {
    listFiles: *const fn (context: *anyopaque, topic_name: []const u8, ext: []const u8, start_after: ?[]const u8, files: *[]File) anyerror![]File,
    reader: *const fn (context: *anyopaque, key: []const u8) anyerror!Reader,
    writer: *const fn (context: *anyopaque, key: []const u8) anyerror!Writer,
    deinit: *const fn (context: *anyopaque) void,
};

pub const Storage = struct {
    context: *anyopaque,
    vtable: *const VTable,

    pub fn listFiles(self: Storage, topic_name: []const u8, ext: []const u8, startAfter: ?[]const u8, files: *[]File) anyerror![]File {
        return self.vtable.listFiles(self.context, topic_name, ext, startAfter, files);
    }

    pub fn reader(self: Storage, key: []const u8) anyerror!Reader {
        return self.vtable.reader(self.context, key);
    }

    pub fn writer(self: Storage, key: []const u8) anyerror!Writer {
        return self.vtable.writer(self.context, key);
    }

    pub fn deinit(self: Storage) void {
        self.vtable.deinit(self.context);
    }
};

pub const Reader = struct {
    context: *anyopaque,
    vtable: *const ReaderVTable,

    pub fn readAt(self: Reader, buf: []u8, offset: usize) anyerror!usize {
        return self.vtable.readAt(self.context, buf, offset);
    }

    pub fn close(self: Reader) void {
        self.vtable.close(self.context);
    }
};

pub const ReaderVTable = struct {
    readAt: *const fn (context: *anyopaque, buf: []u8, offset: usize) anyerror!usize,
    close: *const fn (context: *anyopaque) void,
};

pub const Writer = struct {
    context: *anyopaque,
    vtable: *const WriterVTable,

    pub fn write(self: Writer, buf: []const u8) anyerror!usize {
        return self.vtable.write(self.context, buf);
    }

    pub fn close(self: Writer) void {
        self.vtable.close(self.context);
    }
};

pub const WriterVTable = struct {
    write: *const fn (context: *anyopaque, buf: []const u8) anyerror!usize,
    close: *const fn (context: *anyopaque) void,
};
