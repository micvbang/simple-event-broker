const std = @import("std");

pub const Config = struct {
    access_key_id: []const u8,
    secret_access_key: []const u8,
    session_token: ?[]const u8 = null,
    region: []const u8 = "us-east-1",
    endpoint: ?[]const u8 = null,
    path_style: bool = false,
};

pub const DefaultOptions = struct {
    region: ?[]const u8 = null,
    profile: ?[]const u8 = null,
    credentials_file: ?[]const u8 = null,
    config_file: ?[]const u8 = null,
    endpoint: ?[]const u8 = null,
    path_style: bool = false,
    max_credentials_response_bytes: usize = 64 * 1024,
};

pub const ListOptions = struct {
    prefix: ?[]const u8 = null,
    delimiter: ?[]const u8 = null,
    continuation_token: ?[]const u8 = null,
    max_keys: ?u16 = null,
    max_response_bytes: usize = 16 * 1024 * 1024,
};

pub const Object = struct {
    key: []const u8,
    last_modified: []const u8,
    etag: []const u8,
    size: u64,
};

pub const Page = struct {
    allocator: std.mem.Allocator,
    body: []u8,
    objects: []Object,
    common_prefixes: [][]const u8,
    is_truncated: bool,
    next_continuation_token: ?[]const u8,

    pub fn deinit(self: *Page) void {
        self.allocator.free(self.common_prefixes);
        self.allocator.free(self.objects);
        self.allocator.free(self.body);
        self.* = undefined;
    }
};
