const std = @import("std");
const http = std.http;
const types = @import("types.zig");
const credentials = @import("credentials.zig");
const list_objects = @import("list_objects.zig");
const get_object = @import("get_object.zig");
const put_object = @import("put_object.zig");
const signing = @import("signing.zig");

pub const Client = struct {
    allocator: std.mem.Allocator,
    http_client: http.Client,
    config: types.Config,
    owned_credentials: ?credentials.OwnedCredentials = null,
    discovery: ?credentials.Discovery = null,

    pub fn init(allocator: std.mem.Allocator, io: std.Io, config: types.Config) Client {
        return .{
            .allocator = allocator,
            .http_client = .{
                .allocator = allocator,
                .io = io,
            },
            .config = config,
        };
    }

    pub fn initDefault(
        allocator: std.mem.Allocator,
        io: std.Io,
        environ: *const std.process.Environ.Map,
        options: types.DefaultOptions,
    ) !Client {
        var self: Client = .{
            .allocator = allocator,
            .http_client = .{
                .allocator = allocator,
                .io = io,
            },
            .config = undefined,
            .discovery = .{
                .environ = environ,
                .options = options,
            },
        };
        errdefer self.http_client.deinit();

        var found = try credentials.discoverCredentials(
            allocator,
            io,
            &self.http_client,
            environ,
            options,
        );
        errdefer found.deinit(allocator);

        self.config = .{
            .access_key_id = found.access_key_id,
            .secret_access_key = found.secret_access_key,
            .session_token = found.session_token,
            .region = found.region,
            .endpoint = options.endpoint,
            .path_style = options.path_style,
        };
        self.owned_credentials = found;

        return self;
    }

    pub fn deinit(self: *Client) void {
        if (self.owned_credentials) |*value| {
            value.deinit(self.allocator);
        }

        self.http_client.deinit();
    }

    pub fn listObjects(
        self: *Client,
        bucket: []const u8,
        options: types.ListOptions,
    ) !types.Page {
        try self.refreshCredentials();
        return list_objects.execute(self, bucket, options);
    }

    pub fn getObject(self: *Client, bucket: []const u8, key: []const u8, destination: []u8) ![]u8 {
        try self.refreshCredentials();
        return get_object.execute(self, bucket, key, destination);
    }

    pub fn putObject(self: *Client, bucket: []const u8, key: []const u8, data: []u8) !void {
        try self.refreshCredentials();
        return put_object.execute(self, bucket, key, data);
    }

    pub fn listObjectsPaginator(
        self: *Client,
        bucket: []const u8,
        options: types.ListOptions,
    ) ListObjectsPaginator {
        return .{
            .client = self,
            .bucket = bucket,
            .options = options,
            .token = options.continuation_token,
        };
    }

    fn refreshCredentials(self: *Client) !void {
        const discovery = self.discovery orelse return;
        const current = &(self.owned_credentials orelse return);

        const refresh_window_seconds = 5 * 60;
        const expires_after_refresh_window = current.expiration == null or
            signing.now() + refresh_window_seconds < current.expiration.?;
        if (expires_after_refresh_window) {
            return;
        }

        var replacement = try credentials.discoverCredentials(
            self.allocator,
            self.http_client.io,
            &self.http_client,
            discovery.environ,
            discovery.options,
        );
        errdefer replacement.deinit(self.allocator);

        self.owned_credentials.?.deinit(self.allocator);
        self.owned_credentials = replacement;

        self.config.access_key_id = replacement.access_key_id;
        self.config.secret_access_key = replacement.secret_access_key;
        self.config.session_token = replacement.session_token;
        self.config.region = replacement.region;
    }
};

pub const ListObjectsPaginator = struct {
    client: *Client,
    bucket: []const u8,
    options: types.ListOptions,
    token: ?[]const u8,
    owned_token: ?[]u8 = null,
    done: bool = false,

    pub fn deinit(self: *ListObjectsPaginator) void {
        if (self.owned_token) |value| {
            self.client.allocator.free(value);
        }

        self.* = undefined;
    }

    pub fn next(self: *ListObjectsPaginator) !?types.Page {
        if (self.done) {
            return null;
        }

        var options = self.options;
        options.continuation_token = self.token;

        var page = try self.client.listObjects(self.bucket, options);
        errdefer page.deinit();

        const next_token: ?[]u8 = if (page.is_truncated) blk: {
            const token = page.next_continuation_token orelse
                return error.InvalidResponse;
            break :blk try self.client.allocator.dupe(u8, token);
        } else null;

        if (self.owned_token) |value| {
            self.client.allocator.free(value);
        }

        self.owned_token = next_token;
        self.token = next_token;
        self.done = !page.is_truncated;

        return page;
    }
};
