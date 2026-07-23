const std = @import("std");
const http = std.http;
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const signing = @import("signing.zig");
const Config = types.Config;
const ListOptions = types.ListOptions;
const xml = @import("xml.zig");
const Endpoint = struct {
    origin: []u8,
    host: []u8,
    path: []u8,

    fn deinit(self: Endpoint, a: Allocator) void {
        a.free(self.path);
        a.free(self.host);
        a.free(self.origin);
    }
};

fn endpoint(allocator: Allocator, config: Config, bucket: []const u8) !Endpoint {
    const allocated = config.endpoint == null;
    const base = if (config.endpoint) |configured_endpoint|
        std.mem.trimEnd(u8, configured_endpoint, "/")
    else
        try std.fmt.allocPrint(
            allocator,
            "https://s3.{s}.amazonaws.com",
            .{config.region},
        );
    defer {
        if (allocated) {
            allocator.free(base);
        }
    }

    const uri = std.Uri.parse(base) catch return error.InvalidEndpoint;
    const raw_host = switch (uri.host orelse return error.InvalidEndpoint) {
        .raw => |v| v,
        .percent_encoded => |v| v,
    };
    const virtual = config.endpoint == null and !config.path_style;
    const endpoint_host =
        if (uri.port) |port|
            try std.fmt.allocPrint(allocator, "{s}:{d}", .{ raw_host, port })
        else
            try allocator.dupe(u8, raw_host);
    defer allocator.free(endpoint_host);

    const host = if (virtual)
        try std.fmt.allocPrint(allocator, "{s}.{s}", .{ bucket, endpoint_host })
    else
        try allocator.dupe(u8, endpoint_host);
    errdefer allocator.free(host);

    const origin = try std.fmt.allocPrint(allocator, "{s}://{s}", .{ uri.scheme, host });
    errdefer allocator.free(origin);

    const path = if (virtual)
        try allocator.dupe(u8, "/")
    else
        try std.fmt.allocPrint(allocator, "/{s}", .{bucket});

    return .{ .origin = origin, .host = host, .path = path };
}

fn buildQuery(a: Allocator, o: ListOptions) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);

    if (o.continuation_token) |value| {
        try param(&out, a, "continuation-token", value);
    }
    if (o.delimiter) |value| {
        try param(&out, a, "delimiter", value);
    }

    try param(&out, a, "encoding-type", "url");
    try param(&out, a, "list-type", "2");

    if (o.max_keys) |v| {
        var b: [5]u8 = undefined;
        try param(&out, a, "max-keys", std.fmt.bufPrint(&b, "{d}", .{v}) catch unreachable);
    }

    if (o.prefix) |value| {
        try param(&out, a, "prefix", value);
    }

    return out.toOwnedSlice(a);
}

fn param(out: *std.ArrayList(u8), a: Allocator, key: []const u8, value: []const u8) !void {
    if (out.items.len > 0) {
        try out.append(a, '&');
    }

    try out.appendSlice(a, key);
    try out.append(a, '=');

    const hex = "0123456789ABCDEF";
    for (value) |byte| {
        const is_unreserved = std.ascii.isAlphanumeric(byte) or
            byte == '-' or byte == '_' or byte == '.' or byte == '~';

        if (is_unreserved) {
            try out.append(a, byte);
        } else {
            try out.appendSlice(a, &.{ '%', hex[byte >> 4], hex[byte & 15] });
        }
    }
}

pub fn execute(client: anytype, bucket: []const u8, options: types.ListOptions) !types.Page {
    const query = try buildQuery(client.allocator, options);
    defer client.allocator.free(query);

    const ep = try endpoint(client.allocator, client.config, bucket);
    defer ep.deinit(client.allocator);

    const url = try std.fmt.allocPrint(client.allocator, "{s}{s}?{s}", .{ ep.origin, ep.path, query });
    defer client.allocator.free(url);

    var date: [16]u8 = undefined;
    signing.formatDate(signing.now(), &date);

    const auth = try signing.sign(client.allocator, client.config, ep.host, ep.path, query, &date);
    defer client.allocator.free(auth);

    var headers: [5]http.Header = undefined;
    headers[0] = .{ .name = "X-Amz-Date", .value = &date };
    headers[1] = .{ .name = "X-Amz-Content-Sha256", .value = signing.empty_hash };
    headers[2] = .{ .name = "Authorization", .value = auth };
    headers[3] = .{ .name = "Accept-Encoding", .value = "identity" };
    var n: usize = 4;
    if (client.config.session_token) |token| {
        headers[4] = .{ .name = "X-Amz-Security-Token", .value = token };
        n = 5;
    }

    const uri = std.Uri.parse(url) catch return error.InvalidUri;
    var request = client.http_client.request(.GET, uri, .{ .extra_headers = headers[0..n] }) catch return error.ConnectionFailed;
    defer request.deinit();

    request.sendBodiless() catch return error.SendFailed;
    request.connection.?.flush() catch return error.SendFailed;

    var head_buffer: [8192]u8 = undefined;
    var response = request.receiveHead(&head_buffer) catch return error.ReceiveFailed;
    var transfer: [64]u8 = undefined;
    var decompress: http.Decompress = undefined;
    var window: [std.compress.flate.max_window_len]u8 = undefined;
    const body = response.readerDecompressing(&transfer, &decompress, &window).allocRemaining(client.allocator, .limited(options.max_response_bytes)) catch return error.ResponseTooLarge;
    errdefer client.allocator.free(body);

    if (response.head.status != .ok) {
        return error.S3Error;
    }

    return xml.parsePage(client.allocator, body);
}

test "query sorting and encoding" {
    const q = try buildQuery(std.testing.allocator, .{ .prefix = "a/b c", .continuation_token = "a+b=" });
    defer std.testing.allocator.free(q);
    try std.testing.expectEqualStrings("continuation-token=a%2Bb%3D&encoding-type=url&list-type=2&prefix=a%2Fb%20c", q);
}
