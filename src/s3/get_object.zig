const std = @import("std");
const http = std.http;
const signing = @import("signing.zig");

/// Fetches `key` into caller-owned storage and returns its initialized portion.
/// Request construction and body storage do not use an allocator. std.http may
/// allocate when establishing a connection or initializing TLS state.
pub fn execute(client: anytype, bucket: []const u8, key: []const u8, destination: []u8) ![]u8 {
    var host_buffer: [1024]u8 = undefined;
    var base_buffer: [1536]u8 = undefined;
    var virtual_host_buffer: [1536]u8 = undefined;
    var path_buffer: [4096]u8 = undefined;
    var url_buffer: [5632]u8 = undefined;
    const parts = try buildEndpoint(client.config, bucket, key, &host_buffer, &base_buffer, &virtual_host_buffer, &path_buffer, &url_buffer);

    var date: [16]u8 = undefined;
    signing.formatDate(signing.now(), &date);
    var canonical_buffer: [8192]u8 = undefined;
    var authorization_buffer: [1024]u8 = undefined;
    const authorization = try signing.signInto(client.config, parts.host, parts.path, "", &date, &canonical_buffer, &authorization_buffer);

    var headers: [5]http.Header = undefined;
    headers[0] = .{ .name = "X-Amz-Date", .value = &date };
    headers[1] = .{ .name = "X-Amz-Content-Sha256", .value = signing.empty_hash };
    headers[2] = .{ .name = "Authorization", .value = authorization };
    headers[3] = .{ .name = "Accept-Encoding", .value = "identity" };
    var header_count: usize = 4;
    if (client.config.session_token) |token| {
        headers[4] = .{ .name = "X-Amz-Security-Token", .value = token };
        header_count = 5;
    }

    const uri = std.Uri.parse(parts.url) catch return error.InvalidUri;
    var request = client.http_client.request(.GET, uri, .{ .extra_headers = headers[0..header_count] }) catch return error.ConnectionFailed;
    defer request.deinit();
    request.sendBodiless() catch return error.SendFailed;
    request.connection.?.flush() catch return error.SendFailed;

    var head_buffer: [8192]u8 = undefined;
    var response = request.receiveHead(&head_buffer) catch return error.ReceiveFailed;
    var transfer_buffer: [64]u8 = undefined;
    const body = response.reader(&transfer_buffer);
    if (response.head.status != .ok) {
        _ = body.discardRemaining() catch {};
        return error.S3Error;
    }
    if (response.head.content_length) |content_length| {
        if (content_length > destination.len) {
            _ = body.discardRemaining() catch {};
            return error.BufferTooSmall;
        }
    }

    const length = body.readSliceShort(destination) catch return error.ReceiveFailed;
    if (length == destination.len) {
        var extra: [1]u8 = undefined;
        if ((body.readSliceShort(&extra) catch return error.ReceiveFailed) != 0) {
            _ = body.discardRemaining() catch {};
            return error.BufferTooSmall;
        }
    }
    return destination[0..length];
}

pub const Endpoint = struct { host: []const u8, path: []const u8, url: []const u8 };

pub fn buildEndpoint(config: anytype, bucket: []const u8, key: []const u8, host_buffer: []u8, base_buffer: []u8, virtual_host_buffer: []u8, path_buffer: []u8, url_buffer: []u8) !Endpoint {
    const base = if (config.endpoint) |value| std.mem.trimEnd(u8, value, "/") else std.fmt.bufPrint(base_buffer, "https://s3.{s}.amazonaws.com", .{config.region}) catch return error.RequestTooLong;
    const uri = std.Uri.parse(base) catch return error.InvalidEndpoint;
    const raw_host = switch (uri.host orelse return error.InvalidEndpoint) {
        .raw => |value| value,
        .percent_encoded => |value| value,
    };
    const endpoint_host = if (uri.port) |port| std.fmt.bufPrint(host_buffer, "{s}:{d}", .{ raw_host, port }) catch return error.RequestTooLong else std.fmt.bufPrint(host_buffer, "{s}", .{raw_host}) catch return error.RequestTooLong;
    const virtual = config.endpoint == null and !config.path_style;
    const host = if (virtual) std.fmt.bufPrint(virtual_host_buffer, "{s}.{s}", .{ bucket, endpoint_host }) catch return error.RequestTooLong else endpoint_host;

    var path_writer: std.Io.Writer = .fixed(path_buffer);
    path_writer.writeByte('/') catch return error.RequestTooLong;
    if (!virtual) {
        try encode(&path_writer, bucket, false);
        path_writer.writeByte('/') catch return error.RequestTooLong;
    }
    try encode(&path_writer, key, true);
    const path = path_writer.buffered();
    const url = std.fmt.bufPrint(url_buffer, "{s}://{s}{s}", .{ uri.scheme, host, path }) catch return error.RequestTooLong;
    return .{ .host = host, .path = path, .url = url };
}

fn encode(writer: *std.Io.Writer, value: []const u8, preserve_slash: bool) !void {
    const hex = "0123456789ABCDEF";
    for (value) |byte| {
        const unreserved = std.ascii.isAlphanumeric(byte) or byte == '-' or byte == '_' or byte == '.' or byte == '~' or (preserve_slash and byte == '/');
        if (unreserved) writer.writeByte(byte) catch return error.RequestTooLong else writer.writeAll(&.{ '%', hex[byte >> 4], hex[byte & 15] }) catch return error.RequestTooLong;
    }
}

test "object key path encoding preserves separators" {
    var output: [128]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&output);
    try encode(&writer, "folder/a b+%.txt", true);
    try std.testing.expectEqualStrings("folder/a%20b%2B%25.txt", writer.buffered());
}

test "default endpoint uses virtual-hosted addressing" {
    var host_buffer: [128]u8 = undefined;
    var base_buffer: [128]u8 = undefined;
    var virtual_host_buffer: [128]u8 = undefined;
    var path_buffer: [128]u8 = undefined;
    var url_buffer: [256]u8 = undefined;
    const endpoint = try buildEndpoint(
        @import("types.zig").Config{ .access_key_id = "key", .secret_access_key = "secret", .region = "eu-west-1" },
        "bucket",
        "folder/a b",
        &host_buffer,
        &base_buffer,
        &virtual_host_buffer,
        &path_buffer,
        &url_buffer,
    );
    try std.testing.expectEqualStrings("bucket.s3.eu-west-1.amazonaws.com", endpoint.host);
    try std.testing.expectEqualStrings("/folder/a%20b", endpoint.path);
    try std.testing.expectEqualStrings("https://bucket.s3.eu-west-1.amazonaws.com/folder/a%20b", endpoint.url);
}
