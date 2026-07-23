const std = @import("std");
const http = std.http;
const get_object = @import("get_object.zig");
const signing = @import("signing.zig");

/// Uploads `data` directly from memory. Request construction, signing, and
/// body storage do not use an allocator or stage data on disk. std.http may
/// allocate when establishing a connection or initializing TLS state.
pub fn execute(client: anytype, bucket: []const u8, key: []const u8, data: []u8) !void {
    var host_buffer: [1024]u8 = undefined;
    var base_buffer: [1536]u8 = undefined;
    var virtual_host_buffer: [1536]u8 = undefined;
    var path_buffer: [4096]u8 = undefined;
    var url_buffer: [5632]u8 = undefined;
    const parts = try get_object.buildEndpoint(client.config, bucket, key, &host_buffer, &base_buffer, &virtual_host_buffer, &path_buffer, &url_buffer);

    var payload_hash: [64]u8 = undefined;
    signing.hashPayload(data, &payload_hash);
    var date: [16]u8 = undefined;
    signing.formatDate(signing.now(), &date);
    var canonical_buffer: [8192]u8 = undefined;
    var authorization_buffer: [1024]u8 = undefined;
    const authorization = try signing.signRequestInto(client.config, "PUT", parts.host, parts.path, "", &payload_hash, &date, &canonical_buffer, &authorization_buffer);

    var headers: [5]http.Header = undefined;
    headers[0] = .{ .name = "X-Amz-Date", .value = &date };
    headers[1] = .{ .name = "X-Amz-Content-Sha256", .value = &payload_hash };
    headers[2] = .{ .name = "Authorization", .value = authorization };
    headers[3] = .{ .name = "Accept-Encoding", .value = "identity" };
    var header_count: usize = 4;
    if (client.config.session_token) |token| {
        headers[4] = .{ .name = "X-Amz-Security-Token", .value = token };
        header_count = 5;
    }

    const uri = std.Uri.parse(parts.url) catch return error.InvalidUri;
    var request = client.http_client.request(.PUT, uri, .{ .extra_headers = headers[0..header_count] }) catch return error.ConnectionFailed;
    defer request.deinit();
    request.sendBodyComplete(data) catch return error.SendFailed;

    var head_buffer: [8192]u8 = undefined;
    var response = request.receiveHead(&head_buffer) catch return error.ReceiveFailed;
    var transfer_buffer: [64]u8 = undefined;
    const body = response.reader(&transfer_buffer);
    _ = body.discardRemaining() catch return error.ReceiveFailed;
    if (response.head.status != .ok) return error.S3Error;
}

test "payload hash" {
    var actual: [64]u8 = undefined;
    signing.hashPayload("hello", &actual);
    try std.testing.expectEqualStrings("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", &actual);
}
