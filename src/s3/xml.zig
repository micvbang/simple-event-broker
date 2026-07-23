const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");

pub fn parsePage(allocator: Allocator, body: []u8) !types.Page {
    const object_count = occurrences(body, "<Contents>");
    const objects = try allocator.alloc(types.Object, object_count);
    errdefer allocator.free(objects);

    const prefix_count = occurrences(body, "<CommonPrefixes>");
    const prefixes = try allocator.alloc([]const u8, prefix_count);
    errdefer allocator.free(prefixes);

    var object_iterator = std.mem.splitSequence(u8, body, "<Contents>");
    _ = object_iterator.first();

    var object_index: usize = 0;
    while (object_iterator.next()) |remainder| : (object_index += 1) {
        const block = before(
            @constCast(remainder),
            "</Contents>",
        ) orelse return error.InvalidResponse;

        const size_text = tag(block, "Size") orelse
            return error.InvalidResponse;
        const size = std.fmt.parseInt(u64, size_text, 10) catch
            return error.InvalidResponse;

        objects[object_index] = .{
            .key = try decodedTag(block, "Key"),
            .last_modified = tag(block, "LastModified") orelse
                return error.InvalidResponse,
            .etag = tag(block, "ETag") orelse
                return error.InvalidResponse,
            .size = size,
        };
    }

    var prefix_iterator = std.mem.splitSequence(u8, body, "<CommonPrefixes>");
    _ = prefix_iterator.first();

    var prefix_index: usize = 0;
    while (prefix_iterator.next()) |remainder| : (prefix_index += 1) {
        const block = before(
            @constCast(remainder),
            "</CommonPrefixes>",
        ) orelse return error.InvalidResponse;

        prefixes[prefix_index] = try decodedTag(block, "Prefix");
    }

    const truncated = tag(body, "IsTruncated") orelse
        return error.InvalidResponse;

    return .{
        .allocator = allocator,
        .body = body,
        .objects = objects,
        .common_prefixes = prefixes,
        .is_truncated = std.mem.eql(u8, truncated, "true"),
        .next_continuation_token = tag(body, "NextContinuationToken"),
    };
}

fn occurrences(haystack: []const u8, needle: []const u8) usize {
    var count: usize = 0;
    var remainder = haystack;

    while (std.mem.indexOf(u8, remainder, needle)) |index| {
        count += 1;
        remainder = remainder[index + needle.len ..];
    }

    return count;
}

fn before(value: []u8, delimiter: []const u8) ?[]u8 {
    const end = std.mem.indexOf(u8, value, delimiter) orelse return null;
    return value[0..end];
}

fn tag(value: []u8, name: []const u8) ?[]u8 {
    var opening_buffer: [64]u8 = undefined;
    const opening = std.fmt.bufPrint(
        &opening_buffer,
        "<{s}>",
        .{name},
    ) catch return null;

    var closing_buffer: [65]u8 = undefined;
    const closing = std.fmt.bufPrint(
        &closing_buffer,
        "</{s}>",
        .{name},
    ) catch return null;

    const opening_index = std.mem.indexOf(u8, value, opening) orelse return null;
    const content_start = opening_index + opening.len;
    const content_length = std.mem.indexOf(
        u8,
        value[content_start..],
        closing,
    ) orelse return null;

    return value[content_start .. content_start + content_length];
}

fn decodedTag(value: []u8, name: []const u8) ![]u8 {
    const encoded = tag(value, name) orelse return error.InvalidResponse;

    var read_index: usize = 0;
    var write_index: usize = 0;

    while (read_index < encoded.len) {
        if (encoded[read_index] == '%') {
            if (read_index + 2 >= encoded.len) {
                return error.InvalidResponse;
            }

            const high = std.fmt.charToDigit(encoded[read_index + 1], 16) catch
                return error.InvalidResponse;
            const low = std.fmt.charToDigit(encoded[read_index + 2], 16) catch
                return error.InvalidResponse;

            encoded[write_index] = (high << 4) | low;
            read_index += 3;
        } else {
            encoded[write_index] = encoded[read_index];
            read_index += 1;
        }

        write_index += 1;
    }

    return encoded[0..write_index];
}

test "response-backed page" {
    const document = "<ListBucketResult><IsTruncated>true</IsTruncated><Contents><Key>a%2Fb</Key><LastModified>now</LastModified><ETag>x</ETag><Size>123</Size></Contents><CommonPrefixes><Prefix>a%2F</Prefix></CommonPrefixes><NextContinuationToken>next</NextContinuationToken></ListBucketResult>";
    const body = try std.testing.allocator.dupe(u8, document);

    var page = try parsePage(std.testing.allocator, body);
    defer page.deinit();

    try std.testing.expectEqualStrings("a/b", page.objects[0].key);
    try std.testing.expectEqual(@as(u64, 123), page.objects[0].size);
}
