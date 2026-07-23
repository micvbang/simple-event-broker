const std = @import("std");
const Allocator = std.mem.Allocator;
const Config = @import("types.zig").Config;

pub const empty_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

pub fn sign(allocator: Allocator, config: Config, host: []const u8, path: []const u8, query: []const u8, date: *const [16]u8) ![]u8 {
    var canonical_buffer: [8192]u8 = undefined;
    var authorization_buffer: [1024]u8 = undefined;
    const authorization = try signInto(config, host, path, query, date, &canonical_buffer, &authorization_buffer);
    return allocator.dupe(u8, authorization);
}

/// Writes a SigV4 Authorization value into caller-owned storage without allocating.
pub fn signInto(config: Config, host: []const u8, path: []const u8, query: []const u8, date: *const [16]u8, canonical_buffer: []u8, authorization_buffer: []u8) ![]const u8 {
    return signRequestInto(config, "GET", host, path, query, empty_hash, date, canonical_buffer, authorization_buffer);
}

/// Writes a SigV4 Authorization value for an S3 request into caller-owned storage.
pub fn signRequestInto(config: Config, method: []const u8, host: []const u8, path: []const u8, query: []const u8, payload_hash: []const u8, date: *const [16]u8, canonical_buffer: []u8, authorization_buffer: []u8) ![]const u8 {
    const names = if (config.session_token == null)
        "host;x-amz-content-sha256;x-amz-date"
    else
        "host;x-amz-content-sha256;x-amz-date;x-amz-security-token";

    var canonical: std.Io.Writer = .fixed(canonical_buffer);

    canonical.print("{s}\n{s}\n{s}\nhost:{s}\nx-amz-content-sha256:{s}\nx-amz-date:{s}\n", .{ method, path, query, host, payload_hash, date }) catch return error.RequestTooLong;
    if (config.session_token) |token| {
        canonical.print(
            "x-amz-security-token:{s}\n",
            .{token},
        ) catch return error.RequestTooLong;
    }
    canonical.print("\n{s}\n{s}", .{ names, payload_hash }) catch return error.RequestTooLong;

    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(canonical.buffered(), &digest, .{});

    var digest_hex: [64]u8 = undefined;
    hexLower(&digest, &digest_hex);

    const day = date[0..8];
    var sb: [256]u8 = undefined;
    const sts = std.fmt.bufPrint(&sb, "AWS4-HMAC-SHA256\n{s}\n{s}/{s}/s3/aws4_request\n{s}", .{ date, day, config.region, &digest_hex }) catch return error.RequestTooLong;

    var kb: [256]u8 = undefined;
    const secret = std.fmt.bufPrint(&kb, "AWS4{s}", .{config.secret_access_key}) catch return error.SecretKeyTooLong;

    var kd: [32]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(&kd, day, secret);

    var kr: [32]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(&kr, config.region, &kd);

    var ks: [32]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(&ks, "s3", &kr);

    var key: [32]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(&key, "aws4_request", &ks);

    var signature: [32]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(&signature, sts, &key);

    var signature_hex: [64]u8 = undefined;
    hexLower(&signature, &signature_hex);

    return std.fmt.bufPrint(
        authorization_buffer,
        "AWS4-HMAC-SHA256 Credential={s}/{s}/{s}/s3/aws4_request, SignedHeaders={s}, Signature={s}",
        .{ config.access_key_id, day, config.region, names, &signature_hex },
    ) catch return error.RequestTooLong;
}

/// Computes the lowercase SHA-256 digest required by `x-amz-content-sha256`.
pub fn hashPayload(payload: []const u8, out: *[64]u8) void {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload, &digest, .{});
    hexLower(&digest, out);
}

pub fn now() i64 {
    var tv: std.posix.system.timeval = undefined;
    _ = std.posix.system.gettimeofday(&tv, null);
    return tv.sec;
}

fn leap(y: u32) bool {
    return (y % 4 == 0 and y % 100 != 0) or y % 400 == 0;
}

pub fn formatDate(ts: i64, out: *[16]u8) void {
    const s: u64 = @intCast(ts);
    var days = s / 86400;
    const ds = s % 86400;
    var year: u32 = 1970;
    while (true) {
        const days_in_year: u16 = if (leap(year))
            366
        else
            365;
        if (days < days_in_year) {
            break;
        }
        days -= days_in_year;
        year += 1;
    }
    const month_days = if (leap(year))
        [_]u8{ 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 }
    else
        [_]u8{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

    var month: u32 = 1;
    for (month_days) |n| {
        if (days < n) {
            break;
        }
        days -= n;
        month += 1;
    }
    _ = std.fmt.bufPrint(out, "{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z", .{ year, month, days + 1, ds / 3600, (ds % 3600) / 60, ds % 60 }) catch unreachable;
}

fn hexLower(input: []const u8, out: *[64]u8) void {
    const hex = "0123456789abcdef";
    for (input, 0..) |b, i| {
        out[i * 2] = hex[b >> 4];
        out[i * 2 + 1] = hex[b & 15];
    }
}

test "date formatting" {
    var date: [16]u8 = undefined;
    formatDate(951782400, &date);
    try std.testing.expectEqualStrings("20000229T000000Z", &date);
}
