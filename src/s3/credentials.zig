const std = @import("std");
const http = std.http;
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
pub const DefaultOptions = types.DefaultOptions;
pub const Discovery = struct {
    environ: *const std.process.Environ.Map,
    options: DefaultOptions,
};
pub const OwnedCredentials = struct {
    access_key_id: []u8,
    secret_access_key: []u8,
    session_token: ?[]u8,
    region: []u8,
    expiration: ?i64 = null,
    pub fn deinit(self: *OwnedCredentials, a: Allocator) void {
        a.free(self.access_key_id);
        a.free(self.secret_access_key);
        if (self.session_token) |token| {
            a.free(token);
        }
        a.free(self.region);
        self.* = undefined;
    }
};
const ProfileValues = struct {
    access_key_id: ?[]const u8 = null,
    secret_access_key: ?[]const u8 = null,
    session_token: ?[]const u8 = null,
    region: ?[]const u8 = null,
};

pub fn discoverCredentials(a: Allocator, io: std.Io, client: *http.Client, env: *const std.process.Environ.Map, options: DefaultOptions) !OwnedCredentials {
    const selected_profile = options.profile orelse env.get("AWS_PROFILE") orelse env.get("AWS_DEFAULT_PROFILE");
    if (selected_profile) |profile| {
        return (try credentialsFromProfile(a, io, env, options, profile)) orelse error.ProfileCredentialsNotFound;
    }
    if (env.get("AWS_ACCESS_KEY_ID")) |access| {
        const secret = env.get("AWS_SECRET_ACCESS_KEY") orelse return error.IncompleteEnvironmentCredentials;
        return ownCredentials(a, access, secret, env.get("AWS_SESSION_TOKEN"), resolveRegion(options, env, null), null);
    } else if (env.get("AWS_SECRET_ACCESS_KEY") != null) {
        return error.IncompleteEnvironmentCredentials;
    }

    if (try credentialsFromProfile(a, io, env, options, "default")) |credentials| {
        return credentials;
    }
    if (env.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")) |relative| {
        if (relative.len == 0 or relative[0] != '/') {
            return error.InvalidContainerCredentialsUri;
        }
        const url = try std.fmt.allocPrint(a, "http://169.254.170.2{s}", .{relative});
        defer a.free(url);
        return credentialsFromEndpoint(a, io, client, env, options, url);
    }
    if (env.get("AWS_CONTAINER_CREDENTIALS_FULL_URI")) |url| {
        try validateContainerUrl(url);
        return credentialsFromEndpoint(a, io, client, env, options, url);
    }
    if (isTrue(env.get("AWS_EC2_METADATA_DISABLED"))) {
        return error.CredentialsNotFound;
    }
    return credentialsFromImds(a, io, client, env, options);
}

fn resolveRegion(options: DefaultOptions, env: *const std.process.Environ.Map, profile_region: ?[]const u8) []const u8 {
    return options.region orelse env.get("AWS_REGION") orelse env.get("AWS_DEFAULT_REGION") orelse profile_region orelse "us-east-1";
}

fn ownCredentials(a: Allocator, access: []const u8, secret: []const u8, token: ?[]const u8, region: []const u8, expiration: ?i64) !OwnedCredentials {
    if (access.len == 0 or secret.len == 0) {
        return error.IncompleteCredentials;
    }
    const owned_access = try a.dupe(u8, access);
    errdefer a.free(owned_access);
    const owned_secret = try a.dupe(u8, secret);
    errdefer a.free(owned_secret);
    const owned_token = if (token) |value|
        try a.dupe(u8, value)
    else
        null;
    errdefer {
        if (owned_token) |value| {
            a.free(value);
        }
    }
    const owned_region = try a.dupe(u8, region);
    return .{
        .access_key_id = owned_access,
        .secret_access_key = owned_secret,
        .session_token = owned_token,
        .region = owned_region,
        .expiration = expiration,
    };
}

fn credentialsFromProfile(a: Allocator, io: std.Io, env: *const std.process.Environ.Map, options: DefaultOptions, profile: []const u8) !?OwnedCredentials {
    const home = env.get("HOME");
    const credentials_path = options.credentials_file orelse env.get("AWS_SHARED_CREDENTIALS_FILE") orelse blk: {
        const h = home orelse return null;
        break :blk try std.fmt.allocPrint(a, "{s}/.aws/credentials", .{h});
    };
    const free_credentials_path = options.credentials_file == null and env.get("AWS_SHARED_CREDENTIALS_FILE") == null and home != null;
    defer {
        if (free_credentials_path) {
            a.free(credentials_path);
        }
    }
    const config_path = options.config_file orelse env.get("AWS_CONFIG_FILE") orelse blk: {
        const h = home orelse return null;
        break :blk try std.fmt.allocPrint(a, "{s}/.aws/config", .{h});
    };
    const free_config_path = options.config_file == null and env.get("AWS_CONFIG_FILE") == null and home != null;
    defer {
        if (free_config_path) {
            a.free(config_path);
        }
    }

    const credentials_text = try readSmallFile(a, io, credentials_path, 1024 * 1024);
    defer {
        if (credentials_text) |value| {
            a.free(value);
        }
    }
    const config_text = try readSmallFile(a, io, config_path, 1024 * 1024);
    defer {
        if (config_text) |value| {
            a.free(value);
        }
    }
    const from_credentials = if (credentials_text) |text|
        parseProfile(text, profile, false)
    else
        ProfileValues{};
    const from_config = if (config_text) |text|
        parseProfile(text, profile, true)
    else
        ProfileValues{};
    const access = from_credentials.access_key_id orelse from_config.access_key_id orelse return null;
    const secret = from_credentials.secret_access_key orelse from_config.secret_access_key orelse return error.IncompleteProfileCredentials;
    return @as(?OwnedCredentials, try ownCredentials(a, access, secret, from_credentials.session_token orelse from_config.session_token, resolveRegion(options, env, from_config.region orelse from_credentials.region), null));
}

fn parseProfile(text: []const u8, wanted: []const u8, config_file: bool) ProfileValues {
    var result: ProfileValues = .{};
    var active = false;
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0 or line[0] == '#' or line[0] == ';') {
            continue;
        }
        if (line[0] == '[' and line[line.len - 1] == ']') {
            var section = std.mem.trim(u8, line[1 .. line.len - 1], " \t");
            if (config_file and
                !std.mem.eql(u8, wanted, "default") and
                std.mem.startsWith(u8, section, "profile "))
            {
                section = std.mem.trim(u8, section[8..], " \t");
            }
            active = std.mem.eql(u8, section, wanted);
            continue;
        }
        if (!active) {
            continue;
        }
        const equals = std.mem.indexOfScalar(u8, line, '=') orelse continue;
        const key = std.mem.trim(u8, line[0..equals], " \t");
        const value = std.mem.trim(u8, line[equals + 1 ..], " \t");
        if (std.mem.eql(u8, key, "aws_access_key_id")) {
            result.access_key_id = value;
        } else if (std.mem.eql(u8, key, "aws_secret_access_key")) {
            result.secret_access_key = value;
        } else if (std.mem.eql(u8, key, "aws_session_token") or
            std.mem.eql(u8, key, "aws_security_token"))
        {
            result.session_token = value;
        } else if (std.mem.eql(u8, key, "region")) {
            result.region = value;
        }
    }
    return result;
}

fn readSmallFile(a: Allocator, io: std.Io, path: []const u8, max: usize) !?[]u8 {
    const file = if (std.fs.path.isAbsolute(path))
        std.Io.Dir.openFileAbsolute(io, path, .{}) catch return null
    else
        std.Io.Dir.cwd().openFile(io, path, .{}) catch return null;
    defer file.close(io);
    var buffer: [4096]u8 = undefined;
    var reader = file.reader(io, &buffer);
    return reader.interface.allocRemaining(a, .limited(max)) catch return error.CredentialsFileTooLarge;
}

const CredentialResponse = struct {
    AccessKeyId: []const u8,
    SecretAccessKey: []const u8,
    Token: ?[]const u8 = null,
    Expiration: ?[]const u8 = null,
};

fn credentialsFromEndpoint(a: Allocator, io: std.Io, client: *http.Client, env: *const std.process.Environ.Map, options: DefaultOptions, url: []const u8) !OwnedCredentials {
    var auth_file_body: ?[]u8 = null;
    defer {
        if (auth_file_body) |value| {
            a.free(value);
        }
    }

    const auth = if (env.get("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")) |path| blk: {
        auth_file_body = try readSmallFile(a, io, path, 16 * 1024) orelse return error.AuthorizationTokenFileNotFound;
        break :blk std.mem.trim(u8, auth_file_body.?, " \t\r\n");
    } else env.get("AWS_CONTAINER_AUTHORIZATION_TOKEN");

    if (auth) |value| {
        if (std.mem.indexOfAny(u8, value, "\r\n") != null) {
            return error.InvalidAuthorizationToken;
        }
    }

    const body = try metadataRequest(a, client, .GET, url, auth, null, options.max_credentials_response_bytes);
    defer a.free(body);

    return parseCredentialResponse(a, body, resolveRegion(options, env, null));
}

fn credentialsFromImds(a: Allocator, io: std.Io, client: *http.Client, env: *const std.process.Environ.Map, options: DefaultOptions) !OwnedCredentials {
    _ = io;
    const base = std.mem.trimEnd(u8, env.get("AWS_EC2_METADATA_SERVICE_ENDPOINT") orelse "http://169.254.169.254", "/");
    const token_url = try std.fmt.allocPrint(a, "{s}/latest/api/token", .{base});
    defer a.free(token_url);
    const ttl = http.Header{ .name = "X-Aws-Ec2-Metadata-Token-Ttl-Seconds", .value = "21600" };
    const token_body = metadataRequest(a, client, .PUT, token_url, null, ttl, 16 * 1024) catch |err| blk: {
        if (isTrue(env.get("AWS_EC2_METADATA_V1_DISABLED"))) {
            return err;
        }
        break :blk null;
    };
    defer {
        if (token_body) |value| {
            a.free(value);
        }
    }
    const token = if (token_body) |value|
        std.mem.trim(u8, value, " \t\r\n")
    else
        null;
    const role_url = try std.fmt.allocPrint(a, "{s}/latest/meta-data/iam/security-credentials/", .{base});
    defer a.free(role_url);
    const token_header: ?http.Header = if (token) |value|
        .{ .name = "X-Aws-Ec2-Metadata-Token", .value = value }
    else
        null;

    const roles = try metadataRequest(
        a,
        client,
        .GET,
        role_url,
        null,
        token_header,
        16 * 1024,
    );
    defer a.free(roles);
    const role = std.mem.trim(u8, std.mem.sliceTo(roles, '\n'), " \t\r\n");
    if (role.len == 0) {
        return error.Ec2RoleNotFound;
    }
    const credentials_url = try std.fmt.allocPrint(a, "{s}/latest/meta-data/iam/security-credentials/{s}", .{ base, role });
    defer a.free(credentials_url);
    const body = try metadataRequest(
        a,
        client,
        .GET,
        credentials_url,
        null,
        token_header,
        options.max_credentials_response_bytes,
    );
    defer a.free(body);
    return parseCredentialResponse(a, body, resolveRegion(options, env, null));
}

fn metadataRequest(a: Allocator, client: *http.Client, method: http.Method, url: []const u8, authorization_token: ?[]const u8, extra: ?http.Header, max: usize) ![]u8 {
    var headers: [2]http.Header = undefined;
    var n: usize = 0;
    if (authorization_token) |v| {
        headers[n] = .{ .name = "Authorization", .value = v };
        n += 1;
    }
    if (extra) |v| {
        headers[n] = v;
        n += 1;
    }
    const uri = std.Uri.parse(url) catch return error.InvalidMetadataUri;
    var request = client.request(method, uri, .{ .extra_headers = headers[0..n] }) catch return error.MetadataConnectionFailed;
    defer request.deinit();
    request.sendBodiless() catch return error.MetadataSendFailed;
    request.connection.?.flush() catch return error.MetadataSendFailed;
    var head_buffer: [4096]u8 = undefined;
    var response = request.receiveHead(&head_buffer) catch return error.MetadataReceiveFailed;
    if (response.head.status != .ok) {
        return error.MetadataRequestFailed;
    }
    var transfer_buffer: [64]u8 = undefined;
    return response.reader(&transfer_buffer).allocRemaining(a, .limited(max)) catch return error.MetadataResponseTooLarge;
}

fn parseCredentialResponse(a: Allocator, body: []const u8, region: []const u8) !OwnedCredentials {
    const parsed = std.json.parseFromSlice(CredentialResponse, a, body, .{ .ignore_unknown_fields = true }) catch return error.InvalidCredentialResponse;
    defer parsed.deinit();
    const expiration = if (parsed.value.Expiration) |value|
        parseRfc3339(value) catch return error.InvalidCredentialExpiration
    else
        null;

    return ownCredentials(
        a,
        parsed.value.AccessKeyId,
        parsed.value.SecretAccessKey,
        parsed.value.Token,
        region,
        expiration,
    );
}

fn validateContainerUrl(url: []const u8) !void {
    const uri = std.Uri.parse(url) catch
        return error.InvalidContainerCredentialsUri;

    if (std.mem.eql(u8, uri.scheme, "https")) {
        return;
    }

    if (!std.mem.eql(u8, uri.scheme, "http")) {
        return error.InvalidContainerCredentialsUri;
    }

    const host = switch (uri.host orelse return error.InvalidContainerCredentialsUri) {
        .raw => |value| value,
        .percent_encoded => |value| value,
    };

    const is_allowed_host =
        std.ascii.eqlIgnoreCase(host, "localhost") or
        std.mem.startsWith(u8, host, "127.") or
        std.mem.eql(u8, host, "[::1]") or
        std.mem.eql(u8, host, "::1") or
        std.mem.eql(u8, host, "169.254.170.2") or
        std.mem.eql(u8, host, "169.254.170.23") or
        std.mem.eql(u8, host, "[fd00:ec2::23]") or
        std.mem.eql(u8, host, "fd00:ec2::23");

    if (!is_allowed_host) {
        return error.UnsafeContainerCredentialsUri;
    }
}

fn isTrue(value: ?[]const u8) bool {
    if (value) |text| {
        return std.ascii.eqlIgnoreCase(text, "true");
    }

    return false;
}

fn parseRfc3339(value: []const u8) !i64 {
    const has_expected_layout = value.len >= 20 and
        value[4] == '-' and
        value[7] == '-' and
        value[10] == 'T' and
        value[13] == ':' and
        value[16] == ':' and
        value[value.len - 1] == 'Z';
    if (!has_expected_layout) {
        return error.InvalidTimestamp;
    }

    const year = try std.fmt.parseInt(u32, value[0..4], 10);
    const month = try std.fmt.parseInt(u8, value[5..7], 10);
    const day = try std.fmt.parseInt(u8, value[8..10], 10);
    const hour = try std.fmt.parseInt(u8, value[11..13], 10);
    const minute = try std.fmt.parseInt(u8, value[14..16], 10);
    const second = try std.fmt.parseInt(u8, value[17..19], 10);

    const fields_are_valid = year >= 1970 and
        month >= 1 and month <= 12 and
        day >= 1 and day <= 31 and
        hour <= 23 and
        minute <= 59 and
        second <= 60;
    if (!fields_are_valid) {
        return error.InvalidTimestamp;
    }

    var days: i64 = 0;
    var current_year: u32 = 1970;
    while (current_year < year) : (current_year += 1) {
        const days_in_year: u16 = if (leap(current_year))
            366
        else
            365;
        days += days_in_year;
    }

    const month_days = if (leap(year))
        [_]u8{ 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 }
    else
        [_]u8{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

    for (month_days[0 .. month - 1]) |days_in_month| {
        days += days_in_month;
    }
    days += day - 1;

    return days * 86400 +
        @as(i64, hour) * 3600 +
        @as(i64, minute) * 60 +
        second;
}

test "profile parser handles credentials and config section names" {
    const credentials =
        "[default]\naws_access_key_id = DEFAULT\naws_secret_access_key = default-secret\n" ++
        "[dev]\naws_access_key_id = DEVKEY\naws_secret_access_key = dev-secret\naws_session_token = token\n";
    const dev = parseProfile(credentials, "dev", false);
    try std.testing.expectEqualStrings("DEVKEY", dev.access_key_id.?);
    try std.testing.expectEqualStrings("dev-secret", dev.secret_access_key.?);
    try std.testing.expectEqualStrings("token", dev.session_token.?);
    const config = "[profile dev]\nregion = eu-west-1\n[default]\nregion = us-east-2\n";
    try std.testing.expectEqualStrings("eu-west-1", parseProfile(config, "dev", true).region.?);
    try std.testing.expectEqualStrings("us-east-2", parseProfile(config, "default", true).region.?);
}

test "credential endpoint JSON and expiration parsing" {
    const body = "{\"AccessKeyId\":\"AKID\",\"SecretAccessKey\":\"secret\",\"Token\":\"token\",\"Expiration\":\"2000-02-29T00:00:00Z\"}";
    var credentials = try parseCredentialResponse(std.testing.allocator, body, "eu-north-1");
    defer credentials.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("AKID", credentials.access_key_id);
    try std.testing.expectEqualStrings("eu-north-1", credentials.region);
    try std.testing.expectEqual(@as(?i64, 951782400), credentials.expiration);
}

test "environment credentials require a complete pair" {
    var env = std.process.Environ.Map.init(std.testing.allocator);
    defer env.deinit();
    try env.put("AWS_ACCESS_KEY_ID", "only-access");
    var dummy_client: http.Client = undefined;
    try std.testing.expectError(error.IncompleteEnvironmentCredentials, discoverCredentials(std.testing.allocator, undefined, &dummy_client, &env, .{}));
}

test {
    std.testing.refAllDecls(@This());
}

fn now() i64 {
    var tv: std.posix.system.timeval = undefined;
    _ = std.posix.system.gettimeofday(&tv, null);
    return tv.sec;
}

fn leap(y: u32) bool {
    return (y % 4 == 0 and y % 100 != 0) or y % 400 == 0;
}

test "ECS full URI rejects non-local HTTP endpoints" {
    try validateContainerUrl("https://credentials.example.com/task");
    try validateContainerUrl("http://127.0.0.1:8080/task");
    try validateContainerUrl("http://169.254.170.2/task");
    try std.testing.expectError(error.UnsafeContainerCredentialsUri, validateContainerUrl("http://credentials.example.com/task"));
}

test "environment credentials use AWS_REGION" {
    var env = std.process.Environ.Map.init(std.testing.allocator);
    defer env.deinit();

    try env.put("AWS_ACCESS_KEY_ID", "access-key");
    try env.put("AWS_SECRET_ACCESS_KEY", "secret-key");
    try env.put("AWS_SESSION_TOKEN", "session-token");
    try env.put("AWS_REGION", "eu-central-1");
    try env.put("AWS_DEFAULT_REGION", "us-west-2");

    var dummy_client: http.Client = undefined;
    var credentials = try discoverCredentials(
        std.testing.allocator,
        undefined,
        &dummy_client,
        &env,
        .{},
    );
    defer credentials.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("eu-central-1", credentials.region);
    try std.testing.expectEqualStrings("session-token", credentials.session_token.?);
}
