//! Minimal AWS S3 ListObjectsV2 client with SigV4 and credential discovery.
pub const types = @import("s3/types.zig");
pub const credentials = @import("s3/credentials.zig");
pub const signing = @import("s3/signing.zig");
pub const xml = @import("s3/xml.zig");
pub const list_objects = @import("s3/list_objects.zig");
pub const get_object = @import("s3/get_object.zig");
pub const put_object = @import("s3/put_object.zig");
pub const client = @import("s3/client.zig");

pub const Config = types.Config;
pub const DefaultOptions = types.DefaultOptions;
pub const ListOptions = types.ListOptions;
pub const Object = types.Object;
pub const Page = types.Page;
pub const Client = client.Client;
pub const ListObjectsPaginator = client.ListObjectsPaginator;

test {
    const std = @import("std");
    std.testing.refAllDecls(@This());
}
