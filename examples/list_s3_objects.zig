const std = @import("std");
const s3 = @import("s3");

const bucket = "www.development.cvr.dev";

pub fn main(init: std.process.Init) !void {
    var client = try s3.Client.initDefault(
        init.gpa,
        init.io,
        init.environ_map,
        .{ .path_style = true },
    );
    defer client.deinit();
    std.debug.print("Using AWS region: {s}\n", .{client.config.region});

    var paginator = client.listObjectsPaginator(bucket, .{});
    defer paginator.deinit();

    var object_count: usize = 0;

    while (try paginator.next()) |page_value| {
        var page = page_value;
        defer page.deinit();

        for (page.objects) |object| {
            std.debug.print("{s}\n", .{object.key});
            object_count += 1;
        }
    }

    std.debug.print("Listed {d} objects from {s}\n", .{
        object_count,
        bucket,
    });
}
