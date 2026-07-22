const std = @import("std");
const seb = @import("seb");
const print = std.debug.print;
const assert = std.debug.assert;
const Topic = seb.Topic;
const s3 = seb.s3;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var args = try std.process.Args.iterateAllocator(init.minimal.args, init.gpa);
    defer args.deinit();

    _ = args.next(); // discard program path

    var records_path = args.next();
    if (records_path == null or records_path.?.len == 0) {
        print("you must provide a path to a .record_batch file. Using default path\n", .{});
        records_path = "/home/micvbang/downloads/000000087557.record_batch";
    }

    const f = try seb.record.openPositionalFile(io, records_path.?);
    defer f.close();

    const file_length = try f.length();

    const batch_num_records = 32 * 1024;
    var buffers = try seb.record.Buffers.init(gpa, file_length, batch_num_records);
    defer buffers.deinit();

    const parser = try seb.record.Parser(@TypeOf(f)).init(&buffers, f, file_length);
    defer parser.deinit();

    var batch_pool = try seb.BatchPool.init(gpa, 1, file_length, batch_num_records);
    defer batch_pool.deinit();

    var batch = try batch_pool.get();
    defer batch_pool.put(batch);

    try parser.records(batch, 0, parser.header.num_records);
    for (0..parser.header.num_records) |i| {
        const index: u32 = @intCast(i);
        const record = try batch.record(index);
        print("{d}: {s} len: {d:6}\n", .{ i, record[0..@min(64, record.len)], record.len });
    }
    print("starting paginating!\n", .{});

    { // list s3 objects
        var client = try s3.Client.initDefault(
            init.gpa,
            init.io,
            init.environ_map,
            .{ .path_style = true },
        );
        defer client.deinit();
        print("Using AWS region: {s}\n", .{client.config.region});

        var paginator = client.listObjectsPaginator("www.development.cvr.dev", .{});
        defer paginator.deinit();

        var object_count: usize = 0;

        while (try paginator.next()) |page_value| {
            var page = page_value;
            defer page.deinit();

            for (page.objects) |object| {
                print("{s}\n", .{object.key});
                object_count += 1;
            }
        }

        print("Listed {d} objects from {s}\n", .{
            object_count,
            "www.development.cvr.dev",
        });
    }

    { // TODO: play around with topic

        // const topic = Topic.init(
        //     gpa,
        // );
    }
}
