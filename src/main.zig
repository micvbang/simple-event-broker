const std = @import("std");
const seb = @import("seb");
const print = std.debug.print;
const assert = std.debug.assert;

pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.iterateAllocator(init.minimal.args, init.gpa);
    defer args.deinit();

    _ = args.next(); // discard program path

    var records_path = args.next();
    if (records_path == null or records_path.?.len == 0) {
        print("you must provide a path to a .record_batch file. Using default path\n", .{});
        records_path = "/home/micvbang/downloads/000000087557.record_batch";
    }

    const f = try seb.record.openPositionalFile(init.io, records_path.?);
    defer f.close();

    const file_length = try f.length();

    const batch_num_records = 32 * 1024;
    var buffers = try seb.record.Buffers.init(init.gpa, file_length, batch_num_records);
    defer buffers.deinit();

    const parser = try seb.record.Parser(@TypeOf(f)).init(&buffers, f, file_length);
    defer parser.deinit();

    var batch_pool = try seb.BatchPool.init(init.gpa, 1, file_length, batch_num_records);
    defer batch_pool.deinit();

    var batch = try batch_pool.get();
    defer batch_pool.put(batch);

    try parser.records(batch, 0, parser.header.num_records);
    for (0..parser.header.num_records) |i| {
        const index: u32 = @intCast(i);
        const record = try batch.record(index);
        print("{d}: {s} len: {d:6}\n", .{ i, record[0..@min(64, record.len)], record.len });
    }
}
