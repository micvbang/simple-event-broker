const std = @import("std");
const seb = @import("seb");
const print = std.debug.print;
const assert = std.debug.assert;

const records_batch_path = "/Users/micvbang/projects/simple-event-broker/src/000000000000.record_batch";

pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.iterateAllocator(init.minimal.args, init.gpa);
    defer args.deinit();

    _ = args.next(); // discard program path

    const records_path = args.next() orelse records_batch_path;
    const f = try seb.record.openPositionalFile(init.io, records_path);
    defer f.close();

    const file_length = try f.length();

    const batch_bytes = file_length + 1 * 1024 * 1024;
    const batch_num_records = 32 * 1024;
    var buffers = try seb.record.Buffers.init(init.gpa, batch_bytes, batch_num_records);
    defer buffers.deinit();

    const parser = try seb.record.Parser(@TypeOf(f)).init(&buffers, f, file_length);
    defer parser.deinit();

    var batch_pool = try seb.BatchPool.init(init.gpa, 1, batch_bytes, batch_num_records);
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
