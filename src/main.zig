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

    var batch_pool = try seb.BatchPool.init(init.gpa, 2, batch_bytes, batch_num_records);
    defer batch_pool.deinit();

    var batch_records = try batch_pool.get();
    defer batch_pool.put(batch_records);

    var batch_record = try batch_pool.get();
    defer batch_pool.put(batch_record);

    try parser.records(batch_records, 0, parser.header.num_records);
    for (0..parser.header.num_records) |i| {
        const index: u32 = @intCast(i);
        const record_size = try parser.sizeOf(index, index + 1);
        const b1_record_start = batch_records.offsets[i];
        const b1_record_end = b1_record_start + record_size;

        var sha256: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(batch_records.data[0..record_size], &sha256, .{});

        print("{d}: {x} {s} len: {d:6}\n", .{ i, sha256, batch_records.data[0..@min(80, record_size)], record_size });
        try parser.record(batch_record, @intCast(i));
        defer batch_record.reset();

        const b1_data = batch_records.data[b1_record_start..b1_record_end];
        const b2_data = batch_record.data[0..record_size];
        assert(std.mem.eql(u8, b1_data, b2_data));
    }
}

test "records fails when batch input is too small" {
    const io = std.testing.io;
    const allocator = std.testing.allocator;

    const f = try seb.record.openPositionalFile(io, records_batch_path);
    defer f.close();

    var buffers = try seb.record.Buffers.init(allocator, 10 * 1024 * 1024, 32 * 1024);
    defer buffers.deinit();

    const parser = try seb.record.Parser(@TypeOf(f)).init(&buffers, f, try f.length());
    defer parser.deinit();

    {
        var batch_offsets_too_small = try seb.Batch.init(allocator, 10 * 1024 * 1024, 300);
        defer batch_offsets_too_small.deinit();
        if (parser.records(&batch_offsets_too_small, 0, parser.header.num_records)) |_| {
            unreachable;
        } else |err| {
            assert(err == seb.record.ParserError.BatchSizesTooSmall);
        }
    }

    {
        var batch_data_too_small = try seb.Batch.init(allocator, 1000, 301);
        defer batch_data_too_small.deinit();
        if (parser.records(&batch_data_too_small, 0, parser.header.num_records)) |_| {
            unreachable;
        } else |err| {
            assert(err == seb.record.ParserError.BatchDataTooSmall);
        }
    }
}
