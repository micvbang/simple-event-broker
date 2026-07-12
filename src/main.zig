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
    const parser = try seb.record.Parser(@TypeOf(f)).init(init.gpa, f, file_length);
    defer parser.deinit();

    const batch_bytes = 10 * 1024 * 1024;
    const batch_num_records = 32 * 1024;
    var batch_pool = try seb.BatchPool.init(init.gpa, 2, batch_bytes, batch_num_records);
    defer batch_pool.deinit();

    var batch_records = try batch_pool.pool.get();
    defer batch_pool.pool.put(batch_records);

    var batch_record = try batch_pool.pool.get();
    defer batch_pool.pool.put(batch_record);

    var b1_data_offset: usize = 0;
    try parser.records(batch_records, 0, parser.header.num_records);
    for (0..parser.header.num_records) |i| {
        print("{d}: record: {s}\n", .{ i, batch_records.data[0..@min(80, batch_records.sizes[i])] });
        try parser.record(batch_record, @intCast(i));
        defer batch_record.reset();

        assert(batch_records.sizes[i] == batch_record.sizes[0]);
        const record_size = batch_records.sizes[i];

        const b1_data = batch_records.data[b1_data_offset..][0..record_size];
        const b2_data = batch_record.data[0..record_size];
        assert(std.mem.eql(u8, b1_data, b2_data));

        b1_data_offset += record_size;
    }
}

test "records fails when batch input is too small" {
    const io = std.testing.io;
    const allocator = std.testing.allocator;

    const f = try seb.record.openPositionalFile(io, records_batch_path);
    defer f.close();

    const parser = try seb.record.Parser(@TypeOf(f)).init(allocator, f, try f.length());
    defer parser.deinit();

    {
        var batch_sizes_too_small = try seb.Batch.init(allocator, 10 * 1024 * 1024, 300);
        defer batch_sizes_too_small.deinit();
        if (parser.records(&batch_sizes_too_small, 0, parser.header.num_records)) |_| {
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

test "record and records reads the same" {
    const allocator = std.testing.allocator;

    const batch_bytes = 10 * 1024 * 1024;
    const batch_num_records = 32 * 1024;
    var batch_pool = try seb.BatchPool.init(allocator, 3, batch_bytes, batch_num_records);
    defer batch_pool.deinit();

    const records_num = 8;
    const records_bytes = 32;
    const file_size = seb.record.Header.header_bytes + records_num * (records_bytes + seb.record.Header.record_offset_size);
    var buf: [file_size]u8 = undefined;
    var memory_writer = std.Io.Writer.fixed(&buf);

    const batch = try batch_pool.pool.get();
    defer batch_pool.pool.put(batch);
    seb.testing.randomizeBatch(batch, records_num, records_bytes);

    try seb.record.Write(allocator, &memory_writer, batch.*, seb.testing.now);

    const memory_reader = seb.testing.PositionalBufferReader{ .buf = &buf };
    const parser = try seb.record.Parser(@TypeOf(memory_reader)).init(allocator, memory_reader, file_size);
    defer parser.deinit();

    const batch_multiple_records = try batch_pool.pool.get();
    defer batch_pool.pool.put(batch_multiple_records);

    const batch_single_record = try batch_pool.pool.get();
    defer batch_pool.pool.put(batch_single_record);

    var b1_data_offset: usize = 0;
    try parser.records(batch_multiple_records, 0, parser.header.num_records);
    for (0..parser.header.num_records) |i| {
        try parser.record(batch_single_record, @intCast(i));
        defer batch_single_record.reset();

        assert(batch_multiple_records.sizes[i] == batch_single_record.sizes[0]);
        const record_size = batch_multiple_records.sizes[i];

        const b1_data = batch_multiple_records.data[b1_data_offset..][0..record_size];
        const b2_data = batch_single_record.data[0..record_size];
        assert(std.mem.eql(u8, b1_data, b2_data));

        b1_data_offset += record_size;
    }
}
