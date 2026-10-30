const std = @import("std");
const channel = @import("channel");

const testing = std.testing;
const quic = channel.quic;

fn frameOf(allocator: std.mem.Allocator, payload: []const u8) ![]u8 {
    var list: std.ArrayListUnmanaged(u8) = .{};
    errdefer list.deinit(allocator);
    try quic.appendFrame(&list, allocator, payload);
    return list.toOwnedSlice(allocator);
}

test "quic frames: varint lengths, keepalives skipped" {
    const allocator = testing.allocator;
    for ([_]usize{ 0, 1, 63, 64, 16383, 16384, 70000 }) |size| {
        const payload = try allocator.alloc(u8, size);
        defer allocator.free(payload);
        @memset(payload, 'x');
        const frame = try frameOf(allocator, payload);
        defer allocator.free(frame);
        const want: usize = if (size < 64) 1 else if (size < 16384) 2 else 4;
        try testing.expectEqual(want, frame.len - size);

        // A keepalive in front is skipped
        var reader: quic.FrameReader = .{};
        defer reader.deinit(allocator);
        try reader.push(allocator, &quic.keepalive);
        try reader.push(allocator, frame);
        const next = try frameOf(allocator, "next");
        defer allocator.free(next);
        try reader.push(allocator, next);
        if (size > 0) try testing.expectEqualSlices(u8, payload, (try reader.next()).?);
        try testing.expectEqualStrings("next", (try reader.next()).?);
        try testing.expect((try reader.next()) == null);
    }
}

test "quic frames: 8-byte lengths, split reads, the size limit" {
    const allocator = testing.allocator;
    var reader: quic.FrameReader = .{};
    defer reader.deinit(allocator);
    try reader.push(allocator, &.{ 0xc0, 0, 0, 0, 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o' });
    try testing.expectEqualStrings("hello", (try reader.next()).?);

    // One byte at a time
    const frame = try frameOf(allocator, "x" ** 300);
    defer allocator.free(frame);
    for (frame[0 .. frame.len - 1]) |byte| {
        try reader.push(allocator, &.{byte});
        try testing.expect((try reader.next()) == null);
    }
    try reader.push(allocator, frame[frame.len - 1 ..]);
    try testing.expectEqual(@as(usize, 300), (try reader.next()).?.len);

    // Refused as soon as the length is known, before the payload arrives
    var limited: quic.FrameReader = .{ .max = 10 };
    defer limited.deinit(allocator);
    try limited.push(allocator, &.{11});
    try testing.expectError(error.FrameTooLarge, limited.next());

    var default_limit: quic.FrameReader = .{};
    defer default_limit.deinit(allocator);
    try default_limit.push(allocator, &.{ 0x80 | 0x01, 0, 0, 1 }); // 16 MiB + 1
    try testing.expectError(error.FrameTooLarge, default_limit.next());
    try testing.expectEqual(@as(usize, 16 * 1024 * 1024), quic.max_frame);
}

test "quic hello: built from the header block, parsed with lowercased names" {
    const allocator = testing.allocator;
    const hello = try quic.helloAlloc(allocator, "X-Name: zig\r\nAuthorization: Bearer a:b\r\n\r\n");
    defer allocator.free(hello);
    try testing.expectEqualStrings("{\"hello\":{\"X-Name\":\"zig\",\"Authorization\":\"Bearer a:b\"}}", hello);
    const empty = try quic.helloAlloc(allocator, null);
    defer allocator.free(empty);
    try testing.expectEqualStrings("{\"hello\":{}}", empty);

    var parsed = try quic.parseHello(allocator, "{\"hello\":{\"X-Name\":\"raw\",\"ignored\":1}}");
    defer parsed.deinit(allocator);
    try testing.expectEqualStrings("raw", parsed.headers.get("x-name").?);
    try testing.expect(parsed.headers.get("ignored") == null);

    for ([_][]const u8{ "{\"id\":1,\"path\":\"hello\"}", "{\"hello\":\"x\"}", "[]", "{nope", "" }) |bad| {
        try testing.expectError(error.NotHello, quic.parseHello(allocator, bad));
    }
}

test "quic fingerprints and addresses" {
    const pin = try quic.parseFingerprint("DD:D2:37:00:BD:8A:BD:29:14:15:D8:65:84:BE:C2:80:10:FF:21:DA:A1:C5:9D:B3:4A:7F:36:63:EF:4B:55:9B");
    try testing.expectEqual(@as(u8, 0xdd), pin[0]);
    try testing.expectEqual(@as(u8, 0x9b), pin[31]);
    try testing.expectEqual(pin, try quic.parseFingerprint("ddd23700bd8abd291415d86584bec28010ff21daa1c59db34a7f3663ef4b559b"));
    try testing.expectError(error.InvalidFingerprint, quic.parseFingerprint("nope"));
    try testing.expectError(error.InvalidFingerprint, quic.parseFingerprint("dd:d2"));
    try testing.expectError(error.InvalidFingerprint, quic.parseFingerprint("d:dd2"));

    const allocator = testing.allocator;
    const v4 = try quic.parseTarget(allocator, "quic://127.0.0.1:2049");
    defer v4.deinit(allocator);
    try testing.expectEqualStrings("127.0.0.1", v4.host);
    try testing.expectEqual(@as(u16, 2049), v4.port);
    const v6 = try quic.parseTarget(allocator, "quic://[::1]:4433");
    defer v6.deinit(allocator);
    try testing.expectEqualStrings("::1", v6.host);
    try testing.expectError(error.InvalidAddress, quic.parseTarget(allocator, "quic://localhost"));
    try testing.expectError(error.InvalidAddress, quic.parseTarget(allocator, "ws://localhost:1"));
}

test "quic:// without -Dquic is refused" {
    if (channel.quic_enabled) return error.SkipZigTest;
    const State = struct {};
    var ch = channel.Channel(State).init(testing.allocator);
    defer ch.deinit();
    try testing.expectError(error.QuicNotEnabled, ch.connect(testing.allocator, "quic://127.0.0.1:1", .{}));
}
