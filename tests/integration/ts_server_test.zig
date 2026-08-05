const std = @import("std");
const channel = @import("channel");

const testing = std.testing;
const TestState = struct {};

var client_values_sent: usize = 0;

fn clientHello(_: channel.PostRequest(TestState, void, void)) !?[]const u8 {
    return "client world";
}

fn clientStreamValues(request: channel.StreamRequest(TestState, void, void, i64)) !void {
    var i: i64 = 0;
    while (i < 3) : (i += 1) {
        if (request.emitter.isCancelled()) return;
        try request.emitter.emit(testing.allocator, i);
        std.Thread.sleep(1_000_000);
    }
}

fn clientStreamCancel(request: channel.StreamRequest(TestState, void, void, i64)) !void {
    var i: i64 = 0;
    while (i < 10) : (i += 1) {
        if (request.emitter.isCancelled()) return;
        try request.emitter.emit(testing.allocator, i);
        client_values_sent += 1;
        std.Thread.sleep(1_000_000);
    }
}

fn noopEvent(_: ?*anyopaque, _: []const u8) void {}

fn waitRemoteTopic(client: *channel.Client(TestState), topic: []const u8, expected: bool, timeout_ms: usize) bool {
    var elapsed: usize = 0;
    while (elapsed < timeout_ms) : (elapsed += 1) {
        if (client.isRemoteSubscribed(topic) == expected) return true;
        _ = client.poll() catch {};
        std.Thread.sleep(1_000_000);
    }
    return false;
}

fn expectOkString(result: channel.CallResult([]const u8), expected: ?[]const u8) !void {
    var mutable = result;
    defer mutable.deinit(testing.allocator);
    switch (mutable) {
        .ok => |value| {
            if (expected) |text| {
                try testing.expect(value != null);
                try testing.expectEqualStrings(text, value.?);
            } else {
                try testing.expect(value == null);
            }
        },
        .remote_err => return error.TestUnexpectedResult,
    }
}

fn expectOkVoid(result: channel.CallResult(void)) !void {
    var mutable = result;
    defer mutable.deinit(testing.allocator);
    switch (mutable) {
        .ok => |value| try testing.expect(value == null),
        .remote_err => return error.TestUnexpectedResult,
    }
}

fn expectErr(comptime T: type, result: channel.CallResult(T), expected: []const u8) !void {
    var mutable = result;
    defer mutable.deinit(testing.allocator);
    switch (mutable) {
        .ok => return error.TestUnexpectedResult,
        .remote_err => |value| try testing.expectEqualStrings(expected, value),
    }
}

test "zig client interoperability with ts fixture server" {
    client_values_sent = 0;

    var events = .{
        .names = channel.Subscription([]const u8, []const u8).init(),
    };

    var ch = channel.Channel(TestState).init(testing.allocator);
    defer ch.deinit();
    defer events.names.deinit();

    _ = try ch.post("hello", clientHello);
    _ = try ch.stream(void, i64, void, "stream/values", clientStreamValues);
    _ = try ch.stream(void, i64, void, "stream/cancel", clientStreamCancel);
    _ = try ch.events(&events, "");

    var client = try ch.connect(testing.allocator, "ws://127.0.0.1:2049", .{});
    defer client.deinit();

    try expectOkString(try client.send(testing.allocator, []const u8, "hello", {}), "world");
    {
        var echo = try client.send(testing.allocator, f64, "echo", 0.1337);
        defer echo.deinit(testing.allocator);
        switch (echo) {
            .ok => |value| try testing.expectEqual(@as(?f64, 0.1337), value),
            .remote_err => return error.TestUnexpectedResult,
        }
    }
    try expectOkVoid(try client.send(testing.allocator, void, "empty", {}));
    try expectErr([]const u8, try client.send(testing.allocator, []const u8, "notfound", {}), "api not found");

    {
        var sub_result = try client.subscribe(
            testing.allocator,
            []const u8,
            []const u8,
            void,
            "notfound",
            "1",
            {},
            null,
            noopEvent,
        );
        defer sub_result.deinit(testing.allocator);
        switch (sub_result) {
            .ok => return error.TestUnexpectedResult,
            .remote_err => |value| try testing.expectEqualStrings("subscription not found", value),
        }
    }

    {
        var values = try client.values(testing.allocator, void, i64, void, "stream/values", {}, {});
        defer values.close();

        var index: i64 = 0;
        while (true) {
            var next = try values.next(testing.allocator);
            defer next.deinit(testing.allocator);
            switch (next) {
                .item => |value| {
                    try testing.expectEqual(index, value);
                    index += 1;
                },
                .done => break,
                .remote_err => return error.TestUnexpectedResult,
            }
        }
        try testing.expectEqual(@as(i64, 3), index);
    }

    {
        var values = try client.values(testing.allocator, void, i64, void, "mirror/stream/cancel", {}, {});
        while (true) {
            var next = try values.next(testing.allocator);
            defer next.deinit(testing.allocator);
            switch (next) {
                .item => |value| {
                    if (value == 2) break;
                },
                .done => break,
                .remote_err => return error.TestUnexpectedResult,
            }
        }
        values.close();
        try testing.expectEqual(@as(usize, 10), client_values_sent);
    }

    try expectOkString(
        try client.send(testing.allocator, []const u8, "mirror", {}),
        "client world",
    );

    {
        var values = try client.values(testing.allocator, void, i64, void, "mirror/stream", {}, {});
        defer values.close();

        var index: i64 = 0;
        while (true) {
            var next = try values.next(testing.allocator);
            defer next.deinit(testing.allocator);
            switch (next) {
                .item => |value| {
                    try testing.expectEqual(index, value);
                    index += 1;
                },
                .done => break,
                .remote_err => return error.TestUnexpectedResult,
            }
        }
        try testing.expectEqual(@as(i64, 3), index);
    }

    try expectOkVoid(try client.send(testing.allocator, void, "mirror/events", {}));
    try testing.expect(waitRemoteTopic(&client, "names/1", true, 2000));

    try events.names.send(testing.allocator, "1", "h");
    try testing.expect(waitRemoteTopic(&client, "names/1", false, 2000));

    try expectOkString(
        try client.send(testing.allocator, []const u8, "auth", .{ .name = "tester" }),
        "tester",
    );
    try expectOkString(try client.send(testing.allocator, []const u8, "auth/name", {}), "tester");

    var client2 = try ch.connect(testing.allocator, "ws://127.0.0.1:2049", .{});
    defer client2.deinit();
    try expectErr([]const u8, try client2.send(testing.allocator, []const u8, "auth/name", {}), "unauthorized");
}
