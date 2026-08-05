const std = @import("std");
const channel = @import("channel");

const testing = std.testing;
const TestState = struct {};
const ServerState = struct {
    header_wins: bool,
};

var stream_cancel_count: usize = 0;
var disconnect_count: usize = 0;

fn postHello(_: channel.PostRequest(TestState, void, void)) !?[]const u8 {
    return "world";
}

fn postEcho(request: channel.PostRequest(TestState, i64, void)) !?i64 {
    return request.body;
}

fn postError(_: channel.PostRequest(TestState, void, void)) !?[]const u8 {
    return error.failed;
}

fn streamValues(request: channel.StreamRequest(TestState, void, void, i64)) !void {
    var i: i64 = 0;
    while (i < 3) : (i += 1) {
        try request.emitter.emit(testing.allocator, i);
        std.Thread.sleep(1_000_000);
    }
}

fn streamCancel(request: channel.StreamRequest(TestState, void, void, i64)) !void {
    var i: i64 = 0;
    while (i < 10) : (i += 1) {
        if (request.emitter.isCancelled()) return;
        try request.emitter.emit(testing.allocator, i);
        stream_cancel_count += 1;
        std.Thread.sleep(1_000_000);
    }
}

fn onEvent(user_data: ?*anyopaque, event: []const u8) void {
    const count_ptr: *usize = @ptrCast(@alignCast(user_data orelse return));
    if (std.mem.indexOf(u8, event, "event") != null) {
        count_ptr.* += 1;
    }
}

fn websocketStateCheck(request: channel.PostRequest(ServerState, void, void)) !?bool {
    return request.state.header_wins;
}

fn websocketOtherPost(request: channel.PostRequest(ServerState, void, void)) !?[]const u8 {
    return request.path;
}

fn websocketOtherStream(request: channel.StreamRequest(ServerState, void, void, i64)) !void {
    try request.emitter.emit(testing.allocator, 7);
    try request.emitter.emit(testing.allocator, 8);
}

fn websocketApiHello(_: channel.PostRequest(ServerState, void, void)) !?[]const u8 {
    return "api";
}

fn websocketEcho(request: channel.PostRequest(ServerState, i64, void)) !?i64 {
    return request.body;
}

fn websocketApiStream(request: channel.StreamRequest(ServerState, void, void, i64)) !void {
    try request.emitter.emit(testing.allocator, 9);
}

fn websocketMergedHello(_: channel.PostRequest(ServerState, void, void)) !?[]const u8 {
    return "merged";
}

fn websocketMatchPost(path: []const u8) bool {
    return std.mem.startsWith(u8, path, "other/post/");
}

fn websocketMatchStream(path: []const u8) bool {
    return std.mem.startsWith(u8, path, "other/stream/");
}

fn websocketMakeState(headers: *const std.StringHashMapUnmanaged([]const u8)) !ServerState {
    return .{
        .header_wins = std.mem.eql(u8, headers.get("name").?, "header"),
    };
}

fn websocketOnDisconnect(_: *ServerState, _: channel.Sender(ServerState)) void {
    disconnect_count += 1;
}

fn websocketClientEvent(user_data: ?*anyopaque, event: []const u8) void {
    const count_ptr: *usize = @ptrCast(@alignCast(user_data orelse return));
    if (std.mem.eql(u8, event, "hello")) {
        count_ptr.* += 1;
    }
}

test "process send success and error" {
    var ch = channel.Channel(TestState).init(testing.allocator);
    defer ch.deinit();

    _ = try ch.post("hello", postHello);
    _ = try ch.post("echo", postEcho);
    _ = try ch.post("fail", postError);
    _ = try ch.stream(void, i64, void, "stream/values", streamValues);

    var process = try ch.process(testing.allocator, .{});
    defer process.deinit();

    var hello = try process.send(testing.allocator, []const u8, "hello", {});
    defer hello.deinit(testing.allocator);
    switch (hello) {
        .ok => |value| {
            try testing.expect(value != null);
            try testing.expectEqualStrings("world", value.?);
        },
        .remote_err => return error.TestUnexpectedResult,
    }

    var echo = try process.send(testing.allocator, i64, "echo", 42);
    defer echo.deinit(testing.allocator);
    switch (echo) {
        .ok => |value| try testing.expectEqual(@as(?i64, 42), value),
        .remote_err => return error.TestUnexpectedResult,
    }

    var failed = try process.send(testing.allocator, []const u8, "fail", {});
    defer failed.deinit(testing.allocator);
    switch (failed) {
        .ok => return error.TestUnexpectedResult,
        .remote_err => |value| try testing.expectEqualStrings("failed", value),
    }

    var values = try process.values(testing.allocator, void, i64, void, "stream/values", {}, {});
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

test "process subscription cancel and stream cancel" {
    var events = .{
        .hello = channel.Subscription([]const u8, []const u8).init(),
    };

    var ch = channel.Channel(TestState).init(testing.allocator);
    defer ch.deinit();

    _ = try ch.events(&events, "");
    defer events.hello.deinit();
    _ = try ch.stream(void, i64, void, "stream/cancel", streamCancel);

    var process = try ch.process(testing.allocator, .{});
    defer process.deinit();

    var event_count: usize = 0;
    var subscribe_result = try process.subscribe(
        testing.allocator,
        []const u8,
        []const u8,
        void,
        "hello",
        "test",
        {},
        &event_count,
        onEvent,
    );
    defer subscribe_result.deinit(testing.allocator);

    var handle = switch (subscribe_result) {
        .ok => |value| value,
        .remote_err => return error.TestUnexpectedResult,
    };

    try events.hello.send(testing.allocator, "test", "event 1");
    std.Thread.sleep(2_000_000);
    try testing.expectEqual(@as(usize, 1), event_count);

    handle.cancel();
    try events.hello.send(testing.allocator, "test", "event 2");
    std.Thread.sleep(2_000_000);
    try testing.expectEqual(@as(usize, 1), event_count);

    stream_cancel_count = 0;
    var values = try process.values(testing.allocator, void, i64, void, "stream/cancel", {}, {});

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
    try testing.expectEqual(@as(usize, 10), stream_cancel_count);
}

test "websocket server and client typed api" {
    disconnect_count = 0;

    var server_events = .{
        .updates = channel.Subscription([]const u8, []const u8).init(),
    };
    defer server_events.updates.deinit();

    var merged = channel.Channel(ServerState).init(testing.allocator);
    defer merged.deinit();
    _ = try merged.post("hello", websocketMergedHello);

    var server_ch = channel.Channel(ServerState).init(testing.allocator);
    defer server_ch.deinit();

    _ = try server_ch.post("state/check", websocketStateCheck);
    _ = try server_ch.postOther(void, []const u8, void, websocketMatchPost, websocketOtherPost);
    _ = try server_ch.streamOther(void, i64, void, websocketMatchStream, websocketOtherStream);
    _ = try server_ch.api(.{
        .api = .{
            .hello = channel.postSpec(void, []const u8, void, websocketApiHello),
            .numbers = channel.streamSpec(void, i64, void, websocketApiStream),
        },
    });
    _ = try server_ch.events(&server_events, "");
    _ = try server_ch.merge(&merged, "merged");
    _ = server_ch.onDisconnect(websocketOnDisconnect);

    var server = try server_ch.listen(testing.allocator, 0, .{
        .state = websocketMakeState,
    });
    defer server.deinit();
    try testing.expect(server.port() != 0);

    var client_ch = channel.Channel(TestState).init(testing.allocator);
    defer client_ch.deinit();

    const url = try std.fmt.allocPrint(testing.allocator, "ws://127.0.0.1:{d}/?name=query", .{server.port()});
    defer testing.allocator.free(url);

    var client = try client_ch.connect(testing.allocator, url, .{
        .headers = "name: header\r\n",
    });
    defer client.deinit();

    {
        var state_check = try client.send(testing.allocator, bool, "state/check", {});
        defer state_check.deinit(testing.allocator);
        switch (state_check) {
            .ok => |value| try testing.expectEqual(@as(?bool, true), value),
            .remote_err => return error.TestUnexpectedResult,
        }
    }

    {
        var other = try client.send(testing.allocator, []const u8, "other/post/demo", {});
        defer other.deinit(testing.allocator);
        switch (other) {
            .ok => |value| {
                try testing.expect(value != null);
                try testing.expectEqualStrings("other/post/demo", value.?);
            },
            .remote_err => return error.TestUnexpectedResult,
        }
    }

    {
        var api = try client.send(testing.allocator, []const u8, "api/hello", {});
        defer api.deinit(testing.allocator);
        switch (api) {
            .ok => |value| {
                try testing.expect(value != null);
                try testing.expectEqualStrings("api", value.?);
            },
            .remote_err => return error.TestUnexpectedResult,
        }
    }

    {
        var merged_result = try client.send(testing.allocator, []const u8, "merged/hello", {});
        defer merged_result.deinit(testing.allocator);
        switch (merged_result) {
            .ok => |value| {
                try testing.expect(value != null);
                try testing.expectEqualStrings("merged", value.?);
            },
            .remote_err => return error.TestUnexpectedResult,
        }
    }

    {
        var values = try client.values(testing.allocator, void, i64, void, "other/stream/demo", {}, {});
        defer values.close();

        var first = try values.next(testing.allocator);
        defer first.deinit(testing.allocator);
        var second = try values.next(testing.allocator);
        defer second.deinit(testing.allocator);
        var done = try values.next(testing.allocator);
        defer done.deinit(testing.allocator);

        switch (first) {
            .item => |value| try testing.expectEqual(@as(i64, 7), value),
            else => return error.TestUnexpectedResult,
        }
        switch (second) {
            .item => |value| try testing.expectEqual(@as(i64, 8), value),
            else => return error.TestUnexpectedResult,
        }
        try testing.expect(done == .done);
    }

    {
        var values = try client.values(testing.allocator, void, i64, void, "api/numbers", {}, {});
        defer values.close();

        var first = try values.next(testing.allocator);
        defer first.deinit(testing.allocator);
        var done = try values.next(testing.allocator);
        defer done.deinit(testing.allocator);
        switch (first) {
            .item => |value| try testing.expectEqual(@as(i64, 9), value),
            else => return error.TestUnexpectedResult,
        }
        try testing.expect(done == .done);
    }

    var event_count: usize = 0;
    var subscribe_result = try client.subscribe(
        testing.allocator,
        []const u8,
        []const u8,
        void,
        "updates",
        "1",
        {},
        &event_count,
        websocketClientEvent,
    );
    defer subscribe_result.deinit(testing.allocator);
    var subscription = switch (subscribe_result) {
        .ok => |value| value,
        .remote_err => return error.TestUnexpectedResult,
    };

    try server_events.updates.send(testing.allocator, "1", "hello");
    {
        var elapsed: usize = 0;
        while (event_count < 1 and elapsed < 2000) : (elapsed += 1) {
            if (!(try server.poll()) and !(try client.poll())) {
                std.Thread.sleep(1_000_000);
            }
        }
    }
    try testing.expectEqual(@as(usize, 1), event_count);
    subscription.cancel();

    client.stop();
    client.stop();
    {
        var elapsed: usize = 0;
        while (disconnect_count < 1 and elapsed < 2000) : (elapsed += 1) {
            if (!(try server.poll()) and !(try client.poll())) {
                std.Thread.sleep(1_000_000);
            }
        }
    }
    try testing.expectEqual(@as(usize, 1), disconnect_count);
}

test "websocket client handles serial sends without writer thread" {
    var server_ch = channel.Channel(ServerState).init(testing.allocator);
    defer server_ch.deinit();
    _ = try server_ch.post("echo", websocketEcho);

    var server = try server_ch.listen(testing.allocator, 0, .{});
    defer server.deinit();

    var client_ch = channel.Channel(TestState).init(testing.allocator);
    defer client_ch.deinit();

    const url = try std.fmt.allocPrint(testing.allocator, "ws://127.0.0.1:{d}/", .{server.port()});
    defer testing.allocator.free(url);

    var client = try client_ch.connect(testing.allocator, url, .{});
    defer client.deinit();

    var index: usize = 0;
    while (index < 256) : (index += 1) {
        const expected = @as(i64, @intCast(index));
        var result = try client.send(testing.allocator, i64, "echo", expected);
        defer result.deinit(testing.allocator);
        switch (result) {
            .ok => |value| try testing.expectEqual(@as(?i64, expected), value),
            .remote_err => return error.TestUnexpectedResult,
        }
    }
}
