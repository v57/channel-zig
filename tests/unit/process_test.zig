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

fn postContextKind(request: channel.PostRequest(TestState, void, std.json.Value)) !?[]const u8 {
    return switch (request.context) {
        .object => |map| if (map.get("service")) |service| service.string else "object",
        else => "other",
    };
}

fn streamContextKind(request: channel.StreamRequest(TestState, void, std.json.Value, []const u8)) !void {
    try request.emitter.emit(testing.allocator, switch (request.context) {
        .object => "object",
        else => "other",
    });
}

test "a handler that reads the context gets {} when the caller sends none" {
    var ch = channel.Channel(TestState).init(testing.allocator);
    defer ch.deinit();
    _ = try ch.post("context", postContextKind);
    _ = try ch.stream(void, []const u8, std.json.Value, "context/stream", streamContextKind);

    var process = try ch.process(testing.allocator, .{});
    defer process.deinit();

    var missing = try process.send(testing.allocator, []const u8, "context", {});
    defer missing.deinit(testing.allocator);
    switch (missing) {
        .ok => |value| try testing.expectEqualStrings("object", value.?),
        .remote_err => return error.TestUnexpectedResult,
    }

    const Context = struct { service: []const u8 };
    var sent = try process.sendWithContext(testing.allocator, []const u8, "context", {}, Context{ .service = "abc" });
    defer sent.deinit(testing.allocator);
    switch (sent) {
        .ok => |value| try testing.expectEqualStrings("abc", value.?),
        .remote_err => return error.TestUnexpectedResult,
    }

    var values = try process.values(testing.allocator, void, []const u8, void, "context/stream", {}, {});
    defer values.close();
    var next = try values.next(testing.allocator);
    defer next.deinit(testing.allocator);
    switch (next) {
        .item => |value| try testing.expectEqualStrings("object", value),
        else => return error.TestUnexpectedResult,
    }
}

fn mergeMatchDyn(path: []const u8) bool {
    return std.mem.startsWith(u8, path, "dyn/");
}

fn mergeMatchFeed(path: []const u8) bool {
    return std.mem.eql(u8, path, "feed");
}

fn mergeOtherPost(request: channel.PostRequest(TestState, void, void)) !?[]const u8 {
    return request.path;
}

fn mergeOtherStream(request: channel.StreamRequest(TestState, void, void, i64)) !void {
    try request.emitter.emit(testing.allocator, 1);
}

test "merge keeps postOther/streamOther routes under the prefix" {
    var inner = channel.Channel(TestState).init(testing.allocator);
    defer inner.deinit();
    _ = try inner.postOther(void, []const u8, void, mergeMatchDyn, mergeOtherPost);
    _ = try inner.streamOther(void, i64, void, mergeMatchFeed, mergeOtherStream);

    var outer = channel.Channel(TestState).init(testing.allocator);
    defer outer.deinit();
    _ = try outer.merge(&inner, "m");

    try testing.expectEqual(@as(usize, 1), outer.other_posts.items.len);
    try testing.expect(outer.other_posts.items[0].matches("m/dyn/x"));
    try testing.expect(!outer.other_posts.items[0].matches("dyn/x"));
    try testing.expect(outer.other_streams.items[0].matches("m/feed"));

    var process = try outer.process(testing.allocator, .{});
    defer process.deinit();

    var hit = try process.send(testing.allocator, []const u8, "m/dyn/x", {});
    defer hit.deinit(testing.allocator);
    switch (hit) {
        .ok => |value| try testing.expectEqualStrings("m/dyn/x", value.?),
        .remote_err => return error.TestUnexpectedResult,
    }

    var miss = try process.send(testing.allocator, []const u8, "dyn/x", {});
    defer miss.deinit(testing.allocator);
    switch (miss) {
        .ok => return error.TestUnexpectedResult,
        .remote_err => |value| try testing.expectEqualStrings("api not found", value),
    }

    // Without a prefix the matchers are reused as-is
    var flat = channel.Channel(TestState).init(testing.allocator);
    defer flat.deinit();
    _ = try flat.merge(&inner, "");
    try testing.expect(flat.other_posts.items[0].matches("dyn/x"));
    try testing.expect(flat.other_posts.items[0].matcher == mergeMatchDyn);
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

// ---------------------------------------------------------------------------
// Regression tests for AUDIT2 (hostile server) and AUDIT (hostile client) findings.
// ---------------------------------------------------------------------------

/// A one-shot raw WebSocket server: completes the handshake, waits for the client's first frame,
/// writes `reply` verbatim (any bytes, including malformed frames), then idles until the client
/// disconnects.
const HostileServer = struct {
    server: std.net.Server,
    reply: []const u8,
    thread: ?std.Thread = null,

    fn start(self: *HostileServer, reply: []const u8) !u16 {
        const address = try std.net.Address.parseIp("127.0.0.1", 0);
        self.server = try address.listen(.{ .reuse_address = true });
        self.reply = reply;
        self.thread = try std.Thread.spawn(.{}, run, .{self});
        return self.server.listen_address.getPort();
    }

    fn join(self: *HostileServer) void {
        if (self.thread) |thread| thread.join();
        self.server.deinit();
    }

    fn run(self: *HostileServer) void {
        var conn = self.server.accept() catch return;
        defer conn.stream.close();
        var buf: [8192]u8 = undefined;
        var len: usize = 0;
        while (std.mem.indexOf(u8, buf[0..len], "\r\n\r\n") == null) {
            const n = conn.stream.read(buf[len..]) catch return;
            if (n == 0) return;
            len += n;
        }
        const marker = "sec-websocket-key:";
        const key_start = (std.ascii.indexOfIgnoreCase(buf[0..len], marker) orelse return) + marker.len;
        const key_end = std.mem.indexOfPos(u8, buf[0..len], key_start, "\r\n") orelse return;
        const key = std.mem.trim(u8, buf[key_start..key_end], " ");
        var hash: [20]u8 = undefined;
        var hasher = std.crypto.hash.Sha1.init(.{});
        hasher.update(key);
        hasher.update("258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
        hasher.final(&hash);
        var accept_buf: [28]u8 = undefined;
        const accept = std.base64.standard.Encoder.encode(&accept_buf, &hash);
        var response_buf: [256]u8 = undefined;
        const response = std.fmt.bufPrint(&response_buf, "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: upgrade\r\nsec-websocket-accept: {s}\r\n\r\n", .{accept}) catch return;
        conn.stream.writeAll(response) catch return;
        _ = conn.stream.read(&buf) catch return; // the client's request frame
        conn.stream.writeAll(self.reply) catch return;
        while (true) {
            const n = conn.stream.read(&buf) catch return;
            if (n == 0) return;
        }
    }
};

fn textFrameAlloc(allocator: std.mem.Allocator, payload: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.append(allocator, 0x81);
    if (payload.len < 126) {
        try out.append(allocator, @intCast(payload.len));
    } else if (payload.len <= 0xffff) {
        try out.append(allocator, 126);
        try out.appendSlice(allocator, &std.mem.toBytes(std.mem.nativeToBig(u16, @intCast(payload.len))));
    } else {
        try out.append(allocator, 127);
        try out.appendSlice(allocator, &std.mem.toBytes(std.mem.nativeToBig(u64, payload.len)));
    }
    try out.appendSlice(allocator, payload);
    return out.toOwnedSlice(allocator);
}

/// Sends one call to a hostile server answering with `reply` and returns the error text the call
/// settled with (caller frees). Before the AUDIT2 §1.1 fix every one of these double-freed.
fn callHostile(reply: []const u8) ![]u8 {
    var hostile: HostileServer = undefined;
    const port = try hostile.start(reply);
    defer hostile.join();

    var ch = channel.Channel(TestState).init(testing.allocator);
    defer ch.deinit();
    var client = try ch.connect(testing.allocator, port, .{});
    defer client.deinit();

    var result = try client.send(testing.allocator, i64, "echo", @as(i64, 5));
    defer result.deinit(testing.allocator);
    return switch (result) {
        .ok => error.TestUnexpectedResult,
        .remote_err => |value| try testing.allocator.dupe(u8, value),
    };
}

test "hostile server: bad json reply settles the call with an error" {
    const frame = try textFrameAlloc(testing.allocator, "{not json");
    defer testing.allocator.free(frame);
    const text = try callHostile(frame);
    defer testing.allocator.free(text);
    try testing.expectEqualStrings("invalid message", text);
}

test "hostile server: error-object reply surfaces as an error" {
    const frame = try textFrameAlloc(testing.allocator, "{\"id\":1,\"error\":{\"code\":500,\"message\":\"nope\"}}");
    defer testing.allocator.free(frame);
    const text = try callHostile(frame);
    defer testing.allocator.free(text);
    try testing.expect(std.mem.indexOf(u8, text, "500") != null);
}

test "hostile server: 2^64-1 and 2^63-1 frame lengths don't overflow" {
    for ([_]u64{ std.math.maxInt(u64), std.math.maxInt(i64), max_plus_one }) |length| {
        var frame: [10]u8 = .{ 0x81, 127, 0, 0, 0, 0, 0, 0, 0, 0 };
        std.mem.writeInt(u64, frame[2..10], length, .big);
        const text = try callHostile(&frame);
        defer testing.allocator.free(text);
        try testing.expectEqualStrings("message too large", text);
    }
}
const max_plus_one: u64 = channel.max_message_size + 1;

test "hostile server: deeply nested json doesn't crash" {
    const depth = 100_000;
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(testing.allocator);
    try payload.appendSlice(testing.allocator, "{\"id\":1,\"body\":");
    try payload.appendNTimes(testing.allocator, '[', depth);
    try payload.appendNTimes(testing.allocator, ']', depth);
    try payload.append(testing.allocator, '}');
    const frame = try textFrameAlloc(testing.allocator, payload.items);
    defer testing.allocator.free(frame);
    const text = try callHostile(frame);
    defer testing.allocator.free(text);
    try testing.expectEqualStrings("invalid message", text);
}

test "hostile server: invalid utf-8 doesn't crash" {
    const frame = try textFrameAlloc(testing.allocator, "{\"id\":1,\"body\":\"\xff\xfe\"}");
    defer testing.allocator.free(frame);
    const text = try callHostile(frame);
    defer testing.allocator.free(text);
    try testing.expectEqualStrings("invalid message", text);
}

test "large (8 MiB) reply is accepted" {
    const size = 8 * 1024 * 1024;
    var payload: std.ArrayList(u8) = .empty;
    defer payload.deinit(testing.allocator);
    try payload.appendSlice(testing.allocator, "{\"id\":1,\"body\":\"");
    try payload.appendNTimes(testing.allocator, 'a', size);
    try payload.appendSlice(testing.allocator, "\"}");
    const frame = try textFrameAlloc(testing.allocator, payload.items);
    defer testing.allocator.free(frame);

    var hostile: HostileServer = undefined;
    const port = try hostile.start(frame);
    defer hostile.join();
    var ch = channel.Channel(TestState).init(testing.allocator);
    defer ch.deinit();
    var client = try ch.connect(testing.allocator, port, .{});
    defer client.deinit();
    var result = try client.send(testing.allocator, []const u8, "echo", @as(i64, 5));
    defer result.deinit(testing.allocator);
    switch (result) {
        .ok => |value| try testing.expectEqual(@as(usize, size), value.?.len),
        .remote_err => return error.TestUnexpectedResult,
    }
}

fn pollServerFor(server: anytype, iterations: usize) void {
    var index: usize = 0;
    while (index < iterations) : (index += 1) {
        _ = server.poll() catch {};
        std.Thread.sleep(1_000_000);
    }
}

test "server survives a 2^64-1 frame length and keeps serving" {
    var server_ch = channel.Channel(ServerState).init(testing.allocator);
    defer server_ch.deinit();
    _ = try server_ch.post("echo", websocketEcho);
    var server = try server_ch.listen(testing.allocator, 0, .{});
    defer server.deinit();

    const address = try std.net.Address.parseIp("127.0.0.1", server.port());
    const raw = try std.net.tcpConnectToAddress(address);
    defer raw.close();
    try raw.writeAll("GET / HTTP/1.1\r\nhost: 127.0.0.1\r\nupgrade: websocket\r\nconnection: upgrade\r\nsec-websocket-version: 13\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n");
    pollServerFor(&server, 20);
    var buf: [1024]u8 = undefined;
    const n = try raw.read(&buf);
    try testing.expect(std.mem.startsWith(u8, buf[0..n], "HTTP/1.1 101"));

    // masked text frame announcing 2^64-1 bytes
    try raw.writeAll(&[_]u8{ 0x81, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1, 2, 3, 4 });
    pollServerFor(&server, 20);
    try testing.expectEqual(@as(usize, 0), try raw.read(&buf)); // the server closed it

    var client_ch = channel.Channel(TestState).init(testing.allocator);
    defer client_ch.deinit();
    var client = try client_ch.connect(testing.allocator, server.port(), .{});
    defer client.deinit();
    var result = try client.send(testing.allocator, i64, "echo", @as(i64, 9));
    defer result.deinit(testing.allocator);
    switch (result) {
        .ok => |value| try testing.expectEqual(@as(?i64, 9), value),
        .remote_err => return error.TestUnexpectedResult,
    }
}

test "server answers an undecodable request body with an error" {
    var server_ch = channel.Channel(ServerState).init(testing.allocator);
    defer server_ch.deinit();
    _ = try server_ch.post("echo", websocketEcho);
    var server = try server_ch.listen(testing.allocator, 0, .{});
    defer server.deinit();

    var client_ch = channel.Channel(TestState).init(testing.allocator);
    defer client_ch.deinit();
    var client = try client_ch.connect(testing.allocator, server.port(), .{});
    defer client.deinit();

    var bad = try client.send(testing.allocator, i64, "echo", "not a number");
    defer bad.deinit(testing.allocator);
    switch (bad) {
        .ok => return error.TestUnexpectedResult,
        .remote_err => {},
    }
    // the connection is still usable
    var good = try client.send(testing.allocator, i64, "echo", @as(i64, 3));
    defer good.deinit(testing.allocator);
    switch (good) {
        .ok => |value| try testing.expectEqual(@as(?i64, 3), value),
        .remote_err => return error.TestUnexpectedResult,
    }
}

fn roomTopic(allocator: std.mem.Allocator, request: []const u8) anyerror![]const u8 {
    return std.fmt.allocPrint(allocator, "room-{s}", .{request});
}

fn roomBody(_: std.mem.Allocator, _: []const u8) anyerror!?i64 {
    return 7;
}

fn countEvent(user_data: ?*anyopaque, event: i64) void {
    const sum: *i64 = @ptrCast(@alignCast(user_data orelse return));
    sum.* += event;
}

test "subscription topic/body formatters and falsy bodies" {
    var events = .{
        .rooms = channel.Subscription([]const u8, i64).init(),
    };
    _ = events.rooms.topic(roomTopic).body(roomBody);

    var ch = channel.Channel(TestState).init(testing.allocator);
    defer ch.deinit();
    _ = try ch.events(&events, "");
    defer events.rooms.deinit();

    var process = try ch.process(testing.allocator, .{});
    defer process.deinit();

    var sum: i64 = 0;
    var subscribe_result = try process.subscribe(testing.allocator, []const u8, i64, void, "rooms", "a", {}, &sum, countEvent);
    defer subscribe_result.deinit(testing.allocator);
    switch (subscribe_result) {
        .ok => {},
        .remote_err => return error.TestUnexpectedResult,
    }
    try testing.expect(process.isRemoteSubscribed("rooms/room-a"));

    try events.rooms.send(testing.allocator, "a", 0); // falsy: dropped, like TS
    try testing.expectEqual(@as(i64, 0), sum);
    try events.rooms.send(testing.allocator, "a", 5);
    try testing.expectEqual(@as(i64, 5), sum);
    try events.rooms.sendDefault(testing.allocator, "a"); // body formatter
    try testing.expectEqual(@as(i64, 12), sum);
    try events.rooms.send(testing.allocator, "b", 100); // other topic
    try testing.expectEqual(@as(i64, 12), sum);
}
