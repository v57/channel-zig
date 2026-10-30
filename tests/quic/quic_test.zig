//! QUIC transport tests (zig build test-quic -Dquic=true). The interop tests need the TypeScript reference
//! next to this checkout (../channel): its fixture (tests/server.ts) on CHANNEL_TEST_PORT (2049 by default)
//! and bun for its client.
const std = @import("std");
const channel = @import("channel");
const engine = @import("quic_engine");

const testing = std.testing;
const allocator = testing.allocator;

/// The TypeScript reference's test certificate: localhost, 127.0.0.1 and ::1
const cert_dir = "../channel/tests/quic";
const fingerprint = "DD:D2:37:00:BD:8A:BD:29:14:15:D8:65:84:BE:C2:80:10:FF:21:DA:A1:C5:9D:B3:4A:7F:36:63:EF:4B:55:9B";

fn readPem(name: []const u8) ![]u8 {
    var dir = std.fs.cwd().openDir(cert_dir, .{}) catch return error.SkipZigTest;
    defer dir.close();
    return dir.readFileAlloc(allocator, name, 1 << 20) catch error.SkipZigTest;
}

// Server side

const ServerState = struct {
    name: [64]u8 = undefined,
    name_len: usize = 0,
};

var connected: usize = 0;
var disconnected: usize = 0;
var drop_once = true;
var mirror_buf: [64]u8 = undefined;
var server_events: struct { ticks: channel.Subscription([]const u8, []const u8) } = .{ .ticks = channel.Subscription([]const u8, []const u8).init() };

fn makeState(headers: *const std.StringHashMapUnmanaged([]const u8)) !ServerState {
    var state: ServerState = .{};
    if (headers.get("x-name")) |name| {
        state.name_len = @min(name.len, state.name.len);
        @memcpy(state.name[0..state.name_len], name[0..state.name_len]);
    }
    return state;
}

fn onConnect(_: *ServerState, _: channel.Sender(ServerState)) void {
    connected += 1;
}

fn onDisconnect(_: *ServerState, _: channel.Sender(ServerState)) void {
    disconnected += 1;
}

fn serverHello(_: channel.PostRequest(ServerState, void, void)) !?[]const u8 {
    return "world";
}

fn serverEcho(request: channel.PostRequest(ServerState, std.json.Value, void)) !?std.json.Value {
    return request.body;
}

fn serverName(request: channel.PostRequest(ServerState, void, void)) !?[]const u8 {
    return request.state.name[0..request.state.name_len];
}

/// The server calls the client back while the client waits for this answer
fn serverMirror(request: channel.PostRequest(ServerState, void, void)) !?[]const u8 {
    var result = try request.sender.send(allocator, []const u8, "hello", {});
    defer result.deinit(allocator);
    const text = switch (result) {
        .ok => |value| value orelse return null,
        .remote_err => return error.MirrorFailed,
    };
    const len = @min(text.len, mirror_buf.len);
    @memcpy(mirror_buf[0..len], text[0..len]);
    return mirror_buf[0..len];
}

/// Drops the connection the first time only
fn serverDisconnect(request: channel.PostRequest(ServerState, void, void)) !?[]const u8 {
    if (drop_once) {
        drop_once = false;
        request.sender.stop();
    }
    return "dropped before";
}

fn serverCount(request: channel.StreamRequest(ServerState, void, void, i64)) !void {
    var i: i64 = 0;
    while (i < 3) : (i += 1) try request.emitter.emit(allocator, i);
}

fn serverTick(_: channel.PostRequest(ServerState, void, void)) !?bool {
    try server_events.ticks.send(allocator, "a", "tick");
    return null;
}

const Fixture = struct {
    ch: channel.Channel(ServerState),
    server: channel.Server(ServerState),
    cert: []u8,
    key: []u8,

    fn start(self: *Fixture, address: anytype) !void {
        self.cert = try readPem("cert.pem");
        errdefer allocator.free(self.cert);
        self.key = try readPem("key.pem");
        errdefer allocator.free(self.key);
        connected = 0;
        disconnected = 0;
        drop_once = true;
        server_events.ticks = channel.Subscription([]const u8, []const u8).init();

        self.ch = channel.Channel(ServerState).init(allocator);
        errdefer self.ch.deinit();
        _ = try self.ch.post("hello", serverHello);
        _ = try self.ch.post("echo", serverEcho);
        _ = try self.ch.post("name", serverName);
        _ = try self.ch.post("mirror", serverMirror);
        _ = try self.ch.post("disconnect", serverDisconnect);
        _ = try self.ch.post("tick", serverTick);
        _ = try self.ch.stream(void, i64, void, "count", serverCount);
        _ = try self.ch.events(&server_events, "");
        self.server = try self.ch.listenQuic(allocator, address, .{
            .state = makeState,
            .on_connect = onConnect,
            .on_disconnect = onDisconnect,
            .cert = self.cert,
            .key = self.key,
        });
    }

    fn stop(self: *Fixture) void {
        self.server.deinit();
        self.ch.deinit();
        server_events.ticks.deinit();
        allocator.free(self.cert);
        allocator.free(self.key);
    }

    fn url(self: *Fixture, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "quic://127.0.0.1:{d}", .{self.server.port()});
    }
};

// Client side

const ClientState = struct {};

fn clientHello(_: channel.PostRequest(ClientState, void, void)) !?[]const u8 {
    return "client world";
}

fn clientStreamValues(request: channel.StreamRequest(ClientState, void, void, i64)) !void {
    var i: i64 = 0;
    while (i < 3) : (i += 1) try request.emitter.emit(allocator, i);
}

fn clientChannel() !channel.Channel(ClientState) {
    var ch = channel.Channel(ClientState).init(allocator);
    errdefer ch.deinit();
    _ = try ch.post("hello", clientHello);
    _ = try ch.stream(void, i64, void, "stream/values", clientStreamValues);
    return ch;
}

fn countEvent(user_data: ?*anyopaque, _: []const u8) void {
    const count: *usize = @ptrCast(@alignCast(user_data.?));
    count.* += 1;
}

/// Polls both sides until `done` or 3 s pass.
fn pollUntil(server: ?*channel.Server(ServerState), client: ?*channel.Client(ClientState), context: anytype, comptime done: fn (@TypeOf(context)) bool) bool {
    const deadline = std.time.milliTimestamp() + 3_000;
    while (std.time.milliTimestamp() < deadline) {
        if (done(context)) return true;
        var progressed = false;
        if (server) |s| progressed = (s.poll() catch false) or progressed;
        if (client) |c| progressed = (c.poll() catch false) or progressed;
        if (!progressed) std.Thread.sleep(500_000);
    }
    return done(context);
}

fn expectString(result: anytype, expected: []const u8) !void {
    var mutable = result;
    defer mutable.deinit(allocator);
    switch (mutable) {
        .ok => |value| try testing.expectEqualStrings(expected, value orelse return error.TestUnexpectedResult),
        .remote_err => |text| {
            std.debug.print("remote error: {s}\n", .{text});
            return error.TestUnexpectedResult;
        },
    }
}

fn collectValues(values: anytype) ![3]i64 {
    var got: [3]i64 = undefined;
    var count: usize = 0;
    while (true) {
        var next = try values.next(allocator);
        defer next.deinit(allocator);
        switch (next) {
            .item => |value| {
                if (count == got.len) return error.TestUnexpectedResult;
                got[count] = value;
                count += 1;
            },
            .done => break,
            .remote_err => return error.TestUnexpectedResult,
        }
    }
    if (count != got.len) return error.TestUnexpectedResult;
    return got;
}

test "client and server: posts, hello headers as state, streams, events" {
    var f: Fixture = undefined;
    try f.start("127.0.0.1:0");
    defer f.stop();

    var cch = try clientChannel();
    defer cch.deinit();
    var buf: [64]u8 = undefined;
    var client = try cch.connect(allocator, try f.url(&buf), .{
        .headers = "X-Name: zig\r\n",
        .quic = .{ .ca = f.cert },
    });
    defer client.deinit();

    try expectString(try client.send(allocator, []const u8, "hello", {}), "world");

    // A 40 KB body, larger than one packet
    const text = try allocator.alloc(u8, 40_000);
    defer allocator.free(text);
    for (0..20_000) |i| @memcpy(text[i * 2 .. i * 2 + 2], "é");
    try expectString(try client.send(allocator, []const u8, "echo", text), text);

    // Hello headers become the state, names lowercased
    try expectString(try client.send(allocator, []const u8, "name", {}), "zig");

    {
        var values = try client.values(allocator, void, i64, void, "count", {}, {});
        defer values.close();
        try testing.expectEqual([3]i64{ 0, 1, 2 }, try collectValues(&values));
    }

    {
        var count: usize = 0;
        var subscribed = try client.subscribe(allocator, []const u8, []const u8, void, "ticks", "a", {}, &count, countEvent);
        defer subscribed.deinit(allocator);
        var subscription = switch (subscribed) {
            .ok => |value| value,
            .remote_err => return error.TestUnexpectedResult,
        };
        try server_events.ticks.send(allocator, "a", "tick");
        try server_events.ticks.send(allocator, "b", "not for us");
        try server_events.ticks.send(allocator, "a", "tick");
        const Count = struct {
            fn two(value: *usize) bool {
                return value.* == 2;
            }
        };
        try testing.expect(pollUntil(&f.server, &client, &count, Count.two));
        subscription.cancel();
        _ = pollUntil(&f.server, &client, &count, struct {
            fn never(_: *usize) bool {
                return false;
            }
        }.never);
        try server_events.ticks.send(allocator, "a", "after cancel");
        _ = pollUntil(&f.server, &client, &count, Count.two);
        try testing.expectEqual(@as(usize, 2), count);
    }

    // Many requests one after another on the one stream
    for (0..300) |i| {
        var result = try client.send(allocator, i64, "echo", @as(i64, @intCast(i)));
        defer result.deinit(allocator);
        try testing.expectEqual(@as(?i64, @intCast(i)), result.ok);
    }

    try testing.expectEqual(@as(usize, 1), connected);
    client.stop();
    const Stats = struct {
        fn gone(_: void) bool {
            return disconnected == 1;
        }
    };
    try testing.expect(pollUntil(&f.server, null, {}, Stats.gone));
}

test "certificate trust: CA, server name, pin; wrong pin, unknown CA and wrong name refused" {
    var f: Fixture = undefined;
    try f.start("127.0.0.1:0");
    defer f.stop();
    var buf: [64]u8 = undefined;
    const address = try f.url(&buf);
    var cch = try clientChannel();
    defer cch.deinit();

    {
        var pinned = try cch.connect(allocator, address, .{ .quic = .{ .fingerprint = "ddd23700bd8abd291415d86584bec28010ff21daa1c59db34a7f3663ef4b559b" } });
        defer pinned.deinit();
        try expectString(try pinned.send(allocator, []const u8, "hello", {}), "world");
    }
    {
        var by_name = try cch.connect(allocator, address, .{ .quic = .{ .ca = f.cert, .server_name = "localhost" } });
        defer by_name.deinit();
        try expectString(try by_name.send(allocator, []const u8, "hello", {}), "world");
    }

    try testing.expectError(error.CertificateVerifyFailed, cch.connect(allocator, address, .{}));
    try testing.expectError(error.CertificateFingerprintMismatch, cch.connect(allocator, address, .{ .quic = .{ .fingerprint = "00" ** 32 } }));
    try testing.expectError(error.CertificateVerifyFailed, cch.connect(allocator, address, .{ .quic = .{ .ca = f.cert, .server_name = "example.com" } }));
    try testing.expectError(error.InvalidFingerprint, cch.connect(allocator, address, .{ .quic = .{ .fingerprint = "nope" } }));
    try testing.expectError(error.InvalidAddress, cch.connect(allocator, "quic://127.0.0.1", .{}));

    // The server kept serving through the failed handshakes
    var again = try cch.connect(allocator, address, .{ .quic = .{ .ca = f.cert } });
    defer again.deinit();
    try expectString(try again.send(allocator, []const u8, "hello", {}), "world");
}

// Raw connections, without the Channel client, to check the wire rules

const Raw = struct {
    conn: engine.ClientConn,
    reader: channel.quic.FrameReader = .{},

    fn open(f: *Fixture) !Raw {
        const address = try std.net.Address.parseIp("127.0.0.1", f.server.port());
        var conn = try engine.ClientConn.open(allocator, "127.0.0.1", address, .{ .ca = f.cert });
        errdefer conn.deinit();
        const deadline = std.time.milliTimestamp() + 3_000;
        while (!(try conn.handshake())) {
            _ = try f.server.poll();
            if (std.time.milliTimestamp() > deadline) return error.Timeout;
            std.Thread.sleep(200_000);
        }
        return .{ .conn = conn };
    }

    fn deinit(self: *Raw) void {
        self.reader.deinit(allocator);
        self.conn.deinit();
    }

    fn send(self: *Raw, f: *Fixture, bytes: []const u8) !void {
        var rest = bytes;
        const deadline = std.time.milliTimestamp() + 3_000;
        while (rest.len > 0) {
            const n = try engine.write(self.conn.ssl, rest);
            rest = rest[n..];
            if (n == 0) {
                _ = try f.server.poll();
                self.conn.tick();
                if (std.time.milliTimestamp() > deadline) return error.Timeout;
            }
        }
        self.conn.tick();
    }

    /// The next frame from the server, parsed (caller frees).
    fn next(self: *Raw, f: *Fixture) !std.json.Parsed(std.json.Value) {
        const deadline = std.time.milliTimestamp() + 3_000;
        while (std.time.milliTimestamp() < deadline) {
            if (try self.reader.next()) |payload| return std.json.parseFromSlice(std.json.Value, allocator, payload, .{});
            _ = try f.server.poll();
            const space = try self.reader.writable(allocator, 65536);
            const n = engine.read(self.conn.ssl, space) catch |err| switch (err) {
                error.WouldBlock => {
                    std.Thread.sleep(200_000);
                    continue;
                },
                else => return err,
            };
            self.reader.commit(n);
        }
        return error.Timeout;
    }

    /// Whether the server closed the connection within 2 s.
    fn closedByServer(self: *Raw, f: *Fixture) bool {
        const deadline = std.time.milliTimestamp() + 2_000;
        var buf: [4096]u8 = undefined;
        while (std.time.milliTimestamp() < deadline) {
            _ = f.server.poll() catch {};
            self.conn.tick();
            _ = engine.read(self.conn.ssl, &buf) catch |err| switch (err) {
                error.WouldBlock => {},
                else => return true,
            };
            if (self.conn.isTerminated()) return true;
            std.Thread.sleep(500_000);
        }
        return false;
    }
};

fn frames(parts: []const []const u8) ![]u8 {
    var list: std.ArrayListUnmanaged(u8) = .{};
    errdefer list.deinit(allocator);
    for (parts) |part| {
        if (part.len == 0) try list.append(allocator, 0) else try channel.quic.appendFrame(&list, allocator, part);
    }
    return list.toOwnedSlice(allocator);
}

test "wire rules: raw frames, a keepalive, a batch" {
    var f: Fixture = undefined;
    try f.start("127.0.0.1:0");
    defer f.stop();
    var raw = try Raw.open(&f);
    defer raw.deinit();
    // The server asks for the 30 s idle timeout
    try testing.expectEqual(@as(?u64, 30_000), engine.peerIdleTimeout(raw.conn.ssl));

    const bytes = try frames(&.{
        "{\"hello\":{\"X-Name\":\"raw\",\"ignored\":1}}",
        "", // keepalive
        "[{\"id\":1,\"path\":\"name\"},{\"id\":2,\"path\":\"hello\"}]",
    });
    defer allocator.free(bytes);
    try raw.send(&f, bytes);

    var seen: usize = 0;
    while (seen < 2) : (seen += 1) {
        var message = try raw.next(&f);
        defer message.deinit();
        const object = message.value.object;
        const body = object.get("body").?.string;
        switch (object.get("id").?.integer) {
            1 => try testing.expectEqualStrings("raw", body),
            2 => try testing.expectEqualStrings("world", body),
            else => return error.TestUnexpectedResult,
        }
    }
}

test "wire rules: one stream per connection" {
    var f: Fixture = undefined;
    try f.start("127.0.0.1:0");
    defer f.stop();
    var raw = try Raw.open(&f);
    defer raw.deinit();
    const hello = try frames(&.{"{\"hello\":{}}"});
    defer allocator.free(hello);
    try raw.send(&f, hello);
    // The server has its stream before the next ones arrive
    _ = pollUntil(&f.server, null, {}, struct {
        fn wait(_: void) bool {
            return connected == 1;
        }
    }.wait);

    for ([_]bool{ false, true }) |unidirectional| {
        const extra = try raw.conn.openExtraStream(unidirectional);
        defer engine.freeStream(extra);
        _ = try engine.write(extra, "\x05hello");
        const deadline = std.time.milliTimestamp() + 2_000;
        while (!engine.streamRefused(extra) and std.time.milliTimestamp() < deadline) {
            _ = try f.server.poll();
            raw.conn.tick();
            std.Thread.sleep(500_000);
        }
        try testing.expect(engine.streamRefused(extra));
    }

    // The first stream still works
    const request = try frames(&.{"{\"id\":7,\"path\":\"hello\"}"});
    defer allocator.free(request);
    try raw.send(&f, request);
    var message = try raw.next(&f);
    defer message.deinit();
    try testing.expectEqualStrings("world", message.value.object.get("body").?.string);
}

test "wire rules: a first frame that isn't a hello, invalid JSON and an oversized frame close the connection" {
    var f: Fixture = undefined;
    try f.start("127.0.0.1:0");
    defer f.stop();

    const not_hello = try frames(&.{"{\"id\":1,\"path\":\"hello\"}"});
    defer allocator.free(not_hello);
    const invalid_json = try frames(&.{ "{\"hello\":{}}", "{nope" });
    defer allocator.free(invalid_json);
    const hello = try frames(&.{"{\"hello\":{}}"});
    defer allocator.free(hello);
    const oversized = try std.mem.concat(allocator, u8, &.{ hello, &.{ 0x80 | 0x01, 0, 0, 1 } });
    defer allocator.free(oversized);

    for ([_][]const u8{ not_hello, invalid_json, oversized }) |bytes| {
        var raw = try Raw.open(&f);
        defer raw.deinit();
        try raw.send(&f, bytes);
        try testing.expect(raw.closedByServer(&f));
    }
    // The server is still up
    var raw = try Raw.open(&f);
    defer raw.deinit();
    try raw.send(&f, hello);
}

test "a dropped connection fails its call and stops the client; the server's stop closes connections" {
    var f: Fixture = undefined;
    try f.start("127.0.0.1:0");
    var running = true;
    defer if (running) f.stop();
    const port = f.server.port();
    var buf: [64]u8 = undefined;
    const address = try f.url(&buf);
    var cch = try clientChannel();
    defer cch.deinit();

    {
        var client = try cch.connect(allocator, address, .{ .quic = .{ .ca = f.cert } });
        defer client.deinit();
        try expectString(try client.send(allocator, []const u8, "hello", {}), "world");
        // The server drops the connection while the call waits; like the WebSocket client this one
        // doesn't reconnect, so the call fails
        var dropped = try client.send(allocator, []const u8, "disconnect", {});
        defer dropped.deinit(allocator);
        try testing.expect(dropped == .remote_err);
        try testing.expect(!client.isConnected());
        try testing.expectError(error.NotConnected, client.send(allocator, []const u8, "hello", {}));
    }

    // A new connection works (the second one isn't dropped)
    var client = try cch.connect(allocator, address, .{ .quic = .{ .ca = f.cert } });
    defer client.deinit();
    try expectString(try client.send(allocator, []const u8, "disconnect", {}), "dropped before");
    try testing.expectEqual(@as(usize, 2), connected);

    // The server stopping sends CONNECTION_CLOSE: the client notices without waiting for the idle timeout
    f.server.stop();
    const Gone = struct {
        fn check(c: *channel.Client(ClientState)) bool {
            return !c.isConnected();
        }
    };
    try testing.expect(pollUntil(null, &client, &client, Gone.check));
    f.stop();
    running = false;

    // The port can be served again
    var g: Fixture = undefined;
    try g.start(port);
    defer g.stop();
    var next = try cch.connect(allocator, address, .{ .quic = .{ .ca = g.cert } });
    defer next.deinit();
    try expectString(try next.send(allocator, []const u8, "hello", {}), "world");
}

test "connecting where nothing listens fails" {
    var cch = try clientChannel();
    defer cch.deinit();
    const cert = try readPem("cert.pem");
    defer allocator.free(cert);
    // Loopback answers with ICMP port unreachable, so this fails at once rather than after the 10 s timeout
    try testing.expectError(error.HandshakeFailed, cch.connect(allocator, "quic://127.0.0.1:1", .{ .quic = .{ .ca = cert } }));
}

// Interop with the TypeScript reference

fn fixturePort() u16 {
    const text = std.process.getEnvVarOwned(allocator, "CHANNEL_TEST_PORT") catch return 2049;
    defer allocator.free(text);
    return std.fmt.parseInt(u16, text, 10) catch 2049;
}

fn waitRemoteTopic(client: *channel.Client(ClientState), topic: []const u8, expected: bool) bool {
    const deadline = std.time.milliTimestamp() + 3_000;
    while (std.time.milliTimestamp() < deadline) {
        if (client.isRemoteSubscribed(topic) == expected) return true;
        _ = client.poll() catch {};
        std.Thread.sleep(1_000_000);
    }
    return false;
}

test "interop: Zig client against the TypeScript fixture's QUIC server" {
    const port = fixturePort();
    // An older checkout of the reference whose fixture has no QUIC server
    const fixture = std.fs.cwd().readFileAlloc(allocator, "../channel/tests/server.ts", 1 << 20) catch return error.SkipZigTest;
    defer allocator.free(fixture);
    if (std.mem.indexOf(u8, fixture, "listenQuic") == null) return error.SkipZigTest;
    // The fixture serves QUIC on UDP next to WebSocket on TCP, same port number
    const tcp = std.net.tcpConnectToAddress(try std.net.Address.parseIp("127.0.0.1", port)) catch return error.SkipZigTest;
    tcp.close();
    const cert = try readPem("cert.pem");
    defer allocator.free(cert);

    var events = .{ .names = channel.Subscription([]const u8, []const u8).init() };
    defer events.names.deinit();
    var cch = try clientChannel();
    defer cch.deinit();
    _ = try cch.events(&events, "");

    var buf: [64]u8 = undefined;
    const address = try std.fmt.bufPrint(&buf, "quic://127.0.0.1:{d}", .{port});
    var client = try cch.connect(allocator, address, .{ .headers = "X-Port: zig\r\n", .quic = .{ .ca = cert } });
    defer client.deinit();

    try expectString(try client.send(allocator, []const u8, "hello", {}), "world");
    {
        var echo = try client.send(allocator, f64, "echo", 0.1337);
        defer echo.deinit(allocator);
        try testing.expectEqual(@as(?f64, 0.1337), echo.ok);
    }
    try expectString(try client.send(allocator, []const u8, "mirror", {}), "client world");
    try expectString(try client.send(allocator, []const u8, "auth", .{ .name = "quic" }), "quic");
    try expectString(try client.send(allocator, []const u8, "auth/name", {}), "quic");
    {
        var headers = try client.send(allocator, std.json.Value, "v2/headers", {});
        defer headers.deinit(allocator);
        const value = headers.ok orelse return error.TestUnexpectedResult;
        try testing.expectEqualStrings("zig", value.object.get("x-port").?.string);
    }
    {
        // The fixture calls this client's stream/values
        var values = try client.values(allocator, void, i64, void, "mirror/stream", {}, {});
        defer values.close();
        try testing.expectEqual([3]i64{ 0, 1, 2 }, try collectValues(&values));
    }

    // The fixture subscribes to this client's names/1 and cancels after the first event
    {
        var result = try client.send(allocator, void, "mirror/events", {});
        result.deinit(allocator);
    }
    try testing.expect(waitRemoteTopic(&client, "names/1", true));
    try events.names.send(allocator, "1", "h");
    try testing.expect(waitRemoteTopic(&client, "names/1", false));

    {
        var failed = try client.send(allocator, []const u8, "v2/error/code", {});
        defer failed.deinit(allocator);
        try testing.expect(failed == .remote_err);
    }

    // The fixture drops the connection without answering: the call fails and the client stops
    {
        var reset = try client.send(allocator, void, "v2/drops/reset", {});
        reset.deinit(allocator);
        var dropped = try client.send(allocator, []const u8, "v2/drop", {});
        defer dropped.deinit(allocator);
        try testing.expect(dropped == .remote_err);
        try testing.expect(!client.isConnected());
    }
    var second = try cch.connect(allocator, address, .{ .quic = .{ .fingerprint = fingerprint } });
    defer second.deinit();
    {
        var drops = try second.send(allocator, i64, "v2/drops", {});
        defer drops.deinit(allocator);
        try testing.expectEqual(@as(?i64, 1), drops.ok);
    }
}

fn pollServer(server: *channel.Server(ServerState), stop: *std.atomic.Value(bool)) void {
    while (!stop.load(.acquire)) {
        const progressed = server.poll() catch false;
        if (!progressed) std.Thread.sleep(200_000);
    }
}

test "interop: the TypeScript reference's QUIC client against this server" {
    std.fs.cwd().access("../channel/quic.ts", .{}) catch return error.SkipZigTest;
    var f: Fixture = undefined;
    try f.start("127.0.0.1:0");
    defer f.stop();

    // The server runs on its own thread while bun runs; nothing else touches it meanwhile
    var stop = std.atomic.Value(bool).init(false);
    const thread = try std.Thread.spawn(.{}, pollServer, .{ &f.server, &stop });
    var port_buf: [8]u8 = undefined;
    const port = try std.fmt.bufPrint(&port_buf, "{d}", .{f.server.port()});
    const result = std.process.Child.run(.{
        .allocator = allocator,
        .argv = &.{ "timeout", "30", "bun", "tests/quic/client.ts", port, cert_dir ++ "/cert.pem" },
    }) catch |err| {
        stop.store(true, .release);
        thread.join();
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    // Wait for the server to see the client go, then stop the thread
    const deadline = std.time.milliTimestamp() + 3_000;
    while (disconnected < connected and std.time.milliTimestamp() < deadline) std.Thread.sleep(1_000_000);
    stop.store(true, .release);
    thread.join();

    if (result.term != .Exited or result.term.Exited != 0) {
        std.debug.print("bun failed: {any}\n{s}\n", .{ result.term, result.stderr });
        return error.TestUnexpectedResult;
    }
    try testing.expectEqualStrings(
        "{\"hello\":\"world\",\"echo\":{\"a\":[1,\"b\"]},\"name\":\"ts\",\"mirror\":\"ts world\",\"values\":[0,1,2],\"event\":\"tick\"}\n",
        result.stdout,
    );
    try testing.expectEqual(@as(usize, 1), connected);
    try testing.expectEqual(connected, disconnected);
}
