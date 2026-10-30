const std = @import("std");
const websocket = @import("websocket");
const wire = @import("wire.zig");
const json_codec = @import("json_codec.zig");
const quic_frame = @import("quic_frame.zig");
/// OpenSSL's QUIC with `-Dquic=true`, a stub otherwise (quic_disabled.zig)
const quic_engine = @import("quic_engine");

/// QUIC framing helpers (frames, hello, fingerprints); usable without `-Dquic`.
pub const quic = quic_frame;
/// Whether this build has the QUIC transport (`zig build -Dquic=true`, links the system OpenSSL 3.5+).
pub const quic_enabled = quic_engine.enabled;

pub const Allocator = std.mem.Allocator;
const net = std.net;
const posix = std.posix;

const MessageKind = enum {
    text,
    binary,
};

const TransportMode = enum {
    process,
    websocket_client,
    websocket_server,
    quic_client,
    quic_server,
};

const TransportVTable = struct {
    send_frame: *const fn (ctx: *anyopaque, payload: []u8) anyerror!void,
    close: *const fn (ctx: *anyopaque) void,
    is_connected: *const fn (ctx: *anyopaque) bool,
};

const PumpEntry = struct {
    ptr: ?*anyopaque,
    poll_fn: *const fn (ptr: *anyopaque) anyerror!bool,
};

var g_pump_entries: std.ArrayListUnmanaged(PumpEntry) = .{};
var g_is_pumping = false;

fn registerPumpEntry(ptr: *anyopaque, poll_fn: *const fn (ptr: *anyopaque) anyerror!bool) !void {
    for (g_pump_entries.items) |*entry| {
        if (entry.ptr == null) {
            entry.* = .{
                .ptr = ptr,
                .poll_fn = poll_fn,
            };
            return;
        }
    }
    try g_pump_entries.append(std.heap.page_allocator, .{
        .ptr = ptr,
        .poll_fn = poll_fn,
    });
}

fn unregisterPumpEntry(ptr: *anyopaque) void {
    for (g_pump_entries.items) |*entry| {
        if (entry.ptr == ptr) {
            entry.ptr = null;
            return;
        }
    }
}

fn pumpAll() !bool {
    if (g_is_pumping) return false;
    g_is_pumping = true;
    defer g_is_pumping = false;

    var progressed = false;
    for (g_pump_entries.items) |entry| {
        const ptr = entry.ptr orelse continue;
        progressed = (try entry.poll_fn(ptr)) or progressed;
    }
    return progressed;
}

const OwnedMessage = struct {
    id: ?i64 = null,
    path: ?[]u8 = null,
    stream: ?[]u8 = null,
    sub: ?[]u8 = null,
    unsub: ?[]u8 = null,
    cancel: ?i64 = null,
    topic: ?[]u8 = null,
    body_json: ?[]u8 = null,
    context_json: ?[]u8 = null,
    error_text: ?[]u8 = null,
    done: ?bool = null,

    fn deinit(self: *OwnedMessage, allocator: Allocator) void {
        freeOptionalOwned(allocator, &self.path);
        freeOptionalOwned(allocator, &self.stream);
        freeOptionalOwned(allocator, &self.sub);
        freeOptionalOwned(allocator, &self.unsub);
        freeOptionalOwned(allocator, &self.topic);
        freeOptionalOwned(allocator, &self.body_json);
        freeOptionalOwned(allocator, &self.context_json);
        freeOptionalOwned(allocator, &self.error_text);
        self.* = .{};
    }

    fn toOutgoing(self: *const OwnedMessage) wire.OutgoingMessage {
        return .{
            .id = self.id,
            .path = if (self.path) |value| value else null,
            .stream = if (self.stream) |value| value else null,
            .sub = if (self.sub) |value| value else null,
            .unsub = if (self.unsub) |value| value else null,
            .cancel = self.cancel,
            .topic = if (self.topic) |value| value else null,
            .body_json = if (self.body_json) |value| value else null,
            .context_json = if (self.context_json) |value| value else null,
            .error_text = if (self.error_text) |value| value else null,
            .done = self.done,
        };
    }

    fn intoParsed(self: *OwnedMessage) wire.ParsedMessage {
        const parsed: wire.ParsedMessage = .{
            .id = self.id,
            .path = self.path,
            .stream = self.stream,
            .sub = self.sub,
            .unsub = self.unsub,
            .cancel = self.cancel,
            .topic = self.topic,
            .body_json = self.body_json,
            .context_json = self.context_json,
            .error_text = self.error_text,
            .done = self.done,
        };
        self.* = .{};
        return parsed;
    }
};

const PublisherSink = struct {
    ptr: *anyopaque,
    send: *const fn (ptr: *anyopaque, topic: []const u8, body_json: ?[]const u8) void,
};

const SubscriptionBinding = struct {
    ptr: *anyopaque,
    subscribe: *const fn (ptr: *anyopaque, allocator: Allocator, body_json: ?[]const u8) anyerror!SubscriptionAck,
    add_publisher: *const fn (ptr: *anyopaque, allocator: Allocator, sink: PublisherSink) anyerror!usize,
    remove_publisher: *const fn (ptr: *anyopaque, publisher_id: usize) void,
    set_prefix: *const fn (ptr: *anyopaque, allocator: Allocator, prefix: []const u8) anyerror!void,
};

const SubscriptionAck = struct {
    topic: []u8,
    body_json: ?[]u8 = null,

    fn deinit(self: *SubscriptionAck, allocator: Allocator) void {
        allocator.free(self.topic);
        if (self.body_json) |value| allocator.free(value);
        self.* = .{
            .topic = &.{},
        };
    }
};

const PublisherRegistration = struct {
    binding: SubscriptionBinding,
    publisher_id: usize,
};

pub fn CallResult(comptime T: type) type {
    return union(enum) {
        ok: ?T,
        remote_err: []u8,

        const Self = @This();

        pub fn deinit(self: *Self, allocator: Allocator) void {
            switch (self.*) {
                .ok => |*value| {
                    if (value.*) |*payload| {
                        json_codec.freeOwnedValue(allocator, T, payload);
                    }
                },
                .remote_err => |value| allocator.free(value),
            }
            self.* = .{ .ok = null };
        }
    };
}

pub fn StreamNext(comptime T: type) type {
    return union(enum) {
        item: T,
        done,
        remote_err: []u8,

        const Self = @This();

        pub fn deinit(self: *Self, allocator: Allocator) void {
            switch (self.*) {
                .item => |*value| json_codec.freeOwnedValue(allocator, T, value),
                .remote_err => |value| allocator.free(value),
                .done => {},
            }
            self.* = .done;
        }
    };
}

pub const SubscribeResult = union(enum) {
    ok: SubscriptionHandle,
    remote_err: []u8,

    pub fn deinit(self: *SubscribeResult, allocator: Allocator) void {
        switch (self.*) {
            .ok => {},
            .remote_err => |value| allocator.free(value),
        }
        self.* = .{ .remote_err = "" };
    }
};

pub const SubscriptionHandle = struct {
    ptr: ?*anyopaque = null,
    cancel_fn: ?*const fn (ptr: *anyopaque) void = null,

    pub fn cancel(self: *SubscriptionHandle) void {
        const raw = self.ptr orelse return;
        const cancel_fn = self.cancel_fn orelse return;
        self.ptr = null;
        self.cancel_fn = null;
        cancel_fn(raw);
    }
};

pub fn ConnectOptions(comptime State: type) type {
    return struct {
        state: ?State = null,
        /// Extra headers, "Name: value\r\n" lines. Over QUIC they go in the hello frame.
        headers: ?[]const u8 = null,
        /// TLS settings for `quic://` addresses.
        quic: QuicOptions = .{},
    };
}

/// TLS settings for `quic://` addresses. Without any, the server certificate must be trusted by the system.
pub const QuicOptions = struct {
    /// PEM certificate (or CA) to trust, for self-signed servers.
    ca: ?[]const u8 = null,
    /// SHA-256 fingerprint of the server certificate (hex, colons optional), pinned instead of verifying it.
    fingerprint: ?[]const u8 = null,
    /// Overrides the name sent (SNI) and checked; defaults to the address host.
    server_name: ?[]const u8 = null,
};

pub fn QuicListenOptions(comptime State: type) type {
    return struct {
        state: ?*const fn (headers: *const std.StringHashMapUnmanaged([]const u8)) anyerror!State = null,
        on_connect: ?*const fn (state: *State, sender: Sender(State)) void = null,
        on_disconnect: ?*const fn (state: *State, sender: Sender(State)) void = null,
        /// PEM certificate chain, leaf first.
        cert: []const u8,
        /// PEM private key.
        key: []const u8,
    };
}

pub fn ListenOptions(comptime State: type) type {
    return struct {
        state: ?*const fn (headers: *const std.StringHashMapUnmanaged([]const u8)) anyerror!State = null,
        on_connect: ?*const fn (state: *State, sender: Sender(State)) void = null,
        on_disconnect: ?*const fn (state: *State, sender: Sender(State)) void = null,
    };
}

pub fn PostRequest(comptime State: type, comptime Req: type, comptime Ctx: type) type {
    return struct {
        body: Req,
        context: Ctx,
        sender: Sender(State),
        state: *State,
        path: []const u8,
    };
}

pub fn StreamEmitter(comptime Item: type) type {
    return struct {
        ptr: *anyopaque,
        emit_fn: *const fn (ptr: *anyopaque, allocator: Allocator, item: Item) anyerror!void,
        is_cancelled_fn: *const fn (ptr: *anyopaque) bool,

        pub fn emit(self: *const @This(), allocator: Allocator, item: Item) !void {
            return self.emit_fn(self.ptr, allocator, item);
        }

        pub fn isCancelled(self: *const @This()) bool {
            return self.is_cancelled_fn(self.ptr);
        }
    };
}

pub fn StreamRequest(comptime State: type, comptime Req: type, comptime Ctx: type, comptime Item: type) type {
    return struct {
        body: Req,
        context: Ctx,
        sender: Sender(State),
        state: *State,
        path: []const u8,
        emitter: StreamEmitter(Item),
    };
}

pub fn Values(comptime Item: type) type {
    return struct {
        ptr: *anyopaque,
        next_fn: *const fn (ptr: *anyopaque, allocator: Allocator) anyerror!StreamNext(Item),
        close_fn: *const fn (ptr: *anyopaque) void,

        pub fn next(self: *@This(), allocator: Allocator) !StreamNext(Item) {
            return self.next_fn(self.ptr, allocator);
        }

        pub fn close(self: *@This()) void {
            self.close_fn(self.ptr);
        }
    };
}

pub fn Subscription(comptime Req: type, comptime Event: type) type {
    return struct {
        pub const __channel_subscription_marker = true;

        allocator: ?Allocator = null,
        prefix: []const u8 = "",
        owned_prefix: ?[]u8 = null,
        publishers: std.ArrayListUnmanaged(PublisherSlot) = .{},
        next_publisher_id: usize = 1,
        /// Custom topic formatter (TS `Subscription.topic(make)`). Called with a scratch arena; the
        /// returned slice is copied, so it may point into the arena or static memory.
        topic_fn: ?TopicFn = null,
        /// Custom body formatter (TS `Subscription.body(make)`): its value is returned in the
        /// subscribe ack and used by `sendDefault` / a null `send` event. Called with a scratch
        /// arena; the result is encoded before the arena is freed.
        body_fn: ?BodyFn = null,

        const Self = @This();
        pub const TopicFn = *const fn (allocator: Allocator, request: Req) anyerror![]const u8;
        pub const BodyFn = *const fn (allocator: Allocator, request: Req) anyerror!?Event;

        const PublisherSlot = struct {
            id: usize,
            sink: PublisherSink,
        };

        pub fn init() Self {
            return .{};
        }

        pub fn deinit(self: *Self) void {
            const allocator = self.allocator orelse return;
            if (self.owned_prefix) |value| allocator.free(value);
            self.publishers.deinit(allocator);
            self.* = .{};
        }

        /// Sets the topic formatter (TS `topic(make)`).
        pub fn topic(self: *Self, make: TopicFn) *Self {
            self.topic_fn = make;
            return self;
        }

        /// Sets the body formatter (TS `body(make)`; TS commit 6d42247: it sets the body, never
        /// the topic).
        pub fn body(self: *Self, make: BodyFn) *Self {
            self.body_fn = make;
            return self;
        }

        /// Publishes `event` to subscribers of `request`'s topic. Like TS `publish`, falsy bodies
        /// (null, false, 0, "") are not sent. A null `event` (optional Event) falls back to the
        /// body formatter, like TS `body ?? this._body(request)`.
        pub fn send(self: *Self, allocator: Allocator, request: Req, event: Event) !void {
            if (comptime json_codec.isOptionalType(Event)) {
                if (event == null) return self.sendDefault(allocator, request);
            }
            const body_json = try json_codec.encodeOptionalAlloc(allocator, Event, event);
            defer if (body_json) |value| allocator.free(value);
            try self.publishJson(allocator, request, body_json);
        }

        /// Publishes the body formatter's value for `request` (TS `send(request)` with no body).
        pub fn sendDefault(self: *Self, allocator: Allocator, request: Req) !void {
            const body_json = try self.defaultBodyJson(allocator, request);
            defer if (body_json) |value| allocator.free(value);
            try self.publishJson(allocator, request, body_json);
        }

        fn publishJson(self: *Self, allocator: Allocator, request: Req, body_json: ?[]const u8) !void {
            if (isFalsyJson(body_json)) return;
            const topic_value = try self.topicAlloc(allocator, request);
            defer allocator.free(topic_value);
            self.publishEncoded(topic_value, body_json);
        }

        fn topicAlloc(self: *Self, allocator: Allocator, request: Req) ![]u8 {
            const local_topic = if (self.topic_fn) |make| blk: {
                var arena = std.heap.ArenaAllocator.init(allocator);
                defer arena.deinit();
                break :blk try allocator.dupe(u8, try make(arena.allocator(), request));
            } else try topicFromValueAlloc(allocator, Req, request);
            defer allocator.free(local_topic);
            return joinTopicAlloc(allocator, self.prefix, local_topic);
        }

        fn defaultBodyJson(self: *Self, allocator: Allocator, request: Req) !?[]u8 {
            const make = self.body_fn orelse return null;
            if (Event == void) return null;
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            const value = (try make(arena.allocator(), request)) orelse return null;
            return try json_codec.encodeOptionalAlloc(allocator, Event, value);
        }

        fn erased(self: *Self) SubscriptionBinding {
            return .{
                .ptr = self,
                .subscribe = subscribeErased,
                .add_publisher = addPublisherErased,
                .remove_publisher = removePublisherErased,
                .set_prefix = setPrefixErased,
            };
        }

        fn subscribeErased(ptr: *anyopaque, allocator: Allocator, body_json: ?[]const u8) !SubscriptionAck {
            const self: *Self = @ptrCast(@alignCast(ptr));
            var request = try json_codec.decodeRequired(allocator, Req, body_json);
            defer json_codec.freeOwnedValue(allocator, Req, &request);
            const topic_value = try self.topicAlloc(allocator, request);
            errdefer allocator.free(topic_value);
            return .{
                .topic = topic_value,
                .body_json = try self.defaultBodyJson(allocator, request),
            };
        }

        fn addPublisherErased(ptr: *anyopaque, allocator: Allocator, sink: PublisherSink) !usize {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.allocator = allocator;
            const id = self.next_publisher_id;
            self.next_publisher_id += 1;
            try self.publishers.append(allocator, .{
                .id = id,
                .sink = sink,
            });
            return id;
        }

        fn removePublisherErased(ptr: *anyopaque, publisher_id: usize) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const allocator = self.allocator orelse return;
            var index: usize = 0;
            while (index < self.publishers.items.len) : (index += 1) {
                if (self.publishers.items[index].id == publisher_id) {
                    _ = self.publishers.swapRemove(index);
                    if (self.publishers.items.len == 0) {
                        self.publishers.deinit(allocator);
                        self.publishers = .{};
                    }
                    return;
                }
            }
        }

        fn setPrefixErased(ptr: *anyopaque, allocator: Allocator, prefix: []const u8) !void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.allocator = allocator;
            if (self.owned_prefix) |value| allocator.free(value);
            self.owned_prefix = try allocator.dupe(u8, prefix);
            self.prefix = self.owned_prefix.?;
        }

        fn publishEncoded(self: *Self, event_topic: []const u8, body_json: ?[]const u8) void {
            const allocator = self.allocator;
            const publisher_count = self.publishers.items.len;
            if (publisher_count == 0) {
                return;
            }
            var copy = if (allocator) |alloc| alloc.alloc(PublisherSink, publisher_count) catch null else null;
            if (copy == null) {
                return;
            }
            for (self.publishers.items, 0..) |item, index| {
                copy.?[index] = item.sink;
            }
            defer allocator.?.free(copy.?);

            for (copy.?) |sink| {
                sink.send(sink.ptr, event_topic, body_json);
            }
        }
    };
}

pub fn postSpec(comptime Req: type, comptime Resp: type, comptime Ctx: type, comptime handler: anytype) PostSpec(Req, Resp, Ctx, handler) {
    return .{};
}

pub fn streamSpec(comptime Req: type, comptime Item: type, comptime Ctx: type, comptime handler: anytype) StreamSpec(Req, Item, Ctx, handler) {
    return .{};
}

fn PostSpec(comptime Req: type, comptime Resp: type, comptime Ctx: type, comptime handler: anytype) type {
    return struct {
        pub const __channel_post_spec = true;
        pub const req_type = Req;
        pub const resp_type = Resp;
        pub const ctx_type = Ctx;
        pub const handler_fn = handler;
    };
}

fn StreamSpec(comptime Req: type, comptime Item: type, comptime Ctx: type, comptime handler: anytype) type {
    return struct {
        pub const __channel_stream_spec = true;
        pub const req_type = Req;
        pub const item_type = Item;
        pub const ctx_type = Ctx;
        pub const handler_fn = handler;
    };
}

pub fn Sender(comptime State: type) type {
    return struct {
        peer: *Peer(State),

        const Self = @This();

        pub fn send(
            self: *const Self,
            allocator: Allocator,
            comptime Resp: type,
            path: []const u8,
            body: anytype,
        ) !CallResult(Resp) {
            return self.peer.sendTyped(allocator, @TypeOf(body), Resp, void, path, body, {});
        }

        pub fn sendWithContext(
            self: *const Self,
            allocator: Allocator,
            comptime Resp: type,
            path: []const u8,
            body: anytype,
            context: anytype,
        ) !CallResult(Resp) {
            return self.peer.sendTyped(allocator, @TypeOf(body), Resp, @TypeOf(context), path, body, context);
        }

        pub fn values(
            self: *const Self,
            allocator: Allocator,
            comptime Req: type,
            comptime Item: type,
            comptime Ctx: type,
            path: []const u8,
            body: Req,
            context: Ctx,
        ) !Values(Item) {
            _ = allocator;
            return self.peer.valuesTyped(Req, Item, Ctx, path, body, context);
        }

        pub fn subscribe(
            self: *const Self,
            allocator: Allocator,
            comptime Req: type,
            comptime Event: type,
            comptime Ctx: type,
            path: []const u8,
            body: Req,
            context: Ctx,
            user_data: ?*anyopaque,
            comptime callback: *const fn (?*anyopaque, Event) void,
        ) !SubscribeResult {
            return self.peer.subscribeTyped(allocator, Req, Event, Ctx, path, body, context, user_data, callback);
        }

        pub fn stop(self: *const Self) void {
            self.peer.stop();
        }
    };
}

pub fn Process(comptime State: type) type {
    return struct {
        allocator: Allocator,
        peer: *Peer(State),

        const Self = @This();

        pub fn deinit(self: *Self) void {
            self.peer.destroy();
            self.* = undefined;
        }

        pub fn sender(self: *Self) Sender(State) {
            return .{ .peer = self.peer };
        }

        pub fn send(
            self: *Self,
            allocator: Allocator,
            comptime Resp: type,
            path: []const u8,
            body: anytype,
        ) !CallResult(Resp) {
            return self.sender().send(allocator, Resp, path, body);
        }

        pub fn sendWithContext(
            self: *Self,
            allocator: Allocator,
            comptime Resp: type,
            path: []const u8,
            body: anytype,
            context: anytype,
        ) !CallResult(Resp) {
            return self.sender().sendWithContext(allocator, Resp, path, body, context);
        }

        pub fn values(
            self: *Self,
            allocator: Allocator,
            comptime Req: type,
            comptime Item: type,
            comptime Ctx: type,
            path: []const u8,
            body: Req,
            context: Ctx,
        ) !Values(Item) {
            return self.sender().values(allocator, Req, Item, Ctx, path, body, context);
        }

        pub fn subscribe(
            self: *Self,
            allocator: Allocator,
            comptime Req: type,
            comptime Event: type,
            comptime Ctx: type,
            path: []const u8,
            body: Req,
            context: Ctx,
            user_data: ?*anyopaque,
            comptime callback: *const fn (?*anyopaque, Event) void,
        ) !SubscribeResult {
            return self.sender().subscribe(allocator, Req, Event, Ctx, path, body, context, user_data, callback);
        }

        pub fn stop(self: *Self) void {
            self.peer.stop();
        }

        pub fn isConnected(self: *Self) bool {
            return self.peer.isConnected();
        }

        pub fn isRemoteSubscribed(self: *Self, topic: []const u8) bool {
            return self.peer.isRemoteSubscribed(topic);
        }
    };
}

pub fn Client(comptime State: type) type {
    return struct {
        allocator: Allocator,
        peer: *Peer(State),
        poll_ptr: ?*anyopaque = null,
        poll_fn: ?*const fn (ptr: *anyopaque) anyerror!bool = null,
        destroy_ptr: ?*anyopaque = null,
        destroy_fn: ?*const fn (ptr: *anyopaque, allocator: Allocator) void = null,

        const Self = @This();

        pub fn deinit(self: *Self) void {
            self.peer.destroy();
            if (self.destroy_ptr) |ptr| {
                const destroy = self.destroy_fn orelse unreachable;
                destroy(ptr, self.allocator);
                self.destroy_ptr = null;
                self.destroy_fn = null;
            }
            self.* = undefined;
        }

        pub fn sender(self: *Self) Sender(State) {
            return .{ .peer = self.peer };
        }

        pub fn send(
            self: *Self,
            allocator: Allocator,
            comptime Resp: type,
            path: []const u8,
            body: anytype,
        ) !CallResult(Resp) {
            return self.sender().send(allocator, Resp, path, body);
        }

        pub fn sendWithContext(
            self: *Self,
            allocator: Allocator,
            comptime Resp: type,
            path: []const u8,
            body: anytype,
            context: anytype,
        ) !CallResult(Resp) {
            return self.sender().sendWithContext(allocator, Resp, path, body, context);
        }

        pub fn values(
            self: *Self,
            allocator: Allocator,
            comptime Req: type,
            comptime Item: type,
            comptime Ctx: type,
            path: []const u8,
            body: Req,
            context: Ctx,
        ) !Values(Item) {
            return self.sender().values(allocator, Req, Item, Ctx, path, body, context);
        }

        pub fn subscribe(
            self: *Self,
            allocator: Allocator,
            comptime Req: type,
            comptime Event: type,
            comptime Ctx: type,
            path: []const u8,
            body: Req,
            context: Ctx,
            user_data: ?*anyopaque,
            comptime callback: *const fn (?*anyopaque, Event) void,
        ) !SubscribeResult {
            return self.sender().subscribe(allocator, Req, Event, Ctx, path, body, context, user_data, callback);
        }

        pub fn stop(self: *Self) void {
            self.peer.stop();
        }

        pub fn isConnected(self: *Self) bool {
            return self.peer.isConnected();
        }

        pub fn isRemoteSubscribed(self: *Self, topic: []const u8) bool {
            return self.peer.isRemoteSubscribed(topic);
        }

        pub fn poll(self: *Self) !bool {
            const ptr = self.poll_ptr orelse return false;
            const poll_fn = self.poll_fn orelse unreachable;
            return poll_fn(ptr);
        }
    };
}

pub fn Server(comptime _: type) type {
    return struct {
        allocator: Allocator,
        port_value: u16,
        poll_fn: ?*const fn (ptr: *anyopaque) anyerror!bool = null,
        poll_ptr: ?*anyopaque = null,
        stop_fn: ?*const fn (ptr: *anyopaque) void = null,
        stop_ptr: ?*anyopaque = null,
        destroy_fn: ?*const fn (ptr: *anyopaque, allocator: Allocator) void = null,
        destroy_ptr: ?*anyopaque = null,

        const Self = @This();

        pub fn deinit(self: *Self) void {
            self.stop();
            if (self.destroy_ptr) |ptr| {
                const destroy = self.destroy_fn orelse unreachable;
                destroy(ptr, self.allocator);
                self.destroy_ptr = null;
                self.destroy_fn = null;
            }
            self.* = undefined;
        }

        pub fn stop(self: *Self) void {
            const ptr = self.stop_ptr orelse return;
            const stop_fn = self.stop_fn orelse return;
            stop_fn(ptr);
        }

        pub fn port(self: *const Self) u16 {
            return self.port_value;
        }

        pub fn poll(self: *Self) !bool {
            const ptr = self.poll_ptr orelse return false;
            const poll_fn = self.poll_fn orelse unreachable;
            return poll_fn(ptr);
        }
    };
}

pub fn Channel(comptime State: type) type {
    return struct {
        allocator: Allocator,
        owned_strings: std.ArrayListUnmanaged([]u8) = .{},
        posts: std.StringHashMapUnmanaged(ErasedPostHandler) = .{},
        streams: std.StringHashMapUnmanaged(ErasedStreamHandler) = .{},
        subscriptions: std.StringHashMapUnmanaged(SubscriptionBinding) = .{},
        other_posts: std.ArrayListUnmanaged(OtherPostHandler) = .{},
        other_streams: std.ArrayListUnmanaged(OtherStreamHandler) = .{},
        on_disconnect_fn: ?*const fn (*State, Sender(State)) void = null,

        const Self = @This();

        const ErasedPostHandler = struct {
            invoke: *const fn (channel: *Self, peer: *Peer(State), path: []const u8, body_json: ?[]const u8, context_json: ?[]const u8, id: ?i64) anyerror!void,
        };

        const ErasedStreamHandler = struct {
            invoke: *const fn (channel: *Self, peer: *Peer(State), path: []const u8, body_json: ?[]const u8, context_json: ?[]const u8, id: ?i64) anyerror!void,
        };

        const OtherPostHandler = struct {
            matcher: *const fn ([]const u8) bool,
            handler: ErasedPostHandler,
            /// Set by merge(): the matcher only sees paths under this scope ("m/"), with it stripped.
            scope: []const u8 = "",

            pub fn matches(self: OtherPostHandler, path: []const u8) bool {
                if (!std.mem.startsWith(u8, path, self.scope)) return false;
                return self.matcher(path[self.scope.len..]);
            }
        };

        const OtherStreamHandler = struct {
            matcher: *const fn ([]const u8) bool,
            handler: ErasedStreamHandler,
            /// Set by merge(): the matcher only sees paths under this scope ("m/"), with it stripped.
            scope: []const u8 = "",

            pub fn matches(self: OtherStreamHandler, path: []const u8) bool {
                if (!std.mem.startsWith(u8, path, self.scope)) return false;
                return self.matcher(path[self.scope.len..]);
            }
        };

        pub fn init(allocator: Allocator) Self {
            return .{
                .allocator = allocator,
            };
        }

        pub fn deinit(self: *Self) void {
            const it = self.owned_strings.items;
            for (it) |value| self.allocator.free(value);
            self.owned_strings.deinit(self.allocator);
            self.posts.deinit(self.allocator);
            self.streams.deinit(self.allocator);
            self.subscriptions.deinit(self.allocator);
            self.other_posts.deinit(self.allocator);
            self.other_streams.deinit(self.allocator);
            self.* = undefined;
        }

        pub fn post(
            self: *Self,
            path: []const u8,
            comptime handler: anytype,
        ) !*Self {
            const request_type = inferPostRequestType(handler);
            const Req = postRequestFieldType(request_type, "body");
            const Ctx = postRequestFieldType(request_type, "context");
            const Resp = inferPostResponseType(handler);
            return self.postTyped(Req, Resp, Ctx, path, handler);
        }

        pub fn postTyped(
            self: *Self,
            comptime Req: type,
            comptime Resp: type,
            comptime Ctx: type,
            path: []const u8,
            comptime handler: anytype,
        ) !*Self {
            const owned = try self.recordOwnedString(path);
            try self.posts.put(self.allocator, owned, .{
                .invoke = makePostInvoker(Req, Resp, Ctx, handler),
            });
            return self;
        }

        pub fn postOther(
            self: *Self,
            comptime Req: type,
            comptime Resp: type,
            comptime Ctx: type,
            matcher: *const fn ([]const u8) bool,
            comptime handler: anytype,
        ) !*Self {
            try self.other_posts.append(self.allocator, .{
                .matcher = matcher,
                .handler = .{
                    .invoke = makePostInvoker(Req, Resp, Ctx, handler),
                },
            });
            return self;
        }

        pub fn stream(
            self: *Self,
            comptime Req: type,
            comptime Item: type,
            comptime Ctx: type,
            path: []const u8,
            comptime handler: anytype,
        ) !*Self {
            const owned = try self.recordOwnedString(path);
            try self.streams.put(self.allocator, owned, .{
                .invoke = makeStreamInvoker(Req, Item, Ctx, handler),
            });
            return self;
        }

        pub fn streamOther(
            self: *Self,
            comptime Req: type,
            comptime Item: type,
            comptime Ctx: type,
            matcher: *const fn ([]const u8) bool,
            comptime handler: anytype,
        ) !*Self {
            try self.other_streams.append(self.allocator, .{
                .matcher = matcher,
                .handler = .{
                    .invoke = makeStreamInvoker(Req, Item, Ctx, handler),
                },
            });
            return self;
        }

        pub fn api(self: *Self, comptime spec: anytype) !*Self {
            try self.apiWalk(spec, "");
            return self;
        }

        pub fn events(self: *Self, spec: anytype, prefix: []const u8) !*Self {
            try self.eventsWalk(spec, prefix);
            return self;
        }

        pub fn merge(self: *Self, other: *Self, prefix: []const u8) !*Self {
            var post_it = other.posts.iterator();
            while (post_it.next()) |entry| {
                const merged = try joinTopicAlloc(self.allocator, prefix, entry.key_ptr.*);
                errdefer self.allocator.free(merged);
                try self.owned_strings.append(self.allocator, merged);
                try self.posts.put(self.allocator, merged, entry.value_ptr.*);
            }

            var stream_it = other.streams.iterator();
            while (stream_it.next()) |entry| {
                const merged = try joinTopicAlloc(self.allocator, prefix, entry.key_ptr.*);
                errdefer self.allocator.free(merged);
                try self.owned_strings.append(self.allocator, merged);
                try self.streams.put(self.allocator, merged, entry.value_ptr.*);
            }

            var sub_it = other.subscriptions.iterator();
            while (sub_it.next()) |entry| {
                const merged = try joinTopicAlloc(self.allocator, prefix, entry.key_ptr.*);
                errdefer self.allocator.free(merged);
                try entry.value_ptr.set_prefix(entry.value_ptr.ptr, self.allocator, merged);
                try self.owned_strings.append(self.allocator, merged);
                try self.subscriptions.put(self.allocator, merged, entry.value_ptr.*);
            }

            for (other.other_posts.items) |entry| {
                var scoped = entry;
                scoped.scope = try self.mergedScope(prefix, entry.scope);
                try self.other_posts.append(self.allocator, scoped);
            }

            for (other.other_streams.items) |entry| {
                var scoped = entry;
                scoped.scope = try self.mergedScope(prefix, entry.scope);
                try self.other_streams.append(self.allocator, scoped);
            }

            return self;
        }

        /// Scope for a catch-all merged under `prefix`: "prefix/" + its existing scope.
        fn mergedScope(self: *Self, prefix: []const u8, scope: []const u8) ![]const u8 {
            if (prefix.len == 0 and scope.len == 0) return "";
            // Owned copy, so `other` may be deinitialized after the merge.
            const merged = if (prefix.len == 0)
                try self.allocator.dupe(u8, scope)
            else
                try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ prefix, scope });
            errdefer self.allocator.free(merged);
            try self.owned_strings.append(self.allocator, merged);
            return merged;
        }

        pub fn onDisconnect(self: *Self, callback: *const fn (*State, Sender(State)) void) *Self {
            self.on_disconnect_fn = callback;
            return self;
        }

        pub fn process(self: *Self, allocator: Allocator, state: State) !Process(State) {
            const peer = try Peer(State).createProcess(allocator, self, state);
            return .{
                .allocator = allocator,
                .peer = peer,
            };
        }

        /// `ws://host:port/path`, `host:port`, a port, or `quic://host:port` (needs `-Dquic=true`).
        pub fn connect(self: *Self, allocator: Allocator, address: anytype, options: ConnectOptions(State)) !Client(State) {
            switch (@typeInfo(@TypeOf(address))) {
                .int, .comptime_int => {},
                else => {
                    const text: []const u8 = address;
                    if (std.mem.startsWith(u8, text, "quic://")) return self.connectQuic(allocator, text, options);
                },
            }
            const target = try parseConnectTarget(allocator, address);

            const Transport = ClientTransport(State);
            const transport = try allocator.create(Transport);
            errdefer allocator.destroy(transport);

            const initial_state = options.state orelse std.mem.zeroes(State);
            transport.* = undefined;
            try transport.init(allocator, self, initial_state, target, options.headers);
            errdefer transport.deinit(allocator);

            return .{
                .allocator = allocator,
                .peer = transport.peer,
                .poll_ptr = transport,
                .poll_fn = Transport.pollErased,
                .destroy_ptr = transport,
                .destroy_fn = Transport.destroyErased,
            };
        }

        fn connectQuic(self: *Self, allocator: Allocator, address: []const u8, options: ConnectOptions(State)) !Client(State) {
            if (comptime !quic_engine.enabled) {
                return error.QuicNotEnabled;
            } else {
                const Transport = QuicClientTransport(State);
                const transport = try allocator.create(Transport);
                errdefer allocator.destroy(transport);
                transport.* = undefined;
                try transport.init(allocator, self, options.state orelse std.mem.zeroes(State), address, options);
                return .{
                    .allocator = allocator,
                    .peer = transport.peer,
                    .poll_ptr = transport,
                    .poll_fn = Transport.pollErased,
                    .destroy_ptr = transport,
                    .destroy_fn = Transport.destroyErased,
                };
            }
        }

        /// Serves this channel over QUIC on UDP (`port` or "host:port"). Needs `zig build -Dquic=true`.
        pub fn listenQuic(self: *Self, allocator: Allocator, address: anytype, options: QuicListenOptions(State)) !Server(State) {
            if (comptime !quic_engine.enabled) {
                @compileError("listenQuic needs channel built with -Dquic=true (links the system OpenSSL 3.5+)");
            } else {
                const target = try parseListenTarget(address);
                const Transport = QuicServerTransport(State);
                const transport = try allocator.create(Transport);
                errdefer allocator.destroy(transport);
                transport.* = undefined;
                try transport.init(allocator, self, target, options);
                return .{
                    .allocator = allocator,
                    .port_value = transport.port_value,
                    .poll_fn = Transport.pollErased,
                    .poll_ptr = transport,
                    .stop_fn = Transport.stopErased,
                    .stop_ptr = transport,
                    .destroy_fn = Transport.destroyErased,
                    .destroy_ptr = transport,
                };
            }
        }

        pub fn listen(self: *Self, allocator: Allocator, address: anytype, options: ListenOptions(State)) !Server(State) {
            const target = try parseListenTarget(address);
            const Transport = ServerTransport(State);
            const transport = try allocator.create(Transport);
            errdefer allocator.destroy(transport);
            transport.* = undefined;
            try transport.init(allocator, self, target, options);
            errdefer transport.deinit(allocator);

            return .{
                .allocator = allocator,
                .port_value = transport.port_value,
                .poll_fn = Transport.pollErased,
                .poll_ptr = transport,
                .stop_fn = Transport.stopErased,
                .stop_ptr = transport,
                .destroy_fn = Transport.destroyErased,
                .destroy_ptr = transport,
            };
        }

        fn dispatchIncoming(self: *Self, peer: *Peer(State), message: *wire.ParsedMessage) anyerror!void {
            if (message.path) |path| {
                if (self.posts.get(path)) |handler| {
                    return handler.invoke(self, peer, path, message.body_json, message.context_json, message.id);
                }
                for (self.other_posts.items) |other| {
                    if (other.matches(path)) {
                        return other.handler.invoke(self, peer, path, message.body_json, message.context_json, message.id);
                    }
                }
                if (message.id) |id| {
                    try peer.sendMessage(.{
                        .id = id,
                        .error_text = try peer.allocator.dupe(u8, "api not found"),
                    });
                }
                return;
            }

            if (message.stream) |path| {
                if (message.id == null) {
                    return;
                }
                if (self.streams.get(path)) |handler| {
                    return handler.invoke(self, peer, path, message.body_json, message.context_json, message.id);
                }
                for (self.other_streams.items) |other| {
                    if (other.matches(path)) {
                        return other.handler.invoke(self, peer, path, message.body_json, message.context_json, message.id);
                    }
                }
                try peer.sendMessage(.{
                    .id = message.id.?,
                    .error_text = try peer.allocator.dupe(u8, "api not found"),
                });
                return;
            }

            if (message.cancel) |id| {
                peer.cancelIncoming(id);
                return;
            }

            if (message.sub) |name| {
                const id = message.id orelse return;
                const binding = self.subscriptions.get(name) orelse {
                    try peer.sendMessage(.{
                        .id = id,
                        .error_text = try peer.allocator.dupe(u8, "subscription not found"),
                    });
                    return;
                };
                var ack = binding.subscribe(binding.ptr, self.allocator, message.body_json) catch |err| {
                    return peer.replyError(id, err);
                };
                defer ack.deinit(self.allocator);
                peer.addRemoteTopic(ack.topic);
                try peer.sendMessage(.{
                    .id = id,
                    .topic = try peer.allocator.dupe(u8, ack.topic),
                    .body_json = if (ack.body_json) |value| try peer.allocator.dupe(u8, value) else null,
                });
                return;
            }

            if (message.unsub) |topic| {
                peer.removeRemoteTopic(topic);
                return;
            }

            if (message.topic) |topic| {
                if (message.id) |id| {
                    try peer.completeResponse(id, message);
                }
                peer.dispatchLocalEvent(topic, message.body_json);
                return;
            }

            if (message.id) |id| {
                try peer.completeResponse(id, message);
            }
        }

        fn apiWalk(self: *Self, comptime spec: anytype, prefix: []const u8) !void {
            const T = @TypeOf(spec);
            const info = @typeInfo(T);
            if (info != .@"struct") @compileError("api() expects an anonymous struct tree");
            inline for (info.@"struct".fields) |field| {
                const value = @field(spec, field.name);
                const ValueType = @TypeOf(value);
                const owns_prefix = !std.mem.eql(u8, field.name, "_");
                const next_prefix = if (owns_prefix) try joinTopicAlloc(self.allocator, prefix, field.name) else prefix;
                if (owns_prefix) {
                    errdefer self.allocator.free(@constCast(next_prefix));
                    try self.owned_strings.append(self.allocator, @constCast(next_prefix));
                }
                if (@hasDecl(ValueType, "__channel_post_spec")) {
                    _ = try self.postTyped(
                        ValueType.req_type,
                        ValueType.resp_type,
                        ValueType.ctx_type,
                        next_prefix,
                        ValueType.handler_fn,
                    );
                } else if (@hasDecl(ValueType, "__channel_stream_spec")) {
                    _ = try self.stream(ValueType.req_type, ValueType.item_type, ValueType.ctx_type, next_prefix, ValueType.handler_fn);
                } else {
                    try self.apiWalk(value, next_prefix);
                }
            }
        }

        fn eventsWalk(self: *Self, spec: anytype, prefix: []const u8) !void {
            const SpecType = @TypeOf(spec);
            const pointer_info = switch (@typeInfo(SpecType)) {
                .pointer => |info| info,
                else => @compileError("events() expects a pointer to a struct tree"),
            };
            const info = switch (@typeInfo(pointer_info.child)) {
                .@"struct" => |value| value,
                else => @compileError("events() expects a pointer to a struct tree"),
            };

            inline for (info.fields) |field| {
                const value_ptr = &@field(spec.*, field.name);
                const FieldType = @TypeOf(value_ptr.*);
                try self.owned_strings.ensureUnusedCapacity(self.allocator, 1);
                const next_prefix = try joinTopicAlloc(self.allocator, prefix, field.name);
                self.owned_strings.appendAssumeCapacity(next_prefix);
                if (@hasDecl(FieldType, "__channel_subscription_marker")) {
                    const binding = value_ptr.erased();
                    try binding.set_prefix(binding.ptr, self.allocator, next_prefix);
                    try self.subscriptions.put(self.allocator, next_prefix, binding);
                } else {
                    try self.eventsWalk(value_ptr, next_prefix);
                }
            }
        }

        fn invokeDisconnect(self: *Self, peer: *Peer(State)) void {
            if (self.on_disconnect_fn) |callback| {
                callback(&peer.state, .{ .peer = peer });
            }
        }

        fn recordOwnedString(self: *Self, value: []const u8) ![]u8 {
            const owned = try self.allocator.dupe(u8, value);
            errdefer self.allocator.free(owned);
            try self.owned_strings.append(self.allocator, owned);
            return owned;
        }

        fn inferPostRequestType(comptime handler: anytype) type {
            const fn_info = switch (@typeInfo(@TypeOf(handler))) {
                .@"fn" => |info| info,
                else => @compileError("post() handler must be a function"),
            };
            if (fn_info.params.len != 1) {
                @compileError("post() handler must accept exactly one request argument");
            }

            const param_type = fn_info.params[0].type orelse
                @compileError("post() handler argument type must be known at comptime");
            switch (@typeInfo(param_type)) {
                .@"struct" => {},
                else => @compileError("post() handler argument must be channel.PostRequest(...)"),
            }

            if (postRequestFieldType(param_type, "sender") != Sender(State)) {
                @compileError("post() handler argument must be channel.PostRequest(...)");
            }
            if (postRequestFieldType(param_type, "state") != *State) {
                @compileError("post() handler argument must be channel.PostRequest(...)");
            }
            if (postRequestFieldType(param_type, "path") != []const u8) {
                @compileError("post() handler argument must be channel.PostRequest(...)");
            }

            return param_type;
        }

        fn postRequestFieldType(comptime Request: type, comptime name: []const u8) type {
            const info = switch (@typeInfo(Request)) {
                .@"struct" => |value| value,
                else => @compileError("post() handler argument must be a struct"),
            };
            inline for (info.fields) |field| {
                if (std.mem.eql(u8, field.name, name)) {
                    return field.type;
                }
            }
            @compileError("post() handler argument must be channel.PostRequest(...)");
        }

        fn inferPostResponseType(comptime handler: anytype) type {
            const fn_info = switch (@typeInfo(@TypeOf(handler))) {
                .@"fn" => |info| info,
                else => @compileError("post() handler must be a function"),
            };
            const raw_return = fn_info.return_type orelse
                @compileError("post() handler return type must be known at comptime");
            return unwrapOptionalPayload(unwrapErrorPayload(raw_return));
        }

        fn unwrapErrorPayload(comptime T: type) type {
            return switch (@typeInfo(T)) {
                .error_union => |info| info.payload,
                else => T,
            };
        }

        fn unwrapOptionalPayload(comptime T: type) type {
            return switch (@typeInfo(T)) {
                .optional => |info| info.child,
                else => T,
            };
        }

        fn normalizePostResult(comptime Resp: type, result: anytype) ?Resp {
            return switch (@typeInfo(@TypeOf(result))) {
                .optional => result,
                else => result,
            };
        }

        fn makePostInvoker(
            comptime Req: type,
            comptime Resp: type,
            comptime Ctx: type,
            comptime handler: anytype,
        ) *const fn (*Self, *Peer(State), []const u8, ?[]const u8, ?[]const u8, ?i64) anyerror!void {
            return struct {
                fn invoke(
                    channel: *Self,
                    peer: *Peer(State),
                    path: []const u8,
                    body_json: ?[]const u8,
                    context_json: ?[]const u8,
                    id: ?i64,
                ) !void {
                    // An undecodable request is answered with an error (TS replies with the thrown
                    // error) instead of tearing down the whole connection.
                    var body = json_codec.decodeRequired(channel.allocator, Req, body_json) catch |err| {
                        if (id) |request_id| return peer.replyError(request_id, err);
                        return;
                    };
                    defer json_codec.freeOwnedValue(channel.allocator, Req, &body);
                    var context = json_codec.decodeContext(channel.allocator, Ctx, context_json) catch |err| {
                        if (id) |request_id| return peer.replyError(request_id, err);
                        return;
                    };
                    defer json_codec.freeOwnedValue(channel.allocator, Ctx, &context);
                    const result = handler(PostRequest(State, Req, Ctx){
                        .body = body,
                        .context = context,
                        .sender = .{ .peer = peer },
                        .state = &peer.state,
                        .path = path,
                    }) catch |err| {
                        if (id) |request_id| {
                            try peer.sendMessage(.{
                                .id = request_id,
                                .error_text = try peer.allocator.dupe(u8, @errorName(err)),
                            });
                        }
                        return;
                    };
                    if (id) |request_id| {
                        const body_out = if (Resp == void)
                            null
                        else
                            try json_codec.encodeOptionalAlloc(
                                peer.allocator,
                                ?Resp,
                                normalizePostResult(Resp, result),
                            );
                        // `sendMessage` owns `body_out` from here on, on success and on error.
                        try peer.sendMessage(.{
                            .id = request_id,
                            .body_json = body_out,
                        });
                    }
                }
            }.invoke;
        }

        fn makeStreamInvoker(
            comptime Req: type,
            comptime Item: type,
            comptime Ctx: type,
            comptime handler: anytype,
        ) *const fn (*Self, *Peer(State), []const u8, ?[]const u8, ?[]const u8, ?i64) anyerror!void {
            return struct {
                fn invoke(
                    channel: *Self,
                    peer: *Peer(State),
                    path: []const u8,
                    body_json: ?[]const u8,
                    context_json: ?[]const u8,
                    id: ?i64,
                ) !void {
                    const request_id = id orelse return;
                    // Each resource is released by exactly one defer (an errdefer next to a defer for
                    // the same value double-frees when a later `try` fails).
                    var body = json_codec.decodeRequired(channel.allocator, Req, body_json) catch |err| {
                        return peer.replyError(request_id, err);
                    };
                    defer json_codec.freeOwnedValue(channel.allocator, Req, &body);
                    var context = json_codec.decodeContext(channel.allocator, Ctx, context_json) catch |err| {
                        return peer.replyError(request_id, err);
                    };
                    defer json_codec.freeOwnedValue(channel.allocator, Ctx, &context);
                    const owned_path = try channel.allocator.dupe(u8, path);
                    defer channel.allocator.free(owned_path);

                    const task = try peer.createIncomingStreamTask(request_id);
                    defer peer.releaseIncomingStreamTask(request_id);

                    var emitter_state = TypedEmitterState(State, Item){
                        .peer = peer,
                        .task = task,
                    };
                    const emitter = StreamEmitter(Item){
                        .ptr = &emitter_state,
                        .emit_fn = TypedEmitterState(State, Item).emit,
                        .is_cancelled_fn = TypedEmitterState(State, Item).isCancelled,
                    };
                    handler(StreamRequest(State, Req, Ctx, Item){
                        .body = body,
                        .context = context,
                        .sender = .{ .peer = peer },
                        .state = &peer.state,
                        .path = owned_path,
                        .emitter = emitter,
                    }) catch |err| {
                        try peer.sendMessage(.{
                            .id = task.id,
                            .error_text = try peer.allocator.dupe(u8, @errorName(err)),
                        });
                        return;
                    };
                    if (!task.cancelled) {
                        try peer.sendMessage(.{
                            .id = task.id,
                            .done = true,
                        });
                    }
                }
            }.invoke;
        }
    };
}

fn Peer(comptime State: type) type {
    return struct {
        allocator: Allocator,
        channel: *Channel(State),
        state: State,
        mode: TransportMode,
        transport_ptr: *anyopaque,
        transport_vtable: TransportVTable,
        pending: std.AutoHashMapUnmanaged(i64, PendingEntry) = .{},
        next_id: i64 = 1,
        stream_entries: std.ArrayListUnmanaged(*PendingStream) = .{},
        local_topics: std.StringHashMapUnmanaged(std.ArrayListUnmanaged(*TopicCallbackEntry)) = .{},
        callback_entries: std.ArrayListUnmanaged(*TopicCallbackEntry) = .{},
        remote_topics: std.StringHashMapUnmanaged(void) = .{},
        incoming_streams: std.AutoHashMapUnmanaged(i64, *IncomingStreamTask(State)) = .{},

        publishers: std.ArrayListUnmanaged(PublisherRegistration) = .{},
        stopped: bool = false,
        /// Error text pending calls/streams/subscriptions settle with when this peer stops.
        stop_reason: []const u8 = "disconnected",

        const Self = @This();

        const PendingEntry = union(enum) {
            call: *PendingCall,
            stream: *PendingStream,
            subscribe: *PendingSubscribe,
        };

        const PendingCall = struct {
            done: bool = false,
            body_json: ?[]u8 = null,
            error_text: ?[]u8 = null,
        };

        const RawStreamEvent = struct {
            body_json: ?[]u8 = null,
            error_text: ?[]u8 = null,
            done: bool = false,

            fn deinit(self: *RawStreamEvent, allocator: Allocator) void {
                if (self.body_json) |value| allocator.free(value);
                if (self.error_text) |value| allocator.free(value);
                self.* = .{};
            }
        };

        const PendingStream = struct {
            peer: *Self,
            id: i64,
            closed: bool = false,
            finished: bool = false,
            queue: std.ArrayListUnmanaged(RawStreamEvent) = .{},

            fn next(stream: *PendingStream, allocator: Allocator, comptime Item: type) !StreamNext(Item) {
                while (stream.queue.items.len == 0 and !stream.finished) {
                    if (!(try pumpAll())) {
                        if (stream.peer.stopped) break;
                        std.Thread.sleep(100_000);
                    }
                }
                if (stream.queue.items.len == 0) {
                    return .done;
                }
                var event = stream.queue.orderedRemove(0);
                defer event.deinit(stream.peer.allocator);
                if (event.error_text) |value| {
                    return .{ .remote_err = try allocator.dupe(u8, value) };
                }
                if (event.done) {
                    return .done;
                }
                const decoded = try json_codec.decodeRequired(allocator, Item, event.body_json);
                return .{ .item = decoded };
            }

            fn close(stream: *PendingStream) void {
                if (stream.closed) {
                    return;
                }
                stream.closed = true;
                stream.finished = true;
                _ = stream.peer.pending.remove(stream.id);

                stream.peer.sendMessage(.{
                    .cancel = stream.id,
                }) catch {};
            }

            fn deinit(stream: *PendingStream, allocator: Allocator) void {
                for (stream.queue.items) |*event| event.deinit(allocator);
                stream.queue.deinit(allocator);
            }
        };

        const PendingSubscribe = struct {
            done: bool = false,
            topic: ?[]u8 = null,
            error_text: ?[]u8 = null,
        };

        const TopicCallbackEntry = struct {
            peer: *Self,
            topic: []u8,
            user_data: ?*anyopaque,
            cancelled: bool = false,
            invoke: *const fn (entry: *TopicCallbackEntry, allocator: Allocator, body_json: ?[]const u8) void,

            fn cancel(raw_ptr: *anyopaque) void {
                const self: *TopicCallbackEntry = @ptrCast(@alignCast(raw_ptr));
                if (self.cancelled) return;
                self.cancelled = true;
                self.peer.removeLocalCallback(self);
            }
        };

        pub fn createProcess(allocator: Allocator, channel: *Channel(State), state: State) !*Self {
            const peer = try createTransported(allocator, channel, state, .process, undefined, .{
                .send_frame = processSendFrame,
                .close = processClose,
                .is_connected = processIsConnected,
            });
            peer.transport_ptr = peer;
            return peer;
        }

        pub fn createTransported(
            allocator: Allocator,
            channel: *Channel(State),
            state: State,
            mode: TransportMode,
            transport_ptr: *anyopaque,
            transport_vtable: TransportVTable,
        ) !*Self {
            const peer = try allocator.create(Self);
            errdefer allocator.destroy(peer);
            peer.* = .{
                .allocator = allocator,
                .channel = channel,
                .state = state,
                .mode = mode,
                .transport_ptr = transport_ptr,
                .transport_vtable = transport_vtable,
            };
            try peer.attachPublishers();
            return peer;
        }

        pub fn destroy(self: *Self) void {
            self.stop();
            self.cleanup();
            self.allocator.destroy(self);
        }

        /// Stops the peer and settles every pending call with `reason` (a static string).
        fn stopWithReason(self: *Self, reason: []const u8) void {
            if (self.stopped) return;
            self.stop_reason = reason;
            self.stop();
        }

        pub fn stop(self: *Self) void {
            if (self.stopped) return;
            self.stopped = true;

            self.transport_vtable.close(self.transport_ptr);
            self.channel.invokeDisconnect(self);
            self.clearIncomingStreams();
            self.detachPublishers();
            self.failAllPending();
            self.clearCallbacks();
            self.clearRemoteTopics();
        }

        pub fn isConnected(self: *Self) bool {
            if (self.stopped) return false;
            return self.transport_vtable.is_connected(self.transport_ptr);
        }

        pub fn isRemoteSubscribed(self: *Self, topic: []const u8) bool {
            return self.remote_topics.contains(topic);
        }

        fn sendTyped(
            self: *Self,
            allocator: Allocator,
            comptime Req: type,
            comptime Resp: type,
            comptime Ctx: type,
            path: []const u8,
            body: Req,
            context: Ctx,
        ) !CallResult(Resp) {
            // Ownership: `pending` (and whatever reply buffers it collects) is released exactly once, by
            // the single defer below. The request buffers move into `sendMessage`, which always frees
            // them, so nothing here may free them again (AUDIT2 §1.1: double free on any later error).
            const pending = try self.allocator.create(PendingCall);
            pending.* = .{};
            const id = self.takeNextId();
            self.pending.put(self.allocator, id, .{ .call = pending }) catch |err| {
                self.allocator.destroy(pending);
                return err;
            };
            defer {
                _ = self.pending.remove(id);
                if (pending.error_text) |value| self.allocator.free(value);
                if (pending.body_json) |value| self.allocator.free(value);
                self.allocator.destroy(pending);
            }

            const message = try self.buildRequest(id, .path, path, Req, body, Ctx, context);
            try self.sendMessage(message);

            try self.waitUntil(&pending.done);
            if (!pending.done) return error.NotConnected;

            if (pending.error_text) |value| {
                return .{ .remote_err = try allocator.dupe(u8, value) };
            }
            if (Resp == void or pending.body_json == null) {
                return .{ .ok = null };
            }
            const decoded = try json_codec.decodeRequired(allocator, Resp, pending.body_json);
            return .{ .ok = decoded };
        }

        fn valuesTyped(
            self: *Self,
            comptime Req: type,
            comptime Item: type,
            comptime Ctx: type,
            path: []const u8,
            body: Req,
            context: Ctx,
        ) !Values(Item) {
            const pending = try self.allocator.create(PendingStream);
            const id = self.takeNextId();
            pending.* = .{
                .peer = self,
                .id = id,
            };
            errdefer {
                pending.deinit(self.allocator);
                self.allocator.destroy(pending);
            }
            // `stream_entries` owns the stream once appended; reserve now so the append can't fail
            // after the request is on the wire.
            try self.stream_entries.ensureUnusedCapacity(self.allocator, 1);
            try self.pending.put(self.allocator, id, .{ .stream = pending });
            errdefer _ = self.pending.remove(id);

            // `sendMessage` takes ownership of the request buffers (see sendTyped).
            const message = try self.buildRequest(id, .stream, path, Req, body, Ctx, context);
            try self.sendMessage(message);
            self.stream_entries.appendAssumeCapacity(pending);

            return .{
                .ptr = pending,
                .next_fn = struct {
                    fn call(ptr: *anyopaque, allocator: Allocator) !StreamNext(Item) {
                        const stream: *PendingStream = @ptrCast(@alignCast(ptr));
                        return stream.next(allocator, Item);
                    }
                }.call,
                .close_fn = struct {
                    fn call(ptr: *anyopaque) void {
                        const stream: *PendingStream = @ptrCast(@alignCast(ptr));
                        stream.close();
                    }
                }.call,
            };
        }

        fn subscribeTyped(
            self: *Self,
            allocator: Allocator,
            comptime Req: type,
            comptime Event: type,
            comptime Ctx: type,
            path: []const u8,
            body: Req,
            context: Ctx,
            user_data: ?*anyopaque,
            comptime callback: *const fn (?*anyopaque, Event) void,
        ) !SubscribeResult {
            // Same single-owner scheme as sendTyped.
            const pending = try self.allocator.create(PendingSubscribe);
            pending.* = .{};
            const id = self.takeNextId();
            self.pending.put(self.allocator, id, .{ .subscribe = pending }) catch |err| {
                self.allocator.destroy(pending);
                return err;
            };
            defer {
                _ = self.pending.remove(id);
                if (pending.error_text) |value| self.allocator.free(value);
                if (pending.topic) |value| self.allocator.free(value);
                self.allocator.destroy(pending);
            }

            const message = try self.buildRequest(id, .sub, path, Req, body, Ctx, context);
            try self.sendMessage(message);

            try self.waitUntil(&pending.done);
            if (!pending.done) return error.NotConnected;

            if (pending.error_text) |value| {
                return .{ .remote_err = try allocator.dupe(u8, value) };
            }
            const topic_value = pending.topic orelse return .{ .remote_err = try allocator.dupe(u8, "subscription missing topic") };
            const entry = try self.allocator.create(TopicCallbackEntry);
            entry.* = .{
                .peer = self,
                .topic = try self.allocator.dupe(u8, topic_value),
                .user_data = user_data,
                .invoke = struct {
                    fn call(raw: *TopicCallbackEntry, allocator_inner: Allocator, body_inner: ?[]const u8) void {
                        if (raw.cancelled) return;
                        if (Event == void) {
                            callback(raw.user_data, {});
                            return;
                        }
                        var decoded = json_codec.decodeRequired(allocator_inner, Event, body_inner) catch return;
                        defer json_codec.freeOwnedValue(allocator_inner, Event, &decoded);
                        callback(raw.user_data, decoded);
                    }
                }.call,
            };
            errdefer {
                self.allocator.free(entry.topic);
                self.allocator.destroy(entry);
            }
            try self.registerLocalCallback(entry);
            return .{
                .ok = .{
                    .ptr = entry,
                    .cancel_fn = TopicCallbackEntry.cancel,
                },
            };
        }

        fn replyError(self: *Self, id: i64, err: anyerror) !void {
            try self.sendMessage(.{
                .id = id,
                .error_text = try self.allocator.dupe(u8, @errorName(err)),
            });
        }

        const RequestKind = enum { path, stream, sub };

        /// Builds an outgoing request. On error nothing is leaked; on success the caller must hand
        /// the message to `sendMessage` (which always frees it) and must not free its fields itself.
        fn buildRequest(
            self: *Self,
            id: i64,
            comptime kind: RequestKind,
            path: []const u8,
            comptime Req: type,
            body: Req,
            comptime Ctx: type,
            context: Ctx,
        ) !OwnedMessage {
            var message = OwnedMessage{ .id = id };
            errdefer message.deinit(self.allocator);
            message.body_json = try json_codec.encodeOptionalAlloc(self.allocator, Req, body);
            message.context_json = try json_codec.encodeOptionalAlloc(self.allocator, Ctx, context);
            const owned_path = try self.allocator.dupe(u8, path);
            switch (kind) {
                .path => message.path = owned_path,
                .stream => message.stream = owned_path,
                .sub => message.sub = owned_path,
            }
            return message;
        }

        /// Pumps every transport until `flag` is set or this peer stops. Transport errors are
        /// contained by the transports themselves (they stop the affected peer, which settles its
        /// pending calls), so a hostile frame on another connection can't abort this wait.
        fn waitUntil(self: *Self, flag: *const bool) !void {
            while (!flag.* and !self.stopped) {
                if (!(try pumpAll())) {
                    std.Thread.sleep(100_000);
                }
            }
        }

        fn takeNextId(self: *Self) i64 {
            const id = self.next_id;
            self.next_id += 1;
            return id;
        }

        fn sendMessage(self: *Self, message: OwnedMessage) anyerror!void {
            if (self.mode == .process) {
                var mutable = message;
                var parsed = mutable.intoParsed();
                defer parsed.deinit(self.allocator);
                return self.channel.dispatchIncoming(self, &parsed);
            }

            var owned = message;
            defer owned.deinit(self.allocator);
            if (self.stopped) return error.NotConnected;

            const outgoing = [_]wire.OutgoingMessage{owned.toOutgoing()};
            const payload = try wire.encodeMessagesAlloc(self.allocator, &outgoing);
            defer self.allocator.free(payload);
            self.transport_vtable.send_frame(self.transport_ptr, payload) catch |err| {
                self.stop();
                return err;
            };
            _ = pumpAll() catch {};
        }

        fn completeResponse(self: *Self, id: i64, message: *wire.ParsedMessage) !void {
            const pending = self.pending.getPtr(id) orelse return;
            switch (pending.*) {
                .call => |call| {
                    if (message.body_json) |value| {
                        call.body_json = value;
                        message.body_json = null;
                    }
                    if (message.error_text) |value| {
                        call.error_text = value;
                        message.error_text = null;
                    }
                    call.done = true;
                    _ = self.pending.remove(id);
                },
                .stream => |stream| {
                    var event = RawStreamEvent{
                        .done = message.done orelse false,
                    };
                    if (message.body_json) |value| {
                        event.body_json = value;
                        message.body_json = null;
                    }
                    if (message.error_text) |value| {
                        event.error_text = value;
                        message.error_text = null;
                        stream.finished = true;
                    }
                    if (event.done) {
                        stream.finished = true;
                    }
                    try stream.queue.append(self.allocator, event);
                    if (stream.finished) {
                        _ = self.pending.remove(id);
                    }
                },
                .subscribe => |sub| {
                    if (message.topic) |value| {
                        sub.topic = value;
                        message.topic = null;
                    }
                    if (message.error_text) |value| {
                        sub.error_text = value;
                        message.error_text = null;
                    }
                    sub.done = true;
                    _ = self.pending.remove(id);
                },
            }
        }

        fn handleIncomingFrame(self: *Self, payload: []const u8, kind: MessageKind) !void {
            _ = kind;
            const messages = try wire.parseMessagesAlloc(self.allocator, payload);
            defer {
                for (messages) |*message| message.deinit(self.allocator);
                self.allocator.free(messages);
            }
            for (messages) |*message| {
                try self.channel.dispatchIncoming(self, message);
            }
        }

        fn createIncomingStreamTask(self: *Self, id: i64) !*IncomingStreamTask(State) {
            const task = try self.allocator.create(IncomingStreamTask(State));
            errdefer self.allocator.destroy(task);
            task.* = .{
                .peer = self,
                .id = id,
            };
            try self.incoming_streams.put(self.allocator, id, task);
            return task;
        }

        fn releaseIncomingStreamTask(self: *Self, id: i64) void {
            const removed = self.incoming_streams.fetchRemove(id);
            if (removed) |entry| {
                self.allocator.destroy(entry.value);
            }
        }

        fn cancelIncoming(self: *Self, id: i64) void {
            const task = self.incoming_streams.get(id);
            if (task) |value| {
                value.cancelled = true;
            }
        }

        fn addRemoteTopic(self: *Self, topic: []const u8) void {
            const owned = self.allocator.dupe(u8, topic) catch return;
            self.remote_topics.put(self.allocator, owned, {}) catch {
                self.allocator.free(owned);
            };
        }

        fn removeRemoteTopic(self: *Self, topic: []const u8) void {
            if (self.remote_topics.fetchRemove(topic)) |entry| {
                self.allocator.free(entry.key);
            }
        }

        fn dispatchLocalEvent(self: *Self, topic: []const u8, body_json: ?[]const u8) void {
            const list = self.local_topics.get(topic) orelse {
                return;
            };
            const copy = self.allocator.alloc(*TopicCallbackEntry, list.items.len) catch {
                return;
            };
            for (list.items, 0..) |entry, index| copy[index] = entry;
            defer self.allocator.free(copy);

            for (copy) |entry| {
                if (!entry.cancelled) {
                    entry.invoke(entry, self.allocator, body_json);
                }
            }
        }

        fn registerLocalCallback(self: *Self, entry: *TopicCallbackEntry) !void {
            try self.callback_entries.append(self.allocator, entry);
            const gop = try self.local_topics.getOrPut(self.allocator, entry.topic);
            if (!gop.found_existing) {
                gop.value_ptr.* = .{};
            }
            try gop.value_ptr.append(self.allocator, entry);
        }

        fn removeLocalCallback(self: *Self, entry: *TopicCallbackEntry) void {
            var should_unsub = false;
            if (self.local_topics.getPtr(entry.topic)) |list| {
                var index: usize = 0;
                while (index < list.items.len) : (index += 1) {
                    if (list.items[index] == entry) {
                        _ = list.swapRemove(index);
                        break;
                    }
                }
                if (list.items.len == 0) {
                    list.deinit(self.allocator);
                    _ = self.local_topics.remove(entry.topic);
                    should_unsub = true;
                }
            }
            if (should_unsub) {
                self.sendMessage(.{
                    .unsub = self.allocator.dupe(u8, entry.topic) catch return,
                }) catch {};
            }
        }

        fn attachPublishers(self: *Self) !void {
            var it = self.channel.subscriptions.iterator();
            while (it.next()) |entry| {
                const binding = entry.value_ptr.*;
                const sink = PublisherSink{
                    .ptr = self,
                    .send = publisherSend,
                };
                const publisher_id = try binding.add_publisher(binding.ptr, self.allocator, sink);
                try self.publishers.append(self.allocator, .{
                    .binding = binding,
                    .publisher_id = publisher_id,
                });
            }
        }

        fn detachPublishers(self: *Self) void {
            for (self.publishers.items) |registration| {
                registration.binding.remove_publisher(registration.binding.ptr, registration.publisher_id);
            }
            self.publishers.deinit(self.allocator);
            self.publishers = .{};
        }

        fn failAllPending(self: *Self) void {
            var it = self.pending.iterator();
            while (it.next()) |entry| {
                switch (entry.value_ptr.*) {
                    .call => |call| {
                        if (!call.done) {
                            call.error_text = self.allocator.dupe(u8, self.stop_reason) catch null;
                            call.done = true;
                        }
                    },
                    .stream => |stream| {
                        if (!stream.finished) {
                            stream.queue.append(self.allocator, .{
                                .error_text = self.allocator.dupe(u8, self.stop_reason) catch null,
                            }) catch {};
                            stream.finished = true;
                        }
                    },
                    .subscribe => |sub| {
                        if (!sub.done) {
                            sub.error_text = self.allocator.dupe(u8, self.stop_reason) catch null;
                            sub.done = true;
                        }
                    },
                }
            }
            self.pending.clearRetainingCapacity();
        }

        fn clearIncomingStreams(self: *Self) void {
            var it = self.incoming_streams.iterator();
            while (it.next()) |entry| {
                entry.value_ptr.*.cancelled = true;
            }
        }

        fn clearCallbacks(self: *Self) void {
            var it = self.local_topics.iterator();
            while (it.next()) |entry| {
                entry.value_ptr.deinit(self.allocator);
            }
            self.local_topics.deinit(self.allocator);
            self.local_topics = .{};
            for (self.callback_entries.items) |entry| {
                self.allocator.free(entry.topic);
                self.allocator.destroy(entry);
            }
            self.callback_entries.deinit(self.allocator);
            self.callback_entries = .{};
        }

        fn clearRemoteTopics(self: *Self) void {
            var it = self.remote_topics.iterator();
            while (it.next()) |entry| self.allocator.free(entry.key_ptr.*);
            self.remote_topics.deinit(self.allocator);
            self.remote_topics = .{};
        }

        fn cleanup(self: *Self) void {
            self.detachPublishers();
            self.clearCallbacks();
            self.clearRemoteTopics();
            self.pending.deinit(self.allocator);
            self.incoming_streams.deinit(self.allocator);
            for (self.stream_entries.items) |stream| {
                stream.deinit(self.allocator);
                self.allocator.destroy(stream);
            }
            self.stream_entries.deinit(self.allocator);
        }

        fn publisherSend(raw_ptr: *anyopaque, topic: []const u8, body_json: ?[]const u8) void {
            const self: *Self = @ptrCast(@alignCast(raw_ptr));
            if (!self.isRemoteSubscribed(topic)) return;
            var message = OwnedMessage{};
            message.topic = self.allocator.dupe(u8, topic) catch return;
            if (body_json) |value| {
                message.body_json = self.allocator.dupe(u8, value) catch {
                    message.deinit(self.allocator);
                    return;
                };
            }
            self.sendMessage(message) catch {};
        }

        fn processSendFrame(raw_ptr: *anyopaque, payload: []u8) !void {
            const self: *Self = @ptrCast(@alignCast(raw_ptr));
            try self.handleIncomingFrame(payload, .text);
        }

        fn processClose(raw_ptr: *anyopaque) void {
            _ = raw_ptr;
        }

        fn processIsConnected(raw_ptr: *anyopaque) bool {
            const self: *Self = @ptrCast(@alignCast(raw_ptr));
            return !self.stopped;
        }
    };
}

fn IncomingStreamTask(comptime State: type) type {
    return struct {
        peer: *Peer(State),
        id: i64,
        cancelled: bool = false,
    };
}

fn TypedEmitterState(comptime State: type, comptime Item: type) type {
    return struct {
        peer: *Peer(State),
        task: *IncomingStreamTask(State),

        fn emit(raw_ptr: *anyopaque, allocator: Allocator, item: Item) !void {
            const self: *@This() = @ptrCast(@alignCast(raw_ptr));
            _ = allocator;
            if (self.task.cancelled) return error.Cancelled;
            // Encoded with the peer's allocator: `sendMessage` takes ownership and frees it with that.
            const body_json = try json_codec.encodeOptionalAlloc(self.peer.allocator, Item, item);
            try self.peer.sendMessage(.{
                .id = self.task.id,
                .body_json = body_json,
            });
        }

        fn isCancelled(raw_ptr: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw_ptr));
            return self.task.cancelled;
        }
    };
}

const ConnectTarget = struct {
    host: []u8,
    path: []u8,
    port: u16,
    tls: bool,

    fn deinit(self: ConnectTarget, allocator: Allocator) void {
        allocator.free(self.host);
        allocator.free(self.path);
    }
};

fn ClientTransport(comptime State: type) type {
    return struct {
        allocator: Allocator,
        stream: net.Stream,
        peer: *Peer(State),
        host: []u8,
        path: []u8,
        reader: websocket.proto.Reader,
        reader_buf: []u8,
        buffer_provider: websocket.buffer.Provider,
        connected: bool = true,
        closed: bool = false,

        const Self = @This();

        fn init(
            self: *Self,
            allocator: Allocator,
            channel: *Channel(State),
            state: State,
            target: ConnectTarget,
            headers: ?[]const u8,
        ) !void {
            if (target.tls) return error.TlsUnsupported;
            errdefer target.deinit(allocator);

            const stream = try net.tcpConnectToHost(allocator, target.host, target.port);
            errdefer stream.close();
            try setNonBlocking(stream.handle);

            const reader_buf = try allocator.alloc(u8, 4096);
            errdefer allocator.free(reader_buf);
            var buffer_provider = try websocket.buffer.Provider.init(allocator, .{
                .size = 0,
                .count = 0,
                .max = max_frame_buffer,
            });
            errdefer buffer_provider.deinit();

            self.* = .{
                .allocator = allocator,
                .stream = stream,
                .peer = undefined,
                .host = target.host,
                .path = target.path,
                .reader = undefined,
                .reader_buf = reader_buf,
                .buffer_provider = buffer_provider,
            };
            const over_read = try self.performHandshake(target.port, headers);
            self.reader = websocket.proto.Reader.init(self.reader_buf, &self.buffer_provider, null);
            self.reader.pos = over_read;

            const peer = try Peer(State).createTransported(allocator, channel, state, .websocket_client, self, .{
                .send_frame = sendFrameErased,
                .close = closeErased,
                .is_connected = isConnectedErased,
            });
            errdefer peer.destroy();
            self.peer = peer;
            try registerPumpEntry(self, pollErased);
        }

        fn deinit(self: *Self, allocator: Allocator) void {
            unregisterPumpEntry(self);
            if (!self.closed) {
                self.stream.close();
                self.closed = true;
            }
            self.reader.deinit();
            allocator.free(self.reader_buf);
            self.buffer_provider.deinit();
            allocator.free(self.host);
            allocator.free(self.path);
        }

        fn destroyErased(ptr: *anyopaque, allocator: Allocator) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.deinit(allocator);
            allocator.destroy(self);
        }

        fn sendFrameErased(ptr: *anyopaque, payload: []u8) !void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (!self.connected) return error.NotConnected;
            try writeClientSocketFrame(self.stream.handle, .text, payload);
        }

        fn closeErased(ptr: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (self.closed) return;
            self.closed = true;
            self.connected = false;
            self.stream.close();
        }

        fn isConnectedErased(ptr: *anyopaque) bool {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return self.connected;
        }

        fn pollErased(ptr: *anyopaque) !bool {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return self.poll();
        }

        /// Errors are contained here: anything the server sends that we can't handle (oversized or
        /// malformed frames, undecodable JSON, socket errors) stops this client's peer, which settles
        /// its pending calls with an error. Nothing propagates into `pumpAll`, so one hostile
        /// connection can't abort a wait on another one.
        fn poll(self: *Self) !bool {
            if (!self.connected) return false;
            var progressed = false;

            while (true) {
                guardFrameLengths(&self.reader, max_message_size) catch {
                    self.fail("message too large");
                    return true;
                };
                const maybe_result = self.reader.read() catch {
                    self.fail("invalid websocket frame");
                    return true;
                };
                if (maybe_result) |result| {
                    progressed = true;
                    const message = result[1];
                    defer self.reader.done(message.type);
                    switch (message.type) {
                        .text, .binary => {
                            const kind: MessageKind = switch (message.type) {
                                .text => .text,
                                .binary => .binary,
                                else => unreachable,
                            };
                            self.peer.handleIncomingFrame(message.data, kind) catch {
                                self.fail("invalid message");
                                return true;
                            };
                        },
                        .ping => writeClientSocketFrame(self.stream.handle, .pong, message.data) catch {
                            self.fail("disconnected");
                            return true;
                        },
                        .pong => {},
                        .close => {
                            closeErased(self);
                            self.peer.stop();
                            return true;
                        },
                    }
                    if (!self.connected) return true;
                    continue;
                }

                self.reader.fill(&self.stream) catch |err| switch (err) {
                    error.WouldBlock => return progressed,
                    else => {
                        self.fail("disconnected");
                        return true;
                    },
                };
                progressed = true;
            }
        }

        fn fail(self: *Self, reason: []const u8) void {
            closeErased(self);
            self.peer.stopWithReason(reason);
        }

        fn performHandshake(self: *Self, port: u16, headers: ?[]const u8) !usize {
            var request_buf: [4096]u8 = undefined;
            var pos: usize = 0;

            const raw_key = generateClientKey();
            var encoded_key: [24]u8 = undefined;
            const key = std.base64.standard.Encoder.encode(&encoded_key, &raw_key);

            pos += (try std.fmt.bufPrint(request_buf[pos..], "GET {s} HTTP/1.1\r\ncontent-length: 0\r\nupgrade: websocket\r\nsec-websocket-version: 13\r\nconnection: upgrade\r\nsec-websocket-key: {s}\r\nhost: {s}:{d}\r\n", .{
                self.path,
                key,
                self.host,
                port,
            })).len;
            if (headers) |extra_headers| {
                // + 4 for the optional "\r\n" after the headers and the final "\r\n".
                if (pos + extra_headers.len + 4 > request_buf.len) return error.HeadersTooLarge;
                if (extra_headers.len > 0) {
                    @memcpy(request_buf[pos .. pos + extra_headers.len], extra_headers);
                    pos += extra_headers.len;
                    if (!std.mem.endsWith(u8, extra_headers, "\r\n")) {
                        @memcpy(request_buf[pos .. pos + 2], "\r\n");
                        pos += 2;
                    }
                }
            }
            @memcpy(request_buf[pos .. pos + 2], "\r\n");
            pos += 2;
            try writeSocketAll(self.stream.handle, request_buf[0..pos]);

            const deadline = std.time.milliTimestamp() + 10_000;
            var read_pos: usize = 0;
            var line_start: usize = 0;
            var complete_response: u8 = 0;

            while (true) {
                const n = self.stream.read(self.reader_buf[read_pos..]) catch |err| switch (err) {
                    error.WouldBlock => {
                        if (!(try pumpAll())) {
                            if (std.time.milliTimestamp() > deadline) return error.Timeout;
                            std.Thread.sleep(100_000);
                        }
                        continue;
                    },
                    else => return err,
                };
                if (n == 0) return error.ConnectionClosed;

                read_pos += n;
                while (std.mem.indexOfScalar(u8, self.reader_buf[line_start..read_pos], '\r')) |relative_end| {
                    if (relative_end == 0) {
                        if (complete_response != 15) return error.InvalidHandshakeResponse;
                        const over_read = read_pos - (line_start + 2);
                        std.mem.copyForwards(u8, self.reader_buf[0..over_read], self.reader_buf[line_start + 2 .. read_pos]);
                        return over_read;
                    }

                    const line_end = line_start + relative_end;
                    const line = self.reader_buf[line_start..line_end];
                    line_start = line_end + 2;

                    if (complete_response == 0) {
                        if (!std.ascii.startsWithIgnoreCase(line, "HTTP/1.1 101 ")) return error.InvalidHandshakeResponse;
                        complete_response |= 1;
                        continue;
                    }

                    for (line, 0..) |b, i| {
                        if ('A' <= b and b <= 'Z') {
                            line[i] = b + 32;
                            continue;
                        }
                        if (b != ':') continue;

                        switch (i) {
                            7 => if (std.mem.eql(u8, line[0..i], "upgrade")) {
                                if (!std.ascii.eqlIgnoreCase(std.mem.trim(u8, line[i + 1 ..], &std.ascii.whitespace), "websocket")) {
                                    return error.InvalidUpgradeHeader;
                                }
                                complete_response |= 2;
                            },
                            10 => if (std.mem.eql(u8, line[0..i], "connection")) {
                                if (!std.ascii.eqlIgnoreCase(std.mem.trim(u8, line[i + 1 ..], &std.ascii.whitespace), "upgrade")) {
                                    return error.InvalidConnectionHeader;
                                }
                                complete_response |= 4;
                            },
                            20 => if (std.mem.eql(u8, line[0..i], "sec-websocket-accept")) {
                                var hash: [20]u8 = undefined;
                                var hasher = std.crypto.hash.Sha1.init(.{});
                                hasher.update(key);
                                hasher.update("258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
                                hasher.final(&hash);

                                var accept_buf: [28]u8 = undefined;
                                const expected = std.base64.standard.Encoder.encode(&accept_buf, &hash);
                                const header_value = std.mem.trim(u8, line[i + 1 ..], &std.ascii.whitespace);
                                if (!std.mem.eql(u8, header_value, expected)) {
                                    return error.InvalidWebsocketAcceptHeader;
                                }
                                complete_response |= 8;
                            },
                            else => {},
                        }
                    }
                }

                if (read_pos == self.reader_buf.len) return error.ResponseTooLarge;
                if (std.time.milliTimestamp() > deadline) return error.Timeout;
            }
        }
    };
}

const ListenTarget = struct {
    host: []const u8,
    port: u16,
};

fn ServerTransport(comptime State: type) type {
    return struct {
        allocator: Allocator,
        channel: *Channel(State),
        options: ListenOptions(State),
        listener: posix.socket_t,
        port_value: u16,
        stopped: bool = false,
        handshake_pool: *websocket.server.Handshake.Pool,
        buffer_provider: websocket.buffer.Provider,
        connections: std.ArrayListUnmanaged(*Connection) = .{},

        const Self = @This();

        const Connection = struct {
            allocator: Allocator,
            server: *Self,
            socket: posix.socket_t,
            address: net.Address,
            handshake_state: ?*websocket.server.Handshake.State = null,
            reader: ?websocket.proto.Reader = null,
            reader_buf: ?[]u8 = null,
            peer: ?*Peer(State) = null,
            closed: bool = false,
            removed: bool = false,

            fn create(allocator: Allocator, server: *Self, socket: posix.socket_t, address: net.Address) !*Connection {
                const self = try allocator.create(Connection);
                errdefer allocator.destroy(self);
                self.* = .{
                    .allocator = allocator,
                    .server = server,
                    .socket = socket,
                    .address = address,
                    .handshake_state = try server.handshake_pool.acquire(),
                };
                return self;
            }

            fn sendFrameErased(ptr: *anyopaque, payload: []u8) !void {
                const self: *Connection = @ptrCast(@alignCast(ptr));
                if (self.closed) return error.NotConnected;
                try writeSocketFrame(self.socket, .text, payload);
            }

            fn closeErased(ptr: *anyopaque) void {
                const self: *Connection = @ptrCast(@alignCast(ptr));
                self.closeSocket();
            }

            fn isConnectedErased(ptr: *anyopaque) bool {
                const self: *Connection = @ptrCast(@alignCast(ptr));
                return !self.closed;
            }

            fn closeSocket(self: *Connection) void {
                if (self.closed) return;
                self.closed = true;
                posix.close(self.socket);
            }

            fn destroy(self: *Connection) void {
                if (self.removed) return;
                self.removed = true;
                self.server.removeConnection(self);

                if (self.peer) |peer| {
                    peer.stop();
                    if (self.server.options.on_disconnect) |callback| {
                        callback(&peer.state, .{ .peer = peer });
                    }
                    peer.destroy();
                    self.peer = null;
                } else {
                    self.closeSocket();
                }

                if (self.handshake_state) |state| {
                    state.release();
                    self.handshake_state = null;
                }
                if (self.reader) |*reader| {
                    reader.deinit();
                    self.reader = null;
                }
                if (self.reader_buf) |buf| {
                    self.allocator.free(buf);
                    self.reader_buf = null;
                }
                self.allocator.destroy(self);
            }

            fn poll(self: *Connection) !bool {
                if (self.closed) return false;
                if (self.peer == null) {
                    return self.pollHandshake();
                }
                return self.pollActive();
            }

            fn pollHandshake(self: *Connection) !bool {
                const state = self.handshake_state orelse return false;
                const n = posix.read(self.socket, state.buf[state.len..]) catch |err| switch (err) {
                    error.WouldBlock => return false,
                    error.ConnectionResetByPeer, error.BrokenPipe => {
                        self.destroy();
                        return true;
                    },
                    else => {
                        self.destroy();
                        return err;
                    },
                };
                if (n == 0) {
                    self.destroy();
                    return true;
                }
                state.len += n;

                var handshake = websocket.server.Handshake.parse(state) catch {
                    self.destroy();
                    return true;
                } orelse return true;

                const header_end = (std.mem.indexOf(u8, state.buf[0..state.len], "\r\n\r\n") orelse state.len) + 4;
                const overread_len = if (state.len > header_end) state.len - header_end else 0;
                const reader_buf_len = @max(@as(usize, 4096), overread_len + 1);
                const reader_buf = try self.allocator.alloc(u8, reader_buf_len);
                errdefer self.allocator.free(reader_buf);

                var reader = websocket.proto.Reader.init(reader_buf, &self.server.buffer_provider, null);
                if (overread_len > 0) {
                    @memcpy(reader_buf[0..overread_len], state.buf[header_end..state.len]);
                    reader.pos = overread_len;
                }

                const peer_state = try buildListenState(State, self.allocator, &handshake, self.server.options.state);
                var reply_buf: [2048]u8 = undefined;
                const reply = try websocket.server.Handshake.createReply(handshake.key, handshake.res_headers, false, &reply_buf);
                try writeSocketAll(self.socket, reply);

                const peer = try Peer(State).createTransported(self.allocator, self.server.channel, peer_state, .websocket_server, self, .{
                    .send_frame = sendFrameErased,
                    .close = closeErased,
                    .is_connected = isConnectedErased,
                });
                errdefer peer.destroy();

                state.release();
                self.handshake_state = null;
                self.reader = reader;
                self.reader_buf = reader_buf;
                self.peer = peer;

                if (self.server.options.on_connect) |callback| {
                    callback(&peer.state, .{ .peer = peer });
                }
                return true;
            }

            fn pollActive(self: *Connection) !bool {
                var reader = &(self.reader orelse return false);
                var progressed = false;
                var stream = net.Stream{ .handle = self.socket };

                while (true) {
                    guardFrameLengths(reader, max_message_size) catch {
                        self.destroy();
                        return true;
                    };
                    const maybe_result = reader.read() catch {
                        self.destroy();
                        return true;
                    };
                    if (maybe_result) |result| {
                        progressed = true;
                        const message = result[1];
                        defer reader.done(message.type);
                        switch (message.type) {
                            .text, .binary => {
                                const kind: MessageKind = switch (message.type) {
                                    .text => .text,
                                    .binary => .binary,
                                    else => unreachable,
                                };
                                (self.peer orelse return true).handleIncomingFrame(message.data, kind) catch {
                                    self.destroy();
                                    return true;
                                };
                            },
                            .ping => writeSocketFrame(self.socket, .pong, message.data) catch {
                                self.destroy();
                                return true;
                            },
                            .pong => {},
                            .close => {
                                self.destroy();
                                return true;
                            },
                        }
                        continue;
                    }

                    reader.fill(&stream) catch |err| switch (err) {
                        error.WouldBlock => return progressed,
                        else => {
                            self.destroy();
                            return true;
                        },
                    };
                    progressed = true;
                }
            }
        };

        fn init(
            self: *Self,
            allocator: Allocator,
            channel: *Channel(State),
            target: ListenTarget,
            options: ListenOptions(State),
        ) !void {
            var address = try net.Address.parseIp(target.host, target.port);
            const listener = try posix.socket(address.any.family, posix.SOCK.STREAM | posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK, posix.IPPROTO.TCP);
            errdefer posix.close(listener);

            try posix.setsockopt(listener, posix.IPPROTO.TCP, 1, &std.mem.toBytes(@as(c_int, 1)));
            if (@hasDecl(posix.SO, "REUSEPORT_LB")) {
                try posix.setsockopt(listener, posix.SOL.SOCKET, posix.SO.REUSEPORT_LB, &std.mem.toBytes(@as(c_int, 1)));
            } else if (@hasDecl(posix.SO, "REUSEPORT")) {
                try posix.setsockopt(listener, posix.SOL.SOCKET, posix.SO.REUSEPORT, &std.mem.toBytes(@as(c_int, 1)));
            } else {
                try posix.setsockopt(listener, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
            }

            try posix.bind(listener, &address.any, address.getOsSockLen());
            try posix.listen(listener, 1024);

            var actual: net.Address = undefined;
            var actual_len: posix.socklen_t = @sizeOf(net.Address);
            try posix.getsockname(listener, &actual.any, &actual_len);

            self.* = .{
                .allocator = allocator,
                .channel = channel,
                .options = options,
                .listener = listener,
                .port_value = actual.getPort(),
                .handshake_pool = try websocket.server.Handshake.Pool.init(allocator, 32, 2048, 10, 2),
                .buffer_provider = try websocket.buffer.Provider.init(allocator, .{
                    .size = 0,
                    .count = 0,
                    .max = max_frame_buffer,
                }),
            };
            errdefer self.handshake_pool.deinit();
            errdefer self.buffer_provider.deinit();
            try registerPumpEntry(self, pollErased);
        }

        fn deinit(self: *Self, allocator: Allocator) void {
            self.stop();
            self.connections.deinit(allocator);
            self.handshake_pool.deinit();
            self.buffer_provider.deinit();
        }

        fn stop(self: *Self) void {
            if (self.stopped) return;
            self.stopped = true;
            unregisterPumpEntry(self);
            posix.close(self.listener);

            while (self.connections.items.len > 0) {
                self.connections.items[self.connections.items.len - 1].destroy();
            }
        }

        fn pollErased(ptr: *anyopaque) !bool {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return self.poll();
        }

        fn poll(self: *Self) !bool {
            if (self.stopped) return false;

            var progressed = false;
            while (true) {
                var address: net.Address = undefined;
                var address_len: posix.socklen_t = @sizeOf(net.Address);
                // Accept errors (EMFILE, ECONNABORTED, ...) are per-connection: try again next poll.
                const socket = posix.accept(self.listener, &address.any, &address_len, posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK) catch break;
                const conn = Connection.create(self.allocator, self, socket, address) catch {
                    posix.close(socket);
                    break;
                };
                self.connections.append(self.allocator, conn) catch {
                    conn.removed = true; // not in the list yet; skip removeConnection
                    conn.closeSocket();
                    if (conn.handshake_state) |state| state.release();
                    self.allocator.destroy(conn);
                    break;
                };
                progressed = true;
            }

            var index: usize = 0;
            while (index < self.connections.items.len) {
                const conn = self.connections.items[index];
                // A failing connection is torn down on its own; it never aborts the server poll.
                // `conn` may be freed by `poll`, so check list membership instead of `conn.removed`.
                const polled = conn.poll() catch blk: {
                    if (index < self.connections.items.len and self.connections.items[index] == conn) {
                        conn.destroy();
                    }
                    break :blk true;
                };
                progressed = polled or progressed;
                if (index < self.connections.items.len and self.connections.items[index] == conn) {
                    index += 1;
                }
            }
            return progressed;
        }

        fn removeConnection(self: *Self, target: *Connection) void {
            var index: usize = 0;
            while (index < self.connections.items.len) : (index += 1) {
                if (self.connections.items[index] == target) {
                    _ = self.connections.swapRemove(index);
                    return;
                }
            }
        }

        fn stopErased(ptr: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.stop();
        }

        fn destroyErased(ptr: *anyopaque, allocator: Allocator) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.deinit(allocator);
            allocator.destroy(self);
        }
    };
}

/// Bytes queued for a QUIC stream: frames wait here while the stream applies flow control.
const QuicOutbox = struct {
    bytes: std.ArrayListUnmanaged(u8) = .{},
    start: usize = 0,

    fn deinit(self: *QuicOutbox, allocator: Allocator) void {
        self.bytes.deinit(allocator);
    }

    fn push(self: *QuicOutbox, allocator: Allocator, payload: []const u8) !void {
        try quic_frame.appendFrame(&self.bytes, allocator, payload);
    }

    fn flush(self: *QuicOutbox, allocator: Allocator, stream: *quic_engine.Ssl) !void {
        while (self.start < self.bytes.items.len) {
            const n = try quic_engine.write(stream, self.bytes.items[self.start..]);
            if (n == 0) break;
            self.start += n;
        }
        if (self.start == self.bytes.items.len) {
            self.start = 0;
            if (self.bytes.capacity > 4 * 1024 * 1024) self.bytes.clearAndFree(allocator) else self.bytes.clearRetainingCapacity();
        } else if (self.start >= 1024 * 1024) {
            const left = self.bytes.items.len - self.start;
            std.mem.copyForwards(u8, self.bytes.items[0..left], self.bytes.items[self.start..]);
            self.bytes.items.len = left;
            self.start = 0;
        }
    }
};

const quic_read_chunk: usize = 64 * 1024;

fn resolveAddress(allocator: Allocator, host: []const u8, port: u16) !net.Address {
    if (net.Address.parseIp(host, port)) |address| return address else |_| {}
    const list = try net.getAddressList(allocator, host, port);
    defer list.deinit();
    if (list.addrs.len == 0) return error.UnknownHostName;
    return list.addrs[0];
}

/// Client side of `quic://host:port`: its own UDP socket, one bidirectional stream, hello first.
/// Like the WebSocket client it doesn't reconnect: a lost connection stops the peer.
fn QuicClientTransport(comptime State: type) type {
    return struct {
        allocator: Allocator,
        peer: *Peer(State),
        conn: quic_engine.ClientConn,
        reader: quic_frame.FrameReader = .{},
        outbox: QuicOutbox = .{},
        connected: bool = true,
        next_keepalive_ms: i64,

        const Self = @This();

        fn init(
            self: *Self,
            allocator: Allocator,
            channel: *Channel(State),
            state: State,
            address: []const u8,
            options: ConnectOptions(State),
        ) !void {
            const pin = if (options.quic.fingerprint) |text| try quic_frame.parseFingerprint(text) else null;
            const target = try quic_frame.parseTarget(allocator, address);
            defer target.deinit(allocator);
            const remote = try resolveAddress(allocator, target.host, target.port);

            var conn = try quic_engine.ClientConn.open(allocator, target.host, remote, .{
                .ca = options.quic.ca,
                .fingerprint = pin,
                .server_name = options.quic.server_name,
            });
            errdefer conn.deinit();
            const deadline = std.time.milliTimestamp() + 10_000;
            while (!(try conn.handshake())) {
                if (!(try pumpAll())) {
                    if (std.time.milliTimestamp() > deadline) return error.Timeout;
                    std.Thread.sleep(100_000);
                }
            }

            self.* = .{
                .allocator = allocator,
                .peer = undefined,
                .conn = conn,
                .next_keepalive_ms = std.time.milliTimestamp() + quic_frame.keepalive_interval_ms,
            };
            errdefer self.reader.deinit(allocator);
            errdefer self.outbox.deinit(allocator);

            const hello = try quic_frame.helloAlloc(allocator, options.headers);
            defer allocator.free(hello);
            try self.outbox.push(allocator, hello);
            try self.outbox.flush(allocator, self.conn.ssl);

            const peer = try Peer(State).createTransported(allocator, channel, state, .quic_client, self, .{
                .send_frame = sendFrameErased,
                .close = closeErased,
                .is_connected = isConnectedErased,
            });
            errdefer peer.destroy();
            self.peer = peer;
            try registerPumpEntry(self, pollErased);
        }

        fn deinit(self: *Self, allocator: Allocator) void {
            unregisterPumpEntry(self);
            self.conn.deinit();
            self.reader.deinit(allocator);
            self.outbox.deinit(allocator);
        }

        fn destroyErased(ptr: *anyopaque, allocator: Allocator) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.deinit(allocator);
            allocator.destroy(self);
        }

        fn sendFrameErased(ptr: *anyopaque, payload: []u8) !void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (!self.connected) return error.NotConnected;
            try self.outbox.push(self.allocator, payload);
            self.outbox.flush(self.allocator, self.conn.ssl) catch {
                closeErased(self);
                return error.NotConnected;
            };
        }

        /// Sends CONNECTION_CLOSE; the SSL objects and the socket go in `deinit`.
        fn closeErased(ptr: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (!self.connected) return;
            self.connected = false;
            self.conn.close();
        }

        fn isConnectedErased(ptr: *anyopaque) bool {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return self.connected;
        }

        fn pollErased(ptr: *anyopaque) !bool {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return self.poll();
        }

        /// Errors are contained, as in the WebSocket client: they stop this client's peer.
        fn poll(self: *Self) !bool {
            if (!self.connected) return false;
            self.conn.tick();
            const now = std.time.milliTimestamp();
            if (now >= self.next_keepalive_ms) {
                self.next_keepalive_ms = now + quic_frame.keepalive_interval_ms;
                self.outbox.bytes.appendSlice(self.allocator, &quic_frame.keepalive) catch {};
            }
            self.outbox.flush(self.allocator, self.conn.ssl) catch {
                self.fail("disconnected");
                return true;
            };

            var progressed = false;
            while (true) {
                const space = self.reader.writable(self.allocator, quic_read_chunk) catch {
                    self.fail("out of memory");
                    return true;
                };
                const n = quic_engine.read(self.conn.ssl, space) catch |err| switch (err) {
                    error.WouldBlock => break,
                    else => {
                        self.fail("disconnected");
                        return true;
                    },
                };
                self.reader.commit(n);
                progressed = true;
                while (true) {
                    const payload = (self.reader.next() catch {
                        self.fail("message too large");
                        return true;
                    }) orelse break;
                    self.peer.handleIncomingFrame(payload, .text) catch {
                        self.fail("invalid message");
                        return true;
                    };
                    if (!self.connected) return true;
                }
            }
            if (self.conn.isTerminated()) {
                self.fail("disconnected");
                return true;
            }
            return progressed;
        }

        fn fail(self: *Self, reason: []const u8) void {
            closeErased(self);
            self.peer.stopWithReason(reason);
        }
    };
}

/// `listenQuic`: one UDP socket, a connection per client, each with exactly one bidirectional stream
/// whose first frame is the hello.
fn QuicServerTransport(comptime State: type) type {
    return struct {
        allocator: Allocator,
        channel: *Channel(State),
        options: QuicListenOptions(State),
        listener: quic_engine.Listener,
        listener_open: bool = true,
        port_value: u16,
        stopped: bool = false,
        connections: std.ArrayListUnmanaged(*Connection) = .{},
        /// Connections sending their CONNECTION_CLOSE; freed once done.
        closing: std.ArrayListUnmanaged(Closing) = .{},

        const Self = @This();

        const Closing = struct {
            conn: quic_engine.ServerConn,
            deadline_ms: i64,
        };

        const Connection = struct {
            allocator: Allocator,
            server: *Self,
            conn: quic_engine.ServerConn,
            reader: quic_frame.FrameReader = .{},
            outbox: QuicOutbox = .{},
            peer: ?*Peer(State) = null,
            closed: bool = false,
            removed: bool = false,
            /// Polls of this connection on the stack (a handler waiting on the client re-enters it).
            depth: u32 = 0,

            fn sendFrameErased(ptr: *anyopaque, payload: []u8) !void {
                const self: *Connection = @ptrCast(@alignCast(ptr));
                if (self.closed) return error.NotConnected;
                const stream = self.conn.stream orelse return error.NotConnected;
                try self.outbox.push(self.allocator, payload);
                self.outbox.flush(self.allocator, stream) catch {
                    self.closed = true;
                    return error.NotConnected;
                };
            }

            /// Torn down by the next poll of this connection, once no poll of it is running.
            fn closeErased(ptr: *anyopaque) void {
                const self: *Connection = @ptrCast(@alignCast(ptr));
                self.closed = true;
            }

            fn isConnectedErased(ptr: *anyopaque) bool {
                const self: *Connection = @ptrCast(@alignCast(ptr));
                return !self.closed;
            }

            fn destroy(self: *Connection) void {
                if (self.removed) return;
                self.closed = true;
                if (self.depth > 0) {
                    // A handler of this connection is on the stack, maybe waiting on the client: settle its
                    // calls now, free the connection when the outermost poll returns
                    if (self.peer) |peer| peer.stop();
                    return;
                }
                self.removed = true;
                self.server.removeConnection(self);

                if (self.peer) |peer| {
                    peer.stop();
                    if (self.server.options.on_disconnect) |callback| {
                        callback(&peer.state, .{ .peer = peer });
                    }
                    peer.destroy();
                    self.peer = null;
                }
                self.server.retire(self.conn);
                self.reader.deinit(self.allocator);
                self.outbox.deinit(self.allocator);
                self.allocator.destroy(self);
            }

            fn poll(self: *Connection) bool {
                if (self.closed) {
                    self.destroy();
                    return true;
                }
                self.depth += 1;
                const progressed = self.pollStream();
                self.depth -= 1;
                if (self.closed) {
                    self.destroy();
                    return true;
                }
                return progressed;
            }

            /// Any failure (refused hello or state, invalid frame or JSON, oversized frame, the client
            /// gone) sets `closed`, like the WebSocket server dropping the socket.
            fn pollStream(self: *Connection) bool {
                if (!self.conn.acceptStreams()) {
                    if (self.conn.isTerminated()) self.closed = true;
                    return self.closed;
                }
                const stream = self.conn.stream.?;
                self.outbox.flush(self.allocator, stream) catch {
                    self.closed = true;
                    return true;
                };

                var progressed = false;
                while (!self.closed) {
                    const space = self.reader.writable(self.allocator, quic_read_chunk) catch {
                        self.closed = true;
                        return true;
                    };
                    const n = quic_engine.read(stream, space) catch |err| switch (err) {
                        error.WouldBlock => break,
                        else => {
                            self.closed = true;
                            return true;
                        },
                    };
                    self.reader.commit(n);
                    progressed = true;
                    while (!self.closed) {
                        const payload = (self.reader.next() catch {
                            self.closed = true;
                            return true;
                        }) orelse break;
                        if (self.peer) |peer| {
                            peer.handleIncomingFrame(payload, .text) catch {
                                self.closed = true;
                                return true;
                            };
                        } else {
                            self.welcome(payload) catch {
                                self.closed = true;
                                return true;
                            };
                        }
                    }
                }
                if (self.conn.isTerminated()) self.closed = true;
                return progressed or self.closed;
            }

            /// The first frame: `{"hello": {...}}`, used like the WebSocket upgrade headers.
            fn welcome(self: *Connection, payload: []const u8) !void {
                var hello = try quic_frame.parseHello(self.allocator, payload);
                defer hello.deinit(self.allocator);
                const state = if (self.server.options.state) |make| try make(&hello.headers) else std.mem.zeroes(State);
                const peer = try Peer(State).createTransported(self.allocator, self.server.channel, state, .quic_server, self, .{
                    .send_frame = sendFrameErased,
                    .close = closeErased,
                    .is_connected = isConnectedErased,
                });
                self.peer = peer;
                if (self.server.options.on_connect) |callback| {
                    callback(&peer.state, .{ .peer = peer });
                }
            }
        };

        fn init(
            self: *Self,
            allocator: Allocator,
            channel: *Channel(State),
            target: ListenTarget,
            options: QuicListenOptions(State),
        ) !void {
            const host = if (target.host.len >= 2 and target.host[0] == '[') target.host[1 .. target.host.len - 1] else target.host;
            const address = try net.Address.parseIp(host, target.port);
            var listener = try quic_engine.Listener.open(address, options.cert, options.key);
            errdefer listener.deinit();
            self.* = .{
                .allocator = allocator,
                .channel = channel,
                .options = options,
                .listener = listener,
                .port_value = listener.port,
            };
            try registerPumpEntry(self, pollErased);
        }

        fn deinit(self: *Self, allocator: Allocator) void {
            self.stop();
            self.connections.deinit(allocator);
            self.closing.deinit(allocator);
        }

        /// Drops every connection (each gets a CONNECTION_CLOSE) and closes the socket.
        fn stop(self: *Self) void {
            if (self.stopped) return;
            self.stopped = true;
            unregisterPumpEntry(self);
            // Backwards: destroy swap-removes. One whose handler is running (stop called from it) stays.
            var index = self.connections.items.len;
            while (index > 0) {
                index -= 1;
                if (index < self.connections.items.len) self.connections.items[index].destroy();
            }
            var round: usize = 0;
            while (self.closing.items.len > 0 and round < 5) : (round += 1) {
                self.listener.tick();
                self.reap();
            }
            for (self.closing.items) |*entry| entry.conn.deinit();
            self.closing.clearRetainingCapacity();
            self.listener_open = false;
            self.listener.deinit();
        }

        fn retire(self: *Self, conn: quic_engine.ServerConn) void {
            var mutable = conn;
            // After stop the listener is gone and nothing would drive the shutdown
            if (mutable.shutdown() or !self.listener_open) {
                mutable.deinit();
                return;
            }
            self.closing.append(self.allocator, .{
                .conn = mutable,
                .deadline_ms = std.time.milliTimestamp() + 2_000,
            }) catch mutable.deinit();
        }

        fn reap(self: *Self) void {
            const now = std.time.milliTimestamp();
            var index: usize = 0;
            while (index < self.closing.items.len) {
                const entry = &self.closing.items[index];
                if (entry.conn.shutdown() or entry.conn.isTerminated() or now > entry.deadline_ms) {
                    entry.conn.deinit();
                    _ = self.closing.swapRemove(index);
                    continue;
                }
                index += 1;
            }
        }

        fn pollErased(ptr: *anyopaque) !bool {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return self.poll();
        }

        fn poll(self: *Self) !bool {
            if (self.stopped) return false;
            self.listener.tick();

            var progressed = false;
            while (self.listener.accept()) |accepted| {
                var conn = accepted;
                const connection = self.allocator.create(Connection) catch {
                    conn.deinit();
                    break;
                };
                connection.* = .{
                    .allocator = self.allocator,
                    .server = self,
                    .conn = conn,
                };
                self.connections.append(self.allocator, connection) catch {
                    conn.deinit();
                    self.allocator.destroy(connection);
                    break;
                };
                progressed = true;
            }

            var index: usize = 0;
            while (index < self.connections.items.len) {
                const conn = self.connections.items[index];
                // `conn` may be freed by `poll`, so check list membership instead of its fields
                progressed = conn.poll() or progressed;
                if (index < self.connections.items.len and self.connections.items[index] == conn) {
                    index += 1;
                }
            }
            if (self.stopped) return progressed;
            self.reap();
            return progressed;
        }

        fn removeConnection(self: *Self, target: *Connection) void {
            for (self.connections.items, 0..) |item, index| {
                if (item == target) {
                    _ = self.connections.swapRemove(index);
                    return;
                }
            }
        }

        fn stopErased(ptr: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.stop();
        }

        fn destroyErased(ptr: *anyopaque, allocator: Allocator) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.deinit(allocator);
            allocator.destroy(self);
        }
    };
}

/// Largest message (single frame payload or reassembled fragments) accepted from a peer. Matches
/// the TS/Bun default max payload (16 MiB). Larger messages close the connection.
pub const max_message_size: usize = 16 * 1024 * 1024;
/// Buffer cap handed to the websocket package: a whole frame (payload + up to 14 header bytes).
const max_frame_buffer: usize = max_message_size + 14;

/// Validates the length of every frame header already buffered in `reader` before the websocket
/// package parses it. The package computes `payload_len + header_len` in u64 without an overflow
/// check (proto.zig, `ml = ... + length_of_len + 2`), so a 2^64-1 length panics the process
/// (AUDIT2 L4). The package lives in the zig package cache, so the guard lives here.
/// `Reader.read` may consume several buffered frames in one call (fragments), so all complete
/// headers in the buffer are checked, not just the first.
fn guardFrameLengths(reader: *const websocket.proto.Reader, max_payload: usize) !void {
    const data = reader.buf.data[reader.start..reader.pos];
    var offset: usize = 0;
    if (reader.message_len != 0) {
        // The current frame's header was already validated; skip to whatever follows it.
        offset = reader.message_len;
    }
    while (offset < data.len) {
        const rest = data[offset..];
        if (rest.len < 2) return;
        const length_of_len: usize = switch (rest[1] & 127) {
            126 => 2,
            127 => 8,
            else => 0,
        };
        if (rest.len < 2 + length_of_len) return;
        const payload_len: u64 = switch (length_of_len) {
            2 => std.mem.readInt(u16, rest[2..4], .big),
            8 => std.mem.readInt(u64, rest[2..10], .big),
            else => rest[1] & 127,
        };
        if (payload_len > max_payload) return error.MessageTooLarge;
        const masked = rest[1] & 128 == 128;
        const total: usize = 2 + length_of_len + @as(usize, if (masked) 4 else 0) + @as(usize, @intCast(payload_len));
        if (rest.len < total) return;
        offset += total;
    }
}

fn writeSocketAll(socket: posix.socket_t, data: []const u8) !void {
    var slice = data;
    while (slice.len > 0) {
        const written = posix.write(socket, slice) catch |err| switch (err) {
            error.WouldBlock => {
                if (!(try pumpAll())) {
                    std.Thread.sleep(100_000);
                }
                continue;
            },
            else => return err,
        };
        slice = slice[written..];
    }
}

fn setNonBlocking(fd: posix.fd_t) !void {
    if (!@hasField(posix.O, "NONBLOCK")) return;
    var flags = try posix.fcntl(fd, posix.F.GETFL, 0);
    flags |= @as(usize, 1) << @bitOffsetOf(posix.O, "NONBLOCK");
    _ = try posix.fcntl(fd, posix.F.SETFL, flags);
}

fn generateClientKey() [16]u8 {
    var key: [16]u8 = undefined;
    std.crypto.random.bytes(&key);
    return key;
}

fn writeSocketFrame(socket: posix.socket_t, op_code: websocket.OpCode, data: []const u8) !void {
    var header_buf: [10]u8 = undefined;
    const header = websocket.proto.writeFrameHeader(&header_buf, op_code, data.len, false);
    try writeSocketAll(socket, header);
    try writeSocketAll(socket, data);
}

fn writeClientSocketFrame(socket: posix.socket_t, op_code: websocket.OpCode, data: []const u8) !void {
    var header_buf: [10]u8 = undefined;
    const header = websocket.proto.writeFrameHeader(&header_buf, op_code, data.len, false);
    header_buf[1] |= 0x80;
    try writeSocketAll(socket, header);

    const mask = generateClientKey()[0..4].*;
    try writeSocketAll(socket, &mask);

    var offset: usize = 0;
    var chunk_buf: [4096]u8 = undefined;
    while (offset < data.len) {
        const chunk_len = @min(chunk_buf.len, data.len - offset);
        for (data[offset .. offset + chunk_len], 0..) |byte, index| {
            chunk_buf[index] = byte ^ mask[(offset + index) & 3];
        }
        try writeSocketAll(socket, chunk_buf[0..chunk_len]);
        offset += chunk_len;
    }
}

fn parseConnectTarget(allocator: Allocator, address: anytype) !ConnectTarget {
    switch (@typeInfo(@TypeOf(address))) {
        .int, .comptime_int => {
            return .{
                .host = try allocator.dupe(u8, "127.0.0.1"),
                .path = try allocator.dupe(u8, "/"),
                .port = @intCast(address),
                .tls = false,
            };
        },
        else => {
            const text: []const u8 = address;
            if (!std.mem.startsWith(u8, text, "ws://") and !std.mem.startsWith(u8, text, "wss://")) {
                const separator = std.mem.lastIndexOfScalar(u8, text, ':') orelse return error.InvalidAddress;
                return .{
                    .host = try allocator.dupe(u8, text[0..separator]),
                    .path = try allocator.dupe(u8, "/"),
                    .port = try std.fmt.parseInt(u16, text[separator + 1 ..], 10),
                    .tls = false,
                };
            }

            const uri = try std.Uri.parse(text);
            var host_buffer: [std.Uri.host_name_max]u8 = undefined;
            const host_raw = try uri.getHost(&host_buffer);
            const base_path = if (uri.path.isEmpty())
                try allocator.dupe(u8, "/")
            else
                try std.fmt.allocPrint(allocator, "{f}", .{std.fmt.alt(uri.path, .formatRaw)});
            errdefer allocator.free(base_path);

            const full_path = if (uri.query) |query|
                try std.fmt.allocPrint(allocator, "{s}?{f}", .{ base_path, std.fmt.alt(query, .formatRaw) })
            else
                try allocator.dupe(u8, base_path);
            errdefer allocator.free(full_path);
            allocator.free(base_path);

            return .{
                .host = try allocator.dupe(u8, host_raw),
                .path = full_path,
                .port = uri.port orelse if (std.mem.eql(u8, uri.scheme, "wss")) 443 else 80,
                .tls = std.mem.eql(u8, uri.scheme, "wss"),
            };
        },
    }
}

fn parseListenTarget(address: anytype) !ListenTarget {
    switch (@typeInfo(@TypeOf(address))) {
        .int, .comptime_int => {
            return .{
                .host = "127.0.0.1",
                .port = @intCast(address),
            };
        },
        else => {
            const text: []const u8 = address;
            const separator = std.mem.lastIndexOfScalar(u8, text, ':') orelse return error.InvalidAddress;
            return .{
                .host = text[0..separator],
                .port = try std.fmt.parseInt(u16, text[separator + 1 ..], 10),
            };
        },
    }
}

fn buildListenState(
    comptime State: type,
    allocator: Allocator,
    handshake: *const websocket.Handshake,
    make_state: ?*const fn (headers: *const std.StringHashMapUnmanaged([]const u8)) anyerror!State,
) !State {
    if (make_state == null) {
        return std.mem.zeroes(State);
    }

    var merged: std.StringHashMapUnmanaged([]const u8) = .{};
    defer {
        var it = merged.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            allocator.free(entry.value_ptr.*);
        }
        merged.deinit(allocator);
    }

    if (std.mem.indexOfScalar(u8, handshake.url, '?')) |query_start| {
        var pairs = std.mem.splitScalar(u8, handshake.url[query_start + 1 ..], '&');
        while (pairs.next()) |pair| {
            if (pair.len == 0) continue;
            const separator = std.mem.indexOfScalar(u8, pair, '=') orelse pair.len;
            const key = pair[0..separator];
            const value = if (separator < pair.len) pair[separator + 1 ..] else "";
            try putOwnedHeader(allocator, &merged, key, value);
        }
    }

    var header_it = handshake.headers.iterator();
    while (header_it.next()) |entry| {
        try putOwnedHeader(allocator, &merged, entry.key, entry.value);
    }

    return try make_state.?(&merged);
}

fn putOwnedHeader(
    allocator: Allocator,
    map: *std.StringHashMapUnmanaged([]const u8),
    key: []const u8,
    value: []const u8,
) !void {
    if (map.fetchRemove(key)) |entry| {
        allocator.free(entry.key);
        allocator.free(entry.value);
    }
    try map.put(allocator, try allocator.dupe(u8, key), try allocator.dupe(u8, value));
}

fn pickEphemeralPort(host: []const u8) !u16 {
    const address = try std.net.Address.parseIp(host, 0);
    var server = try address.listen(.{
        .reuse_address = true,
    });
    defer server.stream.close();
    return server.listen_address.getPort();
}

fn freeOptionalOwned(allocator: Allocator, slot: *?[]u8) void {
    if (slot.*) |value| {
        allocator.free(value);
        slot.* = null;
    }
}

/// JS truthiness of an encoded JSON body: TS `publish` drops `null`, `false`, `0` and `""`.
fn isFalsyJson(body_json: ?[]const u8) bool {
    const raw = std.mem.trim(u8, body_json orelse return true, &std.ascii.whitespace);
    if (raw.len == 0) return true;
    if (std.mem.eql(u8, raw, "null") or std.mem.eql(u8, raw, "false") or std.mem.eql(u8, raw, "\"\"")) return true;
    if (raw[0] == '-' or std.ascii.isDigit(raw[0])) {
        const number = std.fmt.parseFloat(f64, raw) catch return false;
        return number == 0;
    }
    return false;
}

fn joinTopicAlloc(allocator: Allocator, prefix: []const u8, suffix: []const u8) ![]u8 {
    if (prefix.len == 0) return allocator.dupe(u8, suffix);
    if (suffix.len == 0) return allocator.dupe(u8, prefix);
    return std.fmt.allocPrint(allocator, "{s}/{s}", .{ prefix, suffix });
}

fn topicFromValueAlloc(allocator: Allocator, comptime T: type, value: T) ![]u8 {
    if (T == void) return allocator.dupe(u8, "");
    return switch (@typeInfo(T)) {
        .pointer => |pointer_info| switch (pointer_info.size) {
            .slice => if (pointer_info.child == u8) allocator.dupe(u8, value) else json_codec.stringifyAlloc(allocator, value),
            else => json_codec.stringifyAlloc(allocator, value),
        },
        .optional => if (value == null) allocator.dupe(u8, "null") else topicFromValueAlloc(allocator, @typeInfo(T).optional.child, value.?),
        .int, .comptime_int => std.fmt.allocPrint(allocator, "{d}", .{value}),
        .bool => allocator.dupe(u8, if (value) "true" else "false"),
        .@"enum" => allocator.dupe(u8, @tagName(value)),
        else => json_codec.stringifyAlloc(allocator, value),
    };
}
