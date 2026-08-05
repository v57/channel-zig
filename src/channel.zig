const std = @import("std");
const websocket = @import("websocket");
const wire = @import("wire.zig");
const json_codec = @import("json_codec.zig");

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
        headers: ?[]const u8 = null,
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

        const Self = @This();

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

        pub fn send(self: *Self, allocator: Allocator, request: Req, event: Event) !void {
            const local_topic = try topicFromValueAlloc(allocator, Req, request);
            defer allocator.free(local_topic);
            const topic = try joinTopicAlloc(allocator, self.prefix, local_topic);
            defer allocator.free(topic);
            const body_json = try json_codec.encodeOptionalAlloc(allocator, Event, event);
            defer if (body_json) |value| allocator.free(value);
            if (Event == void and body_json == null) return;
            self.publishEncoded(topic, body_json);
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
            const local_topic = try topicFromValueAlloc(allocator, Req, request);
            defer allocator.free(local_topic);
            return .{
                .topic = try joinTopicAlloc(allocator, self.prefix, local_topic),
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

        fn publishEncoded(self: *Self, topic: []const u8, body_json: ?[]const u8) void {
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
                sink.send(sink.ptr, topic, body_json);
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
        };

        const OtherStreamHandler = struct {
            matcher: *const fn ([]const u8) bool,
            handler: ErasedStreamHandler,
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

            return self;
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

        pub fn connect(self: *Self, allocator: Allocator, address: anytype, options: ConnectOptions(State)) !Client(State) {
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
                    if (other.matcher(path)) {
                        return other.handler.invoke(self, peer, path, message.body_json, message.context_json, message.id);
                    }
                }
                if (message.id) |id| {
                    try peer.sendMessage(.{
                        .id = id,
                        .error_text = try self.allocator.dupe(u8, "api not found"),
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
                    if (other.matcher(path)) {
                        return other.handler.invoke(self, peer, path, message.body_json, message.context_json, message.id);
                    }
                }
                try peer.sendMessage(.{
                    .id = message.id.?,
                    .error_text = try self.allocator.dupe(u8, "api not found"),
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
                        .error_text = try self.allocator.dupe(u8, "subscription not found"),
                    });
                    return;
                };
                var ack = try binding.subscribe(binding.ptr, self.allocator, message.body_json);
                defer ack.deinit(self.allocator);
                peer.addRemoteTopic(ack.topic);
                try peer.sendMessage(.{
                    .id = id,
                    .topic = try self.allocator.dupe(u8, ack.topic),
                    .body_json = if (ack.body_json) |value| try self.allocator.dupe(u8, value) else null,
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
                const next_prefix = try joinTopicAlloc(self.allocator, prefix, field.name);
                errdefer self.allocator.free(next_prefix);
                try self.owned_strings.append(self.allocator, next_prefix);
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
                    var body = try json_codec.decodeRequired(channel.allocator, Req, body_json);
                    defer json_codec.freeOwnedValue(channel.allocator, Req, &body);
                    var context = try json_codec.decodeRequired(channel.allocator, Ctx, context_json);
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
                                .error_text = try std.fmt.allocPrint(channel.allocator, "{s}", .{@errorName(err)}),
                            });
                        }
                        return;
                    };
                    if (id) |request_id| {
                        const body_out = if (Resp == void)
                            null
                        else
                            try json_codec.encodeOptionalAlloc(
                                channel.allocator,
                                ?Resp,
                                normalizePostResult(Resp, result),
                            );
                        errdefer if (body_out) |value| channel.allocator.free(value);
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
                    var body = try json_codec.decodeRequired(channel.allocator, Req, body_json);
                    errdefer json_codec.freeOwnedValue(channel.allocator, Req, &body);
                    var context = try json_codec.decodeRequired(channel.allocator, Ctx, context_json);
                    errdefer json_codec.freeOwnedValue(channel.allocator, Ctx, &context);
                    const owned_path = try channel.allocator.dupe(u8, path);
                    errdefer channel.allocator.free(owned_path);

                    const task = try peer.createIncomingStreamTask(request_id);
                    errdefer peer.releaseIncomingStreamTask(request_id);

                    defer peer.releaseIncomingStreamTask(request_id);
                    defer channel.allocator.free(owned_path);
                    defer json_codec.freeOwnedValue(channel.allocator, Req, &body);
                    defer json_codec.freeOwnedValue(channel.allocator, Ctx, &context);

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
                            .error_text = try std.fmt.allocPrint(channel.allocator, "{s}", .{@errorName(err)}),
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
            const pending = try self.allocator.create(PendingCall);
            errdefer self.allocator.destroy(pending);
            pending.* = .{};

            const id = self.takeNextId();
            try self.pending.put(self.allocator, id, .{ .call = pending });
            errdefer {
                _ = self.pending.remove(id);
                self.allocator.destroy(pending);
            }

            const body_json = try json_codec.encodeOptionalAlloc(self.allocator, Req, body);
            errdefer if (body_json) |value| self.allocator.free(value);
            const context_json = try json_codec.encodeOptionalAlloc(self.allocator, Ctx, context);
            errdefer if (context_json) |value| self.allocator.free(value);
            try self.sendMessage(.{
                .id = id,
                .path = try self.allocator.dupe(u8, path),
                .body_json = body_json,
                .context_json = context_json,
            });

            while (!pending.done and !self.stopped) {
                if (!(try pumpAll())) {
                    std.Thread.sleep(100_000);
                }
            }
            const error_text = pending.error_text;
            const response_body = pending.body_json;
            defer {
                if (error_text) |value| self.allocator.free(value);
                if (response_body) |value| self.allocator.free(value);
                self.allocator.destroy(pending);
            }

            if (error_text) |value| {
                return .{ .remote_err = try allocator.dupe(u8, value) };
            }
            if (Resp == void or response_body == null) {
                return .{ .ok = null };
            }
            const decoded = try json_codec.decodeRequired(allocator, Resp, response_body);
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
            var pending = try self.allocator.create(PendingStream);
            errdefer self.allocator.destroy(pending);
            const id = self.takeNextId();
            pending.* = .{
                .peer = self,
                .id = id,
            };
            try self.stream_entries.append(self.allocator, pending);

            try self.pending.put(self.allocator, id, .{ .stream = pending });
            errdefer {
                _ = self.pending.remove(id);
                pending.deinit(self.allocator);
                self.allocator.destroy(pending);
            }

            const body_json = try json_codec.encodeOptionalAlloc(self.allocator, Req, body);
            errdefer if (body_json) |value| self.allocator.free(value);
            const context_json = try json_codec.encodeOptionalAlloc(self.allocator, Ctx, context);
            errdefer if (context_json) |value| self.allocator.free(value);
            try self.sendMessage(.{
                .id = id,
                .stream = try self.allocator.dupe(u8, path),
                .body_json = body_json,
                .context_json = context_json,
            });

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
            const pending = try self.allocator.create(PendingSubscribe);
            errdefer self.allocator.destroy(pending);
            pending.* = .{};

            const id = self.takeNextId();
            try self.pending.put(self.allocator, id, .{ .subscribe = pending });
            errdefer {
                _ = self.pending.remove(id);
                self.allocator.destroy(pending);
            }

            const body_json = try json_codec.encodeOptionalAlloc(self.allocator, Req, body);
            errdefer if (body_json) |value| self.allocator.free(value);
            const context_json = try json_codec.encodeOptionalAlloc(self.allocator, Ctx, context);
            errdefer if (context_json) |value| self.allocator.free(value);
            try self.sendMessage(.{
                .id = id,
                .sub = try self.allocator.dupe(u8, path),
                .body_json = body_json,
                .context_json = context_json,
            });

            while (!pending.done and !self.stopped) {
                if (!(try pumpAll())) {
                    std.Thread.sleep(100_000);
                }
            }
            const error_text = pending.error_text;
            const topic = pending.topic;
            defer {
                if (error_text) |value| self.allocator.free(value);
                if (topic) |value| self.allocator.free(value);
                self.allocator.destroy(pending);
            }

            if (error_text) |value| {
                return .{ .remote_err = try allocator.dupe(u8, value) };
            }
            const topic_value = topic orelse return .{ .remote_err = try allocator.dupe(u8, "subscription missing topic") };
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
                            call.error_text = self.allocator.dupe(u8, "disconnected") catch null;
                            call.done = true;
                        }
                    },
                    .stream => |stream| {
                        if (!stream.finished) {
                            stream.queue.append(self.allocator, .{
                                .error_text = self.allocator.dupe(u8, "disconnected") catch null,
                            }) catch {};
                            stream.finished = true;
                        }
                    },
                    .subscribe => |sub| {
                        if (!sub.done) {
                            sub.error_text = self.allocator.dupe(u8, "disconnected") catch null;
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
            self.sendMessage(.{
                .topic = self.allocator.dupe(u8, topic) catch return,
                .body_json = if (body_json) |value| self.allocator.dupe(u8, value) catch return else null,
            }) catch {};
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
            if (self.task.cancelled) return error.Cancelled;
            const body_json = try json_codec.encodeOptionalAlloc(allocator, Item, item);
            errdefer if (body_json) |value| allocator.free(value);
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
                .max = 65_536,
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

        fn poll(self: *Self) !bool {
            if (!self.connected) return false;
            var progressed = false;

            while (true) {
                if (try self.reader.read()) |result| {
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
                                self.connected = false;
                                self.peer.stop();
                                return error.InvalidJsonMessage;
                            };
                        },
                        .ping => try writeClientSocketFrame(self.stream.handle, .pong, message.data),
                        .pong => {},
                        .close => {
                            closeErased(self);
                            self.peer.stop();
                            return true;
                        },
                    }
                    continue;
                }

                self.reader.fill(&self.stream) catch |err| switch (err) {
                    error.WouldBlock => return progressed,
                    error.Closed, error.ConnectionResetByPeer, error.BrokenPipe => {
                        self.connected = false;
                        self.peer.stop();
                        return true;
                    },
                    else => {
                        self.connected = false;
                        self.peer.stop();
                        return err;
                    },
                };
                progressed = true;
            }
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
                    if (try reader.read()) |result| {
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
                            .ping => try writeSocketFrame(self.socket, .pong, message.data),
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
                        error.Closed, error.ConnectionResetByPeer, error.BrokenPipe => {
                            self.destroy();
                            return true;
                        },
                        else => {
                            self.destroy();
                            return err;
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
                    .max = 65_536,
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
                const socket = posix.accept(self.listener, &address.any, &address_len, posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK) catch |err| switch (err) {
                    error.WouldBlock => break,
                    else => return err,
                };
                const conn = try Connection.create(self.allocator, self, socket, address);
                try self.connections.append(self.allocator, conn);
                progressed = true;
            }

            var index: usize = 0;
            while (index < self.connections.items.len) {
                const conn = self.connections.items[index];
                progressed = (try conn.poll()) or progressed;
                if (!conn.removed) {
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
