const std = @import("std");
const channel = @import("channel");
const websocket = @import("websocket");

const Allocator = std.mem.Allocator;
const net = std.net;
const BenchState = struct {};
const TextCountBody = struct {
    text: []const u8,
};
const RequestPath = "text/count";
const RequestBody = TextCountBody{
    .text = "hello",
};
const ProcessIterations = 5_000;
const WebsocketIterations = 1_000;
const BurstBatchSize = 2_048;
const DefaultMaxInFlight = 4_096;

var g_stop_requested = std.atomic.Value(bool).init(false);
var g_server_requests = std.atomic.Value(u64).init(0);

const Mode = enum {
    smoke,
    server,
    client,
    process,
};

const RequestMode = enum {
    burst,
    stream,
};

const ErrorMessage = struct {
    type: []const u8 = "error",
    message: []const u8,
};

const ReadyMessage = struct {
    type: []const u8 = "ready",
    pid: i32,
    port: ?u16 = null,
    clients: ?usize = null,
};

const ServerResultMessage = struct {
    type: []const u8 = "result",
    pid: i32,
    requests: u64,
    elapsedSeconds: f64,
};

const ClientResultMessage = struct {
    type: []const u8 = "result",
    pid: i32,
    requestsSent: u64,
    responsesReceived: u64,
    errors: u64,
    elapsedSeconds: f64,
};

const SmokeResult = struct {
    language: []const u8,
    process_rps: f64,
    websocket_rps: f64,
};

const SendBurstTotals = struct {
    requests_sent: u64 = 0,
    responses_received: u64 = 0,
    errors: u64 = 0,
};

const ResponseCounts = struct {
    completed: u64,
    errors: u64,
};

pub fn main() !void {
    const allocator = std.heap.smp_allocator;
    realMain(allocator) catch |err| {
        try emitJson(ErrorMessage{
            .message = @errorName(err),
        });
        std.process.exit(1);
    };
}

fn realMain(allocator: Allocator) !void {
    switch (try parseMode(allocator)) {
        .smoke => try runSmokeBench(allocator),
        .server => try runServerWorker(allocator),
        .client => try runClientWorker(allocator),
        .process => try runProcessWorker(allocator),
    }
}

fn parseMode(allocator: Allocator) !Mode {
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len <= 1) {
        return .smoke;
    }

    var index: usize = 1;
    while (index < args.len) : (index += 1) {
        const arg = args[index];
        if (std.mem.eql(u8, arg, "--mode")) {
            index += 1;
            if (index >= args.len) return error.MissingMode;
            return parseModeValue(args[index]);
        }
        return parseModeValue(arg);
    }
    return .smoke;
}

fn parseModeValue(value: []const u8) !Mode {
    if (std.mem.eql(u8, value, "server")) return .server;
    if (std.mem.eql(u8, value, "client")) return .client;
    if (std.mem.eql(u8, value, "process")) return .process;
    if (std.mem.eql(u8, value, "smoke")) return .smoke;
    return error.InvalidMode;
}

fn runSmokeBench(allocator: Allocator) !void {
    const process_rps = try runProcessBench(allocator);
    const websocket_rps = try runWebsocketBench(allocator);

    const result = SmokeResult{
        .language = "zig",
        .process_rps = process_rps,
        .websocket_rps = websocket_rps,
    };

    const output = try std.json.Stringify.valueAlloc(allocator, result, .{
        .whitespace = .indent_2,
    });
    defer allocator.free(output);
    try std.fs.File.stdout().writeAll(output);
    try std.fs.File.stdout().writeAll("\n");
}

fn runServerWorker(allocator: Allocator) !void {
    const port = try readPositiveIntEnv("BENCH_PORT");
    installTerminationSignalHandlers();
    g_stop_requested.store(false, .release);
    g_server_requests.store(0, .release);

    var server = try RawBenchmarkServer.listen(allocator, @as(u16, @intCast(port)));
    defer server.deinit();

    var timer = try std.time.Timer.start();
    try emitJson(ReadyMessage{
        .pid = @intCast(std.c.getpid()),
        .port = server.port(),
    });

    while (!g_stop_requested.load(.acquire)) {
        if (!(try server.poll())) {
            std.Thread.sleep(100_000);
        }
    }

    const elapsed_seconds = nanosToSeconds(timer.read());
    try emitJson(ServerResultMessage{
        .pid = @intCast(std.c.getpid()),
        .requests = g_server_requests.load(.acquire),
        .elapsedSeconds = elapsed_seconds,
    });
}

fn runClientWorker(allocator: Allocator) !void {
    const host = try readStringEnvOrDefault(allocator, "BENCH_HOST", "127.0.0.1");
    defer allocator.free(host);
    const port = try readPositiveIntEnv("BENCH_PORT");
    const clients_count = try readPositiveIntEnv("BENCH_CLIENTS");
    const total_requests = try readNonNegativeIntEnv("BENCH_TOTAL_REQUESTS");
    const request_mode = try readRequestMode();
    const max_in_flight = try readPositiveIntEnvOrDefault("BENCH_MAX_IN_FLIGHT", DefaultMaxInFlight);

    if (clients_count == 1 and request_mode == .burst) {
        try emitJson(ReadyMessage{
            .pid = @intCast(std.c.getpid()),
            .clients = 1,
        });

        var timer = try std.time.Timer.start();
        const totals = try runSingleThreadBurst(allocator, host, @as(u16, @intCast(port)), total_requests, max_in_flight);

        const elapsed_seconds = nanosToSeconds(timer.read());
        try emitJson(ClientResultMessage{
            .pid = @intCast(std.c.getpid()),
            .requestsSent = totals.requests_sent,
            .responsesReceived = totals.responses_received,
            .errors = totals.errors,
            .elapsedSeconds = elapsed_seconds,
        });
        return;
    }

    var clients = try allocator.alloc(channel.Client(BenchState), @intCast(clients_count));
    defer allocator.free(clients);

    var ch = channel.Channel(BenchState).init(allocator);
    defer ch.deinit();

    const address = try std.fmt.allocPrint(allocator, "ws://{s}:{d}", .{ host, port });
    defer allocator.free(address);

    var connected: usize = 0;
    while (connected < clients.len) : (connected += 1) {
        clients[connected] = try ch.connect(allocator, address, .{});
    }
    defer {
        for (clients) |*client_instance| {
            client_instance.deinit();
        }
    }

    var senders = try allocator.alloc(channel.Sender(BenchState), clients.len);
    defer allocator.free(senders);
    for (clients, 0..) |*client_instance, index| {
        senders[index] = client_instance.sender();
    }

    try emitJson(ReadyMessage{
        .pid = @intCast(std.c.getpid()),
        .clients = clients.len,
    });

    var timer = try std.time.Timer.start();
    const totals = try runSendBurst(allocator, senders, total_requests);

    const elapsed_seconds = nanosToSeconds(timer.read());
    try emitJson(ClientResultMessage{
        .pid = @intCast(std.c.getpid()),
        .requestsSent = totals.requests_sent,
        .responsesReceived = totals.responses_received,
        .errors = totals.errors,
        .elapsedSeconds = elapsed_seconds,
    });
}

fn runProcessWorker(allocator: Allocator) !void {
    const total_requests = try readNonNegativeIntEnv("BENCH_TOTAL_REQUESTS");

    var ch = channel.Channel(BenchState).init(allocator);
    defer ch.deinit();
    _ = try ch.post(RequestPath, countTextProcess);

    var process = try ch.process(allocator, .{});
    defer process.deinit();

    try emitJson(ReadyMessage{
        .pid = @intCast(std.c.getpid()),
    });

    var timer = try std.time.Timer.start();
    const totals = try runProcessLoop(allocator, &process, total_requests);

    const elapsed_seconds = nanosToSeconds(timer.read());
    try emitJson(ClientResultMessage{
        .pid = @intCast(std.c.getpid()),
        .requestsSent = totals.requests_sent,
        .responsesReceived = totals.responses_received,
        .errors = totals.errors,
        .elapsedSeconds = elapsed_seconds,
    });
}

fn countTextServer(request: channel.PostRequest(BenchState, TextCountBody, void)) !?i64 {
    _ = g_server_requests.fetchAdd(1, .monotonic);
    return @intCast(request.body.text.len);
}

fn countTextProcess(request: channel.PostRequest(BenchState, TextCountBody, void)) !?i64 {
    return @intCast(request.body.text.len);
}

fn runProcessBench(allocator: Allocator) !f64 {
    var ch = channel.Channel(BenchState).init(allocator);
    defer ch.deinit();
    _ = try ch.post("echo", echo);

    var process = try ch.process(allocator, .{});
    defer process.deinit();

    var timer = try std.time.Timer.start();
    var index: usize = 0;
    while (index < ProcessIterations) : (index += 1) {
        var result = try process.send(allocator, i64, "echo", @as(i64, @intCast(index)));
        defer result.deinit(allocator);
        switch (result) {
            .ok => |value| {
                if (value == null or value.? != @as(i64, @intCast(index))) return error.InvalidResult;
            },
            .remote_err => return error.InvalidResult,
        }
    }

    return @as(f64, @floatFromInt(ProcessIterations)) / nanosToSeconds(timer.read());
}

fn runWebsocketBench(allocator: Allocator) !f64 {
    var server_channel = channel.Channel(BenchState).init(allocator);
    defer server_channel.deinit();
    _ = try server_channel.post("echo", echo);

    var server = try server_channel.listen(allocator, 0, .{});
    defer server.deinit();

    var client_channel = channel.Channel(BenchState).init(allocator);
    defer client_channel.deinit();

    const url = try std.fmt.allocPrint(allocator, "ws://127.0.0.1:{d}", .{server.port()});
    defer allocator.free(url);

    var client = try client_channel.connect(allocator, url, .{});
    defer client.deinit();

    var timer = try std.time.Timer.start();
    var index: usize = 0;
    while (index < WebsocketIterations) : (index += 1) {
        var result = try client.send(allocator, i64, "echo", @as(i64, @intCast(index)));
        defer result.deinit(allocator);
        switch (result) {
            .ok => |value| {
                if (value == null or value.? != @as(i64, @intCast(index))) return error.InvalidResult;
            },
            .remote_err => return error.InvalidResult,
        }
    }

    return @as(f64, @floatFromInt(WebsocketIterations)) / nanosToSeconds(timer.read());
}

fn echo(request: channel.PostRequest(BenchState, i64, void)) !?i64 {
    return request.body;
}

fn runProcessLoop(allocator: Allocator, process: *channel.Process(BenchState), total_requests: u64) !SendBurstTotals {
    var totals = SendBurstTotals{};
    var index: u64 = 0;
    while (index < total_requests) : (index += 1) {
        var result = process.send(allocator, i64, RequestPath, RequestBody) catch {
            totals.errors += 1;
            totals.requests_sent += 1;
            continue;
        };
        totals.requests_sent += 1;
        switch (result) {
            .ok => |value| {
                if (value != null and value.? == @as(i64, RequestBody.text.len)) {
                    totals.responses_received += 1;
                } else {
                    totals.errors += 1;
                }
            },
            .remote_err => totals.errors += 1,
        }
        result.deinit(allocator);
    }
    return totals;
}

fn runSendBurst(allocator: Allocator, senders: []const channel.Sender(BenchState), total_requests: u64) !SendBurstTotals {
    if (senders.len == 0) return error.InvalidBenchmarkConfiguration;
    if (total_requests == 0) return .{};

    var totals = SendBurstTotals{};
    var request_index: u64 = 0;
    while (request_index < total_requests) : (request_index += 1) {
        totals.requests_sent += 1;

        var sender = senders[@as(usize, @intCast(request_index % senders.len))];
        var result = sender.send(allocator, i64, RequestPath, RequestBody) catch {
            totals.errors += 1;
            continue;
        };
        switch (result) {
            .ok => |value| {
                if (value != null and value.? == @as(i64, RequestBody.text.len)) {
                    totals.responses_received += 1;
                } else {
                    totals.errors += 1;
                }
            },
            .remote_err => totals.errors += 1,
        }
        result.deinit(allocator);
    }
    return totals;
}

const RawBurstClient = struct {
    allocator: Allocator,
    stream: net.Stream,
    reader: websocket.proto.Reader,
    reader_buf: []u8,
    buffer_provider: websocket.buffer.Provider,

    fn connect(allocator: Allocator, host: []const u8, port: u16) !RawBurstClient {
        const stream = try net.tcpConnectToHost(allocator, host, port);
        errdefer stream.close();

        const reader_buf = try allocator.alloc(u8, 64 * 1024);
        errdefer allocator.free(reader_buf);

        var buffer_provider = try websocket.buffer.Provider.init(allocator, .{
            .size = 0,
            .count = 0,
            .max = 1_048_576,
        });
        errdefer buffer_provider.deinit();

        var client = RawBurstClient{
            .allocator = allocator,
            .stream = stream,
            .reader = undefined,
            .reader_buf = reader_buf,
            .buffer_provider = buffer_provider,
        };
        const over_read = try client.performHandshake(host, port);
        client.reader = websocket.proto.Reader.init(client.reader_buf, &client.buffer_provider, null);
        client.reader.pos = over_read;
        return client;
    }

    fn deinit(self: *RawBurstClient) void {
        self.stream.close();
        self.reader.deinit();
        self.allocator.free(self.reader_buf);
        self.buffer_provider.deinit();
    }

    fn sendTextCountBatch(self: *RawBurstClient, start_id: u64, count: usize) !void {
        const payload = try buildTextCountBatchAlloc(self.allocator, start_id, count);
        defer self.allocator.free(payload);
        try writeClientFrame(&self.stream, .text, payload);
    }

    fn recvCounts(self: *RawBurstClient) !ResponseCounts {
        while (true) {
            if (try self.reader.read()) |result| {
                const message = result[1];
                defer self.reader.done(message.type);
                switch (message.type) {
                    .text, .binary => return countResponseFields(message.data),
                    .ping => try writeClientFrame(&self.stream, .pong, message.data),
                    .pong => {},
                    .close => return error.ConnectionClosed,
                }
                continue;
            }

            try self.reader.fill(&self.stream);
        }
    }

    fn performHandshake(self: *RawBurstClient, host: []const u8, port: u16) !usize {
        var request_buf: [1024]u8 = undefined;
        var pos: usize = 0;

        const raw_key = generateClientKey();
        var encoded_key: [24]u8 = undefined;
        const key = std.base64.standard.Encoder.encode(&encoded_key, &raw_key);

        pos += (try std.fmt.bufPrint(
            request_buf[pos..],
            "GET / HTTP/1.1\r\ncontent-length: 0\r\nupgrade: websocket\r\nsec-websocket-version: 13\r\nconnection: upgrade\r\nsec-websocket-key: {s}\r\nhost: {s}:{d}\r\n\r\n",
            .{ key, host, port },
        )).len;
        try self.stream.writeAll(request_buf[0..pos]);

        var read_pos: usize = 0;
        while (true) {
            const n = try self.stream.read(self.reader_buf[read_pos..]);
            if (n == 0) return error.ConnectionClosed;
            read_pos += n;

            if (std.mem.indexOf(u8, self.reader_buf[0..read_pos], "\r\n\r\n")) |header_end| {
                const header = self.reader_buf[0 .. header_end + 4];
                if (!std.mem.startsWith(u8, header, "HTTP/1.1 101 ")) return error.InvalidHandshakeResponse;
                if (!containsHeaderIgnoreCase(header, "sec-websocket-accept")) {
                    return error.InvalidHandshakeResponse;
                }
                const over_read = read_pos - (header_end + 4);
                if (over_read > 0) {
                    std.mem.copyForwards(u8, self.reader_buf[0..over_read], self.reader_buf[header_end + 4 .. read_pos]);
                }
                return over_read;
            }

            if (read_pos == self.reader_buf.len) return error.ResponseTooLarge;
        }
    }
};

const RawBenchmarkServer = struct {
    allocator: Allocator,
    listener: std.posix.socket_t,
    port_value: u16,
    stopped: bool = false,
    handshake_pool: *websocket.server.Handshake.Pool,
    buffer_provider: websocket.buffer.Provider,
    connections: std.ArrayListUnmanaged(*Connection) = .{},

    const Connection = struct {
        allocator: Allocator,
        server: *RawBenchmarkServer,
        socket: std.posix.socket_t,
        address: net.Address,
        handshake_state: ?*websocket.server.Handshake.State = null,
        reader: ?websocket.proto.Reader = null,
        reader_buf: ?[]u8 = null,
        pending_frame: ?[]u8 = null,
        pending_offset: usize = 0,
        closed: bool = false,
        removed: bool = false,

        fn create(allocator: Allocator, server: *RawBenchmarkServer, socket: std.posix.socket_t, address: net.Address) !*Connection {
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

        fn destroy(self: *Connection) void {
            if (self.removed) return;
            self.removed = true;
            self.server.removeConnection(self);
            self.closeSocket();
            if (self.pending_frame) |frame| {
                self.allocator.free(frame);
                self.pending_frame = null;
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

        fn closeSocket(self: *Connection) void {
            if (self.closed) return;
            self.closed = true;
            std.posix.close(self.socket);
        }

        fn poll(self: *Connection) !bool {
            if (self.closed) return false;

            const progressed = try self.flushPending();
            if (self.closed or self.removed) return true;
            if (self.pending_frame != null) return progressed;

            if (self.handshake_state != null) {
                return (try self.pollHandshake()) or progressed;
            }
            return (try self.pollActive()) or progressed;
        }

        fn flushPending(self: *Connection) !bool {
            const frame = self.pending_frame orelse return false;
            var progressed = false;
            while (self.pending_offset < frame.len) {
                const written = std.posix.write(self.socket, frame[self.pending_offset..]) catch |err| switch (err) {
                    error.WouldBlock => return progressed,
                    error.ConnectionResetByPeer, error.BrokenPipe => {
                        self.destroy();
                        return true;
                    },
                    else => {
                        self.destroy();
                        return err;
                    },
                };
                self.pending_offset += written;
                progressed = true;
            }
            self.allocator.free(frame);
            self.pending_frame = null;
            self.pending_offset = 0;
            return true;
        }

        fn queueFrame(self: *Connection, frame: []u8) !void {
            std.debug.assert(self.pending_frame == null);
            self.pending_frame = frame;
            self.pending_offset = 0;
            _ = try self.flushPending();
        }

        fn pollHandshake(self: *Connection) !bool {
            const state = self.handshake_state orelse return false;
            const n = std.posix.read(self.socket, state.buf[state.len..]) catch |err| switch (err) {
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

            const handshake = websocket.server.Handshake.parse(state) catch {
                self.destroy();
                return true;
            } orelse return true;

            const header_end = (std.mem.indexOf(u8, state.buf[0..state.len], "\r\n\r\n") orelse state.len) + 4;
            const overread_len = if (state.len > header_end) state.len - header_end else 0;
            const reader_buf_len = @max(@as(usize, 64 * 1024), overread_len + 1);
            const reader_buf = try self.allocator.alloc(u8, reader_buf_len);
            errdefer self.allocator.free(reader_buf);

            var reader = websocket.proto.Reader.init(reader_buf, &self.server.buffer_provider, null);
            if (overread_len > 0) {
                @memcpy(reader_buf[0..overread_len], state.buf[header_end..state.len]);
                reader.pos = overread_len;
            }

            var reply_buf: [2048]u8 = undefined;
            const reply = try websocket.server.Handshake.createReply(handshake.key, handshake.res_headers, false, &reply_buf);
            try writeSocketAllNonBlocking(self.socket, reply);

            state.release();
            self.handshake_state = null;
            self.reader = reader;
            self.reader_buf = reader_buf;
            return true;
        }

        fn pollActive(self: *Connection) !bool {
            var reader = &(self.reader orelse return false);
            var progressed = false;
            var stream = net.Stream{ .handle = self.socket };

            while (self.pending_frame == null) {
                if (try reader.read()) |result| {
                    progressed = true;
                    const message = result[1];
                    defer reader.done(message.type);
                    switch (message.type) {
                        .text, .binary => {
                            const payload = processBenchmarkPayload(self.allocator, message.data) catch {
                                self.destroy();
                                return true;
                            };
                            errdefer self.allocator.free(payload);
                            const frame = try encodeServerFrameAlloc(self.allocator, .text, payload);
                            self.allocator.free(payload);
                            try self.queueFrame(frame);
                        },
                        .ping => {
                            const frame = try encodeServerFrameAlloc(self.allocator, .pong, message.data);
                            try self.queueFrame(frame);
                        },
                        .pong => {},
                        .close => {
                            self.destroy();
                            return true;
                        },
                    }
                    if (self.pending_frame != null) return true;
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

            return progressed;
        }
    };

    fn listen(allocator: Allocator, listen_port: u16) !RawBenchmarkServer {
        var address = try net.Address.parseIp("127.0.0.1", listen_port);
        const listener = try std.posix.socket(address.any.family, std.posix.SOCK.STREAM | std.posix.SOCK.CLOEXEC | std.posix.SOCK.NONBLOCK, std.posix.IPPROTO.TCP);
        errdefer std.posix.close(listener);

        try std.posix.setsockopt(listener, std.posix.IPPROTO.TCP, 1, &std.mem.toBytes(@as(c_int, 1)));
        if (@hasDecl(std.posix.SO, "REUSEPORT_LB")) {
            try std.posix.setsockopt(listener, std.posix.SOL.SOCKET, std.posix.SO.REUSEPORT_LB, &std.mem.toBytes(@as(c_int, 1)));
        } else if (@hasDecl(std.posix.SO, "REUSEPORT")) {
            try std.posix.setsockopt(listener, std.posix.SOL.SOCKET, std.posix.SO.REUSEPORT, &std.mem.toBytes(@as(c_int, 1)));
        } else {
            try std.posix.setsockopt(listener, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
        }

        try std.posix.bind(listener, &address.any, address.getOsSockLen());
        try std.posix.listen(listener, 1024);

        var actual: net.Address = undefined;
        var actual_len: std.posix.socklen_t = @sizeOf(net.Address);
        try std.posix.getsockname(listener, &actual.any, &actual_len);

        return .{
            .allocator = allocator,
            .listener = listener,
            .port_value = actual.getPort(),
            .handshake_pool = try websocket.server.Handshake.Pool.init(allocator, 32, 2048, 10, 2),
            .buffer_provider = try websocket.buffer.Provider.init(allocator, .{
                .size = 0,
                .count = 0,
                .max = 2 * 1024 * 1024,
            }),
        };
    }

    fn deinit(self: *RawBenchmarkServer) void {
        self.stop();
        self.connections.deinit(self.allocator);
        self.handshake_pool.deinit();
        self.buffer_provider.deinit();
    }

    fn stop(self: *RawBenchmarkServer) void {
        if (self.stopped) return;
        self.stopped = true;
        std.posix.close(self.listener);
        while (self.connections.items.len > 0) {
            self.connections.items[self.connections.items.len - 1].destroy();
        }
    }

    fn port(self: *const RawBenchmarkServer) u16 {
        return self.port_value;
    }

    fn poll(self: *RawBenchmarkServer) !bool {
        if (self.stopped) return false;

        var progressed = false;
        while (true) {
            var address: net.Address = undefined;
            var address_len: std.posix.socklen_t = @sizeOf(net.Address);
            const socket = std.posix.accept(self.listener, &address.any, &address_len, std.posix.SOCK.CLOEXEC | std.posix.SOCK.NONBLOCK) catch |err| switch (err) {
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

    fn removeConnection(self: *RawBenchmarkServer, target: *Connection) void {
        var index: usize = 0;
        while (index < self.connections.items.len) : (index += 1) {
            if (self.connections.items[index] == target) {
                _ = self.connections.swapRemove(index);
                return;
            }
        }
    }
};

fn runSingleThreadBurst(
    allocator: Allocator,
    host: []const u8,
    port: u16,
    total_requests: u64,
    max_in_flight: u32,
) !SendBurstTotals {
    if (total_requests == 0) return .{};

    var client = try RawBurstClient.connect(allocator, host, port);
    defer client.deinit();

    var totals = SendBurstTotals{};
    var next_id: u64 = 1;
    var in_flight: u64 = 0;
    const in_flight_limit = @max(@as(u64, 1), @as(u64, max_in_flight));

    while (totals.requests_sent < total_requests or in_flight > 0) {
        while (totals.requests_sent < total_requests and in_flight < in_flight_limit) {
            const remaining = total_requests - totals.requests_sent;
            const capacity = in_flight_limit - in_flight;
            const batch_count_u64 = @min(@as(u64, BurstBatchSize), @min(remaining, capacity));
            const batch_count: usize = @intCast(batch_count_u64);
            try client.sendTextCountBatch(next_id, batch_count);
            totals.requests_sent += batch_count_u64;
            next_id += batch_count_u64;
            in_flight += batch_count_u64;
        }

        const counts = try client.recvCounts();
        if (counts.completed == 0) continue;

        in_flight -= @min(in_flight, counts.completed);
        const errors = @min(counts.completed, counts.errors);
        totals.errors += errors;
        totals.responses_received += counts.completed - errors;
    }

    return totals;
}

fn buildTextCountBatchAlloc(allocator: Allocator, start_id: u64, count: usize) ![]u8 {
    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    try out.ensureTotalCapacity(allocator, count * 64);
    const writer = out.writer(allocator);

    if (count == 1) {
        try writeTextCountRequest(writer, start_id);
    } else {
        try writer.writeByte('[');
        var index: usize = 0;
        while (index < count) : (index += 1) {
            if (index != 0) try writer.writeByte(',');
            try writeTextCountRequest(writer, start_id + index);
        }
        try writer.writeByte(']');
    }

    return out.toOwnedSlice(allocator);
}

fn writeTextCountRequest(writer: anytype, id: u64) !void {
    try writer.print("{{\"id\":{d},\"path\":\"{s}\",\"body\":{{\"text\":\"{s}\"}}}}", .{
        id,
        RequestPath,
        RequestBody.text,
    });
}

fn countResponseFields(payload: []const u8) ResponseCounts {
    return .{
        .completed = countOccurrences(payload, "\"id\":"),
        .errors = countOccurrences(payload, "\"error\":"),
    };
}

fn processBenchmarkPayload(allocator: Allocator, payload: []const u8) ![]u8 {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const parsed = try std.json.parseFromSlice(std.json.Value, arena, payload, .{});

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(allocator);
    const writer = out.writer(allocator);

    switch (parsed.value) {
        .object => try writeBenchmarkResponseItem(writer, parsed.value),
        .array => |items| {
            try writer.writeByte('[');
            for (items.items, 0..) |item, index| {
                if (index != 0) try writer.writeByte(',');
                try writeBenchmarkResponseItem(writer, item);
            }
            try writer.writeByte(']');
        },
        else => return error.InvalidJsonMessage,
    }

    return out.toOwnedSlice(allocator);
}

fn writeBenchmarkResponseItem(writer: anytype, value: std.json.Value) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidJsonMessage,
    };

    const id = if (object.get("id")) |field| try parseJsonInt(field) else null;
    const path = if (object.get("path")) |field| switch (field) {
        .string => |text| text,
        else => return error.InvalidJsonMessage,
    } else null;

    _ = g_server_requests.fetchAdd(1, .monotonic);

    if (path == null or !std.mem.eql(u8, path.?, RequestPath)) {
        try writeBenchmarkError(writer, id, "api not found");
        return;
    }

    const body_len = if (object.get("body")) |field|
        switch (field) {
            .object => |body| if (body.get("text")) |text_field|
                switch (text_field) {
                    .string => |text| text.len,
                    else => @as(usize, 0),
                }
            else
                0,
            else => 0,
        }
    else
        0;

    try writeBenchmarkSuccess(writer, id, body_len);
}

fn parseJsonInt(value: std.json.Value) !i64 {
    return switch (value) {
        .integer => |number| std.math.cast(i64, number) orelse error.InvalidJsonMessage,
        .number_string => |text| std.fmt.parseInt(i64, text, 10),
        else => error.InvalidJsonMessage,
    };
}

fn writeBenchmarkSuccess(writer: anytype, id: ?i64, body_len: usize) !void {
    if (id) |request_id| {
        try writer.print("{{\"id\":{d},\"body\":{d}}}", .{ request_id, body_len });
    } else {
        try writer.print("{{\"body\":{d}}}", .{body_len});
    }
}

fn writeBenchmarkError(writer: anytype, id: ?i64, message: []const u8) !void {
    if (id) |request_id| {
        try writer.print("{{\"id\":{d},\"error\":{f}}}", .{ request_id, std.json.fmt(message, .{}) });
    } else {
        try writer.print("{{\"error\":{f}}}", .{std.json.fmt(message, .{})});
    }
}

fn countOccurrences(haystack: []const u8, needle: []const u8) u64 {
    var count: u64 = 0;
    var index: usize = 0;
    while (std.mem.indexOfPos(u8, haystack, index, needle)) |found| {
        count += 1;
        index = found + needle.len;
    }
    return count;
}

fn generateClientKey() [16]u8 {
    var key: [16]u8 = undefined;
    std.crypto.random.bytes(&key);
    return key;
}

/// Case-insensitive header-name search (HTTP header names are
/// case-insensitive; some servers send e.g. `sec-websocket-accept:`).
fn containsHeaderIgnoreCase(header: []const u8, name: []const u8) bool {
    var index: usize = 0;
    while (index + name.len <= header.len) : (index += 1) {
        if (std.ascii.eqlIgnoreCase(header[index .. index + name.len], name)) {
            return true;
        }
    }
    return false;
}

fn writeClientFrame(stream: *net.Stream, op_code: websocket.OpCode, data: []const u8) !void {
    var header_buf: [10]u8 = undefined;
    const header = websocket.proto.writeFrameHeader(&header_buf, op_code, data.len, false);
    header_buf[1] |= 0x80;
    try stream.writeAll(header);

    const mask = generateClientKey()[0..4].*;
    try stream.writeAll(&mask);

    var offset: usize = 0;
    var chunk_buf: [4096]u8 = undefined;
    while (offset < data.len) {
        const chunk_len = @min(chunk_buf.len, data.len - offset);
        for (data[offset .. offset + chunk_len], 0..) |byte, index| {
            chunk_buf[index] = byte ^ mask[(offset + index) & 3];
        }
        try stream.writeAll(chunk_buf[0..chunk_len]);
        offset += chunk_len;
    }
}

fn encodeServerFrameAlloc(allocator: Allocator, op_code: websocket.OpCode, data: []const u8) ![]u8 {
    var header_buf: [10]u8 = undefined;
    const header = websocket.proto.writeFrameHeader(&header_buf, op_code, data.len, false);
    const out = try allocator.alloc(u8, header.len + data.len);
    @memcpy(out[0..header.len], header);
    @memcpy(out[header.len..], data);
    return out;
}

fn writeSocketAllNonBlocking(socket: std.posix.socket_t, data: []const u8) !void {
    var slice = data;
    while (slice.len > 0) {
        const written = std.posix.write(socket, slice) catch |err| switch (err) {
            error.WouldBlock => {
                std.Thread.sleep(100_000);
                continue;
            },
            else => return err,
        };
        slice = slice[written..];
    }
}

fn emitJson(message: anytype) !void {
    const allocator = std.heap.page_allocator;
    const payload = try std.json.Stringify.valueAlloc(allocator, message, .{});
    defer allocator.free(payload);
    try std.fs.File.stdout().writeAll(payload);
    try std.fs.File.stdout().writeAll("\n");
}

fn readStringEnvOrDefault(allocator: Allocator, name: []const u8, default_value: []const u8) ![]u8 {
    return std.process.getEnvVarOwned(allocator, name) catch allocator.dupe(u8, default_value);
}

fn readPositiveIntEnv(name: []const u8) !u32 {
    const allocator = std.heap.page_allocator;
    const raw = try std.process.getEnvVarOwned(allocator, name);
    defer allocator.free(raw);
    const value = try std.fmt.parseInt(i64, raw, 10);
    if (value <= 0) return error.InvalidEnvironment;
    return @intCast(value);
}

fn readNonNegativeIntEnv(name: []const u8) !u64 {
    const allocator = std.heap.page_allocator;
    const raw = try std.process.getEnvVarOwned(allocator, name);
    defer allocator.free(raw);
    const value = try std.fmt.parseInt(i64, raw, 10);
    if (value < 0) return error.InvalidEnvironment;
    return @intCast(value);
}

fn readPositiveIntEnvOrDefault(name: []const u8, default_value: u32) !u32 {
    const allocator = std.heap.page_allocator;
    const raw = std.process.getEnvVarOwned(allocator, name) catch return default_value;
    defer allocator.free(raw);
    const value = try std.fmt.parseInt(i64, raw, 10);
    if (value <= 0) return error.InvalidEnvironment;
    return @intCast(value);
}

fn readRequestMode() !RequestMode {
    const allocator = std.heap.page_allocator;
    const raw = std.process.getEnvVarOwned(allocator, "BENCH_REQUEST_MODE") catch return .burst;
    defer allocator.free(raw);
    if (std.mem.eql(u8, raw, "burst")) return .burst;
    if (std.mem.eql(u8, raw, "stream")) return .stream;
    return error.InvalidEnvironment;
}

fn nanosToSeconds(nanos: u64) f64 {
    return @as(f64, @floatFromInt(nanos)) / @as(f64, std.time.ns_per_s);
}

fn installTerminationSignalHandlers() void {
    const action = std.posix.Sigaction{
        .handler = .{ .handler = signalHandler },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.TERM, &action, null);
    std.posix.sigaction(std.posix.SIG.INT, &action, null);
}

fn signalHandler(sig: c_int) callconv(.c) void {
    _ = sig;
    g_stop_requested.store(true, .release);
}
