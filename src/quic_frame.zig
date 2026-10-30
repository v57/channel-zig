//! Framing of the QUIC transport (channel/docs/quic.md). Pure Zig, built with or without `-Dquic`.
//!
//! Each message on the connection's one bidirectional stream is its length as a QUIC variable-length
//! integer (RFC 9000 §16) followed by that many bytes of UTF-8 JSON: exactly the text a WebSocket message
//! would carry. A zero-length frame is a keepalive and is skipped.
const std = @import("std");
const Allocator = std.mem.Allocator;

pub const alpn = "channel/1";
/// Largest accepted frame, same as the WebSocket limit (Bun's default payload limit).
pub const max_frame: usize = 16 * 1024 * 1024;
/// Both sides set the QUIC idle timeout to 30 s; the client sends a keepalive every 15 s.
pub const idle_timeout_ms: u64 = 30_000;
pub const keepalive_interval_ms: i64 = 15_000;
/// A zero-length frame.
pub const keepalive = [_]u8{0};

/// Writes the varint length prefix of a `len`-byte payload (1, 2, 4 or 8 bytes) into `buf`.
pub fn writeHeader(buf: *[8]u8, len: usize) []const u8 {
    const n: u64 = @intCast(len);
    if (n < 64) {
        buf[0] = @intCast(n);
        return buf[0..1];
    }
    if (n < 16384) {
        std.mem.writeInt(u16, buf[0..2], @as(u16, @intCast(n)) | 0x4000, .big);
        return buf[0..2];
    }
    if (n < (1 << 30)) {
        std.mem.writeInt(u32, buf[0..4], @as(u32, @intCast(n)) | 0x8000_0000, .big);
        return buf[0..4];
    }
    std.mem.writeInt(u64, buf[0..8], n | 0xc000_0000_0000_0000, .big);
    return buf[0..8];
}

/// Appends one frame (length prefix and payload) to `list`.
pub fn appendFrame(list: *std.ArrayListUnmanaged(u8), allocator: Allocator, payload: []const u8) !void {
    var header_buf: [8]u8 = undefined;
    const header = writeHeader(&header_buf, payload.len);
    try list.ensureUnusedCapacity(allocator, header.len + payload.len);
    list.appendSliceAssumeCapacity(header);
    list.appendSliceAssumeCapacity(payload);
}

/// Splits the bytes read from the stream back into frames.
pub const FrameReader = struct {
    buffer: std.ArrayListUnmanaged(u8) = .{},
    start: usize = 0,
    max: usize = max_frame,

    pub fn deinit(self: *FrameReader, allocator: Allocator) void {
        self.buffer.deinit(allocator);
        self.* = undefined;
    }

    /// Space to read at least `min` more bytes into; call `commit` with the count read.
    /// Invalidates the slices `next` returned before.
    pub fn writable(self: *FrameReader, allocator: Allocator, min: usize) ![]u8 {
        if (self.start > 0 and (self.start == self.buffer.items.len or self.start >= 64 * 1024)) {
            const left = self.buffer.items.len - self.start;
            std.mem.copyForwards(u8, self.buffer.items[0..left], self.buffer.items[self.start..]);
            self.buffer.items.len = left;
            self.start = 0;
        }
        try self.buffer.ensureUnusedCapacity(allocator, min);
        return self.buffer.unusedCapacitySlice();
    }

    pub fn commit(self: *FrameReader, count: usize) void {
        self.buffer.items.len += count;
    }

    pub fn push(self: *FrameReader, allocator: Allocator, bytes: []const u8) !void {
        const space = try self.writable(allocator, bytes.len);
        @memcpy(space[0..bytes.len], bytes);
        self.commit(bytes.len);
    }

    /// The next complete non-empty frame, null when more bytes are needed. Keepalives are skipped.
    /// The slice stays valid until the next `writable`/`push`.
    pub fn next(self: *FrameReader) error{FrameTooLarge}!?[]const u8 {
        while (true) {
            const data = self.buffer.items[self.start..];
            if (data.len == 0) return null;
            const size = @as(usize, 1) << @intCast(data[0] >> 6);
            if (data.len < size) return null;
            var length: u64 = data[0] & 0x3f;
            for (data[1..size]) |byte| length = (length << 8) | byte;
            if (length > self.max) return error.FrameTooLarge;
            const total = size + @as(usize, @intCast(length));
            if (data.len < total) return null;
            self.start += total;
            if (length == 0) continue;
            return data[size..total];
        }
    }
};

/// `{"hello":{...}}` for a header block in `ConnectOptions.headers` form ("Name: value\r\n" lines).
pub fn helloAlloc(allocator: Allocator, headers: ?[]const u8) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    var json: std.json.Stringify = .{ .writer = &out.writer };
    try json.beginObject();
    try json.objectField("hello");
    try json.beginObject();
    if (headers) |block| {
        var lines = std.mem.splitScalar(u8, block, '\n');
        while (lines.next()) |raw| {
            const line = std.mem.trim(u8, raw, " \t\r");
            const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
            const name = std.mem.trim(u8, line[0..colon], " \t");
            if (name.len == 0) continue;
            try json.objectField(name);
            try json.write(std.mem.trim(u8, line[colon + 1 ..], " \t"));
        }
    }
    try json.endObject();
    try json.endObject();
    return out.toOwnedSlice();
}

/// Headers of a hello frame: names lowercased, string values only. Keys and values are owned.
pub const Hello = struct {
    headers: std.StringHashMapUnmanaged([]const u8) = .{},

    pub fn deinit(self: *Hello, allocator: Allocator) void {
        var it = self.headers.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            allocator.free(entry.value_ptr.*);
        }
        self.headers.deinit(allocator);
    }
};

/// Parses the first client frame. Anything but `{"hello": {...}}` is `error.NotHello`.
pub fn parseHello(allocator: Allocator, payload: []const u8) !Hello {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, payload, .{}) catch return error.NotHello;
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.NotHello,
    };
    const fields = switch (root.get("hello") orelse return error.NotHello) {
        .object => |object| object,
        else => return error.NotHello,
    };
    var hello: Hello = .{};
    errdefer hello.deinit(allocator);
    var it = fields.iterator();
    while (it.next()) |entry| {
        const value = switch (entry.value_ptr.*) {
            .string => |text| text,
            else => continue,
        };
        const name = try std.ascii.allocLowerString(allocator, entry.key_ptr.*);
        errdefer allocator.free(name);
        const owned = try allocator.dupe(u8, value);
        errdefer allocator.free(owned);
        if (try hello.headers.fetchPut(allocator, name, owned)) |old| {
            allocator.free(old.key);
            allocator.free(old.value);
        }
    }
    return hello;
}

/// A SHA-256 fingerprint in hex, colons optional ("DD:D2:37:…" or "ddd237…").
pub fn parseFingerprint(text: []const u8) error{InvalidFingerprint}![32]u8 {
    var out: [32]u8 = undefined;
    var count: usize = 0;
    var high: ?u4 = null;
    for (text) |char| {
        if (char == ':') {
            if (high != null) return error.InvalidFingerprint;
            continue;
        }
        const nibble: u4 = @intCast(std.fmt.charToDigit(char, 16) catch return error.InvalidFingerprint);
        if (high) |h| {
            if (count == out.len) return error.InvalidFingerprint;
            out[count] = (@as(u8, h) << 4) | nibble;
            count += 1;
            high = null;
        } else {
            high = nibble;
        }
    }
    if (high != null or count != out.len) return error.InvalidFingerprint;
    return out;
}

pub const Target = struct {
    /// Host as written, without IPv6 brackets.
    host: []u8,
    port: u16,

    pub fn deinit(self: Target, allocator: Allocator) void {
        allocator.free(self.host);
    }
};

/// `quic://host:port` (IPv6 hosts in brackets). The port is required.
pub fn parseTarget(allocator: Allocator, address: []const u8) !Target {
    const prefix = "quic://";
    if (!std.mem.startsWith(u8, address, prefix)) return error.InvalidAddress;
    const uri = std.Uri.parse(address) catch return error.InvalidAddress;
    var host_buffer: [std.Uri.host_name_max]u8 = undefined;
    const host = uri.getHost(&host_buffer) catch return error.InvalidAddress;
    const trimmed = if (host.len >= 2 and host[0] == '[' and host[host.len - 1] == ']') host[1 .. host.len - 1] else host;
    if (trimmed.len == 0) return error.InvalidAddress;
    const port = uri.port orelse return error.InvalidAddress;
    return .{ .host = try allocator.dupe(u8, trimmed), .port = port };
}
