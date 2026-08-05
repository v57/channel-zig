const std = @import("std");

pub const Allocator = std.mem.Allocator;

pub const ParsedMessage = struct {
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

    pub fn deinit(self: *ParsedMessage, allocator: Allocator) void {
        freeOptional(allocator, &self.path);
        freeOptional(allocator, &self.stream);
        freeOptional(allocator, &self.sub);
        freeOptional(allocator, &self.unsub);
        freeOptional(allocator, &self.topic);
        freeOptional(allocator, &self.body_json);
        freeOptional(allocator, &self.context_json);
        freeOptional(allocator, &self.error_text);
        self.* = .{};
    }
};

pub const OutgoingMessage = struct {
    id: ?i64 = null,
    path: ?[]const u8 = null,
    stream: ?[]const u8 = null,
    sub: ?[]const u8 = null,
    unsub: ?[]const u8 = null,
    cancel: ?i64 = null,
    topic: ?[]const u8 = null,
    body_json: ?[]const u8 = null,
    context_json: ?[]const u8 = null,
    error_text: ?[]const u8 = null,
    done: ?bool = null,
};

pub fn parseMessagesAlloc(allocator: Allocator, raw: []const u8) ![]ParsedMessage {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw, .{});
    defer parsed.deinit();

    return switch (parsed.value) {
        .object => blk: {
            const out = try allocator.alloc(ParsedMessage, 1);
            errdefer allocator.free(out);
            out[0] = try parseOneAlloc(allocator, parsed.value);
            break :blk out;
        },
        .array => |items| blk: {
            const out = try allocator.alloc(ParsedMessage, items.items.len);
            errdefer allocator.free(out);
            var built: usize = 0;
            errdefer {
                for (out[0..built]) |*message| {
                    message.deinit(allocator);
                }
            }
            for (items.items, 0..) |value, index| {
                out[index] = try parseOneAlloc(allocator, value);
                built += 1;
            }
            break :blk out;
        },
        else => error.InvalidJsonMessage,
    };
}

fn parseOneAlloc(allocator: Allocator, value: std.json.Value) !ParsedMessage {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidJsonMessage,
    };

    var message = ParsedMessage{};
    errdefer message.deinit(allocator);

    if (object.get("id")) |field| {
        message.id = try parseIntField(field);
    }
    if (object.get("path")) |field| {
        message.path = try dupeStringField(allocator, field);
    }
    if (object.get("stream")) |field| {
        message.stream = try dupeStringField(allocator, field);
    }
    if (object.get("sub")) |field| {
        message.sub = try dupeStringField(allocator, field);
    }
    if (object.get("unsub")) |field| {
        message.unsub = try dupeStringField(allocator, field);
    }
    if (object.get("cancel")) |field| {
        message.cancel = try parseIntField(field);
    }
    if (object.get("topic")) |field| {
        message.topic = try dupeStringField(allocator, field);
    }
    if (object.get("body")) |field| {
        message.body_json = try stringifyValueAlloc(allocator, field);
    }
    if (object.get("context")) |field| {
        message.context_json = try stringifyValueAlloc(allocator, field);
    }
    if (object.get("error")) |field| {
        message.error_text = try dupeStringField(allocator, field);
    }
    if (object.get("done")) |field| {
        message.done = switch (field) {
            .bool => |flag| flag,
            else => return error.InvalidJsonMessage,
        };
    }

    return message;
}

pub fn encodeMessageAlloc(allocator: Allocator, message: OutgoingMessage) ![]u8 {
    return encodeMessagesAlloc(allocator, &.{message});
}

pub fn encodeMessagesAlloc(allocator: Allocator, messages: []const OutgoingMessage) ![]u8 {
    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    const writer = out.writer(allocator);

    if (messages.len == 1) {
        try writeObject(writer, messages[0]);
    } else {
        try writer.writeByte('[');
        for (messages, 0..) |message, index| {
            if (index != 0) {
                try writer.writeByte(',');
            }
            try writeObject(writer, message);
        }
        try writer.writeByte(']');
    }

    return out.toOwnedSlice(allocator);
}

fn writeObject(writer: anytype, message: OutgoingMessage) !void {
    try writer.writeByte('{');
    var field_count: usize = 0;

    try writeIntField(writer, &field_count, "id", message.id);
    try writeStringField(writer, &field_count, "path", message.path);
    try writeStringField(writer, &field_count, "stream", message.stream);
    try writeStringField(writer, &field_count, "sub", message.sub);
    try writeStringField(writer, &field_count, "unsub", message.unsub);
    try writeIntField(writer, &field_count, "cancel", message.cancel);
    try writeStringField(writer, &field_count, "topic", message.topic);
    try writeRawJsonField(writer, &field_count, "body", message.body_json);
    try writeRawJsonField(writer, &field_count, "context", message.context_json);
    try writeStringField(writer, &field_count, "error", message.error_text);
    try writeBoolField(writer, &field_count, "done", message.done);

    try writer.writeByte('}');
}

fn writeFieldPrefix(writer: anytype, field_count: *usize, name: []const u8) !void {
    if (field_count.* != 0) {
        try writer.writeByte(',');
    }
    field_count.* += 1;
    try writer.print("{f}", .{std.json.fmt(name, .{})});
    try writer.writeByte(':');
}

fn writeStringField(writer: anytype, field_count: *usize, name: []const u8, value: ?[]const u8) !void {
    const string = value orelse return;
    try writeFieldPrefix(writer, field_count, name);
    try writer.print("{f}", .{std.json.fmt(string, .{})});
}

fn writeIntField(writer: anytype, field_count: *usize, name: []const u8, value: ?i64) !void {
    const number = value orelse return;
    try writeFieldPrefix(writer, field_count, name);
    try writer.print("{d}", .{number});
}

fn writeBoolField(writer: anytype, field_count: *usize, name: []const u8, value: ?bool) !void {
    const flag = value orelse return;
    try writeFieldPrefix(writer, field_count, name);
    try writer.writeAll(if (flag) "true" else "false");
}

fn writeRawJsonField(writer: anytype, field_count: *usize, name: []const u8, value: ?[]const u8) !void {
    const json = value orelse return;
    try writeFieldPrefix(writer, field_count, name);
    try writer.writeAll(json);
}

fn stringifyValueAlloc(allocator: Allocator, value: std.json.Value) ![]u8 {
    return jsonStringifyAlloc(allocator, value);
}

fn dupeStringField(allocator: Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .string => |string| allocator.dupe(u8, string),
        else => error.InvalidJsonMessage,
    };
}

fn parseIntField(value: std.json.Value) !i64 {
    return switch (value) {
        .integer => |number| std.math.cast(i64, number) orelse error.InvalidJsonMessage,
        .number_string => |text| std.fmt.parseInt(i64, text, 10),
        else => error.InvalidJsonMessage,
    };
}

fn freeOptional(allocator: Allocator, slot: *?[]u8) void {
    if (slot.*) |value| {
        allocator.free(value);
        slot.* = null;
    }
}

fn jsonStringifyAlloc(allocator: Allocator, value: anytype) ![]u8 {
    return try std.json.Stringify.valueAlloc(allocator, value, .{});
}
