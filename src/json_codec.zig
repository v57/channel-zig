const std = @import("std");

pub const Allocator = std.mem.Allocator;

pub fn encodeOptionalAlloc(allocator: Allocator, comptime T: type, value: T) !?[]u8 {
    if (T == void) {
        return null;
    }
    return try stringifyAlloc(allocator, value);
}

pub fn decodeRequired(allocator: Allocator, comptime T: type, json: ?[]const u8) !T {
    if (T == void) {
        return {};
    }

    const raw = json orelse return error.MissingJsonField;
    if (comptime isOptionalType(T)) {
        if (std.mem.eql(u8, raw, "null")) {
            return @as(T, null);
        }
    }

    return try std.json.parseFromSliceLeaky(T, allocator, raw, .{
        .allocate = .alloc_always,
    });
}

/// Decodes a request context. TS hands handlers `context ?? {}`, so a missing
/// context is tried as `{}` first and then as `null` (channel-rs parity);
/// handlers that read the context keep working for callers that send none.
pub fn decodeContext(allocator: Allocator, comptime T: type, json: ?[]const u8) !T {
    if (json) |raw| return decodeRequired(allocator, T, raw);
    return decodeRequired(allocator, T, "{}") catch decodeRequired(allocator, T, "null");
}

pub fn decodeOptional(allocator: Allocator, comptime T: type, json: ?[]const u8) !?T {
    if (json == null) {
        return null;
    }
    return try decodeRequired(allocator, T, json);
}

pub fn isOptionalType(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .optional => true,
        else => false,
    };
}

pub fn freeOwnedValue(allocator: Allocator, comptime T: type, value: *T) void {
    if (T == std.json.Value) return freeJsonValue(allocator, value.*);
    switch (@typeInfo(T)) {
        .void,
        .bool,
        .int,
        .float,
        .comptime_int,
        .comptime_float,
        .@"enum",
        .@"fn",
        .@"opaque",
        .noreturn,
        .null,
        .undefined,
        .type,
        .@"anyframe",
        .enum_literal,
        .frame,
        .vector,
        => {},
        .optional => |optional_info| {
            if (value.*) |*some| {
                freeOwnedValue(allocator, optional_info.child, some);
            }
        },
        .array => |array_info| {
            for (value.*) |*item| {
                freeOwnedValue(allocator, array_info.child, item);
            }
        },
        .@"struct" => |struct_info| {
            inline for (struct_info.fields) |field| {
                freeOwnedValue(allocator, field.type, &@field(value.*, field.name));
            }
        },
        .pointer => |pointer_info| {
            if (pointer_info.size == .slice) {
                if (pointer_info.child == u8) {
                    allocator.free(value.*);
                    return;
                }
                for (value.*) |*item| {
                    freeOwnedValue(allocator, pointer_info.child, item);
                }
                allocator.free(@constCast(value.*));
                return;
            }
            if (pointer_info.size == .one) {
                freeOwnedValue(allocator, pointer_info.child, value.*);
                allocator.destroy(value.*);
            }
        },
        .@"union" => {},
        else => {},
    }
}

/// Frees a std.json.Value decoded with `.allocate = .alloc_always`: strings,
/// object keys and containers are all owned.
fn freeJsonValue(allocator: Allocator, value: std.json.Value) void {
    switch (value) {
        .string => |text| allocator.free(text),
        .number_string => |text| allocator.free(text),
        .array => |array| {
            for (array.items) |item| freeJsonValue(allocator, item);
            array.deinit();
        },
        .object => |object| {
            var map = object;
            var it = map.iterator();
            while (it.next()) |entry| {
                allocator.free(entry.key_ptr.*);
                freeJsonValue(allocator, entry.value_ptr.*);
            }
            map.deinit();
        },
        else => {},
    }
}

pub fn stringifyAlloc(allocator: Allocator, value: anytype) ![]u8 {
    return try std.json.Stringify.valueAlloc(allocator, value, .{});
}
