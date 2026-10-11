<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;channel
</h1>

Powerful and lightweight communication built for modern language. Zig implementation of Channel: run a websocket server, a websocket client, or communicate inside your process. Pure Zig, no native C libraries required.

# Installation

Dependency in `build.zig.zon`:

```zig
.{
    .name = .channel_zig,
    .version = "0.0.1",
}
```

# Dependencies

[websocket.zig](https://github.com/karlseguin/websocket.zig) for the WebSocket client and server (pinned in
`build.zig.zon`). JSON is `std.json`.

# Usage

```zig
const std = @import("std");
const channel = @import("channel");

const AppState = struct {};

fn hello(_: channel.PostRequest(AppState, void, void)) !?[]const u8 {
    return "world";
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    var ch = channel.Channel(AppState).init(allocator);
    defer ch.deinit();

    _ = try ch.post("hello", hello);

    var process = try ch.process(allocator, .{});
    defer process.deinit();

    var result = try process.send(allocator, []const u8, "hello", {});
    defer result.deinit(allocator);
    std.debug.print("{s}\n", .{result.ok.?});
}
```

# Public API

- `Channel(State)`
- `Sender(State)`
- `Client(State)`
- `Server(State)`
- `Process(State)`
- `Subscription(Req, Event)`
- `Values(Item)`
- `ConnectOptions(State)`
- `ListenOptions(State)`
- `CallResult(T)`
- `StreamNext(T)`
- `SubscribeResult`
- `post(path, handler)`
- `postTyped(Req, Resp, Ctx, path, handler)`
- `postSpec(...)`
- `streamSpec(...)`

# Features

- typed `Channel(State)` registration
- blocking `Sender.send`, `Sender.values`, and `Sender.subscribe`
- in-memory `process` transport
- websocket `connect` and `listen` transports
- compile-time `api` and `events` builders

Built on:
- [`websocket.zig`](https://github.com/karlseguin/websocket.zig) for websocket transport
- `std.json` for all wire encoding and decoding

# Requirements

- Zig `0.15.1+`
- `bun` for the TS fixture integration test in `scripts/test`

# Scripts

- `./scripts/test`
  Runs unit tests plus the TS fixture interoperability test.
- `./scripts/benchmark`
  Runs the pure Zig benchmark executable.
- `./scripts/coverage`
  Coverage is not ported yet; this currently runs the test suite as a fallback.

# Build

```bash
zig build test-unit
zig build test-integration
zig build benchmark -Doptimize=ReleaseFast
```
