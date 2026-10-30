<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;channel
</h1>

Powerful and lightweight communication built for modern language. Zig implementation of Channel: run a websocket server, a websocket client, or communicate inside your process. Pure Zig, no native C libraries required (only the opt-in QUIC transport links OpenSSL).

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
- `QuicOptions`, `QuicListenOptions(State)`, `listenQuic(...)` (with `-Dquic=true`)
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
- QUIC `connect("quic://…")` and `listenQuic` (experimental, opt-in, see below)
- compile-time `api` and `events` builders

Built on:
- [`websocket.zig`](https://github.com/karlseguin/websocket.zig) for websocket transport
- `std.json` for all wire encoding and decoding

# QUIC transport (experimental)

The same messages over QUIC (spec: `channel/docs/quic.md`): QUIC v1, ALPN `channel/1`, one bidirectional
stream per connection. It is opt-in because it links the system OpenSSL (3.5 or newer, its native QUIC API);
the default build stays pure Zig.

Enable it on the dependency:

```zig
const channel_dep = b.dependency("channel_zig", .{ .target = target, .optimize = optimize, .quic = true });
exe.root_module.addImport("channel", channel_dep.module("channel"));
```

It needs OpenSSL 3.5+ headers and libraries where Zig finds system libraries (`libssl-dev` on Debian 13; elsewhere
point Zig at it, e.g. `zig build --search-prefix "$(brew --prefix openssl@3)"`).
Without `-Dquic=true`, `connect("quic://…")` returns `error.QuicNotEnabled` and `listenQuic` doesn't compile.

```zig
// Server: same handlers, state, streams and events as listen(); the hello headers (names lowercased)
// go to `state` like the WebSocket upgrade headers
var server = try ch.listenQuic(allocator, "0.0.0.0:4433", .{ .cert = cert_pem, .key = key_pem, .state = makeState });
defer server.deinit();
while (true) _ = try server.poll();

// Client
var client = try ch.connect(allocator, "quic://example.com:4433", .{
    .headers = "Authorization: Bearer token\r\n",
    .quic = .{ .ca = cert_pem },
});
defer client.deinit();
```

Certificates: the server always needs a PEM key and certificate chain. The client verifies the server against
the system roots unless `.quic` says otherwise:
- `.ca`: PEM certificate(s) to trust instead, for self-signed servers. The address (DNS name or IP) must be in
  the certificate's subject alternative names.
- `.fingerprint`: the SHA-256 fingerprint of the server certificate (hex, colons optional), pinned instead of
  verifying the chain and name (`openssl x509 -in cert.pem -noout -fingerprint -sha256`).
- `.server_name`: the name sent (SNI) and checked, instead of the address host.

A failed check fails `connect` with `error.CertificateVerifyFailed` or `error.CertificateFingerprintMismatch`.

Every client connection dials from its own UDP socket, never a shared one: Network.framework's QUIC servers
(Swift's HubServer) take one connection per client address and port. The client sends a keepalive every 15 s
(the idle timeout is 30 s) while it's polled. Like this port's WebSocket client, it doesn't reconnect: a lost
connection fails the pending calls and stops the client.

Which languages support QUIC: see `channel/docs/quic.md`.

# Requirements

- Zig `0.15.1+`
- `bun` for the TS fixture integration test in `scripts/test`
- OpenSSL 3.5+ for the QUIC transport and its tests (`-Dquic=true`)

# Scripts

- `./scripts/test`
  Runs unit tests plus the TS fixture interoperability test, then the QUIC tests (`-Dquic=true`): frames,
  wire rules, certificates, and interop with the TS reference's QUIC server and client.
- `./scripts/benchmark`
  Runs the pure Zig benchmark executable.
- `./scripts/coverage`
  Coverage is not ported yet; this currently runs the test suite as a fallback.

# Build

```bash
zig build test-unit
zig build test-integration
zig build test-quic -Dquic=true
zig build benchmark -Doptimize=ReleaseFast
```
