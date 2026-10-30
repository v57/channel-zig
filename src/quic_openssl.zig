//! QUIC over the system OpenSSL (3.5+ native QUIC API), used when channel is built with `-Dquic=true`.
//!
//! Everything runs in OpenSSL's nonblocking, single-threaded mode: the channel transports call `tick` from
//! their `poll`, the same pump that drives the WebSocket sockets.
const std = @import("std");
// Same values as quic_frame.zig (a separate module, so not imported here)
const frame = struct {
    const alpn = "channel/1";
    const idle_timeout_ms: u64 = 30_000;
};
const posix = std.posix;

pub const enabled = true;

pub const c = @cImport({
    @cInclude("openssl/ssl.h");
    @cInclude("openssl/err.h");
    @cInclude("openssl/pem.h");
    @cInclude("openssl/x509.h");
    @cInclude("openssl/x509v3.h");
    @cInclude("openssl/evp.h");
    @cInclude("openssl/opensslv.h");
});

comptime {
    if (c.OPENSSL_VERSION_NUMBER < 0x30500000) @compileError("channel's QUIC transport needs OpenSSL 3.5 or newer");
}

pub const Ssl = c.SSL;

const alpn_wire = [_]u8{frame.alpn.len} ++ frame.alpn.*;
// Defined as macros in ssl.h (SSL_set_mode, SSL_set_tlsext_host_name, BIO_ctrl_set_connected, ...).
const ctrl_mode: c_int = 33;
const mode_partial_write: c_long = 0x1;
const ctrl_set_tlsext_hostname: c_int = 55;
const nametype_host_name: c_long = 0;
const ctrl_dgram_set_connected: c_int = 32;
const ctrl_chain_cert: c_int = 89;

pub const ReadError = error{ WouldBlock, EndOfStream, ConnectionClosed };
pub const WriteError = error{ConnectionClosed};

/// Reads what the stream has, `error.WouldBlock` when nothing yet.
pub fn read(ssl: *Ssl, buf: []u8) ReadError!usize {
    c.ERR_clear_error();
    var n: usize = 0;
    if (c.SSL_read_ex(ssl, buf.ptr, buf.len, &n) == 1) return n;
    return switch (c.SSL_get_error(ssl, 0)) {
        c.SSL_ERROR_WANT_READ, c.SSL_ERROR_WANT_WRITE => error.WouldBlock,
        c.SSL_ERROR_ZERO_RETURN => error.EndOfStream,
        else => error.ConnectionClosed,
    };
}

/// Writes as much as the stream takes now (0 under flow control).
pub fn write(ssl: *Ssl, data: []const u8) WriteError!usize {
    if (data.len == 0) return 0;
    c.ERR_clear_error();
    var n: usize = 0;
    if (c.SSL_write_ex(ssl, data.ptr, data.len, &n) == 1) return n;
    return switch (c.SSL_get_error(ssl, 0)) {
        c.SSL_ERROR_WANT_READ, c.SSL_ERROR_WANT_WRITE => 0,
        else => error.ConnectionClosed,
    };
}

fn setPartialWrite(ssl: *Ssl) void {
    _ = c.SSL_ctrl(ssl, ctrl_mode, mode_partial_write, null);
}

/// Whether the connection has terminated (closed by either side, idle timeout, error).
pub fn connTerminated(conn: *Ssl) bool {
    var info: c.SSL_CONN_CLOSE_INFO = undefined;
    return c.SSL_get_conn_close_info(conn, &info, @sizeOf(c.SSL_CONN_CLOSE_INFO)) == 1;
}

/// Starts an immediate close (one CONNECTION_CLOSE packet) and lets OpenSSL send it.
fn shutdownRapid(conn: *Ssl) bool {
    c.ERR_clear_error();
    const done = c.SSL_shutdown_ex(conn, c.SSL_SHUTDOWN_FLAG_RAPID, null, 0) == 1;
    _ = c.SSL_handle_events(conn);
    return done;
}

fn bioFrom(bytes: []const u8) !*c.BIO {
    return c.BIO_new_mem_buf(bytes.ptr, @intCast(bytes.len)) orelse error.OutOfMemory;
}

fn addressBytes(address: *const std.net.Address) struct { family: c_int, bytes: []const u8, port: u16 } {
    return switch (address.any.family) {
        posix.AF.INET => .{ .family = posix.AF.INET, .bytes = std.mem.asBytes(&address.in.sa.addr), .port = address.in.sa.port },
        else => .{ .family = posix.AF.INET6, .bytes = &address.in6.sa.addr, .port = address.in6.sa.port },
    };
}

fn newBioAddr(address: *const std.net.Address) !*c.BIO_ADDR {
    const raw = addressBytes(address);
    const addr = c.BIO_ADDR_new() orelse return error.OutOfMemory;
    if (c.BIO_ADDR_rawmake(addr, raw.family, raw.bytes.ptr, raw.bytes.len, raw.port) != 1) {
        c.BIO_ADDR_free(addr);
        return error.InvalidAddress;
    }
    return addr;
}

pub const TlsOptions = struct {
    /// PEM certificates to trust instead of the system roots.
    ca: ?[]const u8 = null,
    /// SHA-256 of the server certificate: accepted instead of verifying the chain and name.
    fingerprint: ?[32]u8 = null,
    /// Name sent (SNI) and checked; defaults to the host.
    server_name: ?[]const u8 = null,
};

/// One client connection on its own UDP socket. Messages go over its default bidirectional stream.
pub const ClientConn = struct {
    ctx: *c.SSL_CTX,
    ssl: *Ssl,
    fd: posix.socket_t,
    fingerprint: ?[32]u8,
    closed: bool = false,

    pub fn open(allocator: std.mem.Allocator, host: []const u8, address: std.net.Address, tls: TlsOptions) !ClientConn {
        const fd = try posix.socket(address.any.family, posix.SOCK.DGRAM | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC, posix.IPPROTO.UDP);
        errdefer posix.close(fd);
        try posix.connect(fd, &address.any, address.getOsSockLen());

        const ctx = c.SSL_CTX_new(c.OSSL_QUIC_client_method()) orelse return error.TlsSetupFailed;
        errdefer c.SSL_CTX_free(ctx);
        if (tls.fingerprint != null) {
            // The pin replaces the chain and name checks; compared after the handshake
            c.SSL_CTX_set_verify(ctx, c.SSL_VERIFY_NONE, null);
        } else {
            c.SSL_CTX_set_verify(ctx, c.SSL_VERIFY_PEER, null);
            if (tls.ca) |pem| {
                try addTrusted(ctx, pem);
            } else if (c.SSL_CTX_set_default_verify_paths(ctx) != 1) {
                return error.TlsSetupFailed;
            }
        }

        const ssl = c.SSL_new(ctx) orelse return error.TlsSetupFailed;
        errdefer c.SSL_free(ssl);

        const peer = try newBioAddr(&address);
        defer c.BIO_ADDR_free(peer);
        const bio = c.BIO_new_dgram(fd, c.BIO_NOCLOSE) orelse return error.TlsSetupFailed;
        _ = c.BIO_ctrl(bio, ctrl_dgram_set_connected, 0, peer);
        c.SSL_set_bio(ssl, bio, bio);
        if (c.SSL_set1_initial_peer_addr(ssl, peer) != 1) return error.TlsSetupFailed;

        if (c.SSL_set_alpn_protos(ssl, &alpn_wire, alpn_wire.len) != 0) return error.TlsSetupFailed;
        const name = tls.server_name orelse host;
        const name_z = try allocator.dupeZ(u8, name);
        defer allocator.free(name_z);
        const is_ip = if (std.net.Address.parseIp(name, 0)) |_| true else |_| false;
        if (!is_ip) _ = c.SSL_ctrl(ssl, ctrl_set_tlsext_hostname, nametype_host_name, name_z.ptr);
        if (tls.fingerprint == null) {
            const ok = if (is_ip)
                c.X509_VERIFY_PARAM_set1_ip_asc(c.SSL_get0_param(ssl), name_z.ptr)
            else
                c.SSL_set1_host(ssl, name_z.ptr);
            if (ok != 1) return error.TlsSetupFailed;
        }

        _ = c.SSL_set_value_uint(ssl, c.SSL_VALUE_CLASS_FEATURE_REQUEST, c.SSL_VALUE_QUIC_IDLE_TIMEOUT, frame.idle_timeout_ms);
        if (c.SSL_set_blocking_mode(ssl, 0) != 1) return error.TlsSetupFailed;
        // The server opens no streams; anything it tries is refused
        _ = c.SSL_set_incoming_stream_policy(ssl, c.SSL_INCOMING_STREAM_POLICY_REJECT, 0);
        setPartialWrite(ssl);

        return .{ .ctx = ctx, .ssl = ssl, .fd = fd, .fingerprint = tls.fingerprint };
    }

    /// Advances the handshake: true once it's done and the certificate checks passed.
    pub fn handshake(self: *ClientConn) !bool {
        c.ERR_clear_error();
        const result = c.SSL_connect(self.ssl);
        if (result != 1) {
            return switch (c.SSL_get_error(self.ssl, result)) {
                c.SSL_ERROR_WANT_READ, c.SSL_ERROR_WANT_WRITE => false,
                else => if (c.SSL_get_verify_result(self.ssl) != c.X509_V_OK)
                    error.CertificateVerifyFailed
                else
                    error.HandshakeFailed,
            };
        }
        if (self.fingerprint) |pin| {
            const cert = c.SSL_get0_peer_certificate(self.ssl) orelse return error.CertificateFingerprintMismatch;
            var digest: [c.EVP_MAX_MD_SIZE]u8 = undefined;
            var len: c_uint = 0;
            if (c.X509_digest(cert, c.EVP_sha256(), &digest, &len) != 1 or len != 32) return error.CertificateFingerprintMismatch;
            if (!std.crypto.timing_safe.eql([32]u8, digest[0..32].*, pin)) return error.CertificateFingerprintMismatch;
        }
        return true;
    }

    pub fn tick(self: *ClientConn) void {
        if (!self.closed) _ = c.SSL_handle_events(self.ssl);
    }

    pub fn isTerminated(self: *ClientConn) bool {
        return self.closed or connTerminated(self.ssl);
    }

    /// Sends CONNECTION_CLOSE. The SSL objects stay until `deinit`.
    pub fn close(self: *ClientConn) void {
        if (self.closed) return;
        self.closed = true;
        var tries: usize = 0;
        while (!shutdownRapid(self.ssl) and tries < 3) : (tries += 1) {}
    }

    pub fn deinit(self: *ClientConn) void {
        self.close();
        c.SSL_free(self.ssl);
        c.SSL_CTX_free(self.ctx);
        posix.close(self.fd);
        self.* = undefined;
    }

    /// Opens another stream on this connection (for tests of the server's one-stream rule).
    pub fn openExtraStream(self: *ClientConn, unidirectional: bool) !*Ssl {
        return c.SSL_new_stream(self.ssl, if (unidirectional) c.SSL_STREAM_FLAG_UNI else 0) orelse error.StreamRefused;
    }
};

/// Whether the peer refused a stream: its STOP_SENDING resets our sending part (OpenSSL reports that as a
/// local reset), its RESET_STREAM the receiving part. A closed connection counts too.
pub fn streamRefused(stream: *Ssl) bool {
    const sending = c.SSL_get_stream_write_state(stream);
    return sending == c.SSL_STREAM_STATE_RESET_LOCAL or sending == c.SSL_STREAM_STATE_RESET_REMOTE or
        sending == c.SSL_STREAM_STATE_CONN_CLOSED or c.SSL_get_stream_read_state(stream) == c.SSL_STREAM_STATE_RESET_REMOTE;
}

/// The idle timeout the peer asked for in its transport parameters, in ms.
pub fn peerIdleTimeout(conn: *Ssl) ?u64 {
    var value: u64 = 0;
    if (c.SSL_get_value_uint(conn, c.SSL_VALUE_CLASS_FEATURE_PEER_REQUEST, c.SSL_VALUE_QUIC_IDLE_TIMEOUT, &value) != 1) return null;
    return value;
}

pub fn freeStream(stream: *Ssl) void {
    c.SSL_free(stream);
}

fn addTrusted(ctx: *c.SSL_CTX, pem: []const u8) !void {
    const store = c.SSL_CTX_get_cert_store(ctx) orelse return error.TlsSetupFailed;
    const bio = try bioFrom(pem);
    defer _ = c.BIO_free(bio);
    var count: usize = 0;
    while (c.PEM_read_bio_X509(bio, null, null, null)) |cert| {
        defer c.X509_free(cert);
        if (c.X509_STORE_add_cert(store, cert) != 1) return error.InvalidCertificate;
        count += 1;
    }
    c.ERR_clear_error();
    if (count == 0) return error.InvalidCertificate;
}

fn selectAlpn(
    _: ?*Ssl,
    out: [*c][*c]const u8,
    out_len: [*c]u8,
    in: [*c]const u8,
    in_len: c_uint,
    _: ?*anyopaque,
) callconv(.c) c_int {
    var i: usize = 0;
    while (i < in_len) {
        const len = in[i];
        if (i + 1 + len > in_len) break;
        if (std.mem.eql(u8, in[i + 1 .. i + 1 + len], frame.alpn)) {
            out.* = in + i + 1;
            out_len.* = len;
            return c.SSL_TLSEXT_ERR_OK;
        }
        i += 1 + len;
    }
    return c.SSL_TLSEXT_ERR_ALERT_FATAL;
}

/// A bound UDP socket accepting QUIC connections.
pub const Listener = struct {
    ctx: *c.SSL_CTX,
    ssl: *Ssl,
    fd: posix.socket_t,
    port: u16,

    pub fn open(address: std.net.Address, cert_pem: []const u8, key_pem: []const u8) !Listener {
        const ctx = c.SSL_CTX_new(c.OSSL_QUIC_server_method()) orelse return error.TlsSetupFailed;
        errdefer c.SSL_CTX_free(ctx);
        try useCertificate(ctx, cert_pem, key_pem);
        c.SSL_CTX_set_verify(ctx, c.SSL_VERIFY_NONE, null);
        c.SSL_CTX_set_alpn_select_cb(ctx, selectAlpn, null);

        const fd = try posix.socket(address.any.family, posix.SOCK.DGRAM | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC, posix.IPPROTO.UDP);
        errdefer posix.close(fd);
        try posix.bind(fd, &address.any, address.getOsSockLen());
        var actual: std.net.Address = undefined;
        var actual_len: posix.socklen_t = @sizeOf(std.net.Address);
        try posix.getsockname(fd, &actual.any, &actual_len);

        const ssl = c.SSL_new_listener(ctx, 0) orelse return error.TlsSetupFailed;
        errdefer c.SSL_free(ssl);
        if (c.SSL_set_fd(ssl, fd) != 1) return error.TlsSetupFailed;
        if (c.SSL_set_blocking_mode(ssl, 0) != 1) return error.TlsSetupFailed;
        _ = c.SSL_set_value_uint(ssl, c.SSL_VALUE_CLASS_FEATURE_REQUEST, c.SSL_VALUE_QUIC_IDLE_TIMEOUT, frame.idle_timeout_ms);
        if (c.SSL_listen(ssl) != 1) return error.ListenFailed;
        return .{ .ctx = ctx, .ssl = ssl, .fd = fd, .port = actual.getPort() };
    }

    pub fn tick(self: *Listener) void {
        _ = c.SSL_handle_events(self.ssl);
    }

    pub fn accept(self: *Listener) ?ServerConn {
        c.ERR_clear_error();
        const conn = c.SSL_accept_connection(self.ssl, c.SSL_ACCEPT_CONNECTION_NO_BLOCK) orelse return null;
        // Stream aware: the client's stream is taken with SSL_accept_stream, later ones are refused
        _ = c.SSL_set_default_stream_mode(conn, c.SSL_DEFAULT_STREAM_MODE_NONE);
        _ = c.SSL_set_incoming_stream_policy(conn, c.SSL_INCOMING_STREAM_POLICY_ACCEPT, 0);
        return .{ .conn = conn };
    }

    pub fn deinit(self: *Listener) void {
        c.SSL_free(self.ssl);
        c.SSL_CTX_free(self.ctx);
        posix.close(self.fd);
        self.* = undefined;
    }
};

fn useCertificate(ctx: *c.SSL_CTX, cert_pem: []const u8, key_pem: []const u8) !void {
    const certs = try bioFrom(cert_pem);
    defer _ = c.BIO_free(certs);
    const leaf = c.PEM_read_bio_X509(certs, null, null, null) orelse return error.InvalidCertificate;
    defer c.X509_free(leaf);
    if (c.SSL_CTX_use_certificate(ctx, leaf) != 1) return error.InvalidCertificate;
    while (c.PEM_read_bio_X509(certs, null, null, null)) |chain| {
        // add0: the context owns it now
        if (c.SSL_CTX_ctrl(ctx, ctrl_chain_cert, 0, chain) != 1) {
            c.X509_free(chain);
            return error.InvalidCertificate;
        }
    }
    c.ERR_clear_error();

    const keys = try bioFrom(key_pem);
    defer _ = c.BIO_free(keys);
    const key = c.PEM_read_bio_PrivateKey(keys, null, null, null) orelse return error.InvalidKey;
    defer c.EVP_PKEY_free(key);
    if (c.SSL_CTX_use_PrivateKey(ctx, key) != 1 or c.SSL_CTX_check_private_key(ctx) != 1) return error.InvalidKey;
}

/// An accepted connection and, once the client opened it, its one stream.
pub const ServerConn = struct {
    conn: *Ssl,
    stream: ?*Ssl = null,

    /// Takes the client's bidirectional stream; refuses every other stream. True once there is one.
    pub fn acceptStreams(self: *ServerConn) bool {
        while (c.SSL_accept_stream(self.conn, c.SSL_ACCEPT_STREAM_NO_BLOCK)) |stream| {
            if (self.stream != null or c.SSL_get_stream_type(stream) != c.SSL_STREAM_TYPE_BIDI) {
                if (c.SSL_get_stream_type(stream) == c.SSL_STREAM_TYPE_BIDI) _ = c.SSL_stream_reset(stream, null, 0);
                c.SSL_free(stream);
                continue;
            }
            setPartialWrite(stream);
            self.stream = stream;
            // From now on OpenSSL refuses new streams itself (STOP_SENDING / RESET_STREAM)
            _ = c.SSL_set_incoming_stream_policy(self.conn, c.SSL_INCOMING_STREAM_POLICY_REJECT, 0);
        }
        return self.stream != null;
    }

    pub fn isTerminated(self: *ServerConn) bool {
        return connTerminated(self.conn);
    }

    /// Sends CONNECTION_CLOSE; true once the connection is done and `deinit` can free it.
    pub fn shutdown(self: *ServerConn) bool {
        if (self.stream) |stream| {
            c.SSL_free(stream);
            self.stream = null;
        }
        return shutdownRapid(self.conn);
    }

    pub fn deinit(self: *ServerConn) void {
        if (self.stream) |stream| c.SSL_free(stream);
        c.SSL_free(self.conn);
        self.* = undefined;
    }
};
