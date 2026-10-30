//! Stands in for `quic_openssl.zig` when channel is built without `-Dquic=true`, so the default build
//! links no C library.
pub const enabled = false;
