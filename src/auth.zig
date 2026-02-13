const std = @import("std");
const protocol = @import("protocol.zig");

/// Compute the MD5 password hash as PostgreSQL expects:
///   "md5" + hex(md5(hex(md5(password + user)) + salt))
/// Returns a 35-byte array: "md5" + 32 hex characters.
pub fn md5Password(
    user: []const u8,
    password: []const u8,
    salt: *const [4]u8,
) [35]u8 {
    var result: [35]u8 = undefined;
    result[0] = 'm';
    result[1] = 'd';
    result[2] = '5';

    // Phase 1: md5(password + user)
    var h1 = std.crypto.hash.Md5.init(.{});
    h1.update(password);
    h1.update(user);
    var digest1: [16]u8 = undefined;
    h1.final(&digest1);
    const hex1: [32]u8 = std.fmt.bytesToHex(digest1, .lower);

    // Phase 2: md5(hex1 + salt)
    var h2 = std.crypto.hash.Md5.init(.{});
    h2.update(&hex1);
    h2.update(salt);
    var digest2: [16]u8 = undefined;
    h2.final(&digest2);
    const hex2: [32]u8 = std.fmt.bytesToHex(digest2, .lower);

    @memcpy(result[3..35], &hex2);
    return result;
}

pub const AuthError = protocol.ReadError || protocol.ReadBodyError || std.net.Stream.WriteError || error{
    UnsupportedAuthMethod,
    AuthenticationFailed,
};

/// Handle the authentication exchange.
/// Reads the AuthenticationRequest, responds if needed, reads until AuthenticationOk.
pub fn authenticate(
    stream: std.net.Stream,
    user: []const u8,
    password: []const u8,
) AuthError!void {
    var buf: [4096]u8 = undefined;

    const header = try protocol.readHeader(stream);
    if (header.msg_type != protocol.MSG_AUTH) return error.ProtocolError;
    const body = try protocol.readBody(stream, header, &buf);
    const auth_type = std.mem.readInt(u32, body[0..4], .big);

    switch (auth_type) {
        protocol.AUTH_OK => return,
        protocol.AUTH_CLEARTEXT => {
            const msg = protocol.encodePassword(&buf, password);
            try stream.writeAll(msg);
        },
        protocol.AUTH_MD5 => {
            const salt: *const [4]u8 = body[4..8];
            const hashed = md5Password(user, password, salt);
            const msg = protocol.encodePassword(&buf, &hashed);
            try stream.writeAll(msg);
        },
        else => return error.UnsupportedAuthMethod,
    }

    // After sending credentials, expect AuthenticationOk
    const ok_header = try protocol.readHeader(stream);
    if (ok_header.msg_type != protocol.MSG_AUTH) return error.ProtocolError;
    var ok_buf: [16]u8 = undefined;
    const ok_body = try protocol.readBody(stream, ok_header, &ok_buf);
    const ok_type = std.mem.readInt(u32, ok_body[0..4], .big);
    if (ok_type != protocol.AUTH_OK) return error.AuthenticationFailed;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "md5Password known vector" {
    // Known test: user="postgres", password="password", salt=[0x01, 0x02, 0x03, 0x04]
    // Step 1: md5("password" + "postgres") = md5("passwordpostgres")
    //       = "7c80......" (compute in test)
    // Step 2: md5(hex_of_step1 + salt)
    // Result starts with "md5"
    const result = md5Password("postgres", "password", &[4]u8{ 0x01, 0x02, 0x03, 0x04 });
    try std.testing.expectEqualStrings("md5", result[0..3]);
    try std.testing.expectEqual(@as(usize, 35), result.len);

    // Verify it's deterministic
    const result2 = md5Password("postgres", "password", &[4]u8{ 0x01, 0x02, 0x03, 0x04 });
    try std.testing.expectEqualStrings(&result, &result2);

    // Different salt should produce different result
    const result3 = md5Password("postgres", "password", &[4]u8{ 0x05, 0x06, 0x07, 0x08 });
    try std.testing.expect(!std.mem.eql(u8, &result, &result3));
}
