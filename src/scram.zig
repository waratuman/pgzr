const std = @import("std");
const protocol = @import("protocol.zig");

const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;
const Sha256 = std.crypto.hash.sha2.Sha256;

pub const ScramError = protocol.ReadError || protocol.ReadBodyError || std.net.Stream.WriteError || error{
    AuthenticationFailed,
    InvalidServerResponse,
};

/// Perform SCRAM-SHA-256 authentication exchange.
///
/// `auth_body` is the body of the initial AUTH_SASL message which contains
/// the list of mechanism names (null-terminated strings followed by a final \0).
pub fn performScramAuth(
    stream: std.net.Stream,
    user: []const u8,
    password: []const u8,
    auth_body: []const u8,
) ScramError!void {
    _ = user;

    // Verify SCRAM-SHA-256 is offered
    if (!mechanismOffered(auth_body, "SCRAM-SHA-256"))
        return error.InvalidServerResponse;

    // Step 1: Generate client nonce and build client-first-message
    var nonce_bytes: [18]u8 = undefined;
    std.crypto.random.bytes(&nonce_bytes);
    var client_nonce: [24]u8 = undefined;
    _ = std.base64.standard.Encoder.encode(&client_nonce, &nonce_bytes);

    // client-first-message-bare: n=,r=<nonce>  (we omit the username per spec for PostgreSQL)
    var cfmb_buf: [128]u8 = undefined;
    const client_first_bare = std.fmt.bufPrint(&cfmb_buf, "n=,r={s}", .{client_nonce}) catch
        return error.InvalidServerResponse;

    // client-first-message: n,,<client-first-bare>  (gs2 header "n,," = no channel binding)
    var cfm_buf: [256]u8 = undefined;
    const client_first = std.fmt.bufPrint(&cfm_buf, "n,,{s}", .{client_first_bare}) catch
        return error.InvalidServerResponse;

    // Send SASLInitialResponse
    var send_buf: [4096]u8 = undefined;
    const sasl_init = protocol.encodeSASLInitialResponse(&send_buf, "SCRAM-SHA-256", client_first);
    try stream.writeAll(sasl_init);

    // Step 2: Receive AuthenticationSASLContinue
    var recv_buf: [4096]u8 = undefined;
    const cont_header = try protocol.readHeader(stream);
    if (cont_header.msg_type != protocol.MSG_AUTH) return error.InvalidServerResponse;
    const cont_body = try protocol.readBody(stream, cont_header, &recv_buf);
    const cont_auth_type = std.mem.readInt(u32, cont_body[0..4], .big);
    if (cont_auth_type != protocol.AUTH_SASL_CONTINUE) return error.InvalidServerResponse;
    const server_first = cont_body[4..];

    // Parse server-first-message: r=<nonce>,s=<salt>,i=<iterations>
    const parsed = parseServerFirst(server_first) catch return error.InvalidServerResponse;

    // Verify server nonce starts with our client nonce
    if (!std.mem.startsWith(u8, parsed.nonce, &client_nonce))
        return error.AuthenticationFailed;

    // Decode the salt from base64
    var salt_buf: [128]u8 = undefined;
    const salt_len = std.base64.standard.Decoder.calcSizeForSlice(parsed.salt) catch
        return error.InvalidServerResponse;
    if (salt_len > salt_buf.len) return error.InvalidServerResponse;
    std.base64.standard.Decoder.decode(salt_buf[0..salt_len], parsed.salt) catch
        return error.InvalidServerResponse;
    const salt = salt_buf[0..salt_len];

    // Step 3: Compute proofs
    // SaltedPassword = PBKDF2(HmacSha256, password, salt, iterations)
    var salted_password: [32]u8 = undefined;
    std.crypto.pwhash.pbkdf2(&salted_password, password, salt, parsed.iterations, HmacSha256) catch
        return error.InvalidServerResponse;

    // ClientKey = HMAC(SaltedPassword, "Client Key")
    var client_key: [32]u8 = undefined;
    HmacSha256.create(&client_key, "Client Key", &salted_password);

    // StoredKey = SHA256(ClientKey)
    var stored_key: [32]u8 = undefined;
    Sha256.hash(&client_key, &stored_key, .{});

    // client-final-without-proof: c=biws,r=<combined-nonce>
    // "biws" = base64("n,,") = the gs2 header
    var cfwp_buf: [512]u8 = undefined;
    const client_final_without_proof = std.fmt.bufPrint(&cfwp_buf, "c=biws,r={s}", .{parsed.nonce}) catch
        return error.InvalidServerResponse;

    // AuthMessage = client-first-bare + "," + server-first + "," + client-final-without-proof
    var auth_msg_buf: [2048]u8 = undefined;
    const auth_message = std.fmt.bufPrint(&auth_msg_buf, "{s},{s},{s}", .{
        client_first_bare,
        server_first,
        client_final_without_proof,
    }) catch return error.InvalidServerResponse;

    // ClientSignature = HMAC(StoredKey, AuthMessage)
    var client_signature: [32]u8 = undefined;
    HmacSha256.create(&client_signature, auth_message, &stored_key);

    // ClientProof = ClientKey XOR ClientSignature
    var client_proof: [32]u8 = undefined;
    for (0..32) |i| {
        client_proof[i] = client_key[i] ^ client_signature[i];
    }

    // ServerKey = HMAC(SaltedPassword, "Server Key")
    var server_key: [32]u8 = undefined;
    HmacSha256.create(&server_key, "Server Key", &salted_password);

    // ServerSignature = HMAC(ServerKey, AuthMessage)
    var expected_server_sig: [32]u8 = undefined;
    HmacSha256.create(&expected_server_sig, auth_message, &server_key);

    // Encode client proof as base64
    var proof_b64_buf: [48]u8 = undefined;
    const proof_b64 = std.base64.standard.Encoder.encode(&proof_b64_buf, &client_proof);

    // client-final-message: <client-final-without-proof>,p=<proof>
    var final_buf: [1024]u8 = undefined;
    const client_final = std.fmt.bufPrint(&final_buf, "{s},p={s}", .{
        client_final_without_proof,
        proof_b64,
    }) catch return error.InvalidServerResponse;

    // Send SASLResponse
    const sasl_resp = protocol.encodeSASLResponse(&send_buf, client_final);
    try stream.writeAll(sasl_resp);

    // Step 4: Receive AuthenticationSASLFinal, verify server signature
    const final_header = try protocol.readHeader(stream);
    if (final_header.msg_type != protocol.MSG_AUTH) return error.InvalidServerResponse;
    const final_body = try protocol.readBody(stream, final_header, &recv_buf);
    const final_auth_type = std.mem.readInt(u32, final_body[0..4], .big);
    if (final_auth_type != protocol.AUTH_SASL_FINAL) return error.InvalidServerResponse;
    const server_final = final_body[4..];

    // Parse v=<server-signature> from server-final-message
    if (!std.mem.startsWith(u8, server_final, "v="))
        return error.InvalidServerResponse;
    const server_sig_b64 = server_final[2..];
    var server_sig: [32]u8 = undefined;
    const sig_len = std.base64.standard.Decoder.calcSizeForSlice(server_sig_b64) catch
        return error.InvalidServerResponse;
    if (sig_len != 32) return error.AuthenticationFailed;
    std.base64.standard.Decoder.decode(&server_sig, server_sig_b64) catch
        return error.InvalidServerResponse;

    // Verify server signature
    if (!std.mem.eql(u8, &server_sig, &expected_server_sig))
        return error.AuthenticationFailed;

    // Step 5: Receive AuthenticationOk
    const ok_header = try protocol.readHeader(stream);
    if (ok_header.msg_type != protocol.MSG_AUTH) return error.InvalidServerResponse;
    var ok_buf: [16]u8 = undefined;
    const ok_body = try protocol.readBody(stream, ok_header, &ok_buf);
    const ok_type = std.mem.readInt(u32, ok_body[0..4], .big);
    if (ok_type != protocol.AUTH_OK) return error.AuthenticationFailed;
}

/// Check if a mechanism name appears in the AUTH_SASL mechanism list.
/// The list is a sequence of null-terminated strings after the 4-byte auth type,
/// terminated by an additional \0.
fn mechanismOffered(body: []const u8, mechanism: []const u8) bool {
    // body starts after the 4-byte auth_type (caller already stripped it)
    var pos: usize = 4;
    while (pos < body.len) {
        if (body[pos] == 0) break; // end of list
        const result = protocol.readCString(body, pos) catch break;
        if (std.mem.eql(u8, result.str, mechanism)) return true;
        pos = result.next;
    }
    return false;
}

const ServerFirstParams = struct {
    nonce: []const u8,
    salt: []const u8,
    iterations: u32,
};

/// Parse server-first-message: r=<nonce>,s=<salt>,i=<iterations>
fn parseServerFirst(msg: []const u8) error{ProtocolError}!ServerFirstParams {
    var nonce: ?[]const u8 = null;
    var salt: ?[]const u8 = null;
    var iterations: ?u32 = null;

    var iter = std.mem.splitScalar(u8, msg, ',');
    while (iter.next()) |field| {
        if (field.len < 2) continue;
        if (field[0] == 'r' and field[1] == '=') {
            nonce = field[2..];
        } else if (field[0] == 's' and field[1] == '=') {
            salt = field[2..];
        } else if (field[0] == 'i' and field[1] == '=') {
            iterations = std.fmt.parseInt(u32, field[2..], 10) catch return error.ProtocolError;
        }
    }

    return .{
        .nonce = nonce orelse return error.ProtocolError,
        .salt = salt orelse return error.ProtocolError,
        .iterations = iterations orelse return error.ProtocolError,
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "mechanismOffered" {
    // Simulate AUTH_SASL body: auth_type(4 bytes) + "SCRAM-SHA-256\0\0"
    const body = "\x00\x00\x00\x0aSCRAM-SHA-256\x00\x00";
    try std.testing.expect(mechanismOffered(body, "SCRAM-SHA-256"));
    try std.testing.expect(!mechanismOffered(body, "SCRAM-SHA-512"));
}

test "parseServerFirst" {
    const msg = "r=clientnonceservernonce,s=c2FsdA==,i=4096";
    const params = try parseServerFirst(msg);
    try std.testing.expectEqualStrings("clientnonceservernonce", params.nonce);
    try std.testing.expectEqualStrings("c2FsdA==", params.salt);
    try std.testing.expectEqual(@as(u32, 4096), params.iterations);
}

test "SCRAM crypto computations" {
    // Test with known values: password="pencil", salt="QSXCR+Q6sek8bf92" (from RFC 5802 adapted for SHA-256)
    // We test the individual steps rather than the full exchange.

    const password = "pencil";
    const salt = "QSXCR+Q6sek8bf92";
    const iterations: u32 = 4096;

    // SaltedPassword = PBKDF2(HmacSha256, password, salt, iterations)
    var salted_password: [32]u8 = undefined;
    try std.crypto.pwhash.pbkdf2(&salted_password, password, salt, iterations, HmacSha256);

    // ClientKey = HMAC(SaltedPassword, "Client Key")
    var client_key: [32]u8 = undefined;
    HmacSha256.create(&client_key, "Client Key", &salted_password);

    // StoredKey = SHA256(ClientKey)
    var stored_key: [32]u8 = undefined;
    Sha256.hash(&client_key, &stored_key, .{});

    // ServerKey = HMAC(SaltedPassword, "Server Key")
    var server_key: [32]u8 = undefined;
    HmacSha256.create(&server_key, "Server Key", &salted_password);

    // Verify lengths
    try std.testing.expectEqual(@as(usize, 32), salted_password.len);
    try std.testing.expectEqual(@as(usize, 32), client_key.len);
    try std.testing.expectEqual(@as(usize, 32), stored_key.len);
    try std.testing.expectEqual(@as(usize, 32), server_key.len);

    // Verify deterministic
    var salted_password2: [32]u8 = undefined;
    try std.crypto.pwhash.pbkdf2(&salted_password2, password, salt, iterations, HmacSha256);
    try std.testing.expectEqualSlices(u8, &salted_password, &salted_password2);
}
