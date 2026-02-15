const std = @import("std");

/// Map a PostgreSQL type OID to its canonical name.
/// Returns "oid:N" for unknown OIDs (written into the provided buffer).
pub fn oidToName(oid: u32, buf: *[32]u8) []const u8 {
    return switch (oid) {
        16 => "bool",
        17 => "bytea",
        18 => "char",
        19 => "name",
        20 => "int8",
        21 => "int2",
        23 => "int4",
        24 => "regproc",
        25 => "text",
        26 => "oid",
        28 => "xid",
        29 => "cid",
        114 => "json",
        142 => "xml",
        600 => "point",
        601 => "lseg",
        602 => "path",
        603 => "box",
        604 => "polygon",
        628 => "line",
        650 => "cidr",
        700 => "float4",
        701 => "float8",
        718 => "circle",
        790 => "money",
        829 => "macaddr",
        869 => "inet",
        1042 => "bpchar",
        1043 => "varchar",
        1082 => "date",
        1083 => "time",
        1114 => "timestamp",
        1184 => "timestamptz",
        1186 => "interval",
        1266 => "timetz",
        1560 => "bit",
        1562 => "varbit",
        1700 => "numeric",
        2950 => "uuid",
        3614 => "tsvector",
        3615 => "tsquery",
        3802 => "jsonb",
        3904 => "int4range",
        3906 => "numrange",
        3908 => "tsrange",
        3910 => "tstzrange",
        3912 => "daterange",
        3926 => "int8range",
        4072 => "jsonpath",
        4451 => "int4multirange",
        4532 => "nummultirange",
        4533 => "tsmultirange",
        4534 => "tstzmultirange",
        4535 => "datemultirange",
        4536 => "int8multirange",
        else => {
            const slice = std.fmt.bufPrint(buf, "oid:{d}", .{oid}) catch "oid:?";
            return slice;
        },
    };
}

// ── Tests ─────────────────────────────────────────────────────────────

test "oidToName: known types" {
    var buf: [32]u8 = undefined;
    try std.testing.expectEqualStrings("int4", oidToName(23, &buf));
    try std.testing.expectEqualStrings("text", oidToName(25, &buf));
    try std.testing.expectEqualStrings("bool", oidToName(16, &buf));
    try std.testing.expectEqualStrings("uuid", oidToName(2950, &buf));
    try std.testing.expectEqualStrings("jsonb", oidToName(3802, &buf));
    try std.testing.expectEqualStrings("timestamptz", oidToName(1184, &buf));
    try std.testing.expectEqualStrings("bytea", oidToName(17, &buf));
}

test "oidToName: unknown oid" {
    var buf: [32]u8 = undefined;
    try std.testing.expectEqualStrings("oid:99999", oidToName(99999, &buf));
}
