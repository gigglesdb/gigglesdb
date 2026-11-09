//! Closed PG18 overload metadata and bounded, dependency-free constant planning.
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Volatility {
    Stable,
    Volatile,
}

/// Clock overload metadata is shared by binding and immutable planning.
/// None of these functions may be evaluated during preparation.
pub(super) fn clock_signature(name: &str) -> Option<(DataType, Volatility)> {
    use DataType::*;
    use Volatility::*;
    Some(match name {
        "clock_timestamp" => (TimestampTz, Volatile),
        "now" | "transaction_timestamp" | "statement_timestamp" | "current_timestamp" => {
            (TimestampTz, Stable)
        }
        "localtimestamp" => (Timestamp, Stable),
        "current_date" => (Date, Stable),
        "current_time" => (TimeTz, Stable),
        "localtime" => (Time, Stable),
        _ => return None,
    })
}

/// Sequence routines are volatile even with literal arguments.
pub(super) fn sequence_volatility(name: &str) -> Option<Volatility> {
    sequence_name(name).then_some(Volatility::Volatile)
}
fn signature(name: &str, args: &[DataType], result: &DataType) -> Option<(bool, bool)> {
    use DataType::*;
    if let Some((clock_type, _volatility)) = clock_signature(name) {
        let precision_keyword = matches!(
            name,
            "current_timestamp" | "localtimestamp" | "current_time" | "localtime"
        );
        return (clock_type == *result
            && (args.is_empty() || precision_keyword && args == [Integer]))
        .then_some((false, false));
    }
    match (name, args, result) {
        ("overlay", &[Text, Text, Integer], Text)
        | ("overlay", &[Text, Text, Integer, Integer], Text)
        | ("overlay", &[Bytes, Bytes, Integer], Bytes)
        | ("overlay", &[Bytes, Bytes, Integer, Integer], Bytes) => Some((true, true)), // pg_proc 1405,1404,752,749
        ("float8", &[SmallInt], DoublePrecision) => Some((true, true)), // pg_proc 235
        ("float4", &[SmallInt], Real) => Some((true, true)),            // pg_proc 236
        ("int2", &[DoublePrecision], SmallInt) => Some((true, true)),   // pg_proc 237
        ("int2", &[Real], SmallInt) => Some((true, true)),              // pg_proc 238
        ("float8", &[Real], DoublePrecision) => Some((true, true)),     // pg_proc 311
        ("float4", &[DoublePrecision], Real) => Some((true, true)),     // pg_proc 312
        ("int4", &[SmallInt], Integer) => Some((true, true)),           // pg_proc 313
        ("int2", &[Integer], SmallInt) => Some((true, true)),           // pg_proc 314
        ("float8", &[Integer], DoublePrecision) => Some((true, true)),  // pg_proc 316
        ("int4", &[DoublePrecision], Integer) => Some((true, true)),    // pg_proc 317
        ("float4", &[Integer], Real) => Some((true, true)),             // pg_proc 318
        ("int4", &[Real], Integer) => Some((true, true)),               // pg_proc 319
        ("int4", &[BigInt], Integer) => Some((true, true)),             // pg_proc 480
        ("int8", &[Integer], BigInt) => Some((true, true)),             // pg_proc 481
        ("float8", &[BigInt], DoublePrecision) => Some((true, true)),   // pg_proc 482
        ("int8", &[DoublePrecision], BigInt) => Some((true, true)),     // pg_proc 483
        ("float4", &[BigInt], Real) => Some((true, true)),              // pg_proc 652
        ("int8", &[Real], BigInt) => Some((true, true)),                // pg_proc 653
        ("int2", &[BigInt], SmallInt) => Some((true, true)),            // pg_proc 714
        ("octet_length", &[Bytes], Integer) => Some((true, true)),      // pg_proc 720
        ("get_byte", &[Bytes, Integer], Integer) => Some((true, true)), // pg_proc 721
        ("set_byte", &[Bytes, Integer, Integer], Bytes) => Some((true, true)), // pg_proc 722
        ("get_bit", &[Bytes, BigInt], Integer) => Some((true, true)),   // pg_proc 723
        ("set_bit", &[Bytes, BigInt, Integer], Bytes) => Some((true, true)), // pg_proc 724
        ("int8", &[SmallInt], BigInt) => Some((true, true)),            // pg_proc 754
        ("to_hex", &[Integer], Text) => Some((true, true)),             // pg_proc 2089
        ("to_hex", &[BigInt], Text) => Some((true, true)),              // pg_proc 2090
        ("to_bin", &[Integer], Text) => Some((true, true)),             // pg_proc 6330
        ("to_bin", &[BigInt], Text) => Some((true, true)),              // pg_proc 6331
        ("to_oct", &[Integer], Text) => Some((true, true)),             // pg_proc 6332
        ("to_oct", &[BigInt], Text) => Some((true, true)),              // pg_proc 6333
        ("position", &[Text, Text], Integer) => Some((true, true)),     // pg_proc 849
        ("position", &[Bytes, Bytes], Integer) => Some((true, true)),   // pg_proc 2014
        ("strpos", &[Text, Text], Integer) => Some((true, true)),       // pg_proc 868
        ("lower", &[Text], Text) => Some((true, true)),                 // pg_proc 870
        ("ascii", &[Text], Integer) => Some((true, true)),              // pg_proc 1620
        ("chr", &[SmallInt | Integer], Text) => Some((true, true)), // pg_proc 1621, implicit int2 widening
        ("initcap", &[Text], Text) => Some((true, true)),           // pg_proc 872
        ("quote_ident", &[Text], Text) => Some((true, true)),       // pg_proc 1282
        ("quote_literal", &[Text], Text) => Some((true, true)),     // pg_proc 1283
        ("quote_nullable", &[Text], Text) => Some((true, false)),   // pg_proc 1289
        ("upper", &[Text], Text) => Some((true, true)),             // pg_proc 871
        ("lpad", &[Text, Integer, Text], Text) => Some((true, true)), // pg_proc 873
        ("rpad", &[Text, Integer, Text], Text) => Some((true, true)), // pg_proc 874
        ("ltrim", &[Text, Text], Text) => Some((true, true)),       // pg_proc 875
        ("rtrim", &[Text, Text], Text) => Some((true, true)),       // pg_proc 876
        ("substr", &[Text, Integer, Integer], Text) => Some((true, true)), // pg_proc 877
        ("translate", &[Text, Text, Text], Text) => Some((true, true)), // pg_proc 878
        ("lpad", &[Text, Integer], Text) => Some((true, true)),     // pg_proc 879
        ("rpad", &[Text, Integer], Text) => Some((true, true)),     // pg_proc 880
        ("ltrim", &[Text], Text) => Some((true, true)),             // pg_proc 881
        ("rtrim", &[Text], Text) => Some((true, true)),             // pg_proc 882
        ("substr", &[Text, Integer], Text) => Some((true, true)),   // pg_proc 883
        ("btrim", &[Text, Text], Text) => Some((true, true)),       // pg_proc 884
        ("btrim", &[Text], Text) => Some((true, true)),             // pg_proc 885
        ("substring", &[Text, Integer, Integer], Text) => Some((true, true)), // pg_proc 936
        ("substring", &[Text, Integer], Text) => Some((true, true)), // pg_proc 937
        ("mod", &[SmallInt, SmallInt], SmallInt) => Some((true, true)), // pg_proc 940
        ("mod", &[Integer, Integer], Integer) => Some((true, true)), // pg_proc 941
        ("mod", &[BigInt, BigInt], BigInt) => Some((true, true)),   // pg_proc 947
        ("timezone", &[Interval, TimestampTz], Timestamp) => Some((true, true)), // pg_proc 1026
        ("timezone", &[Text, TimestampTz], Timestamp) => Some((true, true)), // pg_proc 1159
        ("date_part", &[Text, TimestampTz], DoublePrecision) => Some((false, true)), // pg_proc 1171
        ("date_part", &[Text, Interval], DoublePrecision) => Some((true, true)), // pg_proc 1172
        ("timestamptz", &[Date], TimestampTz) => Some((false, true)), // pg_proc 1174
        ("justify_hours", &[Interval], Interval) => Some((true, true)), // pg_proc 1175
        ("timestamptz", &[Date, Time], TimestampTz) => Some((false, true)), // pg_proc 1176
        ("date", &[TimestampTz], Date) => Some((false, true)),      // pg_proc 1178
        ("log10", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1194
        ("age", &[TimestampTz, TimestampTz], Interval) => Some((true, true)), // pg_proc 1199
        ("interval", &[Interval, Integer], Interval) => Some((true, true)), // pg_proc 1200
        ("date_trunc", &[Text, TimestampTz], TimestampTz) => Some((false, true)), // pg_proc 1217
        ("date_trunc", &[Text, Interval], Interval) => Some((true, true)), // pg_proc 1218
        ("date_part", &[Text, TimeTz], DoublePrecision) => Some((true, true)), // pg_proc 1273
        ("date_trunc", &[Text, TimestampTz, Text], TimestampTz) => Some((true, true)), // pg_proc 1284
        ("justify_days", &[Interval], Interval) => Some((true, true)), // pg_proc 1295
        ("time", &[Timestamp], Time) => Some((true, true)),            // pg_proc 1316
        ("length", &[Text], Integer) => Some((true, true)),            // pg_proc 1317
        ("log", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1340
        ("ln", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1341
        ("round", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1342
        ("trunc", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1343
        ("sqrt", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1344
        ("cbrt", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1345
        ("exp", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1347
        ("timestamptz", &[Date, TimeTz], TimestampTz) => Some((true, true)), // pg_proc 1359
        ("power", &[DoublePrecision, DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1368
        ("character_length", &[Text], Integer) => Some((true, true)), // pg_proc 1369
        ("interval", &[Time], Interval) => Some((true, true)),        // pg_proc 1370
        ("isfinite", &[Date], Boolean) => Some((true, true)),         // pg_proc 1373
        ("octet_length", &[Text], Integer) => Some((true, true)),     // pg_proc 1374
        ("factorial", &[BigInt], Numeric) => Some((true, true)),      // pg_proc 1376
        ("char_length", &[Text], Integer) => Some((true, true)),      // pg_proc 1381
        ("date_part", &[Text, Date], DoublePrecision) => Some((true, true)), // pg_proc 1384
        ("date_part", &[Text, Time], DoublePrecision) => Some((true, true)), // pg_proc 1385
        ("age", &[TimestampTz], Interval) => Some((false, true)),     // pg_proc 1386
        ("timetz", &[TimestampTz], TimeTz) => Some((false, true)),    // pg_proc 1388
        ("isfinite", &[TimestampTz], Boolean) => Some((true, true)),  // pg_proc 1389
        ("isfinite", &[Interval], Boolean) => Some((true, true)),     // pg_proc 1390
        ("abs", &[Real], Real) => Some((true, true)),                 // pg_proc 1394
        ("abs", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1395
        ("abs", &[BigInt], BigInt) => Some((true, true)),             // pg_proc 1396
        ("abs", &[Integer], Integer) => Some((true, true)),           // pg_proc 1397
        ("abs", &[SmallInt], SmallInt) => Some((true, true)),         // pg_proc 1398
        ("time", &[Interval], Time) => Some((true, true)),            // pg_proc 1419
        ("log10", &[Numeric], Numeric) => Some((true, true)),         // pg_proc 1481
        ("md5", &[Text], Text) => Some((true, true)),                 // pg_proc 2311
        ("md5", &[Bytes], Text) => Some((true, true)),                // pg_proc 2321
        ("asind", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 2731
        ("acosd", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 2732
        ("atand", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 2733
        ("atan2d", &[DoublePrecision, DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 2734
        ("sind", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 2735
        ("cosd", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 2736
        ("tand", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 2737
        ("cotd", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 2738
        ("asin", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1600
        ("acos", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1601
        ("atan", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1602
        ("atan2", &[DoublePrecision, DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1603
        ("sin", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1604
        ("cos", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1605
        ("tan", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1606
        ("cot", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1607
        ("degrees", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1608
        ("radians", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 1609
        ("pi", &[], DoublePrecision) => Some((true, true)),                 // pg_proc 1610
        ("repeat", &[Text, Integer], Text) => Some((true, true)),           // pg_proc 1622
        ("numeric", &[Numeric, Integer], Numeric) => Some((true, true)),    // pg_proc 1703
        ("abs", &[Numeric], Numeric) => Some((true, true)),                 // pg_proc 1705
        ("sign", &[Numeric], Numeric) => Some((true, true)),                // pg_proc 1706
        ("round", &[Numeric, Integer], Numeric) => Some((true, true)),      // pg_proc 1707
        ("round", &[Numeric], Numeric) => Some((true, true)),               // pg_proc 1708
        ("trunc", &[Numeric, Integer], Numeric) => Some((true, true)),      // pg_proc 1709
        ("trunc", &[Numeric], Numeric) => Some((true, true)),               // pg_proc 1710
        ("ceil", &[Numeric], Numeric) => Some((true, true)),                // pg_proc 1711
        ("floor", &[Numeric], Numeric) => Some((true, true)),               // pg_proc 1712
        ("mod", &[Numeric, Numeric], Numeric) => Some((true, true)),        // pg_proc 1728
        ("sqrt", &[Numeric], Numeric) => Some((true, true)),                // pg_proc 1730
        ("exp", &[Numeric], Numeric) => Some((true, true)),                 // pg_proc 1732
        ("ln", &[Numeric], Numeric) => Some((true, true)),                  // pg_proc 1734
        ("log", &[Numeric, Numeric], Numeric) => Some((true, true)),        // pg_proc 1736
        ("numeric", &[Integer], Numeric) => Some((true, true)),             // pg_proc 1740
        ("log", &[Numeric], Numeric) => Some((true, true)),                 // pg_proc 1741
        ("numeric", &[Real], Numeric) => Some((true, true)),                // pg_proc 1742
        ("numeric", &[DoublePrecision], Numeric) => Some((true, true)),     // pg_proc 1743
        ("int4", &[Numeric], Integer) => Some((true, true)),                // pg_proc 1744
        ("float4", &[Numeric], Real) => Some((true, true)),                 // pg_proc 1745
        ("float8", &[Numeric], DoublePrecision) => Some((true, true)),      // pg_proc 1746
        ("int8", &[Numeric], BigInt) => Some((true, true)),                 // pg_proc 1779
        ("numeric", &[BigInt], Numeric) => Some((true, true)),              // pg_proc 1781
        ("numeric", &[SmallInt], Numeric) => Some((true, true)),            // pg_proc 1782
        ("int2", &[Numeric], SmallInt) => Some((true, true)),               // pg_proc 1783
        ("bit_length", &[Bytes], Integer) => Some((true, true)),            // pg_proc 1810
        ("bit_length", &[Text], Integer) => Some((true, true)),             // pg_proc 1811
        ("encode", &[Bytes, Text], Text) => Some((true, true)),             // pg_proc 1946
        ("decode", &[Text, Text], Bytes) => Some((true, true)),             // pg_proc 1947
        ("timestamp", &[Timestamp, Integer], Timestamp) => Some((true, true)), // pg_proc 1961
        ("timestamptz", &[TimestampTz, Integer], TimestampTz) => Some((true, true)), // pg_proc 1967
        ("time", &[Time, Integer], Time) => Some((true, true)),             // pg_proc 1968
        ("timetz", &[TimeTz, Integer], TimeTz) => Some((true, true)),       // pg_proc 1969
        ("div", &[Numeric, Numeric], Numeric) => Some((true, true)),        // pg_proc 1973
        ("length", &[Bytes], Integer) => Some((true, true)),                // pg_proc 2010
        ("substring", &[Bytes, Integer, Integer], Bytes) => Some((true, true)), // pg_proc 2012
        ("substring", &[Bytes, Integer], Bytes) => Some((true, true)),      // pg_proc 2013
        ("btrim", &[Bytes, Bytes], Bytes) => Some((true, true)),            // pg_proc 2015
        ("time", &[TimestampTz], Time) => Some((false, true)),              // pg_proc 2019
        ("date_trunc", &[Text, Timestamp], Timestamp) => Some((true, true)), // pg_proc 2020
        ("date_part", &[Text, Timestamp], DoublePrecision) => Some((true, true)), // pg_proc 2021
        ("timestamp", &[Date], Timestamp) => Some((true, true)),            // pg_proc 2024
        ("timestamp", &[Date, Time], Timestamp) => Some((true, true)),      // pg_proc 2025
        ("timestamp", &[TimestampTz], Timestamp) => Some((false, true)),    // pg_proc 2027
        ("timestamptz", &[Timestamp], TimestampTz) => Some((false, true)),  // pg_proc 2028
        ("date", &[Timestamp], Date) => Some((true, true)),                 // pg_proc 2029
        ("timezone", &[Text, TimeTz], TimeTz) => Some((false, true)),       // pg_proc 2037
        ("timezone", &[Interval, TimeTz], TimeTz) => Some((true, true)),    // pg_proc 2038
        ("time", &[TimeTz], Time) => Some((true, true)),                    // pg_proc 2046
        ("timetz", &[Time], TimeTz) => Some((false, true)),                 // pg_proc 2047
        ("isfinite", &[Timestamp], Boolean) => Some((true, true)),          // pg_proc 2048
        ("age", &[Timestamp, Timestamp], Interval) => Some((true, true)),   // pg_proc 2058
        ("age", &[Timestamp], Interval) => Some((false, true)),             // pg_proc 2059
        ("timezone", &[Text, Timestamp], TimestampTz) => Some((true, true)), // pg_proc 2069
        ("timezone", &[Interval, Timestamp], TimestampTz) => Some((true, true)), // pg_proc 2070
        ("substring", &[Text, Text], Text) => Some((true, true)),           // pg_proc 2073
        ("substring", &[Text, Text, Text], Text) => Some((true, true)),     // pg_proc 2074
        ("substr", &[Bytes, Integer, Integer], Bytes) => Some((true, true)), // pg_proc 2085
        ("substr", &[Bytes, Integer], Bytes) => Some((true, true)),         // pg_proc 2086
        ("replace", &[Text, Text, Text], Text) => Some((true, true)),       // pg_proc 2087
        ("split_part", &[Text, Text, Integer], Text) => Some((true, true)), // pg_proc 2088
        ("ceiling", &[Numeric], Numeric) => Some((true, true)),             // pg_proc 2167
        ("power", &[Numeric, Numeric], Numeric) => Some((true, true)),      // pg_proc 2169
        ("ceil", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 2308
        ("floor", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 2309
        ("sign", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 2310
        ("ceiling", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 2320
        ("sinh", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 2462
        ("cosh", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 2463
        ("tanh", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 2464
        ("asinh", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 2465
        ("acosh", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 2466
        ("atanh", &[DoublePrecision], DoublePrecision) => Some((true, true)), // pg_proc 2467
        ("bool", &[Integer], Boolean) => Some((true, true)),                // pg_proc 2557
        ("int4", &[Boolean], Integer) => Some((true, true)),                // pg_proc 2558
        ("justify_interval", &[Interval], Interval) => Some((true, true)),  // pg_proc 2711
        ("text", &[Boolean], Text) => Some((true, true)),                   // pg_proc 2971
        ("left", &[Text, Integer], Text) => Some((true, true)),             // pg_proc 3060
        ("right", &[Text, Integer], Text) => Some((true, true)),            // pg_proc 3061
        ("reverse", &[Text], Text) => Some((true, true)),                   // pg_proc 3062
        ("scale", &[Numeric], Integer) => Some((true, true)),               // pg_proc 3281
        ("sha224", &[Bytes], Bytes) => Some((true, true)),                  // pg_proc 3419
        ("sha256", &[Bytes], Bytes) => Some((true, true)),                  // pg_proc 3420
        ("sha384", &[Bytes], Bytes) => Some((true, true)),                  // pg_proc 3421
        ("sha512", &[Bytes], Bytes) => Some((true, true)),                  // pg_proc 3422
        (
            "make_timestamp",
            &[Integer, Integer, Integer, Integer, Integer, DoublePrecision],
            Timestamp,
        ) => Some((true, true)), // pg_proc 3461
        (
            "make_timestamptz",
            &[Integer, Integer, Integer, Integer, Integer, DoublePrecision],
            TimestampTz,
        ) => Some((false, true)), // pg_proc 3462
        (
            "make_timestamptz",
            &[
                Integer,
                Integer,
                Integer,
                Integer,
                Integer,
                DoublePrecision,
                Text,
            ],
            TimestampTz,
        ) => Some((false, true)), // pg_proc 3463
        (
            "make_interval",
            &[
                Integer,
                Integer,
                Integer,
                Integer,
                Integer,
                Integer,
                DoublePrecision,
            ],
            Interval,
        ) => Some((true, true)), // pg_proc 3464
        ("starts_with", &[Text, Text], Boolean) => Some((true, true)),      // pg_proc 3696
        ("make_date", &[Integer, Integer, Integer], Date) => Some((true, true)), // pg_proc 3846
        ("make_time", &[Integer, Integer, DoublePrecision], Time) => Some((true, true)), // pg_proc 3847
        ("min_scale", &[Numeric], Integer) => Some((true, true)), // pg_proc 5042
        ("trim_scale", &[Numeric], Numeric) => Some((true, true)), // pg_proc 5043
        ("gcd", &[Integer, Integer], Integer) => Some((true, true)), // pg_proc 5044
        ("gcd", &[BigInt, BigInt], BigInt) => Some((true, true)), // pg_proc 5045
        ("lcm", &[Integer, Integer], Integer) => Some((true, true)), // pg_proc 5046
        ("lcm", &[BigInt, BigInt], BigInt) => Some((true, true)), // pg_proc 5047
        ("gcd", &[Numeric, Numeric], Numeric) => Some((true, true)), // pg_proc 5048
        ("lcm", &[Numeric, Numeric], Numeric) => Some((true, true)), // pg_proc 5049
        ("date_bin", &[Interval, Timestamp, Timestamp], Timestamp) => Some((true, true)), // pg_proc 6177
        ("date_bin", &[Interval, TimestampTz, TimestampTz], TimestampTz) => Some((true, true)), // pg_proc 6178
        ("ltrim", &[Bytes, Bytes], Bytes) => Some((true, true)), // pg_proc 6195
        ("rtrim", &[Bytes, Bytes], Bytes) => Some((true, true)), // pg_proc 6196
        ("extract", &[Text, Date], Numeric) => Some((true, true)), // pg_proc 6199
        ("extract", &[Text, Time], Numeric) => Some((true, true)), // pg_proc 6200
        ("extract", &[Text, TimeTz], Numeric) => Some((true, true)), // pg_proc 6201
        ("extract", &[Text, Timestamp], Numeric) => Some((true, true)), // pg_proc 6202
        ("extract", &[Text, TimestampTz], Numeric) => Some((false, true)), // pg_proc 6203
        ("extract", &[Text, Interval], Numeric) => Some((true, true)), // pg_proc 6204
        ("timezone", &[TimestampTz], Timestamp) => Some((false, true)), // pg_proc 6334
        ("timezone", &[Timestamp], TimestampTz) => Some((false, true)), // pg_proc 6335
        ("timezone", &[TimeTz], TimeTz) => Some((false, true)),  // pg_proc 6336
        ("bytea", &[SmallInt], Bytes) => Some((true, true)),     // pg_proc 6367
        ("bytea", &[Integer], Bytes) => Some((true, true)),      // pg_proc 6368
        ("bytea", &[BigInt], Bytes) => Some((true, true)),       // pg_proc 6369
        ("int2", &[Bytes], SmallInt) => Some((true, true)),      // pg_proc 6370
        ("int4", &[Bytes], Integer) => Some((true, true)),       // pg_proc 6371
        ("int8", &[Bytes], BigInt) => Some((true, true)),        // pg_proc 6372
        ("reverse", &[Bytes], Bytes) => Some((true, true)),      // pg_proc 6382
        _ => None,
    }
}

fn normalized_signature_types(name: &str, args: &[BoundScalar]) -> Vec<DataType> {
    let mut types: Vec<_> = args.iter().map(|a| a.ty.clone()).collect();
    let seconds = match (name, args.len()) {
        ("make_time", 3) => Some(2),
        ("make_timestamp" | "make_timestamptz", 6 | 7) => Some(5),
        ("make_interval", 0..=7) => Some(6),
        _ => None,
    };
    if let Some(seconds) = seconds {
        for (i, ty) in types.iter_mut().enumerate() {
            if i == seconds && number(ty) {
                *ty = DataType::DoublePrecision;
            } else if integer(ty) {
                *ty = DataType::Integer;
            }
        }
        if name == "make_interval" {
            types.resize(7, DataType::Integer);
            types[6] = DataType::DoublePrecision;
        }
    }
    types
}

fn immutable_cast(from: &DataType, to: &DataType) -> bool {
    from == to
        || number(from) && number(to)
        || integer(from) && *to == DataType::Bytes
        || *from == DataType::Bytes && integer(to)
        || matches!(
            (from, to),
            (DataType::Boolean, DataType::Integer) | (DataType::Integer, DataType::Boolean)
        )
}

impl BoundScalar {
    /// No parameters, rows, clocks or session state can be read by this helper.
    pub(in super::super) fn plan_constant(&self, budget: &mut usize) -> R<Option<Value>> {
        let literal = |value, ty| BoundScalar {
            ty,
            node: Node::Literal(value),
        };
        let empty = Tuple {
            values: vec![],
            version: 0,
        };
        match &self.node {
            Node::Predicate(filter) => Ok(filter
                .plan_constant(budget)?
                .map(|value| value.map_or(Value::Null, Value::Bool))),
            Node::Case(arms, otherwise) => {
                for (condition, result) in arms {
                    match condition.plan_constant(budget)? {
                        Some(Some(true)) => return result.plan_constant(budget),
                        Some(Some(false) | None) => {}
                        None => return Ok(None),
                    }
                }
                otherwise
                    .as_ref()
                    .map_or(Ok(Some(Value::Null)), |result| result.plan_constant(budget))
            }
            Node::Param(_)
            | Node::Column(..)
            | Node::Clock(..)
            | Node::Sequence(..)
            | Node::ZonedOperator(..)
            | Node::CrossCompare(..)
            | Node::SessionCast(..) => Ok(None),
            Node::Literal(v) => {
                charge(budget, v)?;
                Ok(Some(v.clone()))
            }
            Node::TypmodCast(inner, modifier) => {
                let Some(value) = inner.plan_constant(budget)? else {
                    return Ok(None);
                };
                cast::apply(value, &inner.ty, *modifier, budget).map(Some)
            }
            Node::Cast(a) => {
                let value = a.plan_constant(budget)?;
                if !immutable_cast(&a.ty, &self.ty) {
                    return Ok(None);
                }
                let Some(value) = value else {
                    return Ok(None);
                };
                let expression = BoundScalar {
                    ty: self.ty.clone(),
                    node: Node::Cast(Box::new(literal(value, a.ty.clone()))),
                };
                expression.eval(&empty, &[], budget).map(Some)
            }
            Node::Call(name, args) if name == "coalesce" => {
                for a in args {
                    match a.plan_constant(budget)? {
                        Some(Value::Null) => {}
                        Some(value) => return cast_value(value, &self.ty, budget).map(Some),
                        None => return Ok(None),
                    }
                }
                Ok(Some(Value::Null))
            }
            Node::Call(name, args) => {
                let mut values = Vec::with_capacity(args.len());
                let mut dependent = false;
                for a in args {
                    match a.plan_constant(budget)? {
                        Some(v) => values.push(literal(v, a.ty.clone())),
                        None => dependent = true,
                    }
                }
                let types = normalized_signature_types(name, args);
                let Some((true, _strict)) = (arithmetic::internal_name(name)
                    || greatest::known_name(name))
                .then_some((true, true))
                .or_else(|| signature(name, &types, &self.ty)) else {
                    return Ok(None);
                };
                if dependent {
                    return Ok(None);
                }
                let expression = BoundScalar {
                    ty: self.ty.clone(),
                    node: Node::Call(name.clone(), values),
                };
                expression.eval(&empty, &[], budget).map(Some)
            }
        }
    }
}

impl BoundScalar {
    /// CHECK admits only routines and casts marked immutable by this binder's metadata.
    pub(in super::super) fn constraint_immutable(&self) -> bool {
        match &self.node {
            Node::Predicate(filter) => super::super::constraints::immutable(filter),
            Node::Case(arms, otherwise) => {
                arms.iter().all(|(condition, result)| {
                    super::super::constraints::immutable(condition) && result.constraint_immutable()
                }) && otherwise
                    .as_ref()
                    .is_none_or(|result| result.constraint_immutable())
            }
            Node::Literal(_) | Node::Column(..) => true,
            Node::Param(_)
            | Node::Clock(..)
            | Node::Sequence(..)
            | Node::SessionCast(..)
            | Node::ZonedOperator(..) => false,
            // PG18: comparisons through timestamptz or timetz depend on TimeZone (STABLE);
            // date/timestamp and time/interval pairs are immutable.
            Node::CrossCompare(_, a, b, _) => {
                a.constraint_immutable()
                    && b.constraint_immutable()
                    && ![&a.ty, &b.ty]
                        .iter()
                        .any(|ty| matches!(ty, DataType::TimestampTz | DataType::TimeTz))
            }
            Node::TypmodCast(value, _) => value.constraint_immutable(),
            Node::Cast(value) => {
                value.constraint_immutable() && immutable_cast(&value.ty, &self.ty)
            }
            Node::Call(name, args) if name == "coalesce" || greatest::known_name(name) => {
                args.iter().all(Self::constraint_immutable)
            }
            Node::Call(name, args) => {
                args.iter().all(Self::constraint_immutable)
                    && arithmetic::internal_name(name)
                        .then_some((true, true))
                        .or_else(|| {
                            signature(name, &normalized_signature_types(name, args), &self.ty)
                        })
                        .is_some_and(|(immutable, _)| immutable)
            }
        }
    }
    pub(in super::super) fn insert_input_only(&self) -> bool {
        match &self.node {
            Node::Predicate(_) | Node::Case(..) => false,
            Node::Literal(_) | Node::Param(_) => true,
            Node::Column(..) => false,
            Node::Clock(..) => true,
            Node::SessionCast(value, _) | Node::TypmodCast(value, _) => value.insert_input_only(),
            Node::CrossCompare(_, a, b, _) => a.insert_input_only() && b.insert_input_only(),
            Node::Sequence(_, args, _) | Node::ZonedOperator(_, args, _) => {
                args.iter().all(Self::insert_input_only)
            }
            Node::Cast(value) => {
                value.insert_input_only()
                    && (immutable_cast(&value.ty, &self.ty)
                        || value.ty == DataType::Text && number(&self.ty)
                        || self.ty == DataType::Text
                            && (number(&value.ty) || value.ty == DataType::Boolean))
            }
            Node::Call(name, args) if name == "coalesce" || greatest::known_name(name) => {
                args.iter().all(Self::insert_input_only)
            }
            Node::Call(name, args) => {
                if !args.iter().all(Self::insert_input_only) {
                    return false;
                }
                let types = normalized_signature_types(name, args);
                arithmetic::internal_name(name)
                    .then_some((true, true))
                    .or_else(|| signature(name, &types, &self.ty))
                    .is_some_and(|(immutable, _)| immutable)
            }
        }
    }
}

#[cfg(test)]
mod clock_metadata_tests {
    use super::*;

    #[test]
    fn check_immutability_normalizes_constructor_numeric_arguments() {
        for (name, count, result, expected) in [
            ("make_time", 3, DataType::Time, true),
            ("make_timestamp", 6, DataType::Timestamp, true),
            ("make_interval", 0, DataType::Interval, true),
            ("make_interval", 7, DataType::Interval, true),
            ("make_timestamptz", 6, DataType::TimestampTz, false),
        ] {
            let value = BoundScalar {
                ty: result,
                node: Node::Call(
                    name.into(),
                    (0..count)
                        .map(|_| BoundScalar {
                            ty: DataType::BigInt,
                            node: Node::Literal(Value::Null),
                        })
                        .collect(),
                ),
            };
            assert_eq!(value.constraint_immutable(), expected, "{name}/{count}");
            assert_eq!(value.insert_input_only(), expected, "{name}/{count}");
        }
    }

    #[test]
    fn check_immutability_uses_overload_and_cast_metadata() {
        let scalar = |ty| BoundScalar {
            ty,
            node: Node::Literal(Value::Null),
        };
        let cast = |from, to| BoundScalar {
            ty: to,
            node: Node::Cast(Box::new(scalar(from))),
        };
        assert!(cast(DataType::Integer, DataType::BigInt).constraint_immutable());
        assert!(!cast(DataType::TimestampTz, DataType::Timestamp).constraint_immutable());
        assert!(!cast(DataType::Timestamp, DataType::TimestampTz).constraint_immutable());
        for (argument, expected) in [(DataType::Timestamp, true), (DataType::TimestampTz, false)] {
            let extract = BoundScalar {
                ty: DataType::Numeric,
                node: Node::Call(
                    "extract".into(),
                    vec![scalar(DataType::Text), scalar(argument)],
                ),
            };
            assert_eq!(extract.constraint_immutable(), expected);
        }
    }

    #[test]
    fn clock_overloads_are_stable_or_volatile_and_never_immutable() {
        for name in [
            "now",
            "transaction_timestamp",
            "statement_timestamp",
            "current_timestamp",
            "localtimestamp",
            "current_date",
            "current_time",
            "localtime",
            "clock_timestamp",
        ] {
            let (ty, volatility) = clock_signature(name).unwrap();
            assert_eq!(
                volatility,
                if name == "clock_timestamp" {
                    Volatility::Volatile
                } else {
                    Volatility::Stable
                }
            );
            assert_eq!(signature(name, &[], &ty), Some((false, false)));
            let value = BoundScalar {
                ty,
                node: Node::Clock(name.into(), -1, None),
            };
            assert!(!value.constraint_immutable());
            assert_eq!(value.plan_constant(&mut 0).unwrap(), None);
        }
        assert_eq!(
            clock_signature("localtime"),
            Some((DataType::Time, Volatility::Stable))
        );
        assert_eq!(
            clock_signature("current_time"),
            Some((DataType::TimeTz, Volatility::Stable))
        );
        assert_eq!(
            signature("now", &[DataType::Integer], &DataType::TimestampTz),
            None
        );
        assert_eq!(
            signature(
                "current_timestamp",
                &[DataType::Integer],
                &DataType::TimestampTz
            ),
            Some((false, false))
        );
    }
}
