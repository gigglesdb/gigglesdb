use giggles_pg_types::integer::{IntegerError, parse_i16, parse_i32, parse_i64};
// Actual pinned PostgreSQL 18 Parse0/Describe/Bind oracle; seed 680180.
// NUL cases are excluded: PostgreSQL rejects them at text admission.
#[test]
fn postgres18_actual_text_input_corpus() {
    assert_eq!(parse_i16(""), Err(IntegerError::Syntax));
    assert_eq!(parse_i16(" "), Err(IntegerError::Syntax));
    assert_eq!(
        parse_i16("\u{9}\u{a}\u{b}\u{c}\u{d} 1 \u{9}\u{a}\u{b}\u{c}\u{d}"),
        Ok(1)
    );
    assert_eq!(parse_i16("+"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("-"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("++1"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("+ 1"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("01"), Ok(1));
    assert_eq!(parse_i16("00009"), Ok(9));
    assert_eq!(parse_i16("-0"), Ok(0));
    assert_eq!(parse_i16("+0"), Ok(0));
    assert_eq!(parse_i16("_1"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("1_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("1__0"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("1_0"), Ok(10));
    assert_eq!(parse_i16("0x_1"), Ok(1));
    assert_eq!(parse_i16("0X_Ff"), Ok(255));
    assert_eq!(parse_i16("0o_7"), Ok(7));
    assert_eq!(parse_i16("0b_1"), Ok(1));
    assert_eq!(parse_i16("0x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("0x_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("0x__1"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("0b2"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("0o8"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("0xg"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("0x1_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("0x1__2"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("1 2"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("1.0"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("1e0"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("\u{a0}1"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("1\u{a0}"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("\u{661}"), Err(IntegerError::Syntax));
    assert_eq!(
        parse_i16(
            "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"
        ),
        Ok(1)
    );
    assert_eq!(parse_i16("32767"), Ok(32767));
    assert_eq!(parse_i16("32767x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("32767_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("32767 "), Ok(32767));
    assert_eq!(parse_i16("32767_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-32767"), Ok(-32767));
    assert_eq!(parse_i16("-32767x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("-32767_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("-32767 "), Ok(-32767));
    assert_eq!(parse_i16("-32767_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0x7fff"), Ok(32767));
    assert_eq!(parse_i16("0x7fffx"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("-0x7fff"), Ok(-32767));
    assert_eq!(parse_i16("-0x7fffx"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("0o77777"), Ok(32767));
    assert_eq!(parse_i16("0o77777x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("-0o77777"), Ok(-32767));
    assert_eq!(parse_i16("-0o77777x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("0b111111111111111"), Ok(32767));
    assert_eq!(parse_i16("0b111111111111111x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("-0b111111111111111"), Ok(-32767));
    assert_eq!(parse_i16("-0b111111111111111x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("32768"), Err(IntegerError::Range));
    assert_eq!(parse_i16("32768x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("32768_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("32768 "), Err(IntegerError::Range));
    assert_eq!(parse_i16("32768_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-32768"), Ok(-32768));
    assert_eq!(parse_i16("-32768x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("-32768_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("-32768 "), Ok(-32768));
    assert_eq!(parse_i16("-32768_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0x8000"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0x8000x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("-0x8000"), Ok(-32768));
    assert_eq!(parse_i16("-0x8000x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("0o100000"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0o100000x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("-0o100000"), Ok(-32768));
    assert_eq!(parse_i16("-0o100000x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("0b1000000000000000"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0b1000000000000000x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("-0b1000000000000000"), Ok(-32768));
    assert_eq!(parse_i16("-0b1000000000000000x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("32769"), Err(IntegerError::Range));
    assert_eq!(parse_i16("32769x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("32769_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("32769 "), Err(IntegerError::Range));
    assert_eq!(parse_i16("32769_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-32769"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-32769x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("-32769_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("-32769 "), Err(IntegerError::Range));
    assert_eq!(parse_i16("-32769_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0x8001"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0x8001x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("-0x8001"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-0x8001x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("0o100001"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0o100001x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("-0o100001"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-0o100001x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("0b1000000000000001"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0b1000000000000001x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("-0b1000000000000001"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-0b1000000000000001x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("32770"), Err(IntegerError::Range));
    assert_eq!(parse_i16("32770x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("32770_"), Err(IntegerError::Range));
    assert_eq!(parse_i16("32770 "), Err(IntegerError::Range));
    assert_eq!(parse_i16("32770_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-32770"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-32770x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-32770_"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-32770 "), Err(IntegerError::Range));
    assert_eq!(parse_i16("-32770_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0x8002"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0x8002x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("-0x8002"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-0x8002x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("0o100002"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0o100002x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("-0o100002"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-0o100002x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i16("0b1000000000000010"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0b1000000000000010x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-0b1000000000000010"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-0b1000000000000010x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("327680"), Err(IntegerError::Range));
    assert_eq!(parse_i16("327680x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("327680_"), Err(IntegerError::Range));
    assert_eq!(parse_i16("327680 "), Err(IntegerError::Range));
    assert_eq!(parse_i16("327680_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-327680"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-327680x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-327680_"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-327680 "), Err(IntegerError::Range));
    assert_eq!(parse_i16("-327680_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0x50000"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0x50000x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-0x50000"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-0x50000x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0o1200000"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0o1200000x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-0o1200000"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-0o1200000x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0b1010000000000000000"), Err(IntegerError::Range));
    assert_eq!(
        parse_i16("0b1010000000000000000x"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i16("-0b1010000000000000000"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i16("-0b1010000000000000000x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i16("327690"), Err(IntegerError::Range));
    assert_eq!(parse_i16("327690x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("327690_"), Err(IntegerError::Range));
    assert_eq!(parse_i16("327690 "), Err(IntegerError::Range));
    assert_eq!(parse_i16("327690_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-327690"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-327690x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-327690_"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-327690 "), Err(IntegerError::Range));
    assert_eq!(parse_i16("-327690_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0x5000a"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0x5000ax"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-0x5000a"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-0x5000ax"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0o1200012"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0o1200012x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-0o1200012"), Err(IntegerError::Range));
    assert_eq!(parse_i16("-0o1200012x"), Err(IntegerError::Range));
    assert_eq!(parse_i16("0b1010000000000001010"), Err(IntegerError::Range));
    assert_eq!(
        parse_i16("0b1010000000000001010x"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i16("-0b1010000000000001010"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i16("-0b1010000000000001010x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i16("-1_2_3_3_7"), Ok(-12337));
    assert_eq!(parse_i16("+1_0_0_0"), Ok(1000));
    assert_eq!(parse_i16("+2_7_2_2_8"), Ok(27228));
    assert_eq!(parse_i16("+2_8_2_3_5"), Ok(28235));
    assert_eq!(parse_i16("+2_6_5_5_9"), Ok(26559));
    assert_eq!(parse_i16("-1_9_9_6_8"), Ok(-19968));
    assert_eq!(parse_i16("-2_8_4_9_8"), Ok(-28498));
    assert_eq!(parse_i16("-1_8_9_5_2"), Ok(-18952));
    assert_eq!(parse_i16("+1_6_0_4_5"), Ok(16045));
    assert_eq!(parse_i16("+1_9_1_7"), Ok(1917));
    assert_eq!(parse_i16("+1_4_2_0_8"), Ok(14208));
    assert_eq!(parse_i16("-2_6_2_5_3"), Ok(-26253));
    assert_eq!(parse_i16("+2_9_7_9_8"), Ok(29798));
    assert_eq!(parse_i16("-1_0_8_3_0"), Ok(-10830));
    assert_eq!(parse_i16("-2_7_4_4_0"), Ok(-27440));
    assert_eq!(parse_i16("-4_9_5_4"), Ok(-4954));
    assert_eq!(parse_i16("+5_4_1"), Ok(541));
    assert_eq!(parse_i16("-1_0_5_0_6"), Ok(-10506));
    assert_eq!(parse_i16("-2_4_2_6_7"), Ok(-24267));
    assert_eq!(parse_i16("+4_6_6_3"), Ok(4663));
    assert_eq!(parse_i32(""), Err(IntegerError::Syntax));
    assert_eq!(parse_i32(" "), Err(IntegerError::Syntax));
    assert_eq!(
        parse_i32("\u{9}\u{a}\u{b}\u{c}\u{d} 1 \u{9}\u{a}\u{b}\u{c}\u{d}"),
        Ok(1)
    );
    assert_eq!(parse_i32("+"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("-"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("++1"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("+ 1"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("01"), Ok(1));
    assert_eq!(parse_i32("00009"), Ok(9));
    assert_eq!(parse_i32("-0"), Ok(0));
    assert_eq!(parse_i32("+0"), Ok(0));
    assert_eq!(parse_i32("_1"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("1_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("1__0"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("1_0"), Ok(10));
    assert_eq!(parse_i32("0x_1"), Ok(1));
    assert_eq!(parse_i32("0X_Ff"), Ok(255));
    assert_eq!(parse_i32("0o_7"), Ok(7));
    assert_eq!(parse_i32("0b_1"), Ok(1));
    assert_eq!(parse_i32("0x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("0x_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("0x__1"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("0b2"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("0o8"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("0xg"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("0x1_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("0x1__2"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("1 2"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("1.0"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("1e0"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("\u{a0}1"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("1\u{a0}"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("\u{661}"), Err(IntegerError::Syntax));
    assert_eq!(
        parse_i32(
            "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"
        ),
        Ok(1)
    );
    assert_eq!(parse_i32("2147483647"), Ok(2147483647));
    assert_eq!(parse_i32("2147483647x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("2147483647_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("2147483647 "), Ok(2147483647));
    assert_eq!(parse_i32("2147483647_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-2147483647"), Ok(-2147483647));
    assert_eq!(parse_i32("-2147483647x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("-2147483647_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("-2147483647 "), Ok(-2147483647));
    assert_eq!(parse_i32("-2147483647_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("0x7fffffff"), Ok(2147483647));
    assert_eq!(parse_i32("0x7fffffffx"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("-0x7fffffff"), Ok(-2147483647));
    assert_eq!(parse_i32("-0x7fffffffx"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("0o17777777777"), Ok(2147483647));
    assert_eq!(parse_i32("0o17777777777x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("-0o17777777777"), Ok(-2147483647));
    assert_eq!(parse_i32("-0o17777777777x"), Err(IntegerError::Syntax));
    assert_eq!(
        parse_i32("0b1111111111111111111111111111111"),
        Ok(2147483647)
    );
    assert_eq!(
        parse_i32("0b1111111111111111111111111111111x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(
        parse_i32("-0b1111111111111111111111111111111"),
        Ok(-2147483647)
    );
    assert_eq!(
        parse_i32("-0b1111111111111111111111111111111x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(parse_i32("2147483648"), Err(IntegerError::Range));
    assert_eq!(parse_i32("2147483648x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("2147483648_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("2147483648 "), Err(IntegerError::Range));
    assert_eq!(parse_i32("2147483648_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-2147483648"), Ok(-2147483648));
    assert_eq!(parse_i32("-2147483648x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("-2147483648_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("-2147483648 "), Ok(-2147483648));
    assert_eq!(parse_i32("-2147483648_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("0x80000000"), Err(IntegerError::Range));
    assert_eq!(parse_i32("0x80000000x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("-0x80000000"), Ok(-2147483648));
    assert_eq!(parse_i32("-0x80000000x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("0o20000000000"), Err(IntegerError::Range));
    assert_eq!(parse_i32("0o20000000000x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("-0o20000000000"), Ok(-2147483648));
    assert_eq!(parse_i32("-0o20000000000x"), Err(IntegerError::Syntax));
    assert_eq!(
        parse_i32("0b10000000000000000000000000000000"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i32("0b10000000000000000000000000000000x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(
        parse_i32("-0b10000000000000000000000000000000"),
        Ok(-2147483648)
    );
    assert_eq!(
        parse_i32("-0b10000000000000000000000000000000x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(parse_i32("2147483649"), Err(IntegerError::Range));
    assert_eq!(parse_i32("2147483649x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("2147483649_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("2147483649 "), Err(IntegerError::Range));
    assert_eq!(parse_i32("2147483649_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-2147483649"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-2147483649x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("-2147483649_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("-2147483649 "), Err(IntegerError::Range));
    assert_eq!(parse_i32("-2147483649_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("0x80000001"), Err(IntegerError::Range));
    assert_eq!(parse_i32("0x80000001x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("-0x80000001"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-0x80000001x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("0o20000000001"), Err(IntegerError::Range));
    assert_eq!(parse_i32("0o20000000001x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("-0o20000000001"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-0o20000000001x"), Err(IntegerError::Syntax));
    assert_eq!(
        parse_i32("0b10000000000000000000000000000001"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i32("0b10000000000000000000000000000001x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(
        parse_i32("-0b10000000000000000000000000000001"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i32("-0b10000000000000000000000000000001x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(parse_i32("2147483650"), Err(IntegerError::Range));
    assert_eq!(parse_i32("2147483650x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("2147483650_"), Err(IntegerError::Range));
    assert_eq!(parse_i32("2147483650 "), Err(IntegerError::Range));
    assert_eq!(parse_i32("2147483650_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-2147483650"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-2147483650x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-2147483650_"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-2147483650 "), Err(IntegerError::Range));
    assert_eq!(parse_i32("-2147483650_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("0x80000002"), Err(IntegerError::Range));
    assert_eq!(parse_i32("0x80000002x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("-0x80000002"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-0x80000002x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("0o20000000002"), Err(IntegerError::Range));
    assert_eq!(parse_i32("0o20000000002x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("-0o20000000002"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-0o20000000002x"), Err(IntegerError::Syntax));
    assert_eq!(
        parse_i32("0b10000000000000000000000000000010"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i32("0b10000000000000000000000000000010x"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i32("-0b10000000000000000000000000000010"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i32("-0b10000000000000000000000000000010x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i32("21474836480"), Err(IntegerError::Range));
    assert_eq!(parse_i32("21474836480x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("21474836480_"), Err(IntegerError::Range));
    assert_eq!(parse_i32("21474836480 "), Err(IntegerError::Range));
    assert_eq!(parse_i32("21474836480_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-21474836480"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-21474836480x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-21474836480_"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-21474836480 "), Err(IntegerError::Range));
    assert_eq!(parse_i32("-21474836480_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("0x500000000"), Err(IntegerError::Range));
    assert_eq!(parse_i32("0x500000000x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-0x500000000"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-0x500000000x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("0o240000000000"), Err(IntegerError::Range));
    assert_eq!(parse_i32("0o240000000000x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-0o240000000000"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-0o240000000000x"), Err(IntegerError::Range));
    assert_eq!(
        parse_i32("0b10100000000000000000000000000000000"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i32("0b10100000000000000000000000000000000x"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i32("-0b10100000000000000000000000000000000"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i32("-0b10100000000000000000000000000000000x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i32("21474836490"), Err(IntegerError::Range));
    assert_eq!(parse_i32("21474836490x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("21474836490_"), Err(IntegerError::Range));
    assert_eq!(parse_i32("21474836490 "), Err(IntegerError::Range));
    assert_eq!(parse_i32("21474836490_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-21474836490"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-21474836490x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-21474836490_"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-21474836490 "), Err(IntegerError::Range));
    assert_eq!(parse_i32("-21474836490_0x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("0x50000000a"), Err(IntegerError::Range));
    assert_eq!(parse_i32("0x50000000ax"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-0x50000000a"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-0x50000000ax"), Err(IntegerError::Range));
    assert_eq!(parse_i32("0o240000000012"), Err(IntegerError::Range));
    assert_eq!(parse_i32("0o240000000012x"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-0o240000000012"), Err(IntegerError::Range));
    assert_eq!(parse_i32("-0o240000000012x"), Err(IntegerError::Range));
    assert_eq!(
        parse_i32("0b10100000000000000000000000000001010"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i32("0b10100000000000000000000000000001010x"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i32("-0b10100000000000000000000000000001010"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i32("-0b10100000000000000000000000000001010x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i32("-8_4_6_0_3_7_1_8_3"), Ok(-846037183));
    assert_eq!(parse_i32("+1_5_3_6_0_8_6_6_9_1"), Ok(1536086691));
    assert_eq!(parse_i32("-1_5_2_7_0_3_9_8_6_3"), Ok(-1527039863));
    assert_eq!(parse_i32("-9_0_2_5_1_5_0_3_8"), Ok(-902515038));
    assert_eq!(parse_i32("-2_0_2_5_6_8_8_4_8_6"), Ok(-2025688486));
    assert_eq!(parse_i32("+1_6_7_3_5_1_4_1_1_7"), Ok(1673514117));
    assert_eq!(parse_i32("-2_4_5_2_6_2_4_4_9"), Ok(-245262449));
    assert_eq!(parse_i32("-7_3_7_9_6_3_1_0_1"), Ok(-737963101));
    assert_eq!(parse_i32("-1_3_7_8_2_4_2_8_6"), Ok(-137824286));
    assert_eq!(parse_i32("+3_5_8_7_4_3_4_4_4"), Ok(358743444));
    assert_eq!(parse_i32("+1_0_5_6_6_9_9_7_6_9"), Ok(1056699769));
    assert_eq!(parse_i32("+2_0_2_1_4_7_7_9_4_6"), Ok(2021477946));
    assert_eq!(parse_i32("-1_5_4_3_8_3_6_1_0_7"), Ok(-1543836107));
    assert_eq!(parse_i32("-5_0_1_9_6_1_8_7_4"), Ok(-501961874));
    assert_eq!(parse_i32("-1_8_4_0_8_7_7_5_8_6"), Ok(-1840877586));
    assert_eq!(parse_i32("-1_4_8_8_9_3_4_1_1_4"), Ok(-1488934114));
    assert_eq!(parse_i32("-1_8_7_7_6_7_1_1_7_0"), Ok(-1877671170));
    assert_eq!(parse_i32("-1_5_7_8_0_8_1_2_8_2"), Ok(-1578081282));
    assert_eq!(parse_i32("-1_0_9_0_5_3_7_4_5_5"), Ok(-1090537455));
    assert_eq!(parse_i32("-1_0_8_9_5_0_1_7_9_4"), Ok(-1089501794));
    assert_eq!(parse_i64(""), Err(IntegerError::Syntax));
    assert_eq!(parse_i64(" "), Err(IntegerError::Syntax));
    assert_eq!(
        parse_i64("\u{9}\u{a}\u{b}\u{c}\u{d} 1 \u{9}\u{a}\u{b}\u{c}\u{d}"),
        Ok(1)
    );
    assert_eq!(parse_i64("+"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("-"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("++1"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("+ 1"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("01"), Ok(1));
    assert_eq!(parse_i64("00009"), Ok(9));
    assert_eq!(parse_i64("-0"), Ok(0));
    assert_eq!(parse_i64("+0"), Ok(0));
    assert_eq!(parse_i64("_1"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("1_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("1__0"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("1_0"), Ok(10));
    assert_eq!(parse_i64("0x_1"), Ok(1));
    assert_eq!(parse_i64("0X_Ff"), Ok(255));
    assert_eq!(parse_i64("0o_7"), Ok(7));
    assert_eq!(parse_i64("0b_1"), Ok(1));
    assert_eq!(parse_i64("0x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("0x_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("0x__1"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("0b2"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("0o8"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("0xg"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("0x1_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("0x1__2"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("1 2"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("1.0"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("1e0"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("\u{a0}1"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("1\u{a0}"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("\u{661}"), Err(IntegerError::Syntax));
    assert_eq!(
        parse_i64(
            "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"
        ),
        Ok(1)
    );
    assert_eq!(parse_i64("9223372036854775807"), Ok(9223372036854775807));
    assert_eq!(parse_i64("9223372036854775807x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("9223372036854775807_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("9223372036854775807 "), Ok(9223372036854775807));
    assert_eq!(
        parse_i64("9223372036854775807_0x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i64("-9223372036854775807"), Ok(-9223372036854775807));
    assert_eq!(
        parse_i64("-9223372036854775807x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(
        parse_i64("-9223372036854775807_"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(parse_i64("-9223372036854775807 "), Ok(-9223372036854775807));
    assert_eq!(
        parse_i64("-9223372036854775807_0x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i64("0x7fffffffffffffff"), Ok(9223372036854775807));
    assert_eq!(parse_i64("0x7fffffffffffffffx"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("-0x7fffffffffffffff"), Ok(-9223372036854775807));
    assert_eq!(parse_i64("-0x7fffffffffffffffx"), Err(IntegerError::Syntax));
    assert_eq!(
        parse_i64("0o777777777777777777777"),
        Ok(9223372036854775807)
    );
    assert_eq!(
        parse_i64("0o777777777777777777777x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(
        parse_i64("-0o777777777777777777777"),
        Ok(-9223372036854775807)
    );
    assert_eq!(
        parse_i64("-0o777777777777777777777x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(
        parse_i64("0b111111111111111111111111111111111111111111111111111111111111111"),
        Ok(9223372036854775807)
    );
    assert_eq!(
        parse_i64("0b111111111111111111111111111111111111111111111111111111111111111x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(
        parse_i64("-0b111111111111111111111111111111111111111111111111111111111111111"),
        Ok(-9223372036854775807)
    );
    assert_eq!(
        parse_i64("-0b111111111111111111111111111111111111111111111111111111111111111x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(parse_i64("9223372036854775808"), Err(IntegerError::Range));
    assert_eq!(parse_i64("9223372036854775808x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("9223372036854775808_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("9223372036854775808 "), Err(IntegerError::Range));
    assert_eq!(
        parse_i64("9223372036854775808_0x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i64("-9223372036854775808"), Ok(-9223372036854775808));
    assert_eq!(
        parse_i64("-9223372036854775808x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(
        parse_i64("-9223372036854775808_"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(parse_i64("-9223372036854775808 "), Ok(-9223372036854775808));
    assert_eq!(
        parse_i64("-9223372036854775808_0x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i64("0x8000000000000000"), Err(IntegerError::Range));
    assert_eq!(parse_i64("0x8000000000000000x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("-0x8000000000000000"), Ok(-9223372036854775808));
    assert_eq!(parse_i64("-0x8000000000000000x"), Err(IntegerError::Syntax));
    assert_eq!(
        parse_i64("0o1000000000000000000000"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("0o1000000000000000000000x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(
        parse_i64("-0o1000000000000000000000"),
        Ok(-9223372036854775808)
    );
    assert_eq!(
        parse_i64("-0o1000000000000000000000x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(
        parse_i64("0b1000000000000000000000000000000000000000000000000000000000000000"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("0b1000000000000000000000000000000000000000000000000000000000000000x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(
        parse_i64("-0b1000000000000000000000000000000000000000000000000000000000000000"),
        Ok(-9223372036854775808)
    );
    assert_eq!(
        parse_i64("-0b1000000000000000000000000000000000000000000000000000000000000000x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(parse_i64("9223372036854775809"), Err(IntegerError::Range));
    assert_eq!(parse_i64("9223372036854775809x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("9223372036854775809_"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("9223372036854775809 "), Err(IntegerError::Range));
    assert_eq!(
        parse_i64("9223372036854775809_0x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i64("-9223372036854775809"), Err(IntegerError::Range));
    assert_eq!(
        parse_i64("-9223372036854775809x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(
        parse_i64("-9223372036854775809_"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(parse_i64("-9223372036854775809 "), Err(IntegerError::Range));
    assert_eq!(
        parse_i64("-9223372036854775809_0x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i64("0x8000000000000001"), Err(IntegerError::Range));
    assert_eq!(parse_i64("0x8000000000000001x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("-0x8000000000000001"), Err(IntegerError::Range));
    assert_eq!(parse_i64("-0x8000000000000001x"), Err(IntegerError::Syntax));
    assert_eq!(
        parse_i64("0o1000000000000000000001"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("0o1000000000000000000001x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(
        parse_i64("-0o1000000000000000000001"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-0o1000000000000000000001x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(
        parse_i64("0b1000000000000000000000000000000000000000000000000000000000000001"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("0b1000000000000000000000000000000000000000000000000000000000000001x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(
        parse_i64("-0b1000000000000000000000000000000000000000000000000000000000000001"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-0b1000000000000000000000000000000000000000000000000000000000000001x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(parse_i64("9223372036854775810"), Err(IntegerError::Range));
    assert_eq!(parse_i64("9223372036854775810x"), Err(IntegerError::Range));
    assert_eq!(parse_i64("9223372036854775810_"), Err(IntegerError::Range));
    assert_eq!(parse_i64("9223372036854775810 "), Err(IntegerError::Range));
    assert_eq!(
        parse_i64("9223372036854775810_0x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i64("-9223372036854775810"), Err(IntegerError::Range));
    assert_eq!(parse_i64("-9223372036854775810x"), Err(IntegerError::Range));
    assert_eq!(parse_i64("-9223372036854775810_"), Err(IntegerError::Range));
    assert_eq!(parse_i64("-9223372036854775810 "), Err(IntegerError::Range));
    assert_eq!(
        parse_i64("-9223372036854775810_0x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i64("0x8000000000000002"), Err(IntegerError::Range));
    assert_eq!(parse_i64("0x8000000000000002x"), Err(IntegerError::Syntax));
    assert_eq!(parse_i64("-0x8000000000000002"), Err(IntegerError::Range));
    assert_eq!(parse_i64("-0x8000000000000002x"), Err(IntegerError::Syntax));
    assert_eq!(
        parse_i64("0o1000000000000000000002"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("0o1000000000000000000002x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(
        parse_i64("-0o1000000000000000000002"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-0o1000000000000000000002x"),
        Err(IntegerError::Syntax)
    );
    assert_eq!(
        parse_i64("0b1000000000000000000000000000000000000000000000000000000000000010"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("0b1000000000000000000000000000000000000000000000000000000000000010x"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-0b1000000000000000000000000000000000000000000000000000000000000010"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-0b1000000000000000000000000000000000000000000000000000000000000010x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i64("92233720368547758080"), Err(IntegerError::Range));
    assert_eq!(parse_i64("92233720368547758080x"), Err(IntegerError::Range));
    assert_eq!(parse_i64("92233720368547758080_"), Err(IntegerError::Range));
    assert_eq!(parse_i64("92233720368547758080 "), Err(IntegerError::Range));
    assert_eq!(
        parse_i64("92233720368547758080_0x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i64("-92233720368547758080"), Err(IntegerError::Range));
    assert_eq!(
        parse_i64("-92233720368547758080x"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-92233720368547758080_"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-92233720368547758080 "),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-92233720368547758080_0x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i64("0x50000000000000000"), Err(IntegerError::Range));
    assert_eq!(parse_i64("0x50000000000000000x"), Err(IntegerError::Range));
    assert_eq!(parse_i64("-0x50000000000000000"), Err(IntegerError::Range));
    assert_eq!(parse_i64("-0x50000000000000000x"), Err(IntegerError::Range));
    assert_eq!(
        parse_i64("0o12000000000000000000000"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("0o12000000000000000000000x"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-0o12000000000000000000000"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-0o12000000000000000000000x"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("0b1010000000000000000000000000000000000000000000000000000000000000000"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("0b1010000000000000000000000000000000000000000000000000000000000000000x"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-0b1010000000000000000000000000000000000000000000000000000000000000000"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-0b1010000000000000000000000000000000000000000000000000000000000000000x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i64("92233720368547758090"), Err(IntegerError::Range));
    assert_eq!(parse_i64("92233720368547758090x"), Err(IntegerError::Range));
    assert_eq!(parse_i64("92233720368547758090_"), Err(IntegerError::Range));
    assert_eq!(parse_i64("92233720368547758090 "), Err(IntegerError::Range));
    assert_eq!(
        parse_i64("92233720368547758090_0x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i64("-92233720368547758090"), Err(IntegerError::Range));
    assert_eq!(
        parse_i64("-92233720368547758090x"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-92233720368547758090_"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-92233720368547758090 "),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-92233720368547758090_0x"),
        Err(IntegerError::Range)
    );
    assert_eq!(parse_i64("0x5000000000000000a"), Err(IntegerError::Range));
    assert_eq!(parse_i64("0x5000000000000000ax"), Err(IntegerError::Range));
    assert_eq!(parse_i64("-0x5000000000000000a"), Err(IntegerError::Range));
    assert_eq!(parse_i64("-0x5000000000000000ax"), Err(IntegerError::Range));
    assert_eq!(
        parse_i64("0o12000000000000000000012"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("0o12000000000000000000012x"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-0o12000000000000000000012"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-0o12000000000000000000012x"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("0b1010000000000000000000000000000000000000000000000000000000000001010"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("0b1010000000000000000000000000000000000000000000000000000000000001010x"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-0b1010000000000000000000000000000000000000000000000000000000000001010"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-0b1010000000000000000000000000000000000000000000000000000000000001010x"),
        Err(IntegerError::Range)
    );
    assert_eq!(
        parse_i64("-8_0_7_1_3_5_6_3_2_2_3_7_7_5_5_0_5_0"),
        Ok(-807135632237755050)
    );
    assert_eq!(
        parse_i64("+2_0_3_3_2_5_4_2_9_5_5_4_5_7_0_4_1_8_6"),
        Ok(2033254295545704186)
    );
    assert_eq!(
        parse_i64("-1_8_9_9_7_4_1_6_3_2_8_3_3_7_4_8_5_5_9"),
        Ok(-1899741632833748559)
    );
    assert_eq!(
        parse_i64("-7_3_3_9_0_1_6_7_1_9_5_1_3_1_1_2_8_8_9"),
        Ok(-7339016719513112889)
    );
    assert_eq!(
        parse_i64("-4_4_7_4_3_7_4_9_2_2_7_2_1_1_6_9_6_9_7"),
        Ok(-4474374922721169697)
    );
    assert_eq!(
        parse_i64("-1_4_1_6_8_6_6_5_8_9_0_4_9_4_1_8_9_2_2"),
        Ok(-1416866589049418922)
    );
    assert_eq!(
        parse_i64("+8_7_2_2_3_9_6_7_1_2_3_7_3_3_3_4_9_9_1"),
        Ok(8722396712373334991)
    );
    assert_eq!(
        parse_i64("-1_1_0_9_2_9_2_9_1_2_2_2_2_1_0_3_9_1_0"),
        Ok(-1109292912222103910)
    );
    assert_eq!(
        parse_i64("-4_5_7_5_2_8_1_2_2_2_9_0_9_1_2_8_6_0_1"),
        Ok(-4575281222909128601)
    );
    assert_eq!(
        parse_i64("-4_9_4_0_4_2_7_3_5_8_6_5_5_8_9_6_9_7_2"),
        Ok(-4940427358655896972)
    );
    assert_eq!(
        parse_i64("-1_7_2_9_9_1_4_9_6_0_4_7_1_5_5_6_6_0_2"),
        Ok(-1729914960471556602)
    );
    assert_eq!(
        parse_i64("-5_4_0_4_6_8_7_5_6_0_3_6_5_0_8_8_7_6_8"),
        Ok(-5404687560365088768)
    );
    assert_eq!(
        parse_i64("+6_2_6_8_4_5_0_7_4_9_0_4_2_7_9_0_6_8_9"),
        Ok(6268450749042790689)
    );
    assert_eq!(
        parse_i64("+1_2_9_9_3_9_5_2_5_1_5_1_4_7_7_7_6_5_3"),
        Ok(1299395251514777653)
    );
    assert_eq!(
        parse_i64("-2_3_2_8_2_5_3_7_8_4_1_4_7_9_1_8_7_3_7"),
        Ok(-2328253784147918737)
    );
    assert_eq!(
        parse_i64("+8_6_5_2_8_8_6_6_4_1_0_0_0_3_5_2_3_0_6"),
        Ok(8652886641000352306)
    );
    assert_eq!(
        parse_i64("-8_3_3_7_0_9_6_3_2_6_4_8_2_8_0_7_6_4_1"),
        Ok(-8337096326482807641)
    );
    assert_eq!(
        parse_i64("-8_6_9_3_5_4_1_9_8_8_4_4_1_3_9_1_8_8_0"),
        Ok(-8693541988441391880)
    );
    assert_eq!(
        parse_i64("+7_1_9_4_8_7_4_5_4_0_3_1_5_4_1_7_9_4_8"),
        Ok(7194874540315417948)
    );
    assert_eq!(
        parse_i64("+1_7_9_3_7_7_3_4_7_3_4_9_1_4_0_2_4"),
        Ok(17937734734914024)
    );
}
