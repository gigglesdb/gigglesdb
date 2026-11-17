use giggles_icu::Icu;
#[test]
fn locale_case_mappings() {
    let icu = Icu::load().expect("pinned ICU runtime");
    for (locale, op, input, output) in [
        ("tr", "lower", "I", "\u{131}"),
        ("tr", "upper", "i", "\u{130}"),
        ("de", "upper", "\u{df}", "SS"),
        ("und", "initcap", "hello WORLD", "Hello World"),
    ] {
        assert_eq!(icu.case_map(locale, op, input).unwrap(), output);
    }
}
