//! PostgreSQL 18.6 regular expression engine.

mod ctype_tables;
mod regc_color;
mod regc_cvec;
mod regc_lex;
mod regc_locale;
mod regc_nfa;
mod regc_pg_locale;
mod regcomp;
mod rege_dfa;
mod regerror;
mod regexec;
mod regguts;
mod regprefix;

pub use regerror::RegErrorCode;
use regguts::Guts;

/// PostgreSQL's pg_wchar.
pub type Chr = u32;
pub const REG_BASIC: i32 = 0;
pub const REG_EXTENDED: i32 = 0o1;
pub const REG_ADVF: i32 = 0o2;
pub const REG_ADVANCED: i32 = 0o3;
pub const REG_QUOTE: i32 = 0o4;
pub const REG_NOSPEC: i32 = REG_QUOTE;
pub const REG_ICASE: i32 = 0o10;
pub const REG_NOSUB: i32 = 0o20;
pub const REG_EXPANDED: i32 = 0o40;
pub const REG_NLSTOP: i32 = 0o100;
pub const REG_NLANCH: i32 = 0o200;
pub const REG_NEWLINE: i32 = 0o300;
pub const REG_PEND: i32 = 0o400;
pub const REG_EXPECT: i32 = 0o1000;
pub const REG_BOSONLY: i32 = 0o2000;
pub const REG_DUMP: i32 = 0o4000;
pub const REG_FAKE: i32 = 0o10000;
pub const REG_PROGRESS: i32 = 0o20000;

pub const REG_NOTBOL: i32 = 0o1;
pub const REG_NOTEOL: i32 = 0o2;
pub const REG_STARTEND: i32 = 0o4;
pub const REG_FTRACE: i32 = 0o10;
pub const REG_MTRACE: i32 = 0o20;
pub const REG_SMALL: i32 = 0o40;

pub const REG_UBACKREF: i64 = 0o1;
pub const REG_ULOOKAROUND: i64 = 0o2;
pub const REG_UBOUNDS: i64 = 0o4;
pub const REG_UBRACES: i64 = 0o10;
pub const REG_UBSALNUM: i64 = 0o20;
pub const REG_UPBOTCH: i64 = 0o40;
pub const REG_UBBS: i64 = 0o100;
pub const REG_UNONPOSIX: i64 = 0o200;
pub const REG_UUNSPEC: i64 = 0o400;
pub const REG_UUNPORT: i64 = 0o1000;
pub const REG_ULOCALE: i64 = 0o2000;
pub const REG_UEMPTYMATCH: i64 = 0o4000;
pub const REG_UIMPOSSIBLE: i64 = 0o10000;
pub const REG_USHORTEST: i64 = 0o20000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RegexLocale {
    C,
    CUtf8,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RegMatch {
    pub so: isize,
    pub eo: isize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RmDetail {
    pub extend: RegMatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrefixKind {
    NoMatch,
    None,
    Prefix,
    Exact,
}

pub type CancelCheck<'a> = &'a dyn Fn() -> bool;

pub struct Regex {
    guts: Guts,
    nsub: usize,
    info: i64,
    cflags: i32,
    locale: RegexLocale,
}

impl Regex {
    pub fn compile(
        pattern: &[Chr],
        cflags: i32,
        locale: RegexLocale,
    ) -> Result<Self, RegErrorCode> {
        Self::compile_with_cancel(pattern, cflags, locale, None)
    }

    pub fn compile_with_cancel(
        pattern: &[Chr],
        cflags: i32,
        locale: RegexLocale,
        cancel: Option<CancelCheck<'_>>,
    ) -> Result<Self, RegErrorCode> {
        regcomp::compile(pattern, cflags, locale, cancel)
    }

    pub fn nsub(&self) -> usize {
        self.nsub
    }

    pub fn info(&self) -> i64 {
        self.info
    }

    pub fn cflags(&self) -> i32 {
        self.cflags
    }

    pub fn locale(&self) -> RegexLocale {
        self.locale
    }

    pub fn exec(
        &self,
        string: &[Chr],
        search_start: usize,
        details: Option<&mut RmDetail>,
        pmatch: &mut [RegMatch],
        eflags: i32,
    ) -> Result<bool, RegErrorCode> {
        self.exec_with_cancel(string, search_start, details, pmatch, eflags, None)
    }

    pub fn exec_with_cancel(
        &self,
        string: &[Chr],
        search_start: usize,
        details: Option<&mut RmDetail>,
        pmatch: &mut [RegMatch],
        eflags: i32,
        cancel: Option<CancelCheck<'_>>,
    ) -> Result<bool, RegErrorCode> {
        regexec::exec(self, string, search_start, details, pmatch, eflags, cancel)
    }

    pub fn prefix(&self) -> Result<(PrefixKind, Vec<Chr>), RegErrorCode> {
        regprefix::prefix(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn match_nested(depth: usize) -> Result<usize, RegErrorCode> {
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(move || {
                let pattern = "(".repeat(depth) + "x" + &")".repeat(depth);
                let pattern: Vec<_> = pattern.chars().map(Chr::from).collect();
                let regex = Regex::compile(&pattern, REG_ADVANCED, RegexLocale::CUtf8)?;
                let expected = RegMatch { so: 0, eo: 1 };
                let mut matches = vec![RegMatch::default(); regex.nsub() + 1];
                assert!(regex.exec(&[Chr::from('x')], 0, None, &mut matches, 0)?);
                assert!(matches.iter().all(|&span| span == expected));
                Ok(regex.nsub())
            })
            .unwrap()
            .join()
            .unwrap()
    }

    #[test]
    fn five_thousand_captures_on_a_two_megabyte_stack() {
        assert_eq!(match_nested(5_000), Ok(5_000));
    }

    #[test]
    fn twenty_thousand_captures_return_too_big() {
        assert_eq!(match_nested(20_000), Err(RegErrorCode::ETooBig));
    }

    #[test]
    fn dissection_boundary_on_a_two_megabyte_stack() {
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(|| {
                let source = [Chr::from('x')];
                let mut regex = Regex::compile(
                    &[Chr::from('('), source[0], Chr::from(')')],
                    REG_ADVANCED,
                    RegexLocale::CUtf8,
                )
                .unwrap();
                let cnfa = regex.guts.tree[0].cnfa.clone();
                let add_wrapper = |regex: &mut Regex| {
                    let child = regex.guts.root;
                    regex.guts.root = regex.guts.tree.len();
                    regex.guts.tree.push(regguts::Subre {
                        op: b'(',
                        flags: regguts::CAP,
                        capno: 1,
                        backno: 0,
                        min: 1,
                        max: 1,
                        child: Some(child),
                        sibling: None,
                        cnfa: cnfa.clone(),
                    });
                };
                // The oracle's parser rejects this many nested captures at
                // 2 MB. Its execution limit was measured using a cached
                // pattern and stack sizes differing by whole C frames.
                for _ in 1..14_278 {
                    add_wrapper(&mut regex);
                }
                let mut matches = [RegMatch::default(); 2];
                assert_eq!(regex.exec(&source, 0, None, &mut matches, 0), Ok(true));
                add_wrapper(&mut regex);
                assert_eq!(
                    regex.exec(&source, 0, None, &mut matches, 0),
                    Err(RegErrorCode::ETooBig)
                );
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn bounds_report_the_first_error_from_the_lexer() {
        for (pattern, error) in [
            ("x{256", RegErrorCode::EBrace),
            ("x{2550", RegErrorCode::BadBr),
            ("x{256q", RegErrorCode::BadBr),
            ("x{1,", RegErrorCode::EBrace),
            ("x{1,2", RegErrorCode::EBrace),
            ("x{1,2q", RegErrorCode::BadBr),
        ] {
            let pattern: Vec<_> = pattern.chars().map(Chr::from).collect();
            assert!(matches!(
                Regex::compile(&pattern, REG_ADVANCED, RegexLocale::CUtf8),
                Err(actual) if actual == error
            ));
        }
    }

    #[test]
    fn execution_checks_for_cancellation_on_dfa_miss() {
        let regex = Regex::compile(&[Chr::from('x')], REG_ADVANCED, RegexLocale::C).unwrap();
        let calls = std::cell::Cell::new(0);
        let cancelled = || {
            calls.set(calls.get() + 1);
            // Let the execution entry point proceed to the first DFA miss.
            calls.get() == 2
        };
        assert_eq!(
            regex.exec_with_cancel(&[Chr::from('x')], 0, None, &mut [], 0, Some(&cancelled)),
            Err(RegErrorCode::Cancel)
        );
    }

    #[test]
    fn compilation_checks_for_cancellation() {
        let cancelled = || true;
        assert!(matches!(
            Regex::compile_with_cancel(&[], REG_ADVANCED, RegexLocale::C, Some(&cancelled)),
            Err(RegErrorCode::Cancel)
        ));
    }
}
