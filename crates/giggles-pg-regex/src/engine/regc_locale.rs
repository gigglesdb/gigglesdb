/*
 *
 * Copyright (c) 1998, 1999 Henry Spencer.  All rights reserved.
 *
 * Development of this software was funded, in part, by Cray Research Inc.,
 * UUNET Communications Services Inc., Sun Microsystems Inc., and Scriptics
 * Corporation, none of whom are responsible for the results.  The author
 * thanks all of them.
 *
 * Redistribution and use in source and binary forms -- with or without
 * modification -- are permitted for any purpose, provided that
 * redistributions in source form retain this entire copyright notice and
 * indicate the origin and nature of any modifications.
 *
 * I'd appreciate being given credit for this package in the documentation
 * of software which uses it, but that is not a requirement.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * HENRY SPENCER BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Translated to Rust from regc_locale.c.
 */

use super::{CancelCheck, Chr, REG_FAKE, REG_ICASE, RegErrorCode, RegexLocale};
use super::{
    regc_cvec::{Cvec, CvecSpace},
    regc_pg_locale::{case_pair, classify},
    regguts::{Class, MAX_SIMPLE_CHR},
};

const CNAMES: &[(&str, Chr)] = &[
    ("NUL", 0),
    ("SOH", 1),
    ("STX", 2),
    ("ETX", 3),
    ("EOT", 4),
    ("ENQ", 5),
    ("ACK", 6),
    ("BEL", 7),
    ("alert", 7),
    ("BS", 8),
    ("backspace", 8),
    ("HT", 9),
    ("tab", 9),
    ("LF", 10),
    ("newline", 10),
    ("VT", 11),
    ("vertical-tab", 11),
    ("FF", 12),
    ("form-feed", 12),
    ("CR", 13),
    ("carriage-return", 13),
    ("SO", 14),
    ("SI", 15),
    ("DLE", 16),
    ("DC1", 17),
    ("DC2", 18),
    ("DC3", 19),
    ("DC4", 20),
    ("NAK", 21),
    ("SYN", 22),
    ("ETB", 23),
    ("CAN", 24),
    ("EM", 25),
    ("SUB", 26),
    ("ESC", 27),
    ("IS4", 28),
    ("FS", 28),
    ("IS3", 29),
    ("GS", 29),
    ("IS2", 30),
    ("RS", 30),
    ("IS1", 31),
    ("US", 31),
    ("space", 32),
    ("exclamation-mark", 33),
    ("quotation-mark", 34),
    ("number-sign", 35),
    ("dollar-sign", 36),
    ("percent-sign", 37),
    ("ampersand", 38),
    ("apostrophe", 39),
    ("left-parenthesis", 40),
    ("right-parenthesis", 41),
    ("asterisk", 42),
    ("plus-sign", 43),
    ("comma", 44),
    ("hyphen", 45),
    ("hyphen-minus", 45),
    ("period", 46),
    ("full-stop", 46),
    ("slash", 47),
    ("solidus", 47),
    ("zero", 48),
    ("one", 49),
    ("two", 50),
    ("three", 51),
    ("four", 52),
    ("five", 53),
    ("six", 54),
    ("seven", 55),
    ("eight", 56),
    ("nine", 57),
    ("colon", 58),
    ("semicolon", 59),
    ("less-than-sign", 60),
    ("equals-sign", 61),
    ("greater-than-sign", 62),
    ("question-mark", 63),
    ("commercial-at", 64),
    ("left-square-bracket", 91),
    ("backslash", 92),
    ("reverse-solidus", 92),
    ("right-square-bracket", 93),
    ("circumflex", 94),
    ("circumflex-accent", 94),
    ("underscore", 95),
    ("low-line", 95),
    ("grave-accent", 96),
    ("left-brace", 123),
    ("left-curly-bracket", 123),
    ("vertical-line", 124),
    ("right-brace", 125),
    ("right-curly-bracket", 125),
    ("tilde", 126),
    ("DEL", 127),
];

pub(super) fn element(name: &[Chr]) -> Result<Chr, RegErrorCode> {
    if let [chr] = name {
        return Ok(*chr);
    }
    CNAMES
        .iter()
        .find(|(label, _)| name_eq(label, name))
        .map(|(_, chr)| *chr)
        .ok_or(RegErrorCode::ECollate)
}

pub(super) fn name_eq(label: &str, name: &[Chr]) -> bool {
    label.len() == name.len() && label.bytes().map(Chr::from).eq(name.iter().copied())
}

pub(super) fn lookupcclass(name: &[Chr]) -> Result<Class, RegErrorCode> {
    Class::ALL
        .into_iter()
        .find(|class| name_eq(class.name(), name))
        .ok_or(RegErrorCode::ECtype)
}

pub(super) fn allcases(space: &mut CvecSpace, locale: RegexLocale, chr: Chr) -> Cvec {
    space.request(2, 0);
    let (upper, lower) = case_pair(locale, chr);
    let mut cv = Cvec::single(lower);
    if upper != lower {
        cv.chrs.push(upper);
    }
    cv
}

pub(super) fn eclass(space: &mut CvecSpace, locale: RegexLocale, chr: Chr, flags: i32) -> Cvec {
    if flags & REG_FAKE != 0 && chr == u32::from(b'x') {
        space.request(4, 0);
        let mut cv = Cvec {
            chrs: vec![120, 121],
            ..Cvec::default()
        };
        if flags & REG_ICASE != 0 {
            cv.chrs.extend([88, 89]);
        }
        return cv;
    }
    if flags & REG_ICASE != 0 {
        allcases(space, locale, chr)
    } else {
        space.request(1, 0);
        Cvec::single(chr)
    }
}

pub(super) fn range(
    space: &mut CvecSpace,
    locale: RegexLocale,
    start: Chr,
    end: Chr,
    icase: bool,
    cancel: Option<CancelCheck<'_>>,
) -> Result<Cvec, RegErrorCode> {
    if start > end {
        return Err(RegErrorCode::ERange);
    }
    let mut cv = Cvec {
        ranges: vec![(start, end)],
        ..Cvec::default()
    };
    let requested = if icase {
        (end - start + 1).min(100_000) as usize
    } else {
        0
    };
    let max_chrs = space.request(requested, 1);
    if icase {
        for chr in start..=end {
            let (upper, lower) = case_pair(locale, chr);
            for folded in [lower, upper] {
                if !(start..=end).contains(&folded) {
                    if cv.chrs.len() == max_chrs {
                        return Err(RegErrorCode::ETooBig);
                    }
                    cv.chrs.push(folded);
                }
            }
            if cancel.is_some_and(|check| check()) {
                return Err(RegErrorCode::Cancel);
            }
        }
    }
    Ok(cv)
}

pub(super) fn cclasscvec(space: &mut CvecSpace, locale: RegexLocale, mut class: Class, icase: bool) -> Cvec {
    use Class::*;
    if icase && matches!(class, Lower | Upper) {
        class = Alpha;
    }
    let mut cv = Cvec::default();
    match class {
        Ascii => {
            space.request(0, 1);
            cv.ranges.push((0, 127));
        }
        Blank => {
            space.request(2, 0);
            cv.chrs.extend([9, 32]);
        }
        Cntrl => {
            space.request(0, 2);
            cv.ranges.extend([(0, 31), (127, 159)]);
        }
        Xdigit => {
            space.request(0, 3);
            cv.ranges.extend([(48, 57), (97, 102), (65, 70)]);
        }
        _ => {
            let max = if locale == RegexLocale::C {
                127
            } else {
                MAX_SIMPLE_CHR
            };
            cv.class = (locale == RegexLocale::CUtf8).then_some(class);
            let mut chr = 0;
            while chr <= max {
                if !classify(locale, class, chr) {
                    chr += 1;
                    continue;
                }
                let start = chr;
                chr += 1;
                while chr <= max && classify(locale, class, chr) {
                    chr += 1;
                }
                if chr == start + 1 {
                    cv.chrs.push(start);
                } else {
                    cv.ranges.push((start, chr - 1));
                }
            }
        }
    }
    cv
}


#[cfg(test)]
mod tests {
    use super::CNAMES;

    #[test]
    fn character_names_equal_postgres_table() {
        let source = include_str!("../../upstream/regex/regc_locale.c");
        let (_, entries) = source.split_once("cnames[] =").unwrap();
        let (entries, _) = entries.split_once("};").unwrap();
        let codes: Vec<_> = entries
            .lines()
            .map(str::trim)
            .filter(|line| line.starts_with('"'))
            .map(|line| {
                let (name, code) = line.split_once(',').unwrap();
                let name = name.trim_matches('"');
                let literal = code.trim().strip_prefix('\'').unwrap().strip_suffix('\'').unwrap();
                let code = match literal {
                    r"\a" => 7,
                    r"\b" => 8,
                    r"\t" => 9,
                    r"\n" => 10,
                    r"\v" => 11,
                    r"\f" => 12,
                    r"\r" => 13,
                    r"\\" => u32::from(b'\\'),
                    r"\'" => u32::from(b'\''),
                    octal if octal.starts_with('\\') => u32::from_str_radix(&octal[1..], 8).unwrap(),
                    single => {
                        let [byte] = single.as_bytes() else {
                            panic!("unexpected C character literal {single}");
                        };
                        u32::from(*byte)
                    }
                };
                (name, code)
            })
            .collect();
        assert_eq!(CNAMES, codes);
    }
}
