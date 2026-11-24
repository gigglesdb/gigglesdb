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
 * Translated to Rust from regc_pg_locale.c.
 */

use super::regguts::Class;
use super::{Chr, RegexLocale, ctype_tables as tables};

pub(super) fn classify(locale: RegexLocale, class: Class, cp: Chr) -> bool {
    use Class::*;
    match class {
        Ascii => return cp <= 127,
        Blank => return cp == 9 || cp == 32,
        Cntrl => return cp <= 31 || (127..=159).contains(&cp),
        Xdigit => return cp <= 127 && (cp as u8).is_ascii_hexdigit(),
        _ => {}
    }
    if locale == RegexLocale::C {
        if cp > 127 {
            return false;
        }
        let byte = cp as u8;
        return match class {
            Alnum => byte.is_ascii_alphanumeric(),
            Alpha => byte.is_ascii_alphabetic(),
            Digit => byte.is_ascii_digit(),
            Graph => byte.is_ascii_graphic(),
            Lower => byte.is_ascii_lowercase(),
            Print => byte.is_ascii_graphic() || byte == b' ',
            Punct => byte.is_ascii_punctuation(),
            Space => byte.is_ascii_whitespace() || byte == 11,
            Upper => byte.is_ascii_uppercase(),
            Word => byte.is_ascii_alphanumeric() || byte == b'_',
            _ => unreachable!(),
        };
    }
    let ranges = class_ranges(class);
    let i = ranges.partition_point(|&(_, last)| last < cp);
    ranges.get(i).is_some_and(|&(first, _)| first <= cp)
}

fn class_ranges(class: Class) -> &'static [(Chr, Chr)] {
    use Class::*;
    match class {
        Alnum => tables::ALNUM,
        Alpha => tables::ALPHA,
        Ascii => &[],
        Blank => tables::BLANK,
        Cntrl => tables::CNTRL,
        Digit => tables::DIGIT,
        Graph => tables::GRAPH,
        Lower => tables::LOWER,
        Print => tables::PRINT,
        Punct => tables::PUNCT,
        Space => tables::SPACE,
        Upper => tables::UPPER,
        Xdigit => tables::XDIGIT,
        Word => tables::WORD,
    }
}

pub(super) fn case_pair(locale: RegexLocale, cp: Chr) -> (Chr, Chr) {
    match locale {
        RegexLocale::C if cp <= 127 => {
            let byte = cp as u8;
            (
                byte.to_ascii_uppercase().into(),
                byte.to_ascii_lowercase().into(),
            )
        }
        RegexLocale::C => (cp, cp),
        RegexLocale::CUtf8 => tables::CASE
            .binary_search_by_key(&cp, |row| row.0)
            .map_or((cp, cp), |i| (tables::CASE[i].1, tables::CASE[i].2)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeMap, fs, path::Path};

    #[test]
    fn tables_equal_oracle_csvs() {
        let oracle = Path::new(env!("CARGO_MANIFEST_DIR")).join("upstream/oracle");
        let mut ranges = BTreeMap::<_, Vec<_>>::new();
        let csv = fs::read_to_string(oracle.join("classes.csv")).unwrap();
        for row in csv.lines() {
            let fields: Vec<_> = row.split(',').collect();
            ranges
                .entry(fields[0])
                .or_default()
                .push((fields[1].parse().unwrap(), fields[2].parse().unwrap()));
        }
        for class in Class::ALL {
            if class != Class::Ascii {
                assert_eq!(class_ranges(class), ranges.remove(class.name()).unwrap());
            }
        }
        assert!(ranges.is_empty());
        let csv = fs::read_to_string(oracle.join("case.csv")).unwrap();
        let pairs: Vec<_> = csv
            .lines()
            .map(|row| {
                let fields: Vec<_> = row.split(',').map(|field| field.parse().unwrap()).collect();
                (fields[0], fields[1], fields[2])
            })
            .collect();
        assert_eq!(tables::CASE, pairs);
    }
}
