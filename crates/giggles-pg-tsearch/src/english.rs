// Port of PostgreSQL 18.6's Snowball 2.2.0 stem_UTF_8_english.c.
// Copyright (c) 2001, Dr Martin Porter. See NOTICE for the full attribution.
// See NOTICE for the Snowball license and source revision.

fn vowel(c: char) -> bool {
    matches!(c, 'a' | 'e' | 'i' | 'o' | 'u' | 'y')
}

fn region(word: &[char], from: usize) -> usize {
    word[from..]
        .windows(2)
        .position(|pair| vowel(pair[0]) && !vowel(pair[1]))
        .map_or(word.len(), |index| from + index + 2)
}

fn short_vowel(word: &[char]) -> bool {
    match word {
        [a, b] => vowel(*a) && !vowel(*b),
        [.., a, b, c] => !vowel(*a) && vowel(*b) && !vowel(*c) && !matches!(c, 'w' | 'x' | 'Y'),
        _ => false,
    }
}

fn among<T: Copy>(word: &[char], suffixes: &[(&str, T)]) -> Option<(usize, T)> {
    suffixes
        .iter()
        .filter_map(|&(suffix, value)| {
            let start = word.len().checked_sub(suffix.len())?;
            word[start..]
                .iter()
                .copied()
                .eq(suffix.chars())
                .then_some((start, value))
        })
        .min_by_key(|&(start, _)| start)
}

fn replace(word: &mut Vec<char>, start: usize, tail: &str) {
    word.truncate(start);
    word.extend(tail.chars());
}

pub(crate) fn stem(input: String) -> String {
    // exception1
    let exception = match input.as_str() {
        "skis" => "ski",
        "skies" => "sky",
        "dying" => "die",
        "lying" => "lie",
        "tying" => "tie",
        "idly" => "idl",
        "gently" => "gentl",
        "ugly" => "ugli",
        "early" => "earli",
        "only" => "onli",
        "singly" => "singl",
        "sky" | "news" | "howe" | "atlas" | "cosmos" | "bias" | "andes" => return input,
        _ => "",
    };
    if !exception.is_empty() {
        return exception.to_owned();
    }
    let mut word: Vec<_> = input.chars().collect();
    if word.len() < 3 {
        return input;
    }

    // prelude and mark_regions
    if word[0] == '\'' {
        word.remove(0);
    }
    let mut marked_y = false;
    for index in 0..word.len() {
        if word[index] == 'y' && (index == 0 || vowel(word[index - 1])) {
            word[index] = 'Y';
            marked_y = true;
        }
    }
    let original = input.strip_prefix('\'').unwrap_or(&input);
    let r1 = if original.starts_with("commun") {
        6
    } else if original.starts_with("gener") || original.starts_with("arsen") {
        5
    } else {
        region(&word, 0)
    };
    let r2 = region(&word, r1);

    // Step 1a
    if let Some((start, ())) = among(&word, &[("'s'", ()), ("'s", ()), ("'", ())]) {
        word.truncate(start);
    }
    if let Some((start, suffix)) = among(
        &word,
        &[
            ("sses", "ss"),
            ("ied", "i"),
            ("ies", "i"),
            ("s", ""),
            ("us", "="),
            ("ss", "="),
        ],
    ) {
        match suffix {
            "=" => (),
            "i" => replace(&mut word, start, if start > 1 { "i" } else { "ie" }),
            "" if start < 2 || !word[..start - 1].iter().copied().any(vowel) => (),
            _ => replace(&mut word, start, suffix),
        }
    }

    // exception2
    let after_plural: String = word.iter().collect();
    if matches!(
        after_plural.as_str(),
        "succeed" | "proceed" | "exceed" | "canning" | "inning" | "earring" | "herring" | "outing"
    ) {
        return after_plural;
    }

    // Step 1b
    if let Some((start, ends_eed)) = among(
        &word,
        &[
            ("eed", true),
            ("eedly", true),
            ("ed", false),
            ("edly", false),
            ("ing", false),
            ("ingly", false),
        ],
    ) {
        if ends_eed {
            if start >= r1 {
                replace(&mut word, start, "ee");
            }
        } else if word[..start].iter().copied().any(vowel) {
            word.truncate(start);
            match word.as_slice() {
                [.., 'a', 't'] | [.., 'b', 'l'] | [.., 'i', 'z'] => word.push('e'),
                ['a' | 'e' | 'o', a, b] if a == b => (),
                [.., a, b]
                    if a == b
                        && matches!(a, 'b' | 'd' | 'f' | 'g' | 'm' | 'n' | 'p' | 'r' | 't') =>
                {
                    word.pop();
                }
                _ if word.len() == r1 && short_vowel(&word) => word.push('e'),
                _ => (),
            }
        }
    }

    // Step 1c
    if let [_, .., a, 'y' | 'Y'] = word.as_slice()
        && !vowel(*a)
    {
        *word.last_mut().expect("matched final y") = 'i';
    }

    // Step 2
    if let Some((start, tail)) = among(
        &word,
        &[
            ("tional", "tion"),
            ("enci", "ence"),
            ("anci", "ance"),
            ("abli", "able"),
            ("entli", "ent"),
            ("izer", "ize"),
            ("ization", "ize"),
            ("ational", "ate"),
            ("ation", "ate"),
            ("ator", "ate"),
            ("alism", "al"),
            ("aliti", "al"),
            ("alli", "al"),
            ("fulness", "ful"),
            ("fulli", "ful"),
            ("ousli", "ous"),
            ("ousness", "ous"),
            ("iveness", "ive"),
            ("iviti", "ive"),
            ("bli", "ble"),
            ("biliti", "ble"),
            ("ogi", "og"),
            ("lessli", "less"),
            ("li", ""),
        ],
    ) && start >= r1
    {
        let preceding = start.checked_sub(1).map(|i| word[i]);
        if (tail != "og" || preceding == Some('l'))
            && (!tail.is_empty() || preceding.is_some_and(|c| "cdeghkmnrt".contains(c)))
        {
            replace(&mut word, start, tail);
        }
    }

    // Step 3
    if let Some((start, tail)) = among(
        &word,
        &[
            ("tional", "tion"),
            ("ational", "ate"),
            ("alize", "al"),
            ("icate", "ic"),
            ("iciti", "ic"),
            ("ical", "ic"),
            ("ful", ""),
            ("ness", ""),
            ("ative", ""),
        ],
    ) && start >= r1
        && (start >= r2 || !word[start..].iter().copied().eq("ative".chars()))
    {
        replace(&mut word, start, tail);
    }

    // Step 4
    if let Some((start, ion)) = among(
        &word,
        &[
            ("al", false),
            ("ance", false),
            ("ence", false),
            ("er", false),
            ("ic", false),
            ("able", false),
            ("ible", false),
            ("ant", false),
            ("ement", false),
            ("ment", false),
            ("ent", false),
            ("ion", true),
            ("ism", false),
            ("ate", false),
            ("iti", false),
            ("ous", false),
            ("ive", false),
            ("ize", false),
        ],
    ) && start >= r2
        && (!ion || matches!(word.get(start.wrapping_sub(1)), Some('s' | 't')))
    {
        word.truncate(start);
    }

    // Step 5
    let last = word.len().saturating_sub(1);
    match word.as_slice() {
        [.., 'e'] if last >= r2 || (last >= r1 && !short_vowel(&word[..last])) => {
            word.pop();
        }
        [.., 'l', 'l'] if last >= r2 => {
            word.pop();
        }
        _ => (),
    }

    // postlude
    if marked_y {
        for c in &mut word {
            if *c == 'Y' {
                *c = 'y';
            }
        }
    }
    word.into_iter().collect()
}
