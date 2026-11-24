use crate::engine::{REG_NOSUB, RegMatch};

use super::matching::{Text, compile, execute, next_start};
use super::{RegexError, RegexpContext};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Escapes {
    None,
    Other,
    Captures,
}

fn escapes(replacement: &str) -> Escapes {
    let mut result = Escapes::None;
    let mut chars = replacement.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('1'..='9') => return Escapes::Captures,
                Some(_) => result = Escapes::Other,
                None => break,
            }
        }
    }
    result
}

fn append_expanded(
    result: &mut String,
    replacement: &str,
    text: &Text<'_>,
    positions: &[RegMatch],
) {
    let mut chars = replacement.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            result.push(ch);
            continue;
        }
        let index = match chars.peek() {
            Some('\\') => {
                chars.next();
                result.push('\\');
                continue;
            }
            Some('&') => 0,
            Some(ch @ '1'..='9') => (*ch as u8 - b'0') as usize,
            _ => {
                result.push('\\');
                continue;
            }
        };
        chars.next();
        if let Some(value) = text.capture(positions[index]) {
            result.push_str(value);
        }
    }
}

pub(super) fn replace(
    source: &str,
    pattern: &str,
    replacement: &str,
    cflags: i32,
    ctx: &RegexpContext<'_>,
    start: usize,
    n: i32,
) -> Result<String, RegexError> {
    let text = Text::new(source);
    let escapes = escapes(replacement);
    let (cflags, nmatch) = if escapes == Escapes::Captures {
        (cflags, 10)
    } else {
        (cflags | REG_NOSUB, 1)
    };
    let regex = compile(pattern, cflags, ctx)?;
    let mut output = String::new();
    let mut positions = [RegMatch { so: -1, eo: -1 }; 10];
    let mut copied = 0;
    let mut search_start = start;
    let mut occurrence = 0;

    while search_start <= text.wide.len()
        && execute(
            &regex,
            &text.wide,
            search_start,
            &mut positions[..nmatch],
            ctx,
        )?
    {
        let overall = positions[0];
        search_start = next_start(overall);
        occurrence += 1;
        if n > 0 && occurrence != n {
            continue;
        }

        output.push_str(text.slice(copied, overall.so as usize));
        if escapes == Escapes::None {
            output.push_str(replacement);
        } else {
            append_expanded(&mut output, replacement, &text, &positions);
        }
        copied = overall.eo as usize;
        if n > 0 {
            break;
        }
    }
    output.push_str(text.slice(copied, text.wide.len()));
    Ok(output)
}
