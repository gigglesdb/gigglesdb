use super::RegexError;

pub(super) fn translate(pattern: &str, escape: Option<&str>) -> Result<String, RegexError> {
    let mut escape_chars = escape.unwrap_or("\\").chars();
    let escape = escape_chars.next();
    if escape_chars.next().is_some() {
        return Err(RegexError::new("22025", "invalid escape string")
            .with_hint("Escape string must be empty or one character."));
    }

    let mut result = String::from("^(?:");
    let mut after_escape = false;
    let mut separators = 0;
    let mut bracket_depth = 0;
    let mut class_position = 0;
    let wide_escape = escape.is_some_and(|ch| !ch.is_ascii());

    for ch in pattern.chars() {
        if after_escape {
            if ch == '"' && bracket_depth == 0 {
                result.push_str(match separators {
                    0 => "){1,1}?(",
                    1 => "){1,1}(?:",
                    _ => return Err(RegexError::new(
                        "2200C",
                        "SQL regular expression may not contain more than two escape-double-quote separators",
                    )),
                });
                separators += 1;
            } else {
                result.push('\\');
                result.push(ch);
                // The multibyte path in PostgreSQL skips character-class bookkeeping.
                if !wide_escape || ch.is_ascii() {
                    class_position = 3;
                }
            }
            after_escape = false;
        } else if Some(ch) == escape {
            after_escape = true;
        } else if wide_escape && !ch.is_ascii() {
            result.push(ch);
        } else if bracket_depth != 0 {
            if ch == '\\' {
                result.push('\\');
            }
            result.push(ch);
            match ch {
                ']' if class_position > 2 => bracket_depth -= 1,
                '[' => {
                    bracket_depth += 1;
                    class_position = 3;
                }
                '^' => class_position += 1,
                _ => class_position = 3,
            }
        } else {
            match ch {
                '[' => {
                    result.push(ch);
                    bracket_depth = 1;
                    class_position = 1;
                }
                '%' => result.push_str(".*"),
                '_' => result.push('.'),
                '(' => result.push_str("(?:"),
                '\\' | '.' | '^' | '$' => {
                    result.push('\\');
                    result.push(ch);
                }
                _ => result.push(ch),
            }
        }
    }

    result.push_str(")$");
    Ok(result)
}
