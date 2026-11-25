//! LOAD reports library availability from the SQL profile.
use super::{R, SqlError, Token, user_types::exact_sql_error};

pub(super) fn claims(tokens: &[Token]) -> bool {
    matches!(tokens.first(), Some(Token::Word(word)) if word == "load")
}

pub(super) fn execute(filename: &str, superuser: bool) -> R<()> {
    let allowed = filename
        .strip_prefix("$libdir/plugins/")
        .is_some_and(|name| !name.contains('/'));
    if !superuser && !allowed {
        return Err(exact_sql_error(
            SqlError::InsufficientPrivilege,
            format!("access to library \"{filename}\" is not allowed"),
            None,
        ));
    }
    if matches!(filename, "plpgsql" | "$libdir/plpgsql") {
        return Ok(());
    }
    Err(exact_sql_error(
        SqlError::UndefinedFile,
        format!("could not access file \"{filename}\": No such file or directory"),
        None,
    ))
}
