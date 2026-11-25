//! Parse TABLESPACE commands and validate them against the built-in catalog.
use super::{
    Parser, R, SqlError, Token, admin_utility, ident,
    user_types::{exact_sql_error, sql_error_hint, sql_notice},
};
use crate::{
    CoreError,
    roles::{CatalogRole, RoleCatalog, RoleError},
};
use giggles_pg_types::admin_catalog::TABLESPACES;

#[derive(Debug, Clone)]
pub(crate) struct Command {
    name: String,
    action: Action,
}

#[derive(Debug, Clone)]
enum Action {
    Create {
        owner: Option<Owner>,
        location: String,
        options: Vec<OptionSpec>,
    },
    Rename(String),
    Owner(Owner),
    Set(Vec<OptionSpec>),
    Reset(Vec<OptionSpec>),
    Drop {
        if_exists: bool,
    },
}

#[derive(Debug, Clone)]
enum Owner {
    Named(String),
    Current,
}

#[derive(Debug, Clone)]
struct OptionSpec {
    namespace: Option<String>,
    name: String,
    value: Option<String>,
}

pub(super) fn claims(tokens: &[Token]) -> bool {
    matches!(tokens, [Token::Word(verb), Token::Word(kind), ..]
        if matches!(verb.as_str(), "create" | "alter" | "drop") && kind == "tablespace")
}

fn name(p: &mut Parser) -> R<String> {
    Ok(ident::truncate(&p.raw_name()?, 63).to_owned())
}

fn owner(p: &mut Parser) -> R<Owner> {
    if p.word("current_user") || p.word("current_role") || p.word("session_user") {
        Ok(Owner::Current)
    } else {
        name(p).map(Owner::Named)
    }
}

pub(super) fn parse(p: &mut Parser) -> R<Command> {
    let verb = p.raw_name()?;
    p.need("tablespace")?;
    let if_exists = verb == "drop" && p.word("if");
    if if_exists {
        p.need("exists")?;
    }
    let target = name(p)?;
    let action = match verb.as_str() {
        "create" => {
            let owner = p.word("owner").then(|| owner(p)).transpose()?;
            p.need("location")?;
            let location = admin_utility::string(p)?;
            let options = if p.word("with") {
                options(p)?
            } else {
                Vec::new()
            };
            Action::Create {
                owner,
                location,
                options,
            }
        }
        "drop" => Action::Drop { if_exists },
        _ if p.word("rename") => {
            p.need("to")?;
            Action::Rename(name(p)?)
        }
        _ if p.word("owner") => {
            p.need("to")?;
            Action::Owner(owner(p)?)
        }
        _ if p.word("set") => Action::Set(options(p)?),
        _ if p.word("reset") => Action::Reset(options(p)?),
        _ => return Err(SqlError::Syntax),
    };
    Ok(Command {
        name: target,
        action,
    })
}

fn options(p: &mut Parser) -> R<Vec<OptionSpec>> {
    p.punct('(')?;
    let mut options = Vec::new();
    loop {
        let first = p.raw_name()?;
        let (namespace, name) = if p.sym('.') {
            (Some(first), p.raw_name()?)
        } else {
            (None, first)
        };
        let value = if matches!(p.tokens.get(p.at), Some(Token::Op(op)) if op == "=") {
            p.at += 1;
            let sign = match p.tokens.get(p.at) {
                Some(Token::Op(op)) if op == "-" || op == "+" => {
                    p.at += 1;
                    op.as_str()
                }
                _ => "",
            };
            let Some(
                Token::Word(value)
                | Token::QuotedIdent(value)
                | Token::String(value)
                | Token::Number(value),
            ) = p.tokens.get(p.at)
            else {
                return Err(SqlError::Syntax);
            };
            p.at += 1;
            Some(format!("{sign}{value}"))
        } else {
            None
        };
        options.push(OptionSpec {
            namespace,
            name,
            value,
        });
        if !p.sym(',') {
            break;
        }
    }
    p.punct(')')?;
    Ok(options)
}

pub(super) fn exists(name: &str) -> bool {
    TABLESPACES.iter().any(|&(_, builtin)| builtin == name)
}

impl Command {
    pub(super) fn tag(&self) -> &'static str {
        match self.action {
            Action::Create { .. } => "CREATE TABLESPACE",
            Action::Drop { .. } => "DROP TABLESPACE",
            _ => "ALTER TABLESPACE",
        }
    }

    pub(super) fn requires_standalone(&self) -> bool {
        matches!(self.action, Action::Create { .. } | Action::Drop { .. })
    }

    pub(super) fn execute(
        &self,
        actor: Option<&CatalogRole>,
        roles: &RoleCatalog,
    ) -> Result<(), CoreError> {
        let superuser = actor.is_none_or(|role| role.superuser);
        let name = self.name.as_str();
        if let Action::Create {
            owner,
            location,
            options,
        } = &self.action
        {
            if !superuser {
                sql_error_hint("Must be superuser to create a tablespace.");
                return Err(CoreError::Sql(exact_sql_error(
                    SqlError::InsufficientPrivilege,
                    format!("permission denied to create tablespace \"{name}\""),
                    None,
                )));
            }
            if let Some(owner) = owner {
                resolve_owner(owner, actor, roles)?;
            }
            let location = canonical_path(location);
            if location.contains('\'') {
                return Err(CoreError::Sql(exact_sql_error(
                    SqlError::InvalidName,
                    "tablespace location cannot contain single quotes",
                    None,
                )));
            }
            if !location.starts_with('/') {
                return Err(CoreError::Sql(exact_sql_error(
                    SqlError::InvalidObjectDefinition,
                    "tablespace location must be an absolute path",
                    None,
                )));
            }
            if location.len() > 1024 {
                return Err(CoreError::Sql(exact_sql_error(
                    SqlError::InvalidObjectDefinition,
                    format!("tablespace location \"{location}\" is too long"),
                    None,
                )));
            }
            check_new_name(name).map_err(CoreError::Sql)?;
            validate_options(options).map_err(CoreError::Sql)?;
            return Err(CoreError::Sql(exact_sql_error(
                SqlError::UndefinedFile,
                format!("directory \"{location}\" does not exist"),
                None,
            )));
        }
        let new_owner = if let Action::Owner(owner) = &self.action {
            resolve_owner(owner, actor, roles)?
        } else {
            None
        };
        if !exists(name) {
            if matches!(self.action, Action::Drop { if_exists: true }) {
                sql_notice(
                    format!("tablespace \"{name}\" does not exist, skipping"),
                    "00000",
                );
                return Ok(());
            }
            return Err(CoreError::Sql(admin_utility::missing("tablespace", name)));
        }
        if matches!(self.action, Action::Drop { .. }) {
            if !superuser {
                return Err(CoreError::Sql(admin_utility::must_own("tablespace", name)));
            }
            return Err(CoreError::Sql(denied(name)));
        }
        if let Action::Rename(new) = &self.action {
            if !superuser {
                return Err(CoreError::Sql(denied(name)));
            }
            check_new_name(new).map_err(CoreError::Sql)?;
        } else {
            if matches!(self.action, Action::Owner(_)) {
                // The catalog represents the bootstrap superuser with PostgreSQL's built-in OID.
                let bootstrap = roles
                    .list()
                    .map_err(admin_utility::role_error)?
                    .into_iter()
                    .filter(|role| role.superuser)
                    .map(|role| role.oid)
                    .min();
                let new_owner = if actor.is_none() && new_owner.is_none() {
                    bootstrap
                } else {
                    new_owner
                };
                if new_owner == bootstrap {
                    return Ok(());
                }
            }
            if !superuser {
                return Err(CoreError::Sql(admin_utility::must_own("tablespace", name)));
            }
            match &self.action {
                Action::Set(options) => validate_options(options).map_err(CoreError::Sql)?,
                Action::Reset(options) if options.iter().any(|option| option.value.is_some()) => {
                    return Err(CoreError::Sql(exact_sql_error(
                        SqlError::Syntax,
                        "RESET must not include values for parameters",
                        None,
                    )));
                }
                _ => {}
            }
        }
        Err(CoreError::Sql(SqlError::Unsupported))
    }
}

fn denied(name: &str) -> SqlError {
    exact_sql_error(
        SqlError::InsufficientPrivilege,
        format!("permission denied for tablespace {name}"),
        None,
    )
}

fn resolve_owner(
    owner: &Owner,
    actor: Option<&CatalogRole>,
    roles: &RoleCatalog,
) -> Result<Option<u32>, CoreError> {
    match owner {
        Owner::Current => Ok(actor.map(|role| role.oid)),
        Owner::Named(owner) => match roles.get(owner) {
            Ok(Some(role)) => Ok(Some(role.oid)),
            Ok(None) | Err(RoleError::Invalid) => {
                Err(CoreError::Sql(admin_utility::missing("role", owner)))
            }
            Err(error) => Err(admin_utility::role_error(error)),
        },
    }
}

fn check_new_name(name: &str) -> R<()> {
    if name.starts_with("pg_") {
        return Err(exact_sql_error(
            SqlError::ReservedName,
            format!("unacceptable tablespace name \"{name}\""),
            Some("The prefix \"pg_\" is reserved for system tablespaces."),
        ));
    }
    if exists(name) {
        return Err(exact_sql_error(
            SqlError::DuplicateObject,
            format!("tablespace \"{name}\" already exists"),
            None,
        ));
    }
    Ok(())
}

/// Use the path syntax of the PostgreSQL oracle, independently of the host.
fn canonical_path(path: &str) -> String {
    let absolute = path.starts_with('/');
    let mut components = Vec::new();
    for component in path.split('/') {
        match component {
            "" | "." => {}
            ".." if components.last().is_some_and(|part| *part != "..") => {
                components.pop();
            }
            ".." if absolute => {}
            _ => components.push(component),
        }
    }
    if absolute {
        format!("/{}", components.join("/"))
    } else {
        components.join("/")
    }
}

fn validate_options(options: &[OptionSpec]) -> R<()> {
    if let Some(namespace) = options
        .iter()
        .find_map(|option| option.namespace.as_deref())
    {
        return Err(exact_sql_error(
            SqlError::InvalidParameterValue,
            format!("unrecognized parameter namespace \"{namespace}\""),
            None,
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for OptionSpec { name, value, .. } in options {
        if !seen.insert(name) {
            return Err(exact_sql_error(
                SqlError::InvalidParameterValue,
                format!("parameter \"{name}\" specified more than once"),
                None,
            ));
        }
        let value = value.as_deref().unwrap_or("true");
        let out_of_bounds = match name.as_str() {
            "seq_page_cost" | "random_page_cost" => match parse_real(value) {
                Some(number) if !(0.0..=f64::MAX).contains(&number) => Some(format!(
                    "Valid values are between \"0.000000\" and \"{:.6}\".",
                    f64::MAX
                )),
                Some(_) => None,
                None => return Err(invalid_option("floating point", name, value)),
            },
            "effective_io_concurrency" | "maintenance_io_concurrency" => match parse_int(value) {
                Some(number) if !(0..=1000).contains(&number) => {
                    Some("Valid values are between \"0\" and \"1000\".".into())
                }
                Some(_) => None,
                None => return Err(invalid_option("integer", name, value)),
            },
            _ => {
                return Err(exact_sql_error(
                    SqlError::InvalidParameterValue,
                    format!("unrecognized parameter \"{name}\""),
                    None,
                ));
            }
        };
        if let Some(detail) = out_of_bounds {
            return Err(exact_sql_error(
                SqlError::InvalidParameterValue,
                format!("value {value} out of bounds for option \"{name}\""),
                Some(&detail),
            ));
        }
    }
    Ok(())
}

fn parse_real(value: &str) -> Option<f64> {
    let number = giggles_pg_types::Float8::parse(value).ok()?.value();
    // strtod reports ERANGE when it rounds an underflowing value. An
    // exact hexadecimal subnormal can be accepted without rounding.
    (!number.is_nan() && (!number.is_subnormal() || exact_hex_subnormal(value))).then_some(number)
}

fn exact_hex_subnormal(value: &str) -> bool {
    let unsigned = value.trim_ascii().trim_start_matches(['+', '-']);
    let Some(hex) = unsigned
        .strip_prefix("0x")
        .or_else(|| unsigned.strip_prefix("0X"))
    else {
        return false;
    };
    let (mantissa, exponent) = hex.split_once(['p', 'P']).unwrap_or((hex, "0"));
    let Ok(exponent) = exponent.parse::<i64>() else {
        return false;
    };
    let fractional_digits = mantissa
        .split_once('.')
        .map_or(0, |(_, digits)| digits.len());
    let trailing_bits = mantissa
        .chars()
        .rev()
        .filter_map(|c| c.to_digit(16))
        .enumerate()
        .find_map(|(zeros, digit)| {
            (digit != 0).then_some(4 * zeros + digit.trailing_zeros() as usize)
        })
        .unwrap_or(0);
    exponent
        .saturating_sub(4 * fractional_digits as i64)
        .saturating_add(trailing_bits as i64)
        >= -1074
}

fn parse_int(value: &str) -> Option<i32> {
    let value = value.trim_matches(|c: char| c.is_ascii_whitespace());
    let negative = value.starts_with('-');
    let unsigned = value.strip_prefix(['+', '-']).unwrap_or(value);
    let hex = unsigned
        .strip_prefix("0x")
        .or_else(|| unsigned.strip_prefix("0X"));
    let (digits, radix) = if let Some(hex) =
        hex.filter(|hex| hex.as_bytes().first().is_some_and(u8::is_ascii_hexdigit))
    {
        (hex, 16)
    } else if unsigned.starts_with('0') {
        (unsigned, 8)
    } else {
        (unsigned, 10)
    };
    // strtol reads a base-0 prefix. PostgreSQL retries as a real only
    // when the first unconsumed byte is a decimal point or exponent marker.
    let end = digits
        .bytes()
        .take_while(|&byte| (byte as char).is_digit(radix))
        .count();
    if end == digits.len() {
        let number = i64::from_str_radix(digits, radix).ok()?;
        return i32::try_from(if negative { -number } else { number }).ok();
    }
    let tail = if end == 0 { value } else { &digits[end..] };
    if !tail.starts_with(['.', 'e', 'E']) {
        return None;
    }
    let number = parse_real(value)?.round_ties_even();
    (f64::from(i32::MIN)..=f64::from(i32::MAX))
        .contains(&number)
        .then_some(number as i32)
}

fn invalid_option(kind: &str, name: &str, value: &str) -> SqlError {
    exact_sql_error(
        SqlError::InvalidParameterValue,
        format!("invalid value for {kind} option \"{name}\": {value}"),
        None,
    )
}
