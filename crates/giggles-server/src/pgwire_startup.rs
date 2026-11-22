use crate::{
    pgwire::{Reader, error_message},
    transport::IO_TIMEOUT,
};
use std::{collections::BTreeMap, io};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::timeout,
};

pub(crate) async fn read_startup_packet<S: AsyncRead + Unpin>(s: &mut S) -> io::Result<Vec<u8>> {
    timeout(IO_TIMEOUT, async {
        let n = s.read_u32().await?;
        if !(8..=8192).contains(&n) {
            return Err(io::Error::other("startup limit"));
        }
        let mut b = vec![0; n as usize - 4];
        s.read_exact(&mut b).await?;
        Ok(b)
    })
    .await
    .map_err(io::Error::other)?
}

pub(crate) enum StartupPolicy<'a> {
    Agent,
    Postgresql { databases: &'a [String] },
    /// GD-0061 R3-205: the agent (task) listener. The certificate picks the
    /// binding; `user` must name its executor and `database` one of `databases`.
    /// The task comes from `giggles.task` or `options` = `-c giggles.task=<name>`.
    TaskAgent { databases: &'a [String] },
}

/// The task named by an `options` startup value. Only the exact form
/// `-c giggles.task=<name>` is accepted; anything else is `None`.
fn options_task(options: &str) -> Option<&str> {
    let name = options.strip_prefix("-c giggles.task=")?;
    (!name.is_empty() && !name.bytes().any(|b| b.is_ascii_whitespace() || b == b'\\'))
        .then_some(name)
}

pub(crate) struct Startup {
    pub(crate) user: String,
    /// Validated against the listener's configured databases.
    pub(crate) database: String,
    pub(crate) settings: crate::pg_settings::Settings,
    pub(crate) timezone: String,
    /// The task a `TaskAgent` startup selected; `None` for every other policy,
    /// and for a task-agent startup that names none.
    pub(crate) task: Option<String>,
}

pub(crate) fn parse_startup(
    body: &[u8],
    policy: &StartupPolicy<'_>,
) -> Result<Startup, (&'static str, &'static str)> {
    parse_startup_with_diagnostics(body, policy).map_err(|error| (error.code, error.reason))
}

pub(crate) struct StartupError {
    code: &'static str,
    reason: &'static str,
    message: Option<String>,
    detail: Option<String>,
    hint: Option<&'static str>,
}

pub(crate) fn parse_startup_with_diagnostics(
    body: &[u8],
    policy: &StartupPolicy<'_>,
) -> Result<Startup, StartupError> {
    let mut reason = "malformed PostgreSQL startup packet";
    let mut message = None;
    let mut detail = None;
    let mut hint = None;
    let mut timezone = String::from("UTC");
    let mut startup_settings = crate::pg_settings::Settings::default();
    let validate = (|| -> Result<Startup, &'static str> {
        let mut r = Reader { b: body, at: 0 };
        if r.u32()? != 196608 {
            reason = "only PostgreSQL protocol version 3 is supported";
            return Err("0A000");
        }
        let mut fields = BTreeMap::new();
        loop {
            let mut k = r.text()?;
            if k.is_empty() {
                break;
            }
            let v = r.text()?;
            // Only GUC names are case-insensitive. Protocol keys and the
            // certificate-authenticated agent profile retain exact matching.
            if matches!(policy, StartupPolicy::Postgresql { .. }) {
                for canonical in [
                    "client_encoding",
                    "application_name",
                    "DateStyle",
                    "IntervalStyle",
                    "TimeZone",
                    "extra_float_digits",
                ] {
                    if k.eq_ignore_ascii_case(canonical) {
                        k = canonical.into();
                        break;
                    }
                }
            }
            if matches!(policy, StartupPolicy::Postgresql { .. })
                && let Ok(name) = crate::pg_settings::database::lookup_name(&k)
            {
                k = name.value(&startup_settings).0.into();
            }
            if fields.insert(k, v).is_some() {
                return Err("08P01");
            }
        }
        r.end()?;
        let mut task = None;
        if matches!(policy, StartupPolicy::TaskAgent { .. }) {
            let direct = fields.remove("giggles.task");
            let options = fields.remove("options");
            if let Some(options) = &options
                && options_task(options).is_none()
            {
                reason = "startup options must be exactly -c giggles.task=<name>";
                return Err("0A000");
            }
            if direct.is_some() && options.is_some() {
                reason = "startup names the task twice; use giggles.task or options, not both";
                return Err("0A000");
            }
            task = direct.or_else(|| options.as_deref().and_then(options_task).map(str::to_owned));
            if task.as_deref().is_some_and(str::is_empty) {
                reason = "startup giggles.task must name a task";
                return Err("0A000");
            }
        }
        match policy {
            StartupPolicy::Agent => {
                if fields.get("user").map(String::as_str) != Some("giggles") {
                    reason = "startup user must be giggles; authentication uses the client certificate";
                    return Err("28000");
                }
                if fields.get("database").map(String::as_str) != Some("giggles") {
                    reason = "startup database must be giggles";
                    return Err("28000");
                }
            }
            StartupPolicy::Postgresql { databases } | StartupPolicy::TaskAgent { databases } => {
                if fields.get("user").is_none_or(|v| v.is_empty() || v.len() > 63) {
                    reason = "startup user must name a role";
                    return Err("28000");
                }
                let database = fields.get("database");
                let Some(database) = database else {
                    reason = "startup database is required";
                    return Err("3D000");
                };
                if !databases.contains(database) {
                    reason = "database does not exist";
                    return Err("3D000");
                }
            }
        }
        if let Some(rejected) = fields.keys().find(|k| {
            if matches!(policy, StartupPolicy::Postgresql { .. })
                && crate::pg_settings::database::lookup_name(k).is_ok()
            {
                return false;
            }
            !matches!(
                k.as_str(),
                "user"
                    | "database"
                    | "client_encoding"
                    | "application_name"
                    | "DateStyle"
                    | "TimeZone"
                    | "extra_float_digits"
            )
        }) {
            eprintln!("protocol=postgresql rejected stage=startup parameter={rejected:?}");
            if matches!(policy, StartupPolicy::Postgresql { .. }) {
                reason = "unrecognized configuration parameter";
                return Err("42704");
            }
            reason = if matches!(policy, StartupPolicy::TaskAgent { .. }) {
                "unsupported startup parameter; allowed: user, database, giggles.task, options, client_encoding, application_name, DateStyle, TimeZone, extra_float_digits"
            } else {
                "unsupported startup parameter; allowed: user, database, client_encoding, application_name, DateStyle, TimeZone, extra_float_digits"
            };
            return Err("0A000");
        }
        if fields
            .get("client_encoding")
            .is_some_and(|v| !v.eq_ignore_ascii_case("UTF8"))
        {
            reason = "startup client_encoding must be UTF8";
            return Err("0A000");
        }
        if let Some(value) = fields.get("DateStyle") {
            reason = "invalid value for parameter DateStyle";
            startup_settings.datestyle =
                crate::pg_settings::database::date_style(value, startup_settings.datestyle)
                    .map_err(|error| {
                        message = Some(format!("invalid value for parameter \"DateStyle\": \"{value}\""));
                        detail = Some(error.detail());
                        "22023"
                    })?;
        }
        if let Some(value) = fields.get("bytea_output") {
            reason = "invalid value for parameter bytea_output";
            startup_settings.bytea_output = crate::pg_settings::database::bytea_output(value)
                .inspect_err(|_| {
                    message = Some(format!("invalid value for parameter \"bytea_output\": \"{value}\""));
                    hint = Some("Available values: escape, hex.");
                })?;
        }
        if let Some(value) = fields.get("TimeZone") {
            if value.is_empty()
                || value.len() > 128
                || !value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"/_+-:.".contains(&b))
            {
                reason = "startup TimeZone must be a bounded timezone label";
                return Err("0A000");
            }
            timezone = value.clone();
        }
        if fields
            .get("extra_float_digits")
            .is_some_and(|v| v.parse::<i32>().map_or(true, |n| !(-15..=3).contains(&n)))
        {
            reason = "startup extra_float_digits must be an integer from -15 to 3";
            return Err("0A000");
        }
        if let Some(name) = fields.get("application_name") {
            if name.len() > 128 {
                reason = "startup application_name exceeds 128 bytes";
                return Err("54000");
            }
            startup_settings.application_name = name.clone();
        }
        if let Some(digits) = fields.get("extra_float_digits") {
            startup_settings.extra_float_digits = digits.parse().map_err(|_| "22023")?;
        }
        if let Some(style) = fields.get("IntervalStyle") {
            reason = "invalid value for parameter IntervalStyle";
            startup_settings.intervalstyle = crate::pg_settings::database::interval_style(style)?;
        }
        if matches!(policy, StartupPolicy::Postgresql { .. }) {
            for (name, value) in &fields {
                if matches!(name.as_str(), "user" | "database" | "client_encoding" | "application_name" | "DateStyle" | "IntervalStyle" | "TimeZone" | "extra_float_digits" | "bytea_output") {
                    continue;
                }
                reason = "invalid startup configuration parameter";
                let (_, assignment) = crate::pg_settings::database::function_assignment(name, value, false)?;
                let mut session = crate::pg_settings::SessionSettings::default();
                session.current = startup_settings;
                session.apply(&assignment, false);
                startup_settings = session.current;
            }
        }
        startup_settings.timezone = timezone.clone();
        Ok(Startup {
            user: fields.remove("user").unwrap(),
            database: fields.remove("database").unwrap(),
            settings: startup_settings,
            timezone,
            task,
        })
    })();
    validate.map_err(|code| StartupError { code, reason, message, detail, hint })
}

pub(crate) async fn reject_invalid_startup<S: AsyncWrite + Unpin>(
    s: &mut S,
    error: StartupError,
) -> io::Result<()> {
    let Some(message) = error.message else {
        return reject_startup(s, error.code, error.reason).await;
    };
    use crate::pgwire::{frame, z};
    let mut body = Vec::new();
    for (tag, field) in [
        (b'S', Some("FATAL")),
        (b'V', Some("FATAL")),
        (b'C', Some(error.code)),
        (b'M', Some(message.as_str())),
        (b'D', error.detail.as_deref()),
        (b'H', error.hint),
    ] {
        if let Some(field) = field {
            body.push(tag);
            z(field, &mut body);
        }
    }
    body.push(0);
    let mut out = Vec::new();
    frame(b'E', &body, &mut out).map_err(io::Error::other)?;
    write_startup_error(s, out).await
}

pub(crate) async fn reject_startup<S: AsyncWrite + Unpin>(
    s: &mut S,
    code: &'static str,
    reason: &'static str,
) -> io::Result<()> {
    let mut out = vec![];
    error_message(code, reason, &mut out);
    write_startup_error(s, out).await
}

async fn write_startup_error<S: AsyncWrite + Unpin>(s: &mut S, out: Vec<u8>) -> io::Result<()> {
    timeout(IO_TIMEOUT, async {
        s.write_all(&out).await?;
        s.flush().await
    })
    .await
    .map_err(io::Error::other)?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datestyle_startup_accepts_styles_and_reports_invalid_values() {
        let databases = vec!["postgres".into()];
        let policy = StartupPolicy::Postgresql { databases: &databases };
        for (value, canonical) in [("SQL, DMY", "SQL, DMY"), ("german", "German, DMY"), ("postgres, ymd", "Postgres, YMD")] {
            let startup = parse_startup(&body(&[("user", "owner"), ("database", "postgres"), ("datestyle", value)]), &policy).unwrap();
            assert_eq!(startup.settings.datestyle.to_string(), canonical);
        }
        assert_eq!(error(&[("user", "owner"), ("database", "postgres"), ("DateStyle", "ISO, SQL")], &policy),
            ("22023", "invalid value for parameter DateStyle"));
    }

    #[test]
    fn intervalstyle_startup_validates_all_styles_and_case_folded_duplicates() {
        let databases = vec!["giggles".into()];
        let pg = StartupPolicy::Postgresql {
            databases: &databases,
        };
        for style in ["postgres", "postgres_verbose", "sql_standard", "iso_8601"] {
            let startup = parse_startup(
                &body(&[
                    ("user", "giggles"),
                    ("database", "giggles"),
                    ("INTERVALSTYLE", &style.to_ascii_uppercase()),
                ]),
                &pg,
            )
            .ok()
            .unwrap();
            assert_eq!(startup.settings.intervalstyle, style);
        }
        let fields = [
            ("user", "giggles"),
            ("database", "giggles"),
            ("IntervalStyle", "invalid"),
        ];
        assert_eq!(error(&fields, &pg).0, "22023");
        assert_eq!(error(&fields, &StartupPolicy::Agent).0, "0A000");
        assert_eq!(
            error(
                &[
                    ("user", "giggles"),
                    ("database", "giggles"),
                    ("IntervalStyle", "postgres"),
                    ("intervalstyle", "iso_8601")
                ],
                &pg
            )
            .0,
            "08P01"
        );
    }

    #[test]
    fn direct_startup_gucs_use_session_validation() {
        let databases = vec!["giggles".into()];
        let pg = StartupPolicy::Postgresql { databases: &databases };
        let startup = parse_startup(&body(&[("user", "giggles"), ("database", "giggles"), ("SEARCH_PATH", "public"), ("client_min_messages", "warning"), ("statement_timeout", "0")]), &pg).ok().unwrap();
        assert_eq!(startup.settings.search_path, "public");
        assert_eq!(startup.settings.client_min_messages, "warning");
        assert_eq!(error(&[("user", "giggles"), ("database", "giggles"), ("max_identifier_length", "63")], &pg).0, "55P02");
        assert_eq!(error(&[("user", "giggles"), ("database", "giggles"), ("search_path", "public"), ("SEARCH_PATH", "public")], &pg).0, "08P01");
    }

    fn body(fields: &[(&str, &str)]) -> Vec<u8> {
        let mut b = 196608u32.to_be_bytes().to_vec();
        for (key, value) in fields {
            crate::pgwire::z(key, &mut b);
            crate::pgwire::z(value, &mut b);
        }
        b.push(0);
        b
    }

    fn error(fields: &[(&str, &str)], policy: &StartupPolicy<'_>) -> (&'static str, &'static str) {
        parse_startup(&body(fields), policy).err().unwrap()
    }

    #[test]
    fn both_policies_preserve_accepted_fields() {
        let databases = vec![String::from("example")];
        for (user, database, policy) in [
            ("giggles", "giggles", StartupPolicy::Agent),
            ("alice", "example", StartupPolicy::Postgresql { databases: &databases }),
        ] {
            let startup = parse_startup(&body(&[
                ("user", user), ("database", database), ("client_encoding", "utf8"),
                ("DateStyle", "iso"), ("TimeZone", "America/Chicago"),
                ("extra_float_digits", "-15"), ("application_name", "driver"),
            ]), &policy).ok().unwrap();
            assert_eq!(startup.user, user);
            assert_eq!(startup.database, database);
            assert_eq!(startup.timezone, "America/Chicago");
            assert_eq!(startup.settings.timezone, "America/Chicago");
            assert_eq!(startup.settings.application_name, "driver");
            assert_eq!(startup.settings.extra_float_digits, -15);
        }
    }

    #[test]
    fn policy_identity_errors_and_order() {
        let databases = vec![String::from("example")];
        let pg = StartupPolicy::Postgresql { databases: &databases };
        assert_eq!(error(&[("user", "alice"), ("database", "giggles")], &StartupPolicy::Agent),
            ("28000", "startup user must be giggles; authentication uses the client certificate"));
        assert_eq!(error(&[("user", "alice"), ("database", "unknown")], &pg),
            ("3D000", "database does not exist"));
        assert_eq!(error(&[("user", ""), ("database", "unknown")], &pg),
            ("28000", "startup user must name a role"));
        assert_eq!(error(&[("user", "alice")], &pg), ("3D000", "startup database is required"));
        assert_eq!(error(&[("user", "alice"), ("database", "")], &pg), ("3D000", "database does not exist"));
        assert_eq!(error(&[("user", &"a".repeat(64)), ("database", "example")], &pg).0, "28000");
        assert!(parse_startup(&body(&[("user", &"a".repeat(63)), ("database", "example")]), &pg).is_ok());
    }

    #[test]
    fn duplicate_and_unknown_parameters_match_agent() {
        let databases = vec![String::from("giggles")];
        for policy in [StartupPolicy::Agent, StartupPolicy::Postgresql { databases: &databases }] {
            assert_eq!(error(&[("user", "giggles"), ("user", "giggles")], &policy),
                ("08P01", "malformed PostgreSQL startup packet"));
            let expected = if matches!(policy, StartupPolicy::Postgresql { .. }) {
                ("42704", "unrecognized configuration parameter")
            } else {
                ("0A000", "unsupported startup parameter; allowed: user, database, client_encoding, application_name, DateStyle, TimeZone, extra_float_digits")
            };
            assert_eq!(error(&[("user", "giggles"), ("database", "giggles"), ("unknown", "x")], &policy), expected);
        }
    }

    #[test]
    fn postgresql_guc_names_fold_but_agent_and_protocol_keys_do_not() {
        let databases = vec![String::from("giggles")];
        let pg = StartupPolicy::Postgresql {
            databases: &databases,
        };
        for key in ["timezone", "TIMEZONE", "TimeZone"] {
            let startup = parse_startup(
                &body(&[
                    ("user", "giggles"),
                    ("database", "giggles"),
                    (key, "America/Chicago"),
                    ("CLIENT_ENCODING", "utf8"),
                    ("APPLICATION_NAME", "driver"),
                    ("datestyle", "iso"),
                    ("EXTRA_FLOAT_DIGITS", "3"),
                ]),
                &pg,
            )
            .ok()
            .unwrap();
            assert_eq!(startup.timezone, "America/Chicago");
            assert_eq!(startup.settings.application_name, "driver");
            assert_eq!(startup.settings.extra_float_digits, 3);
        }
        for (canonical, variant) in [
            ("TimeZone", "timezone"),
            ("DateStyle", "DATESTYLE"),
            ("client_encoding", "CLIENT_ENCODING"),
            ("application_name", "APPLICATION_NAME"),
            ("extra_float_digits", "EXTRA_FLOAT_DIGITS"),
        ] {
            let fields = [
                ("user", "giggles"),
                ("database", "giggles"),
                (canonical, "invalid"),
                (variant, "invalid"),
            ];
            assert_eq!(
                error(&fields, &pg),
                ("08P01", "malformed PostgreSQL startup packet")
            );
            assert_eq!(
                error(&fields, &StartupPolicy::Agent),
                (
                    "0A000",
                    "unsupported startup parameter; allowed: user, database, client_encoding, application_name, DateStyle, TimeZone, extra_float_digits"
                )
            );
        }
        for key in ["User", "DATABASE", "OPTIONS", "REPLICATION"] {
            assert!(
                parse_startup(
                    &body(&[("user", "giggles"), ("database", "giggles"), (key, "x"),]),
                    &pg
                )
                .is_err()
            );
        }
        assert_eq!(
            error(
                &[
                    ("user", "giggles"),
                    ("database", "giggles"),
                    ("CLIENT_ENCODING", "LATIN1")
                ],
                &pg
            )
            .1,
            "startup client_encoding must be UTF8"
        );
        assert_eq!(
            error(
                &[
                    ("user", "giggles"),
                    ("database", "giggles"),
                    ("timezone", "")
                ],
                &pg
            )
            .1,
            "startup TimeZone must be a bounded timezone label"
        );
    }

    #[test]
    fn protocol_precedes_identity_and_defaults_are_utc() {
        let mut b = body(&[]);
        b[..4].copy_from_slice(&0u32.to_be_bytes());
        assert_eq!(parse_startup(&b, &StartupPolicy::Agent).err().unwrap(),
            ("0A000", "only PostgreSQL protocol version 3 is supported"));
        let startup = parse_startup(&body(&[("user", "giggles"), ("database", "giggles")]), &StartupPolicy::Agent).ok().unwrap();
        assert_eq!(startup.timezone, "UTC");
        assert_eq!(startup.settings.timezone, "UTC");
    }

    #[test]
    fn task_agent_reads_the_task_from_the_parameter_or_exact_options() {
        let databases = vec![String::from("giggles")];
        let agent = StartupPolicy::TaskAgent { databases: &databases };
        let base = [("user", "agent_exec"), ("database", "giggles")];
        let with = |extra: &[(&'static str, &'static str)]| {
            let mut fields = base.to_vec();
            fields.extend_from_slice(extra);
            fields
        };
        let task = |fields: &[(&str, &str)]| parse_startup(&body(fields), &agent).ok().unwrap().task;
        assert_eq!(task(&base), None);
        assert_eq!(task(&with(&[("giggles.task", "inc-436")])).as_deref(), Some("inc-436"));
        assert_eq!(task(&with(&[("options", "-c giggles.task=inc-436")])).as_deref(), Some("inc-436"));
        let startup = parse_startup(&body(&with(&[("giggles.task", "inc-436")])), &agent).ok().unwrap();
        assert_eq!((startup.user.as_str(), startup.database.as_str()), ("agent_exec", "giggles"));
        for options in [
            "-c search_path=public",
            "-c giggles.task=inc-436 -c search_path=public",
            "-c giggles.task=",
            "-cgiggles.task=inc-436",
            " -c giggles.task=inc-436",
            "--giggles.task=inc-436",
            "",
        ] {
            assert_eq!(error(&with(&[("options", options)]), &agent).0, "0A000", "{options:?}");
        }
        assert_eq!(error(&with(&[("giggles.task", "")]), &agent).0, "0A000");
        assert_eq!(
            error(&with(&[("giggles.task", "inc-436"), ("options", "-c giggles.task=inc-436")]), &agent).0,
            "0A000"
        );
        // Other parameters keep the locked agent allow-list, matched exactly.
        assert_eq!(error(&with(&[("search_path", "public")]), &agent).0, "0A000");
        assert_eq!(error(&with(&[("GIGGLES.TASK", "inc-436")]), &agent).0, "0A000");
        assert_eq!(error(&with(&[("replication", "true")]), &agent).0, "0A000");
        // The other policies never accept the task parameters.
        let pg = StartupPolicy::Postgresql { databases: &databases };
        assert!(parse_startup(&body(&with(&[("giggles.task", "inc-436")])), &pg).is_err());
        assert!(parse_startup(&body(&[("user", "giggles"), ("database", "giggles"), ("options", "-c giggles.task=x")]), &StartupPolicy::Agent).is_err());
        // Identity: a role name is required and the database must be served.
        assert_eq!(error(&[("user", ""), ("database", "giggles")], &agent).0, "28000");
        assert_eq!(error(&[("user", "agent_exec"), ("database", "other")], &agent).0, "3D000");
        assert_eq!(error(&[("user", "agent_exec")], &agent).0, "3D000");
    }
}
