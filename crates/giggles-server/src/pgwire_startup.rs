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
}

pub(crate) struct Startup {
    pub(crate) user: String,
    /// Validated against the listener's configured databases. Kept for
    /// current_database() and catalog presentation, which do not read it yet.
    #[allow(dead_code)]
    pub(crate) database: String,
    pub(crate) settings: crate::pg_settings::Settings,
    pub(crate) timezone: String,
}

pub(crate) fn parse_startup(
    body: &[u8],
    policy: &StartupPolicy<'_>,
) -> Result<Startup, (&'static str, &'static str)> {
    let mut reason = "malformed PostgreSQL startup packet";
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
            let k = r.text()?;
            if k.is_empty() {
                break;
            }
            let v = r.text()?;
            if fields.insert(k, v).is_some() {
                return Err("08P01");
            }
        }
        r.end()?;
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
            StartupPolicy::Postgresql { databases } => {
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
        if fields.keys().any(|k| {
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
            reason = "unsupported startup parameter; allowed: user, database, client_encoding, application_name, DateStyle, TimeZone, extra_float_digits";
            return Err("0A000");
        }
        if fields
            .get("client_encoding")
            .is_some_and(|v| !v.eq_ignore_ascii_case("UTF8"))
        {
            reason = "startup client_encoding must be UTF8";
            return Err("0A000");
        }
        // Driver presentation hints have no effect on the current integer/text/
        // boolean/bytes type set. They never change clock or task authority.
        if fields
            .get("DateStyle")
            .is_some_and(|v| !v.eq_ignore_ascii_case("ISO"))
        {
            reason = "startup DateStyle must be ISO";
            return Err("0A000");
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
        startup_settings.timezone = timezone.clone();
        Ok(Startup {
            user: fields.remove("user").unwrap(),
            database: fields.remove("database").unwrap(),
            settings: startup_settings,
            timezone,
        })
    })();
    validate.map_err(|code| (code, reason))
}

pub(crate) async fn reject_startup<S: AsyncWrite + Unpin>(
    s: &mut S,
    code: &'static str,
    reason: &'static str,
) -> io::Result<()> {
    let mut out = vec![];
    error_message(code, reason, &mut out);
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
            assert_eq!(error(&[("user", "giggles"), ("database", "giggles"), ("unknown", "x")], &policy),
                ("0A000", "unsupported startup parameter; allowed: user, database, client_encoding, application_name, DateStyle, TimeZone, extra_float_digits"));
        }
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
}
