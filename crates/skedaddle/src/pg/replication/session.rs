use tokio::io::{AsyncRead, AsyncWrite};

use super::{
    ReplicationError as E, Result,
    wire::{Wire, text},
};

/// Mapping's PostgreSQL text-output contract, applied on the replication wire.
pub const REQUIRED_SETTINGS_SQL: &str = "SET DateStyle = 'ISO, MDY'; SET IntervalStyle = 'postgres'; SET TimeZone = 'UTC'; SET extra_float_digits = 3; SET client_encoding = 'UTF8'; SET standard_conforming_strings = on";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionSettings {
    pub date_style: String,
    pub interval_style: String,
    pub time_zone: String,
    pub extra_float_digits: i32,
    pub client_encoding: String,
    pub standard_conforming_strings: bool,
}

pub(super) async fn setup<S: AsyncRead + AsyncWrite + Unpin>(
    wire: &mut Wire<S>,
) -> Result<SessionSettings> {
    // replication=database accepts normal SQL, exclusively via simple query.
    // SET emits ParameterStatus frames, consumed by Wire::query as well.
    if !wire.query(REQUIRED_SETTINGS_SQL).await?.is_empty() {
        return Err(E::Invalid("unexpected session SET rows"));
    }
    let mut values = Vec::with_capacity(6);
    for name in [
        "DateStyle",
        "IntervalStyle",
        "TimeZone",
        "extra_float_digits",
        "client_encoding",
        "standard_conforming_strings",
    ] {
        let row = wire.query_one(&format!("SHOW {name}")).await?;
        if row.len() != 1 {
            return Err(E::Invalid("session SHOW column count"));
        }
        values.push(text(&row, 0)?.to_owned());
    }
    let standard_conforming_strings = match values[5].as_str() {
        "on" => true,
        "off" => false,
        _ => return Err(E::Invalid("session SHOW boolean")),
    };
    Ok(SessionSettings {
        date_style: values[0].clone(),
        interval_style: values[1].clone(),
        time_zone: values[2].clone(),
        extra_float_digits: values[3]
            .parse()
            .map_err(|_| E::Invalid("session SHOW integer"))?,
        client_encoding: values[4].clone(),
        standard_conforming_strings,
    })
}

#[cfg(test)]
mod tests;
