//! PostgreSQL 18.6 timezone abbreviation sets, embedded under the PostgreSQL License.
use crate::{Error, R};
use giggles_pg_types::temporal::{SessionZone, timezone_info};
use std::collections::BTreeMap;
type Rows = BTreeMap<String, (i32, bool)>;
fn source(name: &str) -> R<&'static str> {
    match name {
        "Africa.txt" => Ok(include_str!("timezonesets/Africa.txt")),
        "America.txt" => Ok(include_str!("timezonesets/America.txt")),
        "Antarctica.txt" => Ok(include_str!("timezonesets/Antarctica.txt")),
        "Asia.txt" => Ok(include_str!("timezonesets/Asia.txt")),
        "Atlantic.txt" => Ok(include_str!("timezonesets/Atlantic.txt")),
        "Australia" => Ok(include_str!("timezonesets/Australia")),
        "Australia.txt" => Ok(include_str!("timezonesets/Australia.txt")),
        "Default" => Ok(include_str!("timezonesets/Default")),
        "Etc.txt" => Ok(include_str!("timezonesets/Etc.txt")),
        "Europe.txt" => Ok(include_str!("timezonesets/Europe.txt")),
        "India" => Ok(include_str!("timezonesets/India")),
        "Indian.txt" => Ok(include_str!("timezonesets/Indian.txt")),
        "Pacific.txt" => Ok(include_str!("timezonesets/Pacific.txt")),
        _ => Err(Error::InvalidSnapshot),
    }
}
fn load(text: &str, now: i64, rows: &mut Rows, depth: usize) -> R<()> {
    if depth > 10 {
        return Err(Error::InvalidSnapshot);
    }
    let mut override_existing = false;
    for line in text.lines() {
        let fields: Vec<_> = line
            .split('#')
            .next()
            .unwrap_or("")
            .split_whitespace()
            .collect();
        if fields.is_empty() {
            continue;
        }
        match fields.as_slice() {
            ["@INCLUDE", name] => load(source(name)?, now, rows, depth + 1)?,
            ["@OVERRIDE"] => override_existing = true,
            [name, value] | [name, value, "D"] => {
                let entry = if let Ok(seconds) = value.parse::<i32>() {
                    (seconds, fields.len() == 3)
                } else {
                    let (_, seconds, dst) =
                        timezone_info(value, now).map_err(|_| Error::InvalidSnapshot)?;
                    (seconds, dst)
                };
                if let Some(old) = rows.get(*name)
                    && *old != entry
                    && !override_existing
                {
                    return Err(Error::InvalidSnapshot);
                }
                rows.insert((*name).to_owned(), entry);
            }
            _ => return Err(Error::InvalidSnapshot),
        }
    }
    Ok(())
}
pub(crate) fn rows(zone: SessionZone, now: i64) -> R<Vec<(String, i32, bool)>> {
    let mut rows = Rows::new();
    load(source("Default")?, now, &mut rows, 0)?;
    for (name, seconds, dst) in zone.abbreviations().map_err(|_| Error::InvalidSnapshot)? {
        rows.entry(name).or_insert((seconds, dst));
    }
    Ok(rows
        .into_iter()
        .map(|(name, (seconds, dst))| (name, seconds, dst))
        .collect())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn includes_and_overrides() {
        let mut rows = Rows::new();
        load("@INCLUDE Etc.txt", 1794844800, &mut rows, 0).unwrap();
        assert_eq!(rows["UTC"], (0, false));
        assert!(load("UTC 3600", 1794844800, &mut rows, 0).is_err());
        load("@OVERRIDE\nUTC 3600 D", 1794844800, &mut rows, 0).unwrap();
        assert_eq!(rows["UTC"], (3600, true));
        assert!(load("@INCLUDE missing", 1794844800, &mut rows, 0).is_err());
    }
}
