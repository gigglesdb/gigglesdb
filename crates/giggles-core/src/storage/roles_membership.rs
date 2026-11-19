//! PostgreSQL role graph and legacy-compatible encoding.
use super::*;

pub(super) fn inherit_default() -> bool {
    true
}
pub(super) fn is_true(value: &bool) -> bool {
    *value
}

pub const PREDEFINED: &[(u32, &str)] = &[
    (3373, "pg_monitor"),
    (3374, "pg_read_all_settings"),
    (3375, "pg_read_all_stats"),
    (3377, "pg_stat_scan_tables"),
    (4200, "pg_signal_backend"),
    (4544, "pg_checkpoint"),
    (4550, "pg_use_reserved_connections"),
    (4569, "pg_read_server_files"),
    (4570, "pg_write_server_files"),
    (4571, "pg_execute_server_program"),
    (6171, "pg_database_owner"),
    (6181, "pg_read_all_data"),
    (6182, "pg_write_all_data"),
    (6304, "pg_create_subscription"),
    (6337, "pg_maintain"),
    (6392, "pg_signal_autovacuum_worker"),
];
pub(super) fn predefined(name: &str) -> Option<CatalogRole> {
    let (oid, _) = PREDEFINED.iter().find(|(_, n)| *n == name)?;
    Some(CatalogRole {
        name: name.into(),
        oid: *oid,
        superuser: false,
        login: false,
        createrole: false,
        password: None,
        generation: 1,
        task_only: false,
        inherit: true,
    })
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MembershipOptions {
    pub admin: Option<bool>,
    pub inherit: Option<bool>,
    pub set: Option<bool>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MembershipOption {
    Admin,
    Inherit,
    Set,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MembershipGrant {
    Granted,
    Updated,
    Unchanged,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipEdge {
    pub role: String,
    pub member: String,
    pub grantor: String,
    pub admin: bool,
    pub inherit: bool,
    pub set: bool,
}
pub(super) fn role_record(table: &RoleTable, name: &str) -> Result<Option<CatalogRole>, RoleError> {
    match raw_get(table, &key(b"role/", name))? {
        Some(value) => decode(&value).map(Some),
        None => Ok(predefined(name)),
    }
}
// Logical grantor identity; bootstrap's display name may change, its key never does.
pub const BOOTSTRAP: &str = "\0bootstrap";

pub(super) fn membership_key(edge: &MembershipEdge, bootstrap: &str) -> Vec<u8> {
    let mut key = grant_key(b"member/", &edge.role, &edge.member);
    if edge.grantor != bootstrap {
        key.push(0);
        key.extend(edge.grantor.as_bytes());
    }
    key
}
pub(super) fn store_edge(table: &mut RoleTable, edge: &MembershipEdge) -> Result<(), RoleError> {
    let bootstrap = BOOTSTRAP.to_owned();
    let key = membership_key(edge, &bootstrap);
    let value = if edge.grantor == bootstrap && !edge.admin && edge.inherit && edge.set {
        Vec::new()
    } else {
        // The key carries logical bootstrap identity. Keep its JSON name valid,
        // and never persist the internal sentinel or confuse it with a named role.
        let mut stored = edge.clone();
        if stored.grantor == BOOTSTRAP { stored.grantor = "bootstrap".into(); }
        json(&stored)?
    };
    table.insert(&key, &value)?;
    Ok(())
}
pub(super) fn membership_edges(table: &RoleTable) -> Result<Vec<MembershipEdge>, RoleError> {
    let bootstrap = BOOTSTRAP.to_owned();
    let mut edges = Vec::new();
    for (key, value) in &table.0 {
        let Some(pair) = key.strip_prefix(b"member/") else {
            continue;
        };
        let pair = std::str::from_utf8(pair).map_err(io)?;
        let parts: Vec<_> = pair.split('\0').collect();
        if !(2..=3).contains(&parts.len()) {
            return Err(RoleError::Invalid);
        }
        let edge = if value.is_empty() {
            if parts.len() != 2 {
                return Err(RoleError::Invalid);
            }
            MembershipEdge {
                role: parts[0].into(),
                member: parts[1].into(),
                grantor: bootstrap.clone(),
                admin: false,
                inherit: true,
                set: true,
            }
        } else {
            let mut edge: MembershipEdge = decode(value)?;
            validate_name(&edge.grantor)?;
            if edge.role != parts[0]
                || edge.member != parts[1]
                || (parts.len() == 3 && edge.grantor != parts[2])
            {
                return Err(RoleError::Invalid);
            }
            // Unsuffixed keys always address the logical bootstrap grant.
            if parts.len() == 2 { edge.grantor = BOOTSTRAP.into(); }
            edge
        };
        edges.push(edge);
    }
    for role in [
        "pg_read_all_settings",
        "pg_read_all_stats",
        "pg_stat_scan_tables",
    ] {
        if raw_get(table, &key(b"role/", "pg_monitor"))?.is_none()
            && raw_get(table, &key(b"role/", role))?.is_none() && !edges
            .iter()
            .any(|e| e.role == role && e.member == "pg_monitor" && e.grantor == bootstrap)
        {
            edges.push(MembershipEdge {
                role: role.into(),
                member: "pg_monitor".into(),
                grantor: bootstrap.clone(),
                admin: false,
                inherit: true,
                set: true,
            });
        }
    }
    Ok(edges)
}
pub(super) fn reachable(
    edges: &[MembershipEdge],
    member: &str,
    follow: impl Fn(&MembershipEdge) -> bool,
) -> BTreeSet<String> {
    let mut seen = BTreeSet::new();
    let mut pending = vec![member.to_owned()];
    while let Some(next) = pending.pop() {
        if seen.insert(next.clone()) {
            pending.extend(
                edges
                    .iter()
                    .filter(|e| e.member == next && follow(e))
                    .map(|e| e.role.clone()),
            );
        }
    }
    seen
}
pub(super) fn extended_record(key: &[u8], value: &[u8]) -> bool {
    if (key.starts_with(b"tgrant/") || key.starts_with(b"sgrant/")) && key.last() == Some(&0) { return true; }
    if key.starts_with(b"role/") {
        return decode::<CatalogRole>(value).is_ok_and(|r| !r.inherit);
    }
    key.strip_prefix(b"member/").is_some_and(|pair| {
        !value.is_empty() || pair.iter().filter(|b| **b == 0).count() > 1
    })
}

/// Empty legacy edges need table context to distinguish stored and synthesized roles.
pub(super) fn synthesized_membership(table: &RoleTable, raw: &[u8]) -> bool {
    raw.strip_prefix(b"member/").is_some_and(|pair| {
        pair.split(|b| *b == 0).take(2).any(|name| {
            std::str::from_utf8(name).ok().is_some_and(|name|
                predefined(name).is_some() && !table.0.contains_key(&key(b"role/", name)))
        })
    })
}
