//! Durable role metadata for trusted local administration.
//!
//! Password values are opaque verifiers supplied by the caller. Callers must
//! never pass plaintext passwords; this catalog does not hash or authenticate.

use super::{Admin, CoreError, Inner, io};
use redb::{Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{collections::BTreeSet, fmt, sync::Arc, sync::atomic::Ordering};

pub const MAX_ROLE_NAME: usize = 63;
pub const MAX_ROLES: usize = 1024;
pub const MAX_VERIFIER: usize = 512;
pub const FIRST_ROLE_OID: u32 = 16384;

const ROLES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("giggles_roles_v1");
const NEXT_OID: &[u8] = b"meta/next_oid";
type ReadTable = redb::ReadOnlyTable<&'static [u8], &'static [u8]>;
type WriteTable<'a> = redb::Table<'a, &'static [u8], &'static [u8]>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogRole {
    pub name: String,
    pub oid: u32,
    pub superuser: bool,
    pub login: bool,
    pub createrole: bool,
    /// An opaque printable ASCII SCRAM-SHA-256 verifier, never plaintext.
    /// `None` means no password.
    pub password: Option<String>,
    /// Increments on every alter (attributes or password). Starts at 1.
    pub generation: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NewRole {
    pub name: String,
    pub superuser: bool,
    pub login: bool,
    pub createrole: bool,
    /// An opaque printable ASCII verifier, never a plaintext password.
    pub password: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RoleChanges {
    pub superuser: Option<bool>,
    pub login: Option<bool>,
    pub createrole: Option<bool>,
    /// `None` preserves the verifier; `Some(None)` clears it.
    pub password: Option<Option<String>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TablePrivilege {
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum SchemaPrivilege {
    Usage,
    Create,
}

#[derive(Debug)]
pub enum RoleError {
    Duplicate,
    Missing,
    Dependent,
    Invalid,
    Limit,
    Core(CoreError),
}

impl fmt::Display for RoleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Duplicate => f.write_str("role already exists"),
            Self::Missing => f.write_str("role does not exist"),
            Self::Dependent => f.write_str("role owns a table"),
            Self::Invalid => f.write_str("invalid role catalog input"),
            Self::Limit => f.write_str("role catalog limit exceeded"),
            Self::Core(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for RoleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Core(error) => Some(error),
            _ => None,
        }
    }
}

impl From<CoreError> for RoleError {
    fn from(error: CoreError) -> Self {
        Self::Core(error)
    }
}

#[derive(Clone)]
pub struct RoleCatalog {
    inner: Arc<Inner>,
}

impl Admin {
    pub fn roles(&self) -> RoleCatalog {
        RoleCatalog {
            inner: Arc::clone(&self.inner),
        }
    }
}

fn validate_name(name: &str) -> Result<(), RoleError> {
    if name.is_empty() || name.len() > MAX_ROLE_NAME || name.contains('\0') {
        return Err(RoleError::Invalid);
    }
    Ok(())
}

fn validate_verifier(verifier: Option<&str>) -> Result<(), RoleError> {
    if let Some(verifier) = verifier
        && (verifier.is_empty()
            || verifier.len() > MAX_VERIFIER
            || !verifier.bytes().all(|byte| (b' '..=b'~').contains(&byte)))
    {
        return Err(RoleError::Invalid);
    }
    Ok(())
}

fn key(prefix: &[u8], name: &str) -> Vec<u8> {
    [prefix, name.as_bytes()].concat()
}

fn grant_key(prefix: &[u8], object: &str, role: &str) -> Vec<u8> {
    [prefix, object.as_bytes(), b"\0", role.as_bytes()].concat()
}

fn json<T: Serialize>(value: &T) -> Result<Vec<u8>, RoleError> {
    serde_json::to_vec(value).map_err(|error| CoreError::Corrupt(error.to_string()).into())
}

fn decode<T: DeserializeOwned>(value: &[u8]) -> Result<T, RoleError> {
    serde_json::from_slice(value).map_err(|error| CoreError::Corrupt(error.to_string()).into())
}

fn raw_get(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    key: &[u8],
) -> Result<Option<Vec<u8>>, RoleError> {
    Ok(table
        .get(key)
        .map_err(io)?
        .map(|value| value.value().to_vec()))
}

fn require_role(table: &WriteTable<'_>, name: &str) -> Result<(), RoleError> {
    if raw_get(table, &key(b"role/", name))?.is_none() {
        return Err(RoleError::Missing);
    }
    Ok(())
}

fn privileges<P: DeserializeOwned + Ord>(value: Option<Vec<u8>>) -> Result<BTreeSet<P>, RoleError> {
    match value {
        Some(value) => Ok(decode::<Vec<P>>(&value)?.into_iter().collect()),
        None => Ok(BTreeSet::new()),
    }
}

impl RoleCatalog {
    fn check_fenced(&self) -> Result<(), RoleError> {
        if self.inner.fenced.load(Ordering::Acquire) {
            return Err(CoreError::Fenced.into());
        }
        Ok(())
    }

    fn read<T>(
        &self,
        inspect: impl FnOnce(Option<&ReadTable>) -> Result<T, RoleError>,
    ) -> Result<T, RoleError> {
        self.check_fenced()?;
        let tx = self.inner.db(super::ROLES)?.begin_read().map_err(io)?;
        self.check_fenced()?;
        match tx.open_table(ROLES) {
            Ok(table) => inspect(Some(&table)),
            Err(redb::TableError::TableDoesNotExist(_)) => inspect(None),
            Err(error) => Err(io(error).into()),
        }
    }

    fn write<T>(
        &self,
        change: impl FnOnce(&mut WriteTable<'_>) -> Result<T, RoleError>,
    ) -> Result<T, RoleError> {
        self.check_fenced()?;
        let mut tx = self.inner.db(super::ROLES)?.begin_write().map_err(io)?;
        self.check_fenced()?;
        tx.set_durability(Durability::Immediate).map_err(io)?;
        let result = {
            let mut table = tx.open_table(ROLES).map_err(io)?;
            change(&mut table)?
        };
        tx.commit().map_err(io)?;
        Ok(result)
    }

    pub fn get(&self, name: &str) -> Result<Option<CatalogRole>, RoleError> {
        self.check_fenced()?;
        validate_name(name)?;
        self.read(|table| match table {
            Some(table) => raw_get(table, &key(b"role/", name))?
                .map(|value| decode(&value))
                .transpose(),
            None => Ok(None),
        })
    }

    pub fn list(&self) -> Result<Vec<CatalogRole>, RoleError> {
        self.check_fenced()?;
        self.read(|table| {
            let mut roles: Vec<CatalogRole> = Vec::new();
            if let Some(table) = table {
                for entry in table.iter().map_err(io)? {
                    let (key, value) = entry.map_err(io)?;
                    if key.value().starts_with(b"role/") {
                        roles.push(decode(value.value())?);
                    }
                }
            }
            roles.sort_by(|left, right| left.name.cmp(&right.name));
            Ok(roles)
        })
    }

    pub fn is_empty(&self) -> Result<bool, RoleError> {
        self.check_fenced()?;
        Ok(self.list()?.is_empty())
    }

    pub fn create(&self, new: NewRole) -> Result<CatalogRole, RoleError> {
        self.check_fenced()?;
        validate_name(&new.name)?;
        validate_verifier(new.password.as_deref())?;
        self.write(|table| {
            let role_key = key(b"role/", &new.name);
            if raw_get(table, &role_key)?.is_some() {
                return Err(RoleError::Duplicate);
            }
            let mut count = 0;
            for entry in table.iter().map_err(io)? {
                if entry.map_err(io)?.0.value().starts_with(b"role/") {
                    count += 1;
                }
            }
            if count >= MAX_ROLES {
                return Err(RoleError::Limit);
            }
            let oid = match raw_get(table, NEXT_OID)? {
                Some(value) => u32::from_be_bytes(
                    value
                        .try_into()
                        .map_err(|_| CoreError::Corrupt("invalid role OID metadata".into()))?,
                ),
                None => FIRST_ROLE_OID,
            };
            if oid < FIRST_ROLE_OID {
                return Err(CoreError::Corrupt("invalid next role OID".into()).into());
            }
            let next_oid = oid.checked_add(1).ok_or(RoleError::Limit)?;
            let role = CatalogRole {
                name: new.name,
                oid,
                superuser: new.superuser,
                login: new.login,
                createrole: new.createrole,
                password: new.password,
                generation: 1,
            };
            table
                .insert(role_key.as_slice(), json(&role)?.as_slice())
                .map_err(io)?;
            table
                .insert(NEXT_OID, next_oid.to_be_bytes().as_slice())
                .map_err(io)?;
            Ok(role)
        })
    }

    pub fn alter(&self, name: &str, changes: RoleChanges) -> Result<CatalogRole, RoleError> {
        self.check_fenced()?;
        validate_name(name)?;
        if let Some(verifier) = &changes.password {
            validate_verifier(verifier.as_deref())?;
        }
        self.write(|table| {
            let role_key = key(b"role/", name);
            let mut role: CatalogRole =
                decode(&raw_get(table, &role_key)?.ok_or(RoleError::Missing)?)?;
            role.generation = role.generation.checked_add(1).ok_or(RoleError::Limit)?;
            if let Some(value) = changes.superuser {
                role.superuser = value;
            }
            if let Some(value) = changes.login {
                role.login = value;
            }
            if let Some(value) = changes.createrole {
                role.createrole = value;
            }
            if let Some(value) = changes.password {
                role.password = value;
            }
            table
                .insert(role_key.as_slice(), json(&role)?.as_slice())
                .map_err(io)?;
            Ok(role)
        })
    }

    pub fn drop(&self, name: &str) -> Result<(), RoleError> {
        self.check_fenced()?;
        validate_name(name)?;
        self.write(|table| {
            require_role(table, name)?;
            let mut remove = Vec::new();
            for entry in table.iter().map_err(io)? {
                let (key, value) = entry.map_err(io)?;
                let bytes = key.value();
                if bytes.starts_with(b"owner/") && value.value() == name.as_bytes() {
                    return Err(RoleError::Dependent);
                }
                if (bytes.starts_with(b"tgrant/") || bytes.starts_with(b"sgrant/"))
                    && bytes.split(|byte| *byte == 0).nth(1) == Some(name.as_bytes())
                {
                    remove.push(bytes.to_vec());
                }
            }
            for key in remove {
                table.remove(key.as_slice()).map_err(io)?;
            }
            table.remove(key(b"role/", name).as_slice()).map_err(io)?;
            Ok(())
        })
    }

    pub fn table_owner(&self, table: &str) -> Result<Option<String>, RoleError> {
        self.check_fenced()?;
        validate_name(table)?;
        self.read(|roles| match roles {
            Some(roles) => raw_get(roles, &key(b"owner/", table))?
                .map(|value| {
                    String::from_utf8(value)
                        .map_err(|error| CoreError::Corrupt(error.to_string()).into())
                })
                .transpose(),
            None => Ok(None),
        })
    }

    pub fn set_table_owner(&self, table: &str, role: &str) -> Result<(), RoleError> {
        self.check_fenced()?;
        validate_name(table)?;
        validate_name(role)?;
        self.write(|roles| {
            require_role(roles, role)?;
            roles
                .insert(key(b"owner/", table).as_slice(), role.as_bytes())
                .map_err(io)?;
            Ok(())
        })
    }

    pub fn forget_table(&self, table: &str) -> Result<(), RoleError> {
        self.check_fenced()?;
        validate_name(table)?;
        self.write(|roles| {
            let prefix = grant_key(b"tgrant/", table, "");
            let mut remove = Vec::new();
            for entry in roles.iter().map_err(io)? {
                let (key, _) = entry.map_err(io)?;
                if key.value().starts_with(&prefix) {
                    remove.push(key.value().to_vec());
                }
            }
            for key in remove {
                roles.remove(key.as_slice()).map_err(io)?;
            }
            roles.remove(key(b"owner/", table).as_slice()).map_err(io)?;
            Ok(())
        })
    }

    fn change_grants<P: Copy + Ord + Serialize + DeserializeOwned>(
        &self,
        prefix: &[u8],
        object: &str,
        role: &str,
        changes: &[P],
        grant: bool,
    ) -> Result<(), RoleError> {
        self.check_fenced()?;
        validate_name(object)?;
        validate_name(role)?;
        self.write(|table| {
            require_role(table, role)?;
            let key = grant_key(prefix, object, role);
            let mut current: BTreeSet<P> = privileges(raw_get(table, &key)?)?;
            for privilege in changes {
                if grant {
                    current.insert(*privilege);
                } else {
                    current.remove(privilege);
                }
            }
            if current.is_empty() {
                table.remove(key.as_slice()).map_err(io)?;
            } else {
                let values: Vec<P> = current.into_iter().collect();
                table
                    .insert(key.as_slice(), json(&values)?.as_slice())
                    .map_err(io)?;
            }
            Ok(())
        })
    }

    fn read_grants<P: Ord + DeserializeOwned>(
        &self,
        prefix: &[u8],
        object: &str,
        role: &str,
    ) -> Result<BTreeSet<P>, RoleError> {
        self.check_fenced()?;
        validate_name(object)?;
        validate_name(role)?;
        self.read(|table| match table {
            Some(table) => privileges(raw_get(table, &grant_key(prefix, object, role))?),
            None => Ok(BTreeSet::new()),
        })
    }

    pub fn grant_table(
        &self,
        table: &str,
        role: &str,
        privileges: &[TablePrivilege],
    ) -> Result<(), RoleError> {
        self.change_grants(b"tgrant/", table, role, privileges, true)
    }

    pub fn revoke_table(
        &self,
        table: &str,
        role: &str,
        privileges: &[TablePrivilege],
    ) -> Result<(), RoleError> {
        self.change_grants(b"tgrant/", table, role, privileges, false)
    }

    pub fn table_privileges(
        &self,
        table: &str,
        role: &str,
    ) -> Result<BTreeSet<TablePrivilege>, RoleError> {
        self.read_grants(b"tgrant/", table, role)
    }

    pub fn grant_schema(
        &self,
        schema: &str,
        role: &str,
        privileges: &[SchemaPrivilege],
    ) -> Result<(), RoleError> {
        self.change_grants(b"sgrant/", schema, role, privileges, true)
    }

    pub fn revoke_schema(
        &self,
        schema: &str,
        role: &str,
        privileges: &[SchemaPrivilege],
    ) -> Result<(), RoleError> {
        self.change_grants(b"sgrant/", schema, role, privileges, false)
    }

    pub fn schema_privileges(
        &self,
        schema: &str,
        role: &str,
    ) -> Result<BTreeSet<SchemaPrivilege>, RoleError> {
        self.read_grants(b"sgrant/", schema, role)
    }

    pub fn visible_tables(&self, role: &str) -> Result<BTreeSet<String>, RoleError> {
        self.check_fenced()?;
        validate_name(role)?;
        self.read(|table| {
            let mut visible = BTreeSet::new();
            if let Some(table) = table {
                for entry in table.iter().map_err(io)? {
                    let (key, value) = entry.map_err(io)?;
                    let bytes = key.value();
                    let name = if let Some(name) = bytes.strip_prefix(b"owner/") {
                        (value.value() == role.as_bytes()).then_some(name)
                    } else if let Some(grant) = bytes.strip_prefix(b"tgrant/") {
                        match grant.iter().position(|byte| *byte == 0) {
                            Some(separator)
                                if &grant[separator + 1..] == role.as_bytes()
                                    && !decode::<Vec<TablePrivilege>>(value.value())?
                                        .is_empty() =>
                            {
                                Some(&grant[..separator])
                            }
                            _ => None,
                        }
                    } else {
                        None
                    };
                    if let Some(name) = name {
                        visible.insert(
                            String::from_utf8(name.to_vec())
                                .map_err(|error| CoreError::Corrupt(error.to_string()))?,
                        );
                    }
                }
            }
            Ok(visible)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Limits;
    use std::path::{Path, PathBuf};

    struct TestDir(PathBuf);

    impl Drop for TestDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).expect("remove closed role test database");
        }
    }

    fn database(test: impl FnOnce(Admin, &Path)) {
        let mut id = [0u8; 16];
        getrandom::fill(&mut id).unwrap();
        let directory = TestDir(
            std::env::temp_dir().join(format!("giggles-roles-{:032x}", u128::from_be_bytes(id))),
        );
        let admin = Admin::create(&directory.0, Limits::default()).unwrap();
        test(admin, &directory.0);
    }

    fn new_role(name: &str) -> NewRole {
        NewRole {
            name: name.into(),
            ..NewRole::default()
        }
    }

    fn main_records(admin: &Admin) -> Vec<(Vec<u8>, Vec<u8>)> {
        let tx = admin.inner.db(super::super::ROLES).unwrap().begin_read().unwrap();
        let table = tx.open_table(super::super::RECORDS).unwrap();
        table
            .iter()
            .unwrap()
            .map(|entry| {
                let (key, value) = entry.unwrap();
                (key.value().to_vec(), value.value().to_vec())
            })
            .collect()
    }

    #[test]
    fn absent_catalog_reads_are_empty_and_do_not_create_the_table() {
        database(|admin, _| {
            let roles = admin.roles();
            assert!(roles.is_empty().unwrap());
            assert_eq!(roles.get("absent").unwrap(), None);
            assert!(roles.list().unwrap().is_empty());
            assert_eq!(roles.table_owner("t").unwrap(), None);
            assert!(roles.table_privileges("t", "absent").unwrap().is_empty());
            assert!(roles.schema_privileges("s", "absent").unwrap().is_empty());
            assert!(roles.visible_tables("absent").unwrap().is_empty());
            let tx = admin.inner.db(super::super::ROLES).unwrap().begin_read().unwrap();
            assert!(matches!(
                tx.open_table(ROLES),
                Err(redb::TableError::TableDoesNotExist(_))
            ));
        });
    }

    #[test]
    fn role_lifecycle_orders_names_and_never_reuses_oids() {
        database(|admin, _| {
            let roles = admin.roles();
            let z = roles.create(new_role("Zulu")).unwrap();
            let a = roles.create(new_role(" Alpha ")).unwrap();
            let lower = roles.create(new_role("alpha")).unwrap();
            assert_eq!(
                (z.oid, a.oid, lower.oid),
                (FIRST_ROLE_OID, FIRST_ROLE_OID + 1, FIRST_ROLE_OID + 2)
            );
            assert_eq!(z.generation, 1);
            assert!(!z.superuser && !z.login && !z.createrole);
            assert_eq!(z.password, None);
            assert_eq!(roles.get("Zulu").unwrap(), Some(z));
            assert_eq!(roles.get("zulu").unwrap(), None);
            assert_eq!(
                roles
                    .list()
                    .unwrap()
                    .iter()
                    .map(|role| role.name.as_str())
                    .collect::<Vec<_>>(),
                [" Alpha ", "Zulu", "alpha"]
            );
            assert!(matches!(
                roles.create(new_role("alpha")),
                Err(RoleError::Duplicate)
            ));
            roles.drop("Zulu").unwrap();
            assert_eq!(roles.get("Zulu").unwrap(), None);
            assert_eq!(
                roles.create(new_role("Zulu")).unwrap().oid,
                FIRST_ROLE_OID + 3
            );
            roles.drop("Zulu").unwrap();
            roles.drop(" Alpha ").unwrap();
            roles.drop("alpha").unwrap();
            assert!(roles.is_empty().unwrap());
            assert_eq!(
                roles.create(new_role("again")).unwrap().oid,
                FIRST_ROLE_OID + 4
            );
        });
    }

    #[test]
    fn alter_preserves_oid_and_increments_generation_and_clears_verifier() {
        database(|admin, _| {
            let roles = admin.roles();
            let original = roles
                .create(NewRole {
                    name: "r".into(),
                    password: Some("SCRAM-SHA-256$opaque".into()),
                    ..NewRole::default()
                })
                .unwrap();
            let changed = roles
                .alter(
                    "r",
                    RoleChanges {
                        superuser: Some(true),
                        login: Some(true),
                        createrole: Some(true),
                        password: None,
                    },
                )
                .unwrap();
            assert!(changed.superuser && changed.login && changed.createrole);
            assert_eq!(changed.password, original.password);
            assert_eq!((changed.oid, changed.generation), (original.oid, 2));
            let cleared = roles
                .alter(
                    "r",
                    RoleChanges {
                        password: Some(None),
                        ..RoleChanges::default()
                    },
                )
                .unwrap();
            assert_eq!(cleared.password, None);
            assert_eq!(cleared.generation, 3);
            let replaced = roles
                .alter(
                    "r",
                    RoleChanges {
                        superuser: Some(false),
                        login: Some(false),
                        createrole: Some(false),
                        password: Some(Some(" ~".into())),
                    },
                )
                .unwrap();
            assert!(!replaced.superuser && !replaced.login && !replaced.createrole);
            assert_eq!(replaced.password.as_deref(), Some(" ~"));
            assert_eq!(
                roles.alter("r", RoleChanges::default()).unwrap().generation,
                5
            );
        });
    }

    #[test]
    fn names_validate_byte_lengths_and_nul_for_all_object_kinds() {
        database(|admin, _| {
            let roles = admin.roles();
            roles.create(new_role("r")).unwrap();
            for invalid in [
                String::new(),
                "x".repeat(MAX_ROLE_NAME + 1),
                "a\0b".into(),
                "é".repeat(32),
            ] {
                assert!(matches!(
                    roles.create(new_role(&invalid)),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(roles.get(&invalid), Err(RoleError::Invalid)));
                assert!(matches!(
                    roles.alter(&invalid, RoleChanges::default()),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(roles.drop(&invalid), Err(RoleError::Invalid)));
                assert!(matches!(
                    roles.table_owner(&invalid),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(
                    roles.set_table_owner(&invalid, "r"),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(
                    roles.set_table_owner("t", &invalid),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(
                    roles.forget_table(&invalid),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(
                    roles.grant_table(&invalid, "r", &[]),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(
                    roles.revoke_table(&invalid, "r", &[]),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(
                    roles.table_privileges(&invalid, "r"),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(
                    roles.grant_table("t", &invalid, &[]),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(
                    roles.revoke_table("t", &invalid, &[]),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(
                    roles.table_privileges("t", &invalid),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(
                    roles.grant_schema(&invalid, "r", &[]),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(
                    roles.revoke_schema(&invalid, "r", &[]),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(
                    roles.schema_privileges(&invalid, "r"),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(
                    roles.grant_schema("s", &invalid, &[]),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(
                    roles.revoke_schema("s", &invalid, &[]),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(
                    roles.schema_privileges("s", &invalid),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(
                    roles.visible_tables(&invalid),
                    Err(RoleError::Invalid)
                ));
            }
            for valid in [
                "x".repeat(MAX_ROLE_NAME),
                format!("{}x", "é".repeat(31)),
                " ".into(),
            ] {
                assert_eq!(roles.create(new_role(&valid)).unwrap().name, valid);
                roles.set_table_owner(&valid, &valid).unwrap();
                roles
                    .grant_schema(&valid, &valid, &[SchemaPrivilege::Usage])
                    .unwrap();
                roles
                    .grant_table(&valid, &valid, &[TablePrivilege::Select])
                    .unwrap();
            }
        });
    }

    #[test]
    fn verifiers_accept_printable_ascii_only_and_invalid_alter_is_atomic() {
        database(|admin, _| {
            let roles = admin.roles();
            let original = roles.create(new_role("r")).unwrap();
            for invalid in [
                String::new(),
                "x".repeat(MAX_VERIFIER + 1),
                "a\0b".into(),
                "\t".into(),
                "\n".into(),
                "\u{7f}".into(),
                "é".into(),
            ] {
                assert!(matches!(
                    roles.create(NewRole {
                        name: "bad".into(),
                        password: Some(invalid.clone()),
                        ..NewRole::default()
                    }),
                    Err(RoleError::Invalid)
                ));
                assert!(matches!(
                    roles.alter(
                        "r",
                        RoleChanges {
                            login: Some(true),
                            password: Some(Some(invalid)),
                            ..RoleChanges::default()
                        }
                    ),
                    Err(RoleError::Invalid)
                ));
                assert_eq!(roles.get("r").unwrap(), Some(original.clone()));
            }
            for valid in [" ".into(), "~".into(), "x".repeat(MAX_VERIFIER)] {
                assert_eq!(
                    roles
                        .alter(
                            "r",
                            RoleChanges {
                                password: Some(Some(valid.clone())),
                                ..RoleChanges::default()
                            }
                        )
                        .unwrap()
                        .password,
                    Some(valid)
                );
            }
            assert_eq!(roles.list().unwrap().len(), 1);
        });
    }

    #[test]
    fn missing_roles_and_owned_table_dependencies_do_not_partially_mutate() {
        database(|admin, _| {
            let roles = admin.roles();
            assert!(matches!(
                roles.alter("missing", RoleChanges::default()),
                Err(RoleError::Missing)
            ));
            assert!(matches!(roles.drop("missing"), Err(RoleError::Missing)));
            assert!(matches!(
                roles.set_table_owner("t", "missing"),
                Err(RoleError::Missing)
            ));
            assert!(matches!(
                roles.grant_table("t", "missing", &[]),
                Err(RoleError::Missing)
            ));
            assert!(matches!(
                roles.revoke_table("t", "missing", &[]),
                Err(RoleError::Missing)
            ));
            assert!(matches!(
                roles.grant_schema("s", "missing", &[]),
                Err(RoleError::Missing)
            ));
            assert!(matches!(
                roles.revoke_schema("s", "missing", &[]),
                Err(RoleError::Missing)
            ));
            roles.create(new_role("r")).unwrap();
            roles.create(new_role("other")).unwrap();
            roles.set_table_owner("t", "r").unwrap();
            roles
                .grant_table("t", "r", &[TablePrivilege::Select])
                .unwrap();
            roles
                .grant_schema("s", "r", &[SchemaPrivilege::Usage])
                .unwrap();
            roles
                .grant_table("t", "other", &[TablePrivilege::Delete])
                .unwrap();
            assert!(matches!(roles.drop("r"), Err(RoleError::Dependent)));
            assert!(roles.get("r").unwrap().is_some());
            assert_eq!(
                roles.table_privileges("t", "r").unwrap(),
                BTreeSet::from([TablePrivilege::Select])
            );
            assert_eq!(
                roles.schema_privileges("s", "r").unwrap(),
                BTreeSet::from([SchemaPrivilege::Usage])
            );
            roles.set_table_owner("t", "other").unwrap();
            roles.drop("r").unwrap();
            assert!(roles.table_privileges("t", "r").unwrap().is_empty());
            assert!(roles.schema_privileges("s", "r").unwrap().is_empty());
            assert_eq!(
                roles.table_privileges("t", "other").unwrap(),
                BTreeSet::from([TablePrivilege::Delete])
            );
            roles.create(new_role("r")).unwrap();
            assert!(roles.visible_tables("r").unwrap().is_empty());
        });
    }

    #[test]
    fn grants_union_and_revoke_idempotently_and_delete_empty_keys() {
        database(|admin, _| {
            let roles = admin.roles();
            roles.create(new_role("r")).unwrap();
            roles
                .grant_table(
                    "t",
                    "r",
                    &[
                        TablePrivilege::Select,
                        TablePrivilege::Insert,
                        TablePrivilege::Select,
                    ],
                )
                .unwrap();
            roles
                .grant_table("t", "r", &[TablePrivilege::Update, TablePrivilege::Delete])
                .unwrap();
            roles.grant_table("t", "r", &[]).unwrap();
            let all = [
                TablePrivilege::Select,
                TablePrivilege::Insert,
                TablePrivilege::Update,
                TablePrivilege::Delete,
            ];
            assert_eq!(
                roles.table_privileges("t", "r").unwrap(),
                BTreeSet::from(all)
            );
            roles
                .revoke_table("t", "r", &[TablePrivilege::Insert, TablePrivilege::Insert])
                .unwrap();
            assert_eq!(
                roles.table_privileges("t", "r").unwrap(),
                BTreeSet::from([
                    TablePrivilege::Select,
                    TablePrivilege::Update,
                    TablePrivilege::Delete
                ])
            );
            roles.revoke_table("t", "r", &all).unwrap();
            roles.revoke_table("t", "r", &all).unwrap();
            assert!(roles.table_privileges("t", "r").unwrap().is_empty());
            roles
                .grant_schema("s", "r", &[SchemaPrivilege::Usage, SchemaPrivilege::Usage])
                .unwrap();
            roles
                .grant_schema("s", "r", &[SchemaPrivilege::Create])
                .unwrap();
            assert_eq!(
                roles.schema_privileges("s", "r").unwrap(),
                BTreeSet::from([SchemaPrivilege::Usage, SchemaPrivilege::Create])
            );
            roles
                .revoke_schema("s", "r", &[SchemaPrivilege::Usage])
                .unwrap();
            roles
                .revoke_schema("s", "r", &[SchemaPrivilege::Usage])
                .unwrap();
            assert_eq!(
                roles.schema_privileges("s", "r").unwrap(),
                BTreeSet::from([SchemaPrivilege::Create])
            );
            roles
                .revoke_schema("s", "r", &[SchemaPrivilege::Create])
                .unwrap();
            roles.revoke_schema("s", "r", &[]).unwrap();
            roles
                .read(|table| {
                    let table = table.unwrap();
                    assert!(raw_get(table, &grant_key(b"tgrant/", "t", "r"))?.is_none());
                    assert!(raw_get(table, &grant_key(b"sgrant/", "s", "r"))?.is_none());
                    Ok(())
                })
                .unwrap();
        });
    }

    #[test]
    fn visibility_and_forget_table_include_owners_and_grants_without_prefix_collisions() {
        database(|admin, _| {
            let roles = admin.roles();
            roles.create(new_role("r")).unwrap();
            roles.create(new_role("other")).unwrap();
            roles.set_table_owner("owned", "r").unwrap();
            roles.set_table_owner("t", "r").unwrap();
            roles
                .grant_table("t", "r", &[TablePrivilege::Select])
                .unwrap();
            roles
                .grant_table("t", "other", &[TablePrivilege::Insert])
                .unwrap();
            roles
                .grant_table("tt", "r", &[TablePrivilege::Delete])
                .unwrap();
            roles
                .grant_schema("t", "r", &[SchemaPrivilege::Usage])
                .unwrap();
            assert_eq!(
                roles.visible_tables("r").unwrap(),
                BTreeSet::from(["owned".into(), "t".into(), "tt".into()])
            );
            assert_eq!(
                roles.visible_tables("other").unwrap(),
                BTreeSet::from(["t".into()])
            );
            roles.forget_table("t").unwrap();
            roles.forget_table("t").unwrap();
            roles.forget_table("absent").unwrap();
            assert_eq!(roles.table_owner("t").unwrap(), None);
            assert!(roles.table_privileges("t", "r").unwrap().is_empty());
            assert!(roles.table_privileges("t", "other").unwrap().is_empty());
            assert_eq!(
                roles.visible_tables("r").unwrap(),
                BTreeSet::from(["owned".into(), "tt".into()])
            );
            assert!(roles.visible_tables("other").unwrap().is_empty());
            assert_eq!(
                roles.schema_privileges("t", "r").unwrap(),
                BTreeSet::from([SchemaPrivilege::Usage])
            );
        });
    }

    #[test]
    fn role_metadata_persists_without_changing_the_relational_catalog_or_main_records() {
        database(|admin, path| {
            let before = admin.catalog().unwrap();
            let records = main_records(&admin);
            let roles = admin.roles();
            roles.create(new_role("r")).unwrap();
            let expected = roles
                .alter(
                    "r",
                    RoleChanges {
                        login: Some(true),
                        password: Some(Some("opaque-verifier".into())),
                        ..RoleChanges::default()
                    },
                )
                .unwrap();
            roles.set_table_owner("owned", "r").unwrap();
            roles
                .grant_table("granted", "r", &[TablePrivilege::Update])
                .unwrap();
            roles
                .grant_schema("public", "r", &[SchemaPrivilege::Usage])
                .unwrap();
            assert_eq!(admin.catalog().unwrap(), before);
            assert_eq!(main_records(&admin), records);
            drop(roles);
            drop(admin);
            let reopened = Admin::open(path, Limits::default()).unwrap();
            let roles = reopened.roles();
            assert_eq!(roles.get("r").unwrap(), Some(expected));
            assert_eq!(roles.table_owner("owned").unwrap().as_deref(), Some("r"));
            assert_eq!(
                roles.table_privileges("granted", "r").unwrap(),
                BTreeSet::from([TablePrivilege::Update])
            );
            assert_eq!(
                roles.schema_privileges("public", "r").unwrap(),
                BTreeSet::from([SchemaPrivilege::Usage])
            );
            assert_eq!(
                roles.create(new_role("next")).unwrap().oid,
                FIRST_ROLE_OID + 1
            );
            assert_eq!(reopened.catalog().unwrap(), before);
        });
    }

    #[test]
    fn every_public_result_checks_fencing_before_validation_or_missing_data() {
        database(|admin, _| {
            let roles = admin.roles();
            admin.inner.fenced.store(true, Ordering::Release);
            macro_rules! fenced {
                ($call:expr) => {
                    assert!(matches!($call, Err(RoleError::Core(CoreError::Fenced))));
                };
            }
            fenced!(roles.get(""));
            fenced!(roles.list());
            fenced!(roles.is_empty());
            fenced!(roles.create(new_role("")));
            fenced!(roles.alter("", RoleChanges::default()));
            fenced!(roles.drop(""));
            fenced!(roles.table_owner(""));
            fenced!(roles.set_table_owner("", ""));
            fenced!(roles.forget_table(""));
            fenced!(roles.grant_table("", "", &[]));
            fenced!(roles.revoke_table("", "", &[]));
            fenced!(roles.table_privileges("", ""));
            fenced!(roles.grant_schema("", "", &[]));
            fenced!(roles.revoke_schema("", "", &[]));
            fenced!(roles.schema_privileges("", ""));
            fenced!(roles.visible_tables(""));
            admin.inner.fenced.store(false, Ordering::Release);
            assert!(roles.is_empty().unwrap());
        });
    }

    #[test]
    fn role_count_oid_and_generation_limits_fail_without_partial_writes() {
        database(|admin, _| {
            let roles = admin.roles();
            roles
                .write(|table| {
                    for index in 0..MAX_ROLES {
                        let role = CatalogRole {
                            name: format!("r{index:04}"),
                            oid: FIRST_ROLE_OID + index as u32,
                            superuser: false,
                            login: false,
                            createrole: false,
                            password: None,
                            generation: 1,
                        };
                        table
                            .insert(
                                key(b"role/", &role.name).as_slice(),
                                json(&role)?.as_slice(),
                            )
                            .map_err(io)?;
                    }
                    table
                        .insert(
                            NEXT_OID,
                            (FIRST_ROLE_OID + MAX_ROLES as u32).to_be_bytes().as_slice(),
                        )
                        .map_err(io)?;
                    Ok(())
                })
                .unwrap();
            assert_eq!(roles.list().unwrap().len(), MAX_ROLES);
            assert!(matches!(
                roles.create(new_role("extra")),
                Err(RoleError::Limit)
            ));
            assert!(matches!(
                roles.create(new_role("r0000")),
                Err(RoleError::Duplicate)
            ));
            roles.drop("r0000").unwrap();
            assert_eq!(
                roles.create(new_role("extra")).unwrap().oid,
                FIRST_ROLE_OID + MAX_ROLES as u32
            );
            roles.drop("extra").unwrap();
            roles
                .write(|table| {
                    table
                        .insert(NEXT_OID, u32::MAX.to_be_bytes().as_slice())
                        .map_err(io)?;
                    let role_key = key(b"role/", "r0001");
                    let mut role: CatalogRole = decode(&raw_get(table, &role_key)?.unwrap())?;
                    role.generation = u64::MAX;
                    table
                        .insert(role_key.as_slice(), json(&role)?.as_slice())
                        .map_err(io)?;
                    Ok(())
                })
                .unwrap();
            assert!(matches!(
                roles.create(new_role("overflow")),
                Err(RoleError::Limit)
            ));
            assert_eq!(roles.get("overflow").unwrap(), None);
            let before = roles.get("r0001").unwrap();
            assert!(matches!(
                roles.alter(
                    "r0001",
                    RoleChanges {
                        login: Some(true),
                        ..RoleChanges::default()
                    }
                ),
                Err(RoleError::Limit)
            ));
            assert_eq!(roles.get("r0001").unwrap(), before);
        });
    }
}
