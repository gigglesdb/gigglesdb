//! Wire encoding for the read-only catalog adapter. Catalog values never become
//! executable application SQL or bypass the session's authority gate.
use crate::{pgwire, transport::MAX_RESPONSE};
use giggles_pg_catalog::{Field, PgType, ResultSet, Value};
use giggles_core::task::Released;
type Result<T> = std::result::Result<T, &'static str>;
pub(crate) fn value_bytes(value: &Value) -> Result<usize> {
    let extra = match value {
        Value::Text(s) | Value::Regtype(_, s) => s.len(),
        Value::Bytes(b) => b.len(),
        Value::Numeric(v) => v.to_string().len(),
        Value::Array(values) | Value::Record(values) => {
            values.iter().try_fold(0usize, |sum, v| {
                sum.checked_add(value_bytes(v)?).ok_or("54000")
            })?
        }
        _ => 0,
    };
    let bytes = std::mem::size_of::<Value>()
        .checked_add(extra)
        .ok_or("54000")?;
    if bytes > MAX_RESPONSE {
        return Err("54000");
    }
    Ok(bytes)
}

pub(crate) fn error(error: giggles_pg_catalog::Error) -> &'static str {
    use giggles_pg_catalog::Error as E;
    match error {
        E::Inquiry(state) => state,
        E::Syntax | E::TypeModifierNotAllowed(_) | E::TypeNameSyntax { .. } => "42601",
        E::MixedRelations | E::Mutation => "42501",
        E::UnknownColumn => "42703",
        E::AmbiguousColumn => "42702",
        E::Type => "42804",
        E::DivisionByZero => "22012",
        E::Parameters => "08P01",
        E::Limit => "54000",
        E::InvalidSnapshot => "55000",
        E::InvalidPrivilege(_) | E::InvalidTypeModifier(_) => "22023",
        E::UnknownRelation(_) => "42P01",
        E::UnknownRole(_) => "42704",
        E::Unsupported | E::TaskRestrictedRelation => "0A000",
    }
}

struct SnapshotContext {
    metadata: giggles_core::CatalogMetadata,
    identity: SnapshotIdentity,
    registry: giggles_core::roles::RoleCatalogImage,
    owners: std::collections::BTreeMap<String, String>,
}
struct SnapshotIdentity {
    task: bool,
    zone: giggles_pg_types::temporal::SessionZone,
    backend_pid: i32,
    backend_ssl: bool,
    locks: Vec<(u64, u64, String, bool)>,
    session_user: Option<String>,
    user: String,
    database: String,
}
fn identity(session: &crate::Session) -> SnapshotIdentity {
    SnapshotIdentity { task: session.task().is_some(),
        zone: session.zone().unwrap_or(giggles_pg_types::temporal::SessionZone::Fixed(0)),
        backend_pid: session.backend_pid, backend_ssl: session.backend_ssl,
        locks: session.sql.held_table_locks().into_iter().map(|(a,b,c,d)| (a,b,c.into(),d)).collect(),
        session_user: session.session_user_name().map(str::to_owned),
        user: match session.authority_role() { crate::Role::Database { name } => name.clone(), _ => "giggles".into() },
        database: session.database.as_deref().unwrap_or("giggles").into() }
}
/// Capture only identity here. Catalog projection and authorization visibility are
/// deferred to the first non-NULL call and cached for this execution.
pub(crate) fn privilege_callback(session: &crate::Session) -> giggles_core::PrivilegeCallback {
    use std::sync::{Arc, Mutex};
    let identity = identity(session);
    let role = session.authority_role().clone();
    let state = Mutex::new((Some(identity), None));
    Arc::new(move |name, args, metadata| {
        let mut state = state.lock().map_err(|_| giggles_core::CoreError::Fenced)?;
        if state.1.is_none() {
            use giggles_core::{CoreError, roles::RoleError};
            let identity = state.0.take().ok_or(CoreError::Fenced)?;
            let mut catalog = metadata.catalog()?;
            let registry = metadata.role_image();
            // A failed capture is a failed committed owner read when the
            // statement needs one. Check owners before mapping role errors.
            let owners = owner_keys(&catalog).into_iter().map(|key| {
                Ok(metadata.table_owner_in_image(registry.as_ref().ok(), &key)?.map(|owner| (key,owner)))
            }).collect::<std::result::Result<Vec<_>, CoreError>>()?
                .into_iter().flatten().collect::<std::collections::BTreeMap<_, _>>();
            let registry = registry.map_err(|error| match (&role, error) {
                (crate::Role::Application { .. }, RoleError::Core(CoreError::Fenced)) =>
                    CoreError::PrivilegeInquiry("XX000"),
                (crate::Role::Application { .. } | crate::Role::Database { .. }, error) =>
                    crate::service::role_lookup_error(error),
                _ => CoreError::Policy(giggles_core::Error::Scope),
            })?;
            let visible = authorized_tables_in_image(&role, &registry, owners.iter().map(|(key, owner)| (key.as_str(), owner.as_str())))?;
            catalog.tables.retain(|table| visible.as_ref().is_none_or(|visible| visible.contains(
                &giggles_core::relational::relation_key(catalog.schemas.tables.get(&table.id)
                    .map_or("public", String::as_str), &table.name))));
            let tables = catalog.tables.clone();
            let indexes = catalog.indexes.clone();
            let path = metadata.search_path().to_vec();
            let result = snapshot_owned(SnapshotContext { identity, owners, metadata: catalog, registry: registry.clone() },
                tables, indexes);
            registry.check_fenced().map_err(crate::service::role_lookup_error)?;
            let mut snapshot = result.map_err(giggles_core::CoreError::PrivilegeInquiry)?;
            snapshot.search_path = path;
            state.1 = Some(snapshot);
        }
        let snapshot = state.1.as_mut().ok_or(giggles_core::CoreError::Fenced)?;
        // A cursor keeps its catalog image, but each FETCH supplies its evaluation path.
        snapshot.search_path = metadata.search_path().to_vec();
        let args = args.iter().map(|value| match value {
            giggles_core::relational::Value::Null => Ok(Value::Null),
            giggles_core::relational::Value::Text(value) => Ok(Value::Text(value.clone())),
            giggles_core::relational::Value::Int(value) => Ok(Value::Int(*value)),
            _ => Err(giggles_core::CoreError::PrivilegeInquiry("42804")),
        }).collect::<std::result::Result<Vec<_>, _>>()?;
        giggles_pg_catalog::privilege_inquiry::evaluate(name, &args, &snapshot.user,
            snapshot.session_user.as_deref().unwrap_or(&snapshot.user), snapshot)
            .map_err(|e| giggles_core::CoreError::PrivilegeInquiry(error(e)))
    })
}
/// The ordinary catalog adapter and execution callback select the same tables.
pub(crate) fn authorized_tables<'a>(role: &crate::Role, registry: &giggles_core::roles::RoleCatalog,
    execution_owners: impl Iterator<Item = (&'a str, &'a str)>)
    -> std::result::Result<Option<std::collections::BTreeSet<String>>, giggles_core::CoreError> {
    match role {
        crate::Role::Application { tables, .. } => Ok(Some(tables.clone())),
        crate::Role::Database { .. } => {
            let image = registry.image().map_err(crate::service::role_lookup_error)?;
            authorized_tables_in_image(role, &image, execution_owners)
        }
        _ => Err(giggles_core::CoreError::Policy(giggles_core::Error::Scope)),
    }
}

fn authorized_tables_in_image<'a>(role: &crate::Role, registry: &giggles_core::roles::RoleCatalogImage,
    execution_owners: impl Iterator<Item = (&'a str, &'a str)>)
    -> std::result::Result<Option<std::collections::BTreeSet<String>>, giggles_core::CoreError> {
    use crate::service::role_lookup_error;
    use giggles_core::{CoreError, Error};
    match role {
        crate::Role::Application { tables, .. } => Ok(Some(tables.clone())),
        crate::Role::Database { name } => {
            let role = registry.get(name).map_err(role_lookup_error)?
                .ok_or(CoreError::Policy(Error::Authentication))?;
            if role.superuser { return Ok(None); }
            let privileges = registry.privileges_of(name).map_err(role_lookup_error)?;
            let capabilities = registry.builtin_capabilities_of(name).map_err(role_lookup_error)?;
            if capabilities.contains("pg_read_all_data") || capabilities.contains("pg_write_all_data") {
                return Ok(None);
            }
            let mut visible = std::collections::BTreeSet::new();
            for name in &privileges { visible.extend(registry.visible_tables(name).map_err(role_lookup_error)?); }
            // Captured execution ownership includes transaction-private CREATE TABLE.
            visible.extend(execution_owners.filter(|(_, owner)| privileges.contains(*owner))
                .map(|(key, _)| key.to_owned()));
            Ok(Some(visible))
        }
        _ => Err(CoreError::Policy(Error::Scope)),
    }
}
fn owner_keys(metadata: &giggles_core::CatalogMetadata) -> Vec<String> {
    metadata.tables.iter().map(|table| giggles_core::relational::relation_key(
        metadata.schemas.tables.get(&table.id).map_or("public", String::as_str), &table.name))
        .chain(metadata.sequences.iter().map(|(_, name)| name.clone())).collect()
}
pub(crate) fn snapshot(
    session: &crate::Session,
    read: crate::service::CatalogRead,
) -> Result<giggles_pg_catalog::CatalogSnapshot> {
    let crate::service::CatalogRead { metadata, tables, indexes, .. } = read;
    let owners = owner_keys(&metadata).into_iter().filter_map(|key|
        session.sql.table_owner(&key).ok().flatten().map(|owner| (key,owner))).collect();
    let snapshot = snapshot_owned(SnapshotContext {
        identity: identity(session), metadata, owners,
        registry: session.service.admin.roles().image().map_err(|_| "XX000")?,
    }, tables, indexes)?;
    if session.task().is_none() { return Ok(snapshot); }
    let view = session.task_catalog_view().map_err(|e| crate::service::sqlstate(&e))?;
    Ok(task_filtered(snapshot, &view))
}
#[allow(clippy::needless_update)]
fn snapshot_owned(
    mut context: SnapshotContext,
    tables: Vec<giggles_core::relational::TableDef>, indexes: Vec<giggles_core::IndexInfo>,
) -> Result<giggles_pg_catalog::CatalogSnapshot> {
    use giggles_core::relational::DataType as C;
    use giggles_pg_catalog::ScalarType as P;

    let epoch = context.metadata.presentation.epoch;
    if context.metadata.schemas.epoch != epoch || context.metadata.types.epoch != epoch {
        return Err("40001");
    }
    let mut column_typmods = std::mem::take(&mut context.metadata.typmods);
    let types = &context.metadata.types;
    // Storage-only heap keys must disappear before deriving any catalog relation.
    // Side maps and indexes use stable column IDs, so removing the marker here
    // also keeps all user attribute positions contiguous.
    let tables = tables
        .into_iter()
        .map(|mut table| {
            if let Some(hidden) = table.hidden_rowid() {
                table.columns.remove(hidden);
                table.primary_key.clear();
            }
            table
        })
        .collect::<Vec<_>>();
    let columns = tables
        .iter()
        .flat_map(|t| t.columns.iter().map(|c| c.id))
        .collect::<std::collections::BTreeSet<_>>();
    column_typmods.retain(|id, _| columns.contains(id));
    let visible: std::collections::BTreeSet<_> = tables.iter().map(|table| table.id).collect();
    let (roles, table_owners) = catalog_roles(&context, &tables);
    let retained_task_names: std::collections::BTreeSet<String> = if context.identity.task {
        roles.iter().filter(|role| task_retains_role(&context, &tables, &role.name))
            .map(|role| role.name.clone()).collect()
    } else { Default::default() };
    let retained_bootstrap_oid = if context.identity.task && retained_task_names.contains(&roles[0].name) {
        context.registry.get(&roles[0].name).ok().flatten().map(|role| role.oid)
    } else { None };
    let owner_oid = |name: &str| {
        if context.identity.task {
            context.registry.get(name).ok().flatten().map(|role| {
                if role.name == roles[0].name && retained_task_names.contains(name) { 10 } else { role.oid }
            })
        } else {
            roles.iter().find(|role| role.name == name).map(|role| role.oid)
        }
    };
    let mut table_acls = std::collections::BTreeMap::new();
    let schemas_for_acl = context.metadata.schemas.clone();
    for table in &tables {
        use giggles_core::roles::TablePrivilege as C;
        use giggles_pg_catalog::{AclEntry, AclPrivilege as P};
        let key = giggles_core::relational::relation_key(
            schemas_for_acl
                .tables
                .get(&table.id)
                .map_or("public", String::as_str),
            &table.name,
        );
        let owner = *table_owners.get(&table.id).unwrap_or(&10);
        let mut grants = vec![];
        for (role, privileges) in context.registry.table_acl(&key).map_err(|_| "XX000")? {
            let grantee = if role.is_empty() { 0 } else {
                if context.identity.task {
                    let Some(actual) = context.registry.get(&role).map_err(|_| "XX000")? else { continue; };
                    if !retained_task_names.contains(&actual.name) { continue; }
                }
                let Some(role) = roles.iter().find(|r| r.name == role) else { continue; };
                role.oid
            };
            if !privileges.is_empty() {
                let mut acl = vec![];
                for privilege in privileges {
                    match privilege {
                        C::Select => acl.push(P::Select),
                        C::Insert => acl.push(P::Insert),
                        C::Update => acl.push(P::Update),
                        // The authorization store intentionally shares DELETE/TRUNCATE.
                        C::Delete => acl.extend([P::Delete, P::Truncate]),
                        C::Trigger => acl.push(P::Trigger),
                    }
                }
                // SQL grants have no grant option; the store models the owner as
                // the granting authority, not the identity of a superuser actor.
                grants.push(AclEntry {
                    grantee,
                    grantor: owner,
                    privileges: acl,
                    grant_option: false,
                });
            }
        }
        if !grants.is_empty() {
            grants.insert(
                0,
                AclEntry {
                    grantee: owner,
                    grantor: owner,
                    privileges: vec![
                        P::Insert,
                        P::Select,
                        P::Update,
                        P::Delete,
                        P::Truncate,
                        P::References,
                        P::Trigger,
                        P::Maintain,
                    ],
                    grant_option: false,
                },
            );
            table_acls.insert(table.id, grants);
        }
    }
    let user_types = types
        .types
        .iter()
        .map(|(id, ty)| {
            use giggles_core::relational::TypeDef;
            let (kind, base, typmod, not_null, labels) = match ty {
                TypeDef::Enum { labels, .. } => ('e', 0, -1, false, labels.clone()),
                TypeDef::Domain {
                    base,
                    typmod,
                    not_null,
                    ..
                } => (
                    'd',
                    crate::pgwire::oid(base),
                    typmod.unwrap_or(-1),
                    *not_null,
                    vec![],
                ),
                TypeDef::Composite { .. } => ('c', 0, -1, false, vec![]),
            };
            giggles_pg_catalog::UserType {
                oid: giggles_core::relational::type_oid(*id).expect("validated type ID"),
                name: ty.name().into(),
                schema: ty.schema().into(),
                kind,
                base,
                typmod,
                not_null,
                labels,
                label_oids: types.enum_label_oids.get(id).cloned().unwrap_or_default(),
            }
        })
        .collect();
    let mut column_type_oids: std::collections::BTreeMap<_, _> = types
        .column_types
        .iter()
        .filter(|(id, _)| columns.contains(id))
        .map(|(id, ty)| {
            (
                *id,
                giggles_core::relational::type_oid(*ty).expect("validated type ID")
                    + u32::from(types.enum_array_columns.contains(id)),
            )
        })
        .collect();
    let catalog = context.metadata;
    let types = catalog.types;
    let metadata = catalog.schemas;
    let schemas: std::collections::BTreeMap<String,(u32,u32)> = metadata
        .schemas
        .into_iter()
        .map(|(name, (id, owner))| {
            let oid = u32::try_from(id)
                .ok()
                .and_then(|id| id.checked_mul(4))
                .and_then(|id| id.checked_add(65536))
                .ok_or("54000")?;
            let owner = owner_oid(&owner).unwrap_or(10);
            Ok((name, (oid, owner)))
        })
        .collect::<Result<_>>()?;
    let collations = catalog.collations.into_iter().map(|(oid,c)| -> Result<Vec<Value>> {
            let ns = if c.schema == "public" { 2200 } else { schemas.get(&c.schema).map(|(oid,_)| *oid).ok_or("XX000")? };
            let owner = owner_oid(&c.owner).unwrap_or(10);
            let text = |v: Option<String>| v.map_or(Value::Null, Value::Text);
            Ok(vec![Value::Int(i64::from(oid)), Value::Text(c.name), Value::Int(i64::from(ns)), Value::Int(i64::from(owner)), Value::Text(c.provider), Value::Bool(c.deterministic), Value::Int(i64::from(c.encoding)), text(c.collate), text(c.ctype), text(c.locale), text(c.rules), text(c.version), text(c.comment)])
        }).collect::<Result<Vec<_>>>()?;
    let generation = catalog.generation;
    let mut presentation = catalog.presentation;
    presentation.views.retain(|id, _| visible.contains(id));
    for view in presentation.views.values() {
        column_type_oids.extend(view.table.columns.iter().zip(&view.type_oids).map(|(c, oid)| (c.id, *oid)));
    }
    presentation.defaults.retain(|id, _| columns.contains(id));
    presentation.identities.retain(|id, _| columns.contains(id));
    presentation.primary_names.retain(|id, _| visible.contains(id));
    presentation.comments.retain(|(table, column), _| visible.contains(table)
        && (*column == 0 || columns.contains(column)));
    let extensions = giggles_core::relational::BUILTIN_EXTENSIONS.iter().map(|b| {
        let installed = if b.name == "plpgsql" { Some((13643,10,11,"1.0".into())) } else {
            types.extensions.get(b.name).map(|i| {
                let namespace = if i.schema == "pg_catalog" { 11 } else if i.schema == "public" { 2200 }
                    else { schemas_for_acl.schemas.get(&i.schema).map_or(2200, |(id,_)| giggles_core::relational::type_oid(*id).unwrap_or(2200)) };
                let owner = if retained_bootstrap_oid == Some(i.owner) { 10 } else { i.owner };
                (i.oid, owner, namespace, i.version.clone())
            })
        };
        giggles_pg_catalog::ExtensionDef { name: b.name.into(), default_version: b.default_version.into(), versions: b.versions.iter().map(|s| (*s).into()).collect(),
            comment: b.comment.into(), relocatable: b.relocatable, superuser: b.superuser, trusted: b.trusted,
            schema: b.schema.map(str::to_owned), requires: b.requires.iter().map(|s| (*s).into()).collect(), installed }
    }).collect();
    let sequences = catalog.sequences.into_iter()
        .filter_map(|(oid,name)| {
            let owner = context.owners.get(&name).cloned()?;
            let owner = owner_oid(&owner)?;
            let (schema,name) = name.split_once('\0').unwrap_or(("public",&name));
            Some(giggles_pg_catalog::SequenceDef {oid,schema:schema.into(),name:name.into(),owner,acl:None})
        }).collect();
    let mut schema_privileges = std::collections::BTreeMap::new();
    for role in &roles {
        if context.identity.task && !retained_task_names.contains(&role.name) { continue; }
        for name in schemas.keys().map(String::as_str).chain(["public"]) {
            use giggles_core::roles::SchemaPrivilege as P;
            let granted = context.registry.schema_privileges(name,&role.name).map_err(|_| "XX000")?;
            if !granted.is_empty() {schema_privileges.insert((role.oid,name.to_owned()),[granted.contains(&P::Create),granted.contains(&P::Usage)]);}
        }
    }
    let (gin_defs, gin_indexes): (Vec<_>, std::collections::BTreeMap<_, _>) = presentation.gin_indexes
        .into_iter()
        .filter(|(_, index)| visible.contains(&index.table) && index.columns.iter().all(|id| columns.contains(id)))
        .map(|(id, index)| (
            giggles_pg_catalog::IndexDef {
                id, table: index.table, name: index.name.rsplit('\0').next().unwrap_or_default().into(),
                columns: index.columns, unique: false,
            },
            (id, giggles_pg_catalog::GinIndexMetadata { opclasses: index.opclasses, options: index.options }),
        ))
        .unzip();
    let (routines, triggers) = if context.identity.task { Default::default() } else {
        let oid = |id| giggles_core::relational::type_oid(id).map_err(|_| "54000");
        let routines = presentation.routines.into_iter().map(|(id, (schema, r))| Ok((oid(id)?,
            giggles_pg_catalog::RoutineDef {
                schema, name: r.name, owner: r.owner, body: r.body,
                volatility: char::from(r.volatility), strict: r.strict, security_definer: r.security_definer,
                leakproof: r.leakproof, parallel: char::from(r.parallel), cost: giggles_core::relational::Float4::from_bits(r.cost_bits),
            }))).collect::<Result<_>>()?;
        let triggers = presentation.triggers.into_iter().filter(|t| visible.contains(&t.table)).map(|t| Ok(
            giggles_pg_catalog::TriggerDef {
                oid: oid(t.id)?, table: t.table, routine: oid(t.routine)?, name: t.name,
                kind: t.events | u8::from(t.row) | (u8::from(t.before) << 1),
                enabled: char::from(t.enabled), update_columns: t.update_columns, args: t.args,
                condition: t.when, condition_columns: t.when_columns,
            })).collect::<Result<_>>()?;
        (routines, triggers)
    };
    let snapshot = giggles_pg_catalog::CatalogSnapshot {
        routines,
        triggers,
        schema_privileges,
        sequences,
        extensions,
        timezone: context.identity.zone,
        views: presentation.views.into_iter().map(|(id, v)| (id, v.definition)).collect(),
        comments: presentation.comments,
        column_defaults: presentation.defaults, column_identities: presentation.identities,
        generated_columns: presentation.generated_columns.iter().filter(|(id, _)| columns.contains(id))
            .map(|(id, generation)| (*id, giggles_pg_catalog::GeneratedColumn {
                kind: if generation.stored { 's' } else { 'v' },
                expression: generation.sql.clone(),
                pretty_expression: generation.pretty_expression(),
                references: generation.references.clone(),
            })).collect(),
        gin_indexes,
        primary_names: presentation.primary_names,
        constraints: presentation.constraints.into_iter().filter(|c| visible.contains(&c.table)
            && (c.foreign_table == 0 || visible.contains(&c.foreign_table)))
            .map(|c| giggles_pg_catalog::ConstraintDef {
                id: c.id, table: c.table, name: c.name, kind: c.kind, columns: c.columns,
                foreign_table: c.foreign_table, foreign_columns: c.foreign_columns,
                foreign_key_target: c.foreign_key_target.map(|target| match target {
                    giggles_core::relational::ForeignKeyTarget::PrimaryKey => giggles_pg_catalog::ForeignKeyTarget::PrimaryKey,
                    giggles_core::relational::ForeignKeyTarget::Index(id) => giggles_pg_catalog::ForeignKeyTarget::Index(id),
                }),
                update: c.update, delete: c.delete, deferrable: c.deferrable, deferred: c.deferred,
                validated: c.validated, expression: c.expression, index: c.index,
            }).collect(),
        generation,
        backend_pid: context.identity.backend_pid,
        prepared_statements: Vec::new(),
        table_locks: context.identity.locks.into_iter()
            .filter(|(_, table, _, _)| visible.contains(table))
            .map(|(txn, table, mode, fastpath)| (txn, table, context.identity.backend_pid, mode, fastpath)).collect(),
        backend_ssl: context.identity.backend_ssl,
        schemas,
        table_schemas: metadata
            .tables
            .into_iter()
            .filter(|(id, _)| visible.contains(id))
            .collect(),
        collations,
        user_types,
        collation_oids: catalog.collation_oids.clone(),
        column_type_oids,
        roles,
        table_owners,
        table_acls,
        session_user: context.identity.session_user,
        role_memberships: {
            let catalog = &context.registry;
            let roles = catalog.list().map_err(|_| "XX000")?;
            let bootstrap = roles.iter().filter(|r| r.superuser).min_by_key(|r| r.oid).map(|r| r.name.as_str()).unwrap_or("postgres");
            let oid = |name: &str| roles.iter().find(|r| r.name == name).map(|r| Value::Int(i64::from(r.oid))).unwrap_or(Value::Null);
            catalog.edges().map_err(|_| "XX000")?.into_iter().enumerate().map(|(n, edge)| vec![
                Value::Int(100000 + n as i64), oid(&edge.role), oid(&edge.member), oid(if edge.grantor == giggles_core::roles::BOOTSTRAP_GRANTOR { bootstrap } else { &edge.grantor }),
                Value::Bool(edge.admin), Value::Bool(edge.inherit), Value::Bool(edge.set),
            ]).collect()
        },
        role_privileges: if context.identity.task {
            // task_filtered rebuilds this over retained edges only.
            std::collections::BTreeMap::new()
        } else {
            let catalog = &context.registry;
            let listed = catalog.list().map_err(|_| "XX000")?;
            let mut result = std::collections::BTreeMap::new();
            for member in &listed {
                let inherited = catalog.privileges_of(&member.name).map_err(|_| "XX000")?;
                for role in &listed {
                    // Core owns graph semantics; the adapter supplies a read-only snapshot.
                    result.insert((member.name.clone(), role.name.clone()), [
                        member.superuser || catalog.is_member(&member.name, &role.name).map_err(|_| "XX000")?,
                        member.superuser || inherited.contains(&role.name),
                        member.superuser || catalog.can_set(&member.name, &role.name).map_err(|_| "XX000")?,
                        member.superuser || catalog.edges().map_err(|_| "XX000")?.iter().any(|edge|
                            edge.role == role.name && edge.admin
                            && catalog.is_member(&member.name, &edge.member).unwrap_or(false)),
                    ]);
                }
            }
            result
        },
        user: context.identity.user,
        database: context.identity.database,
        column_typmods,
        index_predicates: indexes.iter().filter_map(|i| i.predicate.as_ref().map(|p| (i.id, p.clone()))).collect(),
        indexes: indexes
            .into_iter()
            .filter(|index| {
                visible.contains(&index.table)
                    && index.columns.iter().all(|id| columns.contains(id))
            })
            .map(|index| giggles_pg_catalog::IndexDef {
                id: index.id,
                table: index.table,
                name: index.name,
                columns: index.columns,
                unique: index.unique,
            })
            .chain(gin_defs)
            .collect(),
        tables: tables
            .into_iter()
            .map(|table| giggles_pg_catalog::TableDef {
                id: table.id,
                name: table.name,
                primary_key: table.primary_key,
                columns: table
                    .columns
                    .into_iter()
                    .map(|column| giggles_pg_catalog::ColumnDef {
                        id: column.id,
                        name: column.name,
                        not_null: column.not_null || presentation.generated_columns.get(&column.id).is_some_and(|generation| generation.not_null),
                        data_type: match column.data_type {
                            C::Boolean => P::Boolean,
                            C::SmallInt => P::SmallInt,
                            C::Integer => P::Integer,
                            C::BigInt => P::BigInt,
                            C::Text => P::Text,
                            C::RegType | C::ExpressionArray(_) => P::Text,
                            C::Varchar => P::Varchar,
                            C::Bpchar => P::Bpchar,
                            C::Uuid => P::Uuid,
                            C::Jsonb => P::Jsonb,
                            C::IntArray => P::IntArray,
                            C::BigIntArray => P::BigIntArray,
                            C::TextArray => P::TextArray,
                            C::UuidArray => P::UuidArray,
                            C::TsVector => P::TsVector,
                            C::TsQuery => P::TsQuery,
                            C::RegConfig | C::RegDictionary | C::Oid | C::PgChar
                            | C::RealArray | C::PgCharArray | C::SmallIntArray | C::RegDictionaryArray => P::Text,

                            C::Bytes => P::Bytes,
                            C::Numeric => P::Numeric,
                            C::Real => P::Real,
                            C::DoublePrecision => P::DoublePrecision,
                            C::Date => P::Date,
                            C::Time => P::Time,
                            C::TimeTz => P::TimeTz,
                            C::Timestamp => P::Timestamp,
                            C::TimestampTz => P::TimestampTz,
                            C::Interval => P::Interval,
                        },
                    })
                    .collect(),
            })
            .collect(),
        // Forward-compatible with CatalogSnapshot fields added by giggles-pg-catalog (track G).
        ..giggles_pg_catalog::CatalogSnapshot::default()
    };
    Ok(snapshot)
}

/// GD-0061 R3-312: a task session's catalog. `tables` already holds only the
/// task's table with its shown columns (`Session::read_task_catalog`);
/// this drops everything else that could name a hidden column or another
/// relation: constraints, indexes, comments, defaults and key names that
/// mention one, views, other schemas, other roles and their grants, and user
/// types no shown column uses.
fn task_filtered(
    mut snapshot: giggles_pg_catalog::CatalogSnapshot,
    view: &crate::service::TaskCatalogView,
) -> giggles_pg_catalog::CatalogSnapshot {
    let shown = |id: &u64| view.columns.contains(id);
    snapshot.tables.retain(|table| table.id == view.table);
    let keyed = snapshot
        .tables
        .iter()
        .any(|table| !table.primary_key.is_empty());
    snapshot.views.clear();
    snapshot.routines.clear();
    snapshot.triggers.clear();
    snapshot.sequences.clear();
    snapshot.comments.retain(|(table, column), text| {
        *table == view.table && (*column == 0 || shown(column)) && !view.mentions_hidden(text)
    });
    snapshot
        .column_defaults
        .retain(|id, text| shown(id) && !view.mentions_hidden(text));
    snapshot.column_identities.retain(|id, _| shown(id));
    snapshot
        .primary_names
        .retain(|table, name| keyed && *table == view.table && !view.mentions_hidden(name));
    snapshot.constraints.retain(|c| {
        c.table == view.table
            && c.columns.iter().all(shown)
            && (c.foreign_table == 0
                || (c.foreign_table == view.table && c.foreign_columns.iter().all(shown)))
            && !view.mentions_hidden(&c.name)
            && c.expression.as_deref().is_none_or(|e| !view.mentions_hidden(e))
    });
    snapshot.indexes.retain(|index| {
        index.table == view.table
            && index.columns.iter().all(shown)
            && !view.mentions_hidden(&index.name)
    });
    snapshot.table_locks.retain(|(_, table, _, _, _)| *table == view.table);
    snapshot.table_schemas.retain(|table, _| *table == view.table);
    snapshot
        .schemas
        .retain(|name, _| *name == view.schema || name == "public");
    let owner = snapshot.table_owners.get(&view.table).copied();
    let user = snapshot.user.clone();
    snapshot
        .roles
        .retain(|role| role.oid == 10 || role.name == user || Some(role.oid) == owner);
    let roles: std::collections::BTreeSet<u32> = snapshot.roles.iter().map(|r| r.oid).collect();
    snapshot.role_memberships.retain(|edge| {
        matches!((edge.get(1), edge.get(2)), (Some(Value::Int(parent)), Some(Value::Int(member)))
            if u32::try_from(*parent).is_ok_and(|oid| roles.contains(&oid))
                && u32::try_from(*member).is_ok_and(|oid| roles.contains(&oid)))
    });
    for edge in &mut snapshot.role_memberships {
        if let Some(grantor) = edge.get_mut(3)
            && !matches!(grantor, Value::Int(oid) if u32::try_from(*oid).is_ok_and(|oid| roles.contains(&oid))) {
            *grantor = Value::Null;
        }
    }
    // Membership OIDs are presentation-only. Number task rows from retained
    // data so hidden registry edges cannot shift a visible row's identity.
    snapshot.role_memberships.sort_by_key(|edge| {
        let oid = |column| match edge[column] { Value::Int(oid) => oid, _ => -1 };
        (oid(1), oid(2), oid(3), matches!(edge[4], Value::Bool(true)),
            matches!(edge[5], Value::Bool(true)), matches!(edge[6], Value::Bool(true)))
    });
    for (n, edge) in snapshot.role_memberships.iter_mut().enumerate() {
        edge[0] = Value::Int(100000 + n as i64);
    }
    // Recompute the closure after removing hidden edges. Keeping full-registry
    // pairs would expose paths through roles absent from this task's catalog.
    snapshot.role_privileges.clear();
    for member in &snapshot.roles {
        let closure = |option: usize| {
            let mut seen = std::collections::BTreeSet::from([member.oid]);
            let mut pending = vec![member.oid];
            while let Some(current) = pending.pop() {
                for edge in &snapshot.role_memberships {
                    if edge[2] == Value::Int(i64::from(current))
                        && (option == 0 || edge[option] == Value::Bool(true))
                        && let Value::Int(parent) = edge[1]
                        && let Ok(parent) = u32::try_from(parent)
                        && seen.insert(parent) {
                        pending.push(parent);
                    }
                }
            }
            seen
        };
        let membership = closure(0);
        let inherited = closure(5);
        let set = closure(6);
        for parent in &snapshot.roles {
            let admin = snapshot.role_memberships.iter().any(|edge|
                edge[1] == Value::Int(i64::from(parent.oid)) && edge[4] == Value::Bool(true)
                    && matches!(edge[2], Value::Int(oid) if u32::try_from(oid).is_ok_and(|oid| membership.contains(&oid))));
            snapshot.role_privileges.insert((member.name.clone(), parent.name.clone()), [
                member.superuser || membership.contains(&parent.oid),
                member.superuser || inherited.contains(&parent.oid),
                member.superuser || set.contains(&parent.oid),
                member.superuser || admin,
            ]);
        }
    }
    let safe_owner = |owner: &mut u32| { if !roles.contains(owner) { *owner = 10; } };
    for (_, owner) in snapshot.schemas.values_mut() { safe_owner(owner); }
    for extension in &mut snapshot.extensions {
        if let Some((_, owner, _, _)) = &mut extension.installed { safe_owner(owner); }
    }
    // Only shown columns can make a user collation visible.
    snapshot.collation_oids.retain(|id, _| shown(id));
    let collations: std::collections::BTreeSet<u32> = snapshot.collation_oids.values().flatten().copied().collect();
    snapshot.collations.retain(|row| matches!(row.first(), Some(Value::Int(oid))
        if u32::try_from(*oid).is_ok_and(|oid| collations.contains(&oid))));
    for row in &mut snapshot.collations {
        if let Some(owner) = row.get_mut(3)
            && !matches!(owner, Value::Int(oid) if u32::try_from(*oid).is_ok_and(|oid| roles.contains(&oid))) {
            *owner = Value::Int(10);
        }
    }
    snapshot.schema_privileges.retain(|(role, schema), _|
        roles.contains(role) && snapshot.schemas.contains_key(schema));
    snapshot.table_owners.retain(|table, _| *table == view.table);
    snapshot.table_acls.retain(|table, acl| {
        if *table != view.table { return false; }
        // The builder prepends exactly one synthetic owner entry when explicit
        // grants exist. An explicit owner grant is a separate entry, so removing
        // the synthetic entry alone cannot erase a genuine owner ACL.
        let synthetic = acl.first().cloned();
        acl.retain(|entry| entry.grantee == 0 || roles.contains(&entry.grantee));
        !(acl.is_empty() || (acl.len() == 1 && acl.first() == synthetic.as_ref()
            && synthetic.as_ref().is_some_and(|entry|
                Some(&entry.grantee) == snapshot.table_owners.get(table)
                    && entry.grantor == entry.grantee)))
    });
    snapshot.column_acls.retain(|(table, column), acl| {
        acl.retain(|entry| entry.grantee == 0 || roles.contains(&entry.grantee));
        *table == view.table && shown(column) && !acl.is_empty()
    });
    snapshot.column_type_oids.retain(|id, _| shown(id));
    snapshot.column_typmods.retain(|id, _| shown(id));
    let used: std::collections::BTreeSet<u32> = snapshot.column_type_oids.values().copied().collect();
    snapshot.user_types.retain(|ty| used.contains(&ty.oid));
    snapshot
}

/// Only the finite catalog scalar types can enter its evaluator.
pub(crate) fn application_schemas(
    session: &crate::Session,
    snapshot: &giggles_pg_catalog::CatalogSnapshot,
) -> std::collections::BTreeMap<String, Vec<Field>> {
    use giggles_pg_catalog::ScalarType as S;
    snapshot
        .tables
        .iter()
        .filter_map(|table| {
            let fields = table
                .columns
                .iter()
                .enumerate()
                .filter(|(i, c)| {
                    !(table.primary_key == [*i]
                        && c.name == giggles_core::relational::HIDDEN_ROWID
                        && c.data_type == S::BigInt
                        && c.not_null)
                })
                .map(|(_, column)| {
                    Some(Field {
                        name: column.name.clone(),
                        data_type: match column.data_type {
                            S::Boolean => PgType::Bool,
                            S::SmallInt => PgType::Int2,
                            S::Integer => PgType::Int4,
                            S::BigInt => PgType::Int8,
                            S::Text | S::Varchar | S::Bpchar => PgType::Text,
                            S::Bytes => PgType::Bytea,
                            S::Numeric => PgType::Numeric,
                            S::Real => PgType::Float4,
                            S::DoublePrecision => PgType::Float8,
                            _ => return None,
                        },
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            // NUL-delimited keys cannot collide with legal SQL identifiers.
            let (schema, _) = snapshot.table_schema(table.id);
            let mut entries = vec![(format!("\0{schema}\0{}", table.name), fields.clone())];
            if session.sql.schema_in_search_path(schema) {
                entries.push((table.name.clone(), fields));
            }
            Some(entries)
        })
        .flatten()
        .collect()
}

/// Detection only: placeholder types must never reach catalog evaluation.
pub(crate) fn task_detection_schemas(
    session: &crate::Session,
    snapshot: &giggles_pg_catalog::CatalogSnapshot,
) -> std::collections::BTreeMap<String, Vec<Field>> {
    let mut detection = snapshot.clone();
    for table in &mut detection.tables {
        for column in &mut table.columns {
            use giggles_pg_catalog::ScalarType as S;
            if !matches!(column.data_type,
                S::Boolean | S::SmallInt | S::Integer | S::BigInt | S::Text | S::Varchar
                | S::Bpchar | S::Bytes | S::Numeric | S::Real | S::DoublePrecision
            ) {
                column.data_type = S::Text;
            }
        }
    }
    application_schemas(session, &detection)
}

pub(crate) fn with_search_path(
    mut snapshot: giggles_pg_catalog::CatalogSnapshot,
    path: &str,
) -> giggles_pg_catalog::CatalogSnapshot {
    let mut seen = std::collections::BTreeSet::new();
    let inherits = |role: &giggles_pg_catalog::RoleDef| {
        role.name == snapshot.user
            || snapshot.role_privileges.get(&(snapshot.user.clone(), role.name.clone()))
                .is_some_and(|privileges| privileges[1])
    };
    let all_schema_usage = snapshot.roles.iter().any(|role| {
        (role.name == snapshot.user && role.superuser)
            || (matches!(role.name.as_str(), "pg_read_all_data" | "pg_write_all_data") && inherits(role))
    });
    snapshot.search_path = giggles_core::relational::schema_search_path(path, &snapshot.user)
        .into_iter()
        .filter(|name| match name.as_str() {
            "public" | "pg_catalog" | "information_schema" => true,
            "pg_toast" => all_schema_usage || snapshot.roles.iter().any(|role| role.oid == 10 && inherits(role)),
            _ => snapshot.schemas.get(name).is_some_and(|(_, owner)| {
                all_schema_usage || snapshot.roles.iter().filter(|role| inherits(role)).any(|role| {
                    role.oid == *owner
                        || snapshot.schema_privileges.get(&(role.oid, name.clone()))
                            .is_some_and(|privileges| privileges[1])
                })
            }),
        })
        .filter(|name| seen.insert(name.clone()))
        .collect();
    snapshot
}

fn application_relation_query(name: &str) -> Result<String> {
    // Identifiers came from the binder; quote them again for the core parser.
    let quote = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let target = if let Some(qualified) = name.strip_prefix('\0') {
        let (schema, table) = qualified.split_once('\0').ok_or("XX000")?;
        format!("{}.{}", quote(schema), quote(table))
    } else {
        quote(name)
    };
    Ok(format!(
        "SELECT * FROM {target} LIMIT {}",
        giggles_pg_catalog::MAX_ROWS + 1
    ))
}

pub(crate) fn authorize_application_relations(
    session: &mut crate::Session,
    plan: &giggles_pg_catalog::Plan,
) -> Result<()> {
    for name in plan.user_relations().keys() {
        session.prepare(&application_relation_query(name)?)
            .map_err(|error| crate::service::sqlstate(&error))?;
    }
    Ok(())
}

pub(crate) fn with_application_rows(
    session: &mut crate::Session,
    plan: &giggles_pg_catalog::Plan,
    mut snapshot: giggles_pg_catalog::CatalogSnapshot,
) -> Result<giggles_pg_catalog::CatalogSnapshot> {
    use giggles_core::relational::Value as V;
    let mut bytes = 0usize;
    for (name, fields) in plan.user_relations() {
        let sql = application_relation_query(name)?;
        // GD-0061 R3-312: the rows go through the release protocol before
        // they are read. In a task session the generated `SELECT *` is
        // expanded to the shown columns by the task's column rules, and the
        // rows are scoped, masked and charged; elsewhere release is a move.
        let unreleased = session
            .execute_sql_unreleased(&sql, &[])
            .map_err(|e| crate::service::sqlstate(&e))?;
        let result = session
            .release_page(unreleased)
            .map_err(|e| crate::service::sqlstate(&e))?
            .into_inner();
        if result.rows.len() > giggles_pg_catalog::MAX_ROWS {
            return Err("54000");
        }
        let mut rows = Vec::new();
        for row in result.rows {
            let mut values = Vec::new();
            for value in row.values {
                let value = match value {
                    V::Null => Value::Null,
                    V::Bool(v) => Value::Bool(v),
                    V::Int(v) => Value::Int(v),
                    V::Text(v) => Value::Text(v),
                    V::Bytes(v) => Value::Bytes(v),
                    V::Numeric(v) => Value::Numeric(v),
                    V::Float4(v) => Value::Float4(v),
                    V::Float8(v) => Value::Float8(v),
                    _ => return Err("0A000"),
                };
                bytes = bytes.checked_add(value_bytes(&value)?).ok_or("54000")?;
                if bytes > giggles_pg_catalog::MAX_BYTES {
                    return Err("54000");
                }
                values.push(value);
            }
            rows.push(values);
        }
        snapshot.user_relations.insert(
            name.clone(),
            ResultSet {
                fields: fields.clone(),
                rows,
            },
        );
    }
    Ok(snapshot)
}

/// Match actual registry identities before assigning presentation OIDs. In
/// particular, a stored bootstrap namesake is not the synthetic bootstrap row.
fn task_retains_role(context: &SnapshotContext, tables: &[giggles_core::relational::TableDef], name: &str) -> bool {
    name == context.identity.user || tables.iter().any(|table| {
        let schema = context.metadata.schemas.tables.get(&table.id).map_or("public", String::as_str);
        let key = giggles_core::relational::relation_key(schema, &table.name);
        context.owners.get(&key).is_some_and(|owner| owner == name)
    })
}

/// Presentation-only role rows and owners of the already authorization-filtered
/// tables, read from the role registry so `pg_get_userbyid(relowner)` names the
/// creating role as PostgreSQL does. OID 10 stays the bootstrap row; a registry
/// role literally named `giggles` takes that slot. Registry failures fall back
/// to the bootstrap-only default, which every caller already accepted.
fn catalog_roles(
    context: &SnapshotContext,
    tables: &[giggles_core::relational::TableDef],
) -> (
    Vec<giggles_pg_catalog::RoleDef>,
    std::collections::BTreeMap<u64, u32>,
) {
    let default = giggles_pg_catalog::CatalogSnapshot::default();
    let registry = &context.registry;
    let Ok(listed) = registry.list() else {
        return (default.roles, default.table_owners);
    };
    let mut roles = default.roles;
    let bootstrap = roles[0].name.clone();
    let mut oids = std::collections::BTreeMap::new();
    oids.insert(bootstrap.clone(), 10u32);
    // Keep role-name bytes well under the catalog's 64 KiB snapshot input bound.
    let mut name_bytes = bootstrap.len();
    // Task identities and all owner lookups come from the full registry before
    // applying the presentation budget. Hidden names cannot consume that budget.
    if context.identity.task {
        oids.extend(listed.iter().map(|role| (role.name.clone(), if role.name == bootstrap { 10 } else { role.oid })));
    }
    for role in listed {
        if context.identity.task && !task_retains_role(context, tables, &role.name) {
            continue;
        }
        if role.name == bootstrap {
            roles[0].superuser = role.superuser;
            roles[0].login = role.login;
            roles[0].createrole = role.createrole;
            continue;
        }
        name_bytes += role.name.len();
        if role.oid == 10 || name_bytes > 32 * 1024 {
            continue;
        }
        oids.insert(role.name.clone(), role.oid);
        roles.retain(|existing| existing.name != role.name);
        roles.push(giggles_pg_catalog::RoleDef {
            oid: role.oid,
            superuser: role.superuser,
            login: role.login,
            createrole: role.createrole,
            createdb: role.superuser,
            inherit: role.inherit,
            replication: role.superuser,
            bypassrls: role.superuser,
            connlimit: -1,
            name: role.name,
        });
    }
    let owners = tables
        .iter()
        .filter_map(|table| {
            let schemas = context.metadata.schemas.clone();
            let key = giggles_core::relational::relation_key(
                schemas
                    .tables
                    .get(&table.id)
                    .map_or("public", String::as_str),
                &table.name,
            );
            let owner = context.owners.get(&key)?;
            Some((table.id, *oids.get(owner)?))
        })
        .collect();
    (roles, owners)
}

fn is_vector(ty: PgType) -> bool {
    matches!(ty, PgType::Int2Vector | PgType::OidVector)
}
fn element(ty: PgType) -> Option<PgType> {
    match ty {
        PgType::Int2Vector => Some(PgType::Int2),
        PgType::OidVector => Some(PgType::Oid),
        _ => ty.array_element(),
    }
}

pub(crate) fn parameter_type(oid: u32) -> Result<PgType> {
    Ok(match oid {
        16 => PgType::Bool,
        17 => PgType::Bytea,
        18 => PgType::Char,
        19 => PgType::Name,
        20 => PgType::Int8,
        21 => PgType::Int2,
        23 => PgType::Int4,
        25 | 1043 => PgType::Text,
        26 => PgType::Oid,
        28 => PgType::Xid,
        1700 => PgType::Numeric,
        700 => PgType::Float4,
        701 => PgType::Float8,
        1005 => PgType::Int2Array,
        1007 => PgType::Int4Array,
        1016 => PgType::Int8Array,
        1000 => PgType::BoolArray,
        1001 => PgType::ByteaArray,
        1002 => PgType::CharArray,
        1003 => PgType::NameArray,
        22 => PgType::Int2Vector,
        30 => PgType::OidVector,
        1028 => PgType::OidArray,
        1009 => PgType::TextArray,
        _ => return Err("0A000"),
    })
}

/// OID text input uses base-zero integer syntax and also accepts the signed
/// 32-bit representation used by older PostgreSQL clients.
pub(crate) fn parse_oid_text(text: &str) -> Result<u32> {
    let text = text.trim_ascii();
    let negative = text.starts_with('-');
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    let (digits, radix) = if let Some(hex) = digits.strip_prefix("0x").or_else(|| digits.strip_prefix("0X")) {
        (hex, 16)
    } else if digits.starts_with('0') {
        (digits, 8)
    } else {
        (digits, 10)
    };
    if digits.is_empty() || !digits.chars().all(|c| c.is_digit(radix)) {
        return Err("22P02");
    }
    let magnitude = u32::from_str_radix(digits, radix).map_err(|_| "22003")?;
    if negative {
        if magnitude > i32::MIN.unsigned_abs() {
            return Err("22003");
        }
        return Ok(magnitude.wrapping_neg());
    }
    Ok(magnitude)
}

pub(crate) fn decode(bytes: &[u8], ty: PgType, format: u16) -> Result<Value> {
    use giggles_core::relational::{DataType as T, Value as V};
    if bytes.len() > crate::transport::MAX_FRAME {
        return Err("54000");
    }
    if format > 1 {
        return Err("08P01");
    }
    if ty == PgType::Xid && format == 0 {
        let text = std::str::from_utf8(bytes).map_err(|_| "22021")?;
        return giggles_pg_catalog::parse_xid(text)
            .map(|v| Value::Int(v.into()))
            .map_err(|_| "22P02");
    }
    if matches!(ty, PgType::Oid | PgType::Xid) {
        return if format == 0 {
            parse_oid_text(std::str::from_utf8(bytes).map_err(|_| "22021")?)
                .map(|v| Value::Int(v.into()))
        } else {
            let bytes: [u8; 4] = bytes.try_into().map_err(|_| "22P03")?;
            Ok(Value::Int(u32::from_be_bytes(bytes).into()))
        };
    }
    if let Some(scalar) = element(ty) {
        if format == 0 {
            return Err("0A000");
        }
        let mut reader = pgwire::Reader { b: bytes, at: 0 };
        let dimensions = reader.u32()?;
        let nulls = reader.u32()?;
        if nulls > 1 || reader.u32()? != scalar.oid() {
            return Err("22P03");
        }
        if dimensions == 0 {
            reader.end()?;
            return Ok(Value::Array(vec![]));
        }
        if dimensions != 1 {
            return Err("0A000");
        }
        let count = reader.u32()? as usize;
        if count > 8192 {
            return Err("54000");
        }
        if reader.u32()? != u32::from(!is_vector(ty)) {
            return Err("0A000");
        }
        let mut values = Vec::new();
        for _ in 0..count {
            let length = reader.u32()?;
            values.push(if length == u32::MAX {
                if nulls == 0 || is_vector(ty) {
                    return Err("22P03");
                }
                Value::Null
            } else {
                decode(reader.take(length as usize)?, scalar, 1)?
            });
        }
        reader.end()?;
        return Ok(Value::Array(values));
    }
    // PostgreSQL internal "char" uses a zero byte for its empty value.
    // Text format renders that value as an empty string, not SQL NULL.
    if ty == PgType::Char && format == 1 && bytes == [0] {
        return Ok(Value::Text(String::new()));
    }
    let data_type = match ty {
        PgType::Bool => T::Boolean,
        PgType::Int2 => T::SmallInt,
        PgType::Int4 => T::Integer,
        PgType::Int8 => T::BigInt,
        PgType::Text | PgType::Varchar | PgType::Name | PgType::Char => T::Text,
        PgType::Bytea => T::Bytes,
        PgType::Numeric => T::Numeric,
        PgType::Float4 => T::Real,
        PgType::Float8 => T::DoublePrecision,
        _ => return Err("0A000"),
    };
    if (ty == PgType::Char && (bytes.len() > 1 || (format == 1 && bytes.is_empty())))
        || (ty == PgType::Name && bytes.len() > 63)
    {
        return Err("22P03");
    }
    if matches!(data_type, T::Text) && bytes.contains(&0) {
        return Err("22021");
    }
    Ok(match pgwire::decode(bytes, &data_type, format)? {
        V::Null => Value::Null,
        V::Bool(v) => Value::Bool(v),
        V::Int(v) => Value::Int(v),
        V::Text(v) => Value::Text(v),
        V::Bytes(v) => Value::Bytes(v),
        V::Numeric(v) => Value::Numeric(v),
        V::Float4(v) => Value::Float4(v),
        V::Float8(v) => Value::Float8(v),
        V::Date(_)
        | V::Time(_)
        | V::TimeTz(_)
        | V::Timestamp(_)
        | V::TimestampTz(_)
        | V::Interval(_) => return Err("0A000"),
    })
}

pub(crate) fn description(fields: &[Field], formats: &[u16], out: &mut Vec<u8>) -> Result<()> {
    if fields.len() > 256 {
        return Err("54000");
    }
    if fields.is_empty() {
        return pgwire::frame(b'n', &[], out);
    }
    let mut bytes = (fields.len() as u16).to_be_bytes().to_vec();
    for (i, field) in fields.iter().enumerate() {
        if field.name.len() > 65536 || field.name.contains('\0') {
            return Err("54000");
        }
        if field.data_type == PgType::Record {
            return Err("0A000");
        }
        bytes.extend(field.name.as_bytes());
        bytes.push(0);
        bytes.extend(0u32.to_be_bytes());
        bytes.extend(0i16.to_be_bytes());
        bytes.extend(field.data_type.oid().to_be_bytes());
        let size = field.data_type.type_size();
        bytes.extend(size.to_be_bytes());
        bytes.extend((-1i32).to_be_bytes());
        bytes.extend(pgwire::format_at(formats, i).to_be_bytes());
        if bytes.len() > MAX_RESPONSE {
            return Err("54000");
        }
    }
    pgwire::frame(b'T', &bytes, out)
}

pub(crate) fn encode(value: &Value, ty: PgType, format: u16) -> Result<Option<Vec<u8>>> {
    encode_with_settings(value, ty, format, 0, Default::default())
}
fn encode_with_settings(
    value: &Value,
    ty: PgType,
    format: u16,
    extra: i32,
    bytea_output: giggles_pg_types::bytea::ByteaOutput,
) -> Result<Option<Vec<u8>>> {
    if format > 1 {
        return Err("08P01");
    }
    if *value == Value::Null {
        return Ok(None);
    }
    let bytes = match (value, ty) {
        (Value::Array(_), PgType::AclItemArray) => {
            // ACL items are stored as their PostgreSQL text representation.
            // PostgreSQL has no aclitem binary send function; do not substitute
            // text bytes into a binary array claiming an aclitem element OID.
            if format == 1 {
                return Err("42883");
            }
            return encode_with_settings(value, PgType::TextArray, 0, extra, bytea_output);
        }
        (Value::Numeric(v), PgType::Numeric) => {
            if format == 1 {
                v.to_binary()
            } else {
                v.to_string().into_bytes()
            }
        }
        (Value::Float4(v), PgType::Float4) => {
            if format == 1 {
                v.to_binary().to_vec()
            } else {
                v.to_text_with_extra_float_digits(extra)
                    .map_err(|_| "22003")?
                    .into_bytes()
            }
        }
        (Value::Float8(v), PgType::Float8) => {
            if format == 1 {
                v.to_binary().to_vec()
            } else {
                v.to_text_with_extra_float_digits(extra)
                    .map_err(|_| "22003")?
                    .into_bytes()
            }
        }
        (Value::Bool(v), PgType::Bool) => {
            if format == 0 {
                if *v { b"t".to_vec() } else { b"f".to_vec() }
            } else {
                vec![u8::from(*v)]
            }
        }
        (Value::Regtype(oid, name), PgType::Regtype) => {
            if format == 0 { name.as_bytes().to_vec() } else { oid.to_be_bytes().to_vec() }
        }
        (Value::Int(value), PgType::TimestampTz) => {
            let time = giggles_pg_types::temporal::TimestampTz::from_microseconds(*value)
                .map_err(|_| "22008")?;
            if format == 0 {
                time.format_in_zone(giggles_pg_types::temporal::SessionZone::Fixed(0))
                    .map_err(|_| "22008")?.into_bytes()
            } else {
                time.to_binary().to_vec()
            }
        }
        (Value::Int(v), PgType::Int2) => {
            let v = i16::try_from(*v).map_err(|_| "22003")?;
            if format == 0 {
                v.to_string().into_bytes()
            } else {
                v.to_be_bytes().to_vec()
            }
        }
        (Value::Int(v), PgType::Int4) => {
            let v = i32::try_from(*v).map_err(|_| "22003")?;
            if format == 0 {
                v.to_string().into_bytes()
            } else {
                v.to_be_bytes().to_vec()
            }
        }
        (Value::Record(fields), PgType::Regproc | PgType::Regprocedure) => {
            let [Value::Int(oid), Value::Text(name)] = fields.as_slice() else { return Err("42804"); };
            let oid = u32::try_from(*oid).map_err(|_| "22003")?;
            if format == 0 { name.as_bytes().to_vec() } else { oid.to_be_bytes().to_vec() }
        }
        (Value::Int(v), PgType::Oid | PgType::Xid | PgType::Regproc) => {
            let v = u32::try_from(*v).map_err(|_| "22003")?;
            let regproc = if ty == PgType::Regproc {
                Some(giggles_pg_catalog::builtin_regproc_name(v).ok_or("0A000")?)
            } else {
                None
            };
            if format == 1 {
                v.to_be_bytes().to_vec()
            } else if let Some(name) = regproc {
                name.as_bytes().to_vec()
            } else {
                v.to_string().into_bytes()
            }
        }
        (Value::Int(v), PgType::Int8) => {
            if format == 0 {
                v.to_string().into_bytes()
            } else {
                v.to_be_bytes().to_vec()
            }
        }
        (Value::Text(v), PgType::Interval) => {
            let interval = giggles_pg_types::temporal::Interval::parse(v).map_err(|_| "22007")?;
            if format == 1 { interval.to_binary().to_vec() } else { v.as_bytes().to_vec() }
        }
        (Value::Text(v), PgType::Text | PgType::Varchar | PgType::Name | PgType::Char | PgType::PgNodeTree) => {
            if v.contains('\0')
                || (ty == PgType::Char && v.len() > 1)
                || (ty == PgType::Name && v.len() > 63)
            {
                return Err("22021");
            }
            if v.len() > MAX_RESPONSE {
                return Err("54000");
            }
            if ty == PgType::Char && format == 1 && v.is_empty() {
                vec![0]
            } else {
                v.as_bytes().to_vec()
            }
        }
        (Value::Bytes(v), PgType::Bytea) => {
            if format == 0 {
                if bytea_output.text_len(v) > MAX_RESPONSE {
                    return Err("54000");
                }
                bytea_output.format(v).into_bytes()
            } else {
                if v.len() > MAX_RESPONSE / 2 {
                    return Err("54000");
                }
                v.clone()
            }
        }
        (Value::Array(values), array) if element(array).is_some() => {
            if values.len() > 8192 {
                return Err("54000");
            }
            if is_vector(array) && values.contains(&Value::Null) {
                return Err("42804");
            }
            let scalar = element(array).unwrap();
            let mut encoded = Vec::new();
            if format == 0 {
                if !is_vector(array) {
                    encoded.push(b'{');
                }
                for (i, value) in values.iter().enumerate() {
                    if i != 0 {
                        encoded.push(if is_vector(array) { b' ' } else { b',' });
                    }
                    match encode_with_settings(value, scalar, 0, 0, bytea_output)? {
                        None => encoded.extend(b"NULL"),
                        Some(item) => {
                            if encoded
                                .len()
                                .checked_add(item.len().saturating_mul(2) + 3)
                                .ok_or("54000")?
                                > MAX_RESPONSE
                            {
                                return Err("54000");
                            }
                            // array_out quotes empty, NULL-like and delimiter-bearing elements.
                            let quote = item.is_empty() || item.eq_ignore_ascii_case(b"NULL")
                                || item.iter().any(|b| b.is_ascii_whitespace() || matches!(b, b',' | b'{' | b'}' | b'"' | b'\\'));
                            if quote || matches!(scalar, PgType::Text | PgType::Varchar | PgType::Char) {
                                encoded.push(b'"');
                                for byte in item {
                                    if matches!(byte, b'"' | b'\\') {
                                        encoded.push(b'\\');
                                    }
                                    encoded.push(byte);
                                }
                                encoded.push(b'"');
                            } else {
                                encoded.extend(item);
                            }
                        }
                    }
                    if encoded.len() > MAX_RESPONSE {
                        return Err("54000");
                    }
                }
                if !is_vector(array) {
                    encoded.push(b'}');
                }
            } else {
                encoded.extend(i32::from(!values.is_empty()).to_be_bytes());
                encoded.extend(i32::from(values.contains(&Value::Null)).to_be_bytes());
                encoded.extend(scalar.oid().to_be_bytes());
                if !values.is_empty() {
                    encoded.extend((values.len() as i32).to_be_bytes());
                    encoded.extend(i32::from(!is_vector(array)).to_be_bytes());
                }
                for value in values {
                    match encode(value, scalar, 1)? {
                        None => encoded.extend((-1i32).to_be_bytes()),
                        Some(item) => {
                            if encoded.len().checked_add(item.len() + 4).ok_or("54000")?
                                > MAX_RESPONSE
                            {
                                return Err("54000");
                            }
                            encoded.extend((item.len() as i32).to_be_bytes());
                            encoded.extend(item);
                        }
                    }
                    if encoded.len() > MAX_RESPONSE {
                        return Err("54000");
                    }
                }
            }
            encoded
        }
        (_, PgType::Record) => return Err("0A000"),
        _ => return Err("42804"),
    };
    if bytes.len() > MAX_RESPONSE {
        return Err("54000");
    }
    Ok(Some(bytes))
}

pub(crate) fn retained_bytes(result: &ResultSet) -> Result<usize> {
    if result.rows.len() > giggles_pg_catalog::MAX_ROWS || result.fields.len() > 256 {
        return Err("54000");
    }
    let mut total = 0usize;
    for field in &result.fields {
        total = total.checked_add(field.name.len() + 32).ok_or("54000")?;
    }
    for row in &result.rows {
        if row.len() != result.fields.len() {
            return Err("XX000");
        }
        for (value, field) in row.iter().zip(&result.fields) {
            // Text representation bounds both escaping growth and retained data.
            let length = encode(value, field.data_type, 0)?.map_or(0, |b| b.len());
            total = total
                .checked_add(length + std::mem::size_of::<Value>() + 4)
                .ok_or("54000")?;
            if let Value::Array(values) = value {
                total = total
                    .checked_add(
                        values
                            .len()
                            .checked_mul(std::mem::size_of::<Value>())
                            .ok_or("54000")?,
                    )
                    .ok_or("54000")?;
            }
            if total > MAX_RESPONSE {
                return Err("54000");
            }
        }
    }
    Ok(total)
}

/// Encodes rows `start..end` of a released catalog result as DataRow
/// messages. It takes only `Released` (GD-0061 R3-314).
pub(crate) fn rows(
    settings: &crate::pg_settings::Settings,
    result: &Released<ResultSet>,
    formats: &[u16],
    start: usize,
    end: usize,
    out: &mut Vec<u8>,
) -> Result<()> {
    let result = result.get();
    let rows = result.rows.get(start..end).ok_or("XX000")?;
    for row in rows {
        if row.len() != result.fields.len() {
            return Err("XX000");
        }
        let mut bytes = (row.len() as u16).to_be_bytes().to_vec();
        for (i, (value, field)) in row.iter().zip(&result.fields).enumerate() {
            let format = pgwire::format_at(formats, i);
            let encoded = if let (0, PgType::TimestampTz, Value::Int(us)) =
                (format, field.data_type, value)
            {
                use giggles_pg_types::temporal::{SessionZone, TimestampTz};
                let (zone, _) = SessionZone::setting(&settings.timezone).map_err(|_| "22023")?;
                let time = TimestampTz::from_microseconds(*us).map_err(|_| "22008")?;
                Some(time.format_with_style(zone, settings.datestyle.style, settings.datestyle.order)
                    .map_err(|_| "22008")?.into_bytes())
            } else {
                encode_with_settings(value, field.data_type, format,
                    settings.extra_float_digits, settings.bytea_output)?
            };
            match encoded {
                None => bytes.extend((-1i32).to_be_bytes()),
                Some(value) => {
                    bytes.extend((value.len() as i32).to_be_bytes());
                    bytes.extend(value);
                }
            }
            if bytes.len() > MAX_RESPONSE {
                return Err("54000");
            }
        }
        pgwire::frame(b'D', &bytes, out)?;
    }
    Ok(())
}

// Initialized when the first Service is constructed, stable for this process.
static STARTUP: std::sync::OnceLock<Option<i64>> = std::sync::OnceLock::new();
pub(crate) fn initialize_startup() {
    STARTUP.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|d| i64::try_from(d.as_secs()).ok())
    });
}
pub(crate) fn execute(
    plan: &giggles_pg_catalog::Plan,
    snapshot: &giggles_pg_catalog::CatalogSnapshot,
    parameters: &[Value],
) -> std::result::Result<ResultSet, giggles_pg_catalog::Error> {
    giggles_pg_catalog::execute_with_startup(
        plan,
        snapshot,
        parameters,
        STARTUP.get().copied().flatten(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::types::{FromSql, Type};


    #[test]
    fn extension_catalog_rows() {
        let path=crate::test_support::fixture_root().join(format!("extension-catalog-{}",std::process::id()));
        let admin=giggles_core::Admin::create(&path,giggles_core::Limits::default()).unwrap();
        admin.roles().create(giggles_core::roles::NewRole { name:"ext_owner".into(), login:true, superuser:true, ..Default::default() }).unwrap();
        let service=crate::Service::new(admin,vec![],crate::clock::Clock::new(0,3600).unwrap()).unwrap();
        let mut session=service.role_session("ext_owner").unwrap();
        session.execute_sql("CREATE EXTENSION \"uuid-ossp\"",&[]).unwrap();
        assert_eq!(session.execute_sql("SELECT uuid_nil()",&[]).unwrap().rows[0].values,vec![giggles_core::relational::Value::Text("00000000-0000-0000-0000-000000000000".into())]);
        let read = session.read_catalog().unwrap();
        let snap = snapshot(&session, read).unwrap();
        let owner=snap.roles.iter().find(|r|r.name == "ext_owner").unwrap().oid;
        assert_eq!(snap.extensions.iter().find(|e|e.name == "uuid-ossp").unwrap().installed.as_ref().unwrap().1,owner);
        for (sql, expected) in [
            ("SELECT extname,extversion FROM pg_extension ORDER BY extname",vec![vec![Value::Text("plpgsql".into()),Value::Text("1.0".into())],vec![Value::Text("uuid-ossp".into()),Value::Text("1.1".into())]]),
            ("SELECT name,installed FROM pg_available_extension_versions ORDER BY name",vec![vec![Value::Text("plpgsql".into()),Value::Bool(true)],vec![Value::Text("uuid-ossp".into()),Value::Bool(true)]]),
        ] {
            let plan=giggles_pg_catalog::plan(sql).unwrap().unwrap();
            assert_eq!(giggles_pg_catalog::execute(&plan,&snap,&[]).unwrap().rows,expected);
        }
        drop(session);drop(service);std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn held_table_lock_catalog_parity() {
        use giggles_core::{Admin, Limits, roles::NewRole, SqlTableLockMode as M};
        // Legacy has no table lock manager; V2 exposes its transaction-held locks.
        for engine in [giggles_core::Engine::Legacy, #[cfg(feature = "engine-rocksdb")] giggles_core::Engine::V2] {
            let holds_locks = engine == giggles_core::Engine::V2;
            let path = crate::test_support::fixture_root().join(format!("lock-catalog-{engine:?}-{}", std::process::id()));
            let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
            admin.roles().create(NewRole { name: "lock_owner".into(), login: true, superuser: true, ..Default::default() }).unwrap();
            let service = crate::Service::new(admin, vec![], crate::clock::Clock::new(0, 3600).unwrap()).unwrap();
            let mut session = service.role_session("lock_owner").unwrap();
            session.backend_pid = 42;
            session.execute_sql("CREATE TABLE t (id INT PRIMARY KEY)", &[]).unwrap();
            let read = |session: &mut crate::Session| {
                let read = session.read_catalog().unwrap();
                let snap = snapshot(session, read).unwrap();
                let plan = giggles_pg_catalog::plan("SELECT mode, granted FROM pg_locks WHERE relation = 't'::regclass AND pid = pg_backend_pid() ORDER BY mode").unwrap().unwrap();
                giggles_pg_catalog::execute(&plan, &snap, &[]).unwrap().rows
            };
            for (mode, name) in [(M::AccessShare, "AccessShareLock"), (M::RowShare, "RowShareLock"), (M::RowExclusive, "RowExclusiveLock"), (M::ShareUpdateExclusive, "ShareUpdateExclusiveLock"), (M::Share, "ShareLock"), (M::ShareRowExclusive, "ShareRowExclusiveLock"), (M::ExclusiveTable, "ExclusiveLock"), (M::AccessExclusive, "AccessExclusiveLock")] {
                for nowait in [false, true] {
                    session.execute_sql("BEGIN", &[]).unwrap();
                    session.sql.lock_tables(&["t".into()], mode, nowait).unwrap();
                    let expected = if holds_locks { vec![vec![Value::Text(name.into()), Value::Bool(true)]] } else { vec![] };
                    assert_eq!(read(&mut session), expected, "{engine:?}: {name}");
                    session.execute_sql(if nowait { "COMMIT" } else { "ROLLBACK" }, &[]).unwrap();
                    assert!(read(&mut session).is_empty());
                }
            }
            session.execute_sql("BEGIN", &[]).unwrap();
            session.sql.lock_tables(&["t".into()], M::ShareRowExclusive, false).unwrap();
            session.sql.lock_tables(&["t".into()], M::AccessShare, false).unwrap();
            assert_eq!(read(&mut session).len(), if holds_locks { 2 } else { 0 });
            session.execute_sql("ROLLBACK", &[]).unwrap();
            for sql in ["SELECT * FROM t", "INSERT INTO t VALUES (1)", "UPDATE t SET id=2", "DELETE FROM t"] {
                session.execute_sql("BEGIN", &[]).unwrap();
                session.execute_sql(sql, &[]).unwrap();
                let mode = if sql.starts_with("SELECT") { "AccessShareLock" } else { "RowExclusiveLock" };
                let rows = read(&mut session);
                if holds_locks {
                    assert!(rows.iter().any(|r| r[0] == Value::Text(mode.into())), "{sql}");
                } else {
                    assert!(rows.is_empty(), "Legacy holds no table locks: {sql}");
                }
                session.execute_sql("ROLLBACK", &[]).unwrap();
            }
            assert_eq!(encode(&Value::Text("-03:00:00".into()), PgType::Interval, 0).unwrap(), Some(b"-03:00:00".to_vec()));
            assert_eq!(encode(&Value::Text("-03:00:00".into()), PgType::Interval, 1).unwrap(), Some(giggles_pg_types::temporal::Interval::parse("-03:00:00").unwrap().to_binary().to_vec()));
            drop(session); drop(service);
            std::fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn catalog_remainder_by_zero_has_postgresql_sqlstate() {
        let plan = giggles_pg_catalog::plan("SELECT pg_catalog.txid_current() % 0::bigint")
            .unwrap()
            .unwrap();
        let failure = giggles_pg_catalog::execute(&plan, &Default::default(), &[]).unwrap_err();
        assert_eq!(error(failure), "22012");
    }

    #[test]
    fn acl_defaults_encode_as_text_arrays_with_acl_and_array_quoting() {
        let plan = giggles_pg_catalog::plan(
            "SELECT n.tableoid, n.oid, n.nspname, n.nspowner, n.nspacl, acldefault('n', n.nspowner) AS acldefault FROM pg_namespace n",
        ).unwrap().unwrap();
        let mut snapshot = giggles_pg_catalog::CatalogSnapshot::default();
        snapshot.roles[0].name = "a\"b\\c".into();
        let result = giggles_pg_catalog::execute(&plan, &snapshot, &[]).unwrap();
        assert_eq!(result.fields[5].data_type.oid(), 1034);
        for row in result.rows {
            assert_eq!(encode(&row[4], PgType::AclItemArray, 0).unwrap(), None);
            let bytes = encode(&row[5], PgType::AclItemArray, 0).unwrap().unwrap();
            // Array escaping surrounds the independently quoted ACL role names.
            let expected: &[u8] = if row[3] == Value::Int(6171) {
                br#"{"pg_database_owner=UC/pg_database_owner"}"#
            } else {
                br#"{"\"a\"\"b\\c\"=UC/\"a\"\"b\\c\""}"#
            };
            assert_eq!(bytes, expected);
            assert_eq!(encode(&row[5], PgType::AclItemArray, 1), Err("42883"));
        }
        assert_eq!(
            encode(
                &Value::Array(vec![Value::Text("giggles=UC/giggles".into())]),
                PgType::AclItemArray,
                0
            )
            .unwrap(),
            Some(b"{\"giggles=UC/giggles\"}".to_vec())
        );
    }

    #[test]
    fn catalog_refresh_generation_survives_restart_and_tracks_commits() {
        use giggles_core::{Admin, Limits, roles::NewRole};
        fn read(session: &mut crate::Session) -> giggles_pg_catalog::CatalogSnapshot {
            let read = session.read_catalog().unwrap();
            snapshot(session, read).unwrap()
        }
        fn make_service(admin: Admin) -> std::sync::Arc<crate::Service> {
            crate::Service::new(admin, vec![], crate::clock::Clock::new(0, 3600).unwrap()).unwrap()
        }
        let path = crate::test_support::fixture_root().join(format!(
            "datagrip-generation-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let admin = Admin::create(&path, Limits::default()).unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "refresh_owner".into(),
                login: true,
                superuser: true,
                ..NewRole::default()
            })
            .unwrap();
        let service = make_service(admin);
        let mut session = service.role_session("refresh_owner").unwrap();
        let before = read(&mut session).generation;
        session
            .execute_sql("CREATE TABLE refresh_table (id BIGINT PRIMARY KEY)", &[])
            .unwrap();
        let created = read(&mut session);
        assert!(created.generation > before);
        let p = giggles_pg_catalog::plan(
            "select xmin, age(xmin) from pg_class where relname='refresh_table'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            execute(&p, &created, &[]).unwrap().rows,
            [vec![
                Value::Int(i64::from(created.generation as u32)),
                Value::Int(0)
            ]]
        );
        session
            .execute_sql("INSERT INTO refresh_table VALUES (1)", &[])
            .unwrap();
        let committed = read(&mut session).generation;
        assert!(committed > created.generation);
        session.execute_sql("BEGIN", &[]).unwrap();
        session
            .execute_sql("INSERT INTO refresh_table VALUES (2)", &[])
            .unwrap();
        assert_eq!(read(&mut session).generation, committed);
        session.execute_sql("ROLLBACK", &[]).unwrap();
        assert_eq!(read(&mut session).generation, committed);
        drop(session);
        drop(service);
        let reopened = make_service(Admin::open(&path, Limits::default()).unwrap());
        let mut session = reopened.role_session("refresh_owner").unwrap();
        assert_eq!(read(&mut session).generation, committed);
        session
            .execute_sql(
                "CREATE TABLE refresh_after_restart (id BIGINT PRIMARY KEY)",
                &[],
            )
            .unwrap();
        assert!(read(&mut session).generation > committed);
        drop(session);
        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn heap_storage_key_is_removed_before_catalog_derivation() {
        use giggles_core::{Admin, Limits, roles::NewRole};
        let path = crate::test_support::fixture_root().join(format!(
            "catalog-heap-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let admin = Admin::create(&path, Limits::default()).unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "heap_owner".into(),
                login: true,
                superuser: true,
                ..Default::default()
            })
            .unwrap();
        let service =
            crate::Service::new(admin, vec![], crate::clock::Clock::new(0, 3600).unwrap()).unwrap();
        let mut session = service.role_session("heap_owner").unwrap();
        for sql in [
            "CREATE TABLE heap_table (a VARCHAR(7), b NUMERIC(8,2))",
            "CREATE TABLE pk_table (id BIGINT PRIMARY KEY, a VARCHAR(9))",
        ] {
            session
                .execute_sql(sql, &[])
                .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        }
        let mut read = session.read_catalog().unwrap();
        let heap = read.tables.iter().find(|t| t.name == "heap_table").unwrap();
        let hidden = heap.columns[heap.hidden_rowid().unwrap()].id;
        // Heap secondary-index execution is outside this adapter test. Supply
        // stable-ID catalog descriptors to prove their attribute mapping survives.
        read.indexes.push(giggles_core::IndexInfo {
            id: 99,
            table: heap.id,
            name: "heap_b".into(),
            columns: vec![heap.columns.iter().find(|c| c.name == "b").unwrap().id],
            unique: false,
            predicate: None,
        });
        read.indexes.push(giggles_core::IndexInfo {
            id: 100,
            table: heap.id,
            name: "hidden_storage_index".into(),
            columns: vec![hidden],
            unique: true,
            predicate: None,
        });
        // Even stale presentation metadata for the storage marker is removed.
        read.metadata.typmods.insert(hidden, 123);
        read.metadata.types.column_types.insert(hidden, 999);
        let snapshot = snapshot(&session, read).unwrap();
        assert!(!snapshot.column_typmods.contains_key(&hidden));
        assert!(!snapshot.column_type_oids.contains_key(&hidden));
        let heap = snapshot
            .tables
            .iter()
            .find(|t| t.name == "heap_table")
            .unwrap();
        assert!(heap.primary_key.is_empty());
        assert_eq!(
            heap.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        let pk = snapshot
            .tables
            .iter()
            .find(|t| t.name == "pk_table")
            .unwrap();
        assert_eq!(pk.primary_key, [0]);
        let run = |sql: &str| {
            let p = giggles_pg_catalog::plan(sql)
                .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
                .unwrap();
            execute(&p, &snapshot, &[])
                .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
                .rows
        };
        assert_eq!(
            run(
                "select a.attname,a.attnum,a.atttypmod from pg_attribute a join pg_class c on c.oid=a.attrelid where c.relname='heap_table' AND a.attnum>0 order by a.attnum"
            ),
            [
                vec![Value::Text("a".into()), Value::Int(1), Value::Int(11)],
                vec![Value::Text("b".into()), Value::Int(2), Value::Int(524294)],
            ]
        );
        assert_eq!(
            run(
                "select column_name,ordinal_position from information_schema.columns where table_name='heap_table' order by ordinal_position"
            ),
            [
                vec![Value::Text("a".into()), Value::Int(1)],
                vec![Value::Text("b".into()), Value::Int(2)],
            ]
        );
        assert!(run("select conname from pg_constraint where conrelid=(select oid from pg_class where relname='heap_table') and contype='p'").is_empty());
        assert!(run("select indexrelid from pg_index where indrelid=(select oid from pg_class where relname='heap_table') and indisprimary").is_empty());
        assert_eq!(
            run(
                "select indkey from pg_index where indrelid=(select oid from pg_class where relname='heap_table')"
            ),
            [vec![Value::Array(vec![Value::Int(2)])]]
        );
        assert_eq!(
            run("select relnatts,relhasindex from pg_class where relname='heap_table'"),
            [vec![Value::Int(2), Value::Bool(true)]]
        );
        assert_eq!(
            run(
                "select conname from pg_constraint where conrelid=(select oid from pg_class where relname='pk_table') and contype='p'"
            ),
            [vec![Value::Text("pk_table_pkey".into())]]
        );
        assert_eq!(
            run(
                "select indkey,indisprimary from pg_index where indrelid=(select oid from pg_class where relname='pk_table')"
            ),
            [vec![Value::Array(vec![Value::Int(1)]), Value::Bool(true)]]
        );
        drop(session);
        drop(service);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn xid_parameters_round_trip_unsigned_text_and_binary() {
        assert_eq!(parameter_type(28), Ok(PgType::Xid));
        for xid in [0, 42, u32::MAX] {
            let value = Value::Int(xid.into());
            for format in [0, 1] {
                let bytes = encode(&value, PgType::Xid, format).unwrap().unwrap();
                assert_eq!(decode(&bytes, PgType::Xid, format), Ok(value.clone()));
            }
        }
        assert_eq!(
            decode(b" -1 ", PgType::Xid, 0),
            Ok(Value::Int(u32::MAX.into()))
        );
        assert_eq!(decode(b"4294967296", PgType::Xid, 0), Err("22P02"));
        assert_eq!(decode(&[0; 3], PgType::Xid, 1), Err("22P03"));
    }

    #[test]
    fn empty_internal_char_has_distinct_text_binary_and_null_encodings() {
        let empty = Value::Text(String::new());
        assert_eq!(encode(&empty, PgType::Char, 0).unwrap(), Some(vec![]));
        assert_eq!(encode(&empty, PgType::Char, 1).unwrap(), Some(vec![0]));
        assert_eq!(encode(&Value::Null, PgType::Char, 1).unwrap(), None);
        assert_eq!(decode(&[], PgType::Char, 0).unwrap(), empty);
        assert_eq!(decode(&[0], PgType::Char, 1).unwrap(), empty);
        assert!(decode(&[], PgType::Char, 1).is_err());
        assert!(decode(&[0], PgType::Char, 0).is_err());
        assert!(encode(&Value::Text("ab".into()), PgType::Char, 0).is_err());
        for format in [0, 1] {
            let v = Value::Text("r".into());
            let bytes = encode(&v, PgType::Char, format).unwrap().unwrap();
            assert_eq!(decode(&bytes, PgType::Char, format).unwrap(), v);
        }
    }
    #[test]
    fn oidvector_preserves_unsigned_elements_zero_origin_and_vector_syntax() {
        assert_eq!(parameter_type(30), Ok(PgType::OidVector));
        let value = Value::Array(vec![Value::Int(0), Value::Int(u32::MAX.into())]);
        assert_eq!(
            encode(&value, PgType::OidVector, 0).unwrap().unwrap(),
            b"0 4294967295"
        );
        let wire = encode(&value, PgType::OidVector, 1).unwrap().unwrap();
        assert_eq!(&wire[8..12], &26u32.to_be_bytes());
        assert_eq!(&wire[16..20], &0u32.to_be_bytes());
        assert_eq!(decode(&wire, PgType::OidVector, 1).unwrap(), value);
        let mut wrong = wire.clone();
        wrong[16..20].copy_from_slice(&1u32.to_be_bytes());
        assert_eq!(decode(&wrong, PgType::OidVector, 1), Err("0A000"));
        wrong = wire.clone();
        wrong[8..12].copy_from_slice(&23u32.to_be_bytes());
        assert_eq!(decode(&wrong, PgType::OidVector, 1), Err("22P03"));
        for value in [Value::Null, Value::Int(-1), Value::Int(4_294_967_296)] {
            assert!(encode(&Value::Array(vec![value]), PgType::OidVector, 1).is_err());
        }
        let empty = Value::Array(vec![]);
        let bytes = encode(&empty, PgType::OidVector, 1).unwrap().unwrap();
        assert_eq!(decode(&bytes, PgType::OidVector, 1).unwrap(), empty);
    }
    #[test]
    fn vectors_use_zero_based_binary_bounds_and_names_escape_as_arrays() {
        let value = Value::Array(vec![Value::Int(2), Value::Int(1)]);
        assert_eq!(
            encode(&value, PgType::Int2Vector, 0).unwrap().unwrap(),
            b"2 1"
        );
        let bytes = encode(&value, PgType::Int2Vector, 1).unwrap().unwrap();
        assert_eq!(&bytes[16..20], &0i32.to_be_bytes());
        assert_eq!(decode(&bytes, PgType::Int2Vector, 1).unwrap(), value);
        assert!(encode(&Value::Array(vec![Value::Null]), PgType::Int2Vector, 1).is_err());
        let names = Value::Array(vec![Value::Text("a,b".into()), Value::Text("NULL".into())]);
        assert_eq!(
            encode(&names, PgType::NameArray, 0).unwrap().unwrap(),
            br#"{"a,b","NULL"}"#
        );
    }
    #[test]
    fn actual_driver_decodes_unsigned_oid_and_signed_scalar_boundaries() {
        let oid = encode(&Value::Int(u32::MAX.into()), PgType::Oid, 1)
            .unwrap()
            .unwrap();
        assert_eq!(u32::from_sql(&Type::OID, &oid).unwrap(), u32::MAX);
        assert_eq!(encode(&Value::Int(-1), PgType::Oid, 0), Err("22003"));
        assert_eq!(encode(&Value::Int(i64::MAX), PgType::Int2, 0), Err("22003"));
        let small = encode(&Value::Int(i16::MIN.into()), PgType::Int2, 1)
            .unwrap()
            .unwrap();
        assert_eq!(i16::from_sql(&Type::INT2, &small).unwrap(), i16::MIN);
    }
    #[test]
    fn actual_driver_decodes_array_nulls_empty_and_escaped_text() {
        let value = Value::Array(vec![
            Value::Text("NULL".into()),
            Value::Null,
            Value::Text("a,\"b\\c".into()),
            Value::Text("Î»".into()),
        ]);
        let binary = encode(&value, PgType::TextArray, 1).unwrap().unwrap();
        assert_eq!(
            Vec::<Option<String>>::from_sql(&Type::TEXT_ARRAY, &binary).unwrap(),
            vec![
                Some("NULL".into()),
                None,
                Some("a,\"b\\c".into()),
                Some("Î»".into())
            ]
        );
        assert_eq!(
            String::from_utf8(encode(&value, PgType::TextArray, 0).unwrap().unwrap()).unwrap(),
            "{\"NULL\",NULL,\"a,\\\"b\\\\c\",\"Î»\"}"
        );
        let empty = encode(&Value::Array(vec![]), PgType::OidArray, 1)
            .unwrap()
            .unwrap();
        assert!(
            Vec::<u32>::from_sql(&Type::OID_ARRAY, &empty)
                .unwrap()
                .is_empty()
        );
        let oid = encode(
            &Value::Array(vec![Value::Int(u32::MAX.into())]),
            PgType::OidArray,
            1,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            Vec::<u32>::from_sql(&Type::OID_ARRAY, &oid).unwrap(),
            vec![u32::MAX]
        );
    }
    #[test]
    fn wrong_types_nested_arrays_and_internal_records_fail_closed() {
        assert_eq!(
            encode(&Value::Text("true".into()), PgType::Bool, 0),
            Err("42804")
        );
        assert_eq!(
            encode(&Value::Record(vec![]), PgType::Record, 0),
            Err("0A000")
        );
        assert_eq!(
            encode(
                &Value::Array(vec![Value::Array(vec![])]),
                PgType::Int2Array,
                1
            ),
            Err("42804")
        );
        assert_eq!(
            encode(&Value::Text("a\0b".into()), PgType::Text, 0),
            Err("22021")
        );
        assert_eq!(encode(&Value::Null, PgType::Text, 2), Err("08P01"));
    }
    #[test]
    fn retained_budget_checks_shape_and_escaped_output_before_release() {
        let fields = vec![Field {
            name: "values".into(),
            data_type: PgType::TextArray,
        }];
        let valid = ResultSet {
            fields: fields.clone(),
            rows: vec![vec![Value::Array(vec![Value::Text("x".into())])]],
        };
        assert!(retained_bytes(&valid).unwrap() > 1);
        assert_eq!(
            retained_bytes(&ResultSet {
                fields: fields.clone(),
                rows: vec![vec![]]
            }),
            Err("XX000")
        );
        let huge = ResultSet {
            fields,
            rows: vec![vec![Value::Array(vec![Value::Text(
                "\\".repeat(MAX_RESPONSE / 2),
            )])]],
        };
        assert_eq!(retained_bytes(&huge), Err("54000"));
    }

    #[test]
    fn t44b_nominal_row_description_and_null_framing() {
        let fields = vec![
            Field {
                name: "routine".into(),
                data_type: PgType::Regproc,
            },
            Field {
                name: "expression".into(),
                data_type: PgType::PgNodeTree,
            },
        ];
        for format in [0, 1] {
            let mut actual = vec![];
            description(&fields, &[format], &mut actual).unwrap();
            let mut body = 2u16.to_be_bytes().to_vec();
            for (name, oid, size) in [("routine", 24u32, 4i16), ("expression", 194, -1)] {
                body.extend(name.as_bytes());
                body.push(0);
                body.extend(0u32.to_be_bytes());
                body.extend(0i16.to_be_bytes());
                body.extend(oid.to_be_bytes());
                body.extend(size.to_be_bytes());
                body.extend((-1i32).to_be_bytes());
                body.extend(format.to_be_bytes());
            }
            let expected = [
                vec![b'T'],
                ((body.len() + 4) as u32).to_be_bytes().to_vec(),
                body,
            ]
            .concat();
            assert_eq!(actual, expected);
            let mut actual = vec![];
            rows(
                &Default::default(),
                &Released::synthetic(ResultSet {
                    fields: fields.clone(),
                    rows: vec![vec![Value::Null, Value::Null]],
                }),
                &[format],
                0,
                1,
                &mut actual,
            )
            .unwrap();
            assert_eq!(
                actual,
                [
                    vec![b'D'],
                    14u32.to_be_bytes().to_vec(),
                    2u16.to_be_bytes().to_vec(),
                    (-1i32).to_be_bytes().to_vec(),
                    (-1i32).to_be_bytes().to_vec()
                ]
                .concat()
            );
        }
    }
    #[test]
    fn t44b_nominal_nonnull_io_and_parameter_oids_stay_closed() {
        for ty in [PgType::Regproc, PgType::PgNodeTree] {
            assert_eq!(parameter_type(ty.oid()), Err("0A000"));
            for format in [0, 1] {
                assert_eq!(encode(&Value::Null, ty, format), Ok(None));
                assert_eq!(encode(&Value::Bytes(vec![]), ty, format), Err("42804"));
                if ty == PgType::Regproc {
                    assert_eq!(encode(&Value::Int(0), ty, format),
                        Ok(Some(if format == 0 { b"-".to_vec() } else { vec![0; 4] })));
                    assert_eq!(encode(&Value::Int(u32::MAX.into()), ty, format), Err("0A000"));
                } else {
                    assert_eq!(encode(&Value::Int(0), ty, format), Err("42804"));
                }
                assert_eq!(encode(&Value::Text(String::new()), ty, format),
                    if ty == PgType::PgNodeTree { Ok(Some(vec![])) } else { Err("42804") });
                assert_eq!(decode(&[], ty, format), Err("0A000"));
                assert_eq!(decode(&[0, 0, 0, 0], ty, format), Err("0A000"));
            }
        }
        assert_eq!(parameter_type(1008), Err("0A000"));
        assert_eq!(parameter_type(u32::MAX), Err("0A000"));
        assert_eq!(parameter_type(26), Ok(PgType::Oid));
        assert_eq!(
            encode(&Value::Int(7), PgType::Oid, 1),
            Ok(Some(7u32.to_be_bytes().to_vec()))
        );
    }
    #[test]
    fn dbeaver_constraint_star_encodes_nonnull_node_tree() {
        use giggles_pg_catalog::{CatalogSnapshot, ColumnDef, ConstraintDef, ScalarType, TableDef};
        let snapshot = CatalogSnapshot {
            tables: vec![TableDef { id: 1, name: "child".into(), columns: vec![ColumnDef { id: 1, name: "id".into(), data_type: ScalarType::Integer, not_null: false }], primary_key: vec![] }],
            constraints: vec![ConstraintDef { id: 10, table: 1, name: "child_check".into(), kind: 'c', columns: vec![1], expression: Some("(id >= 0)".into()), validated: true, ..Default::default() }],
            ..Default::default()
        };
        let sql = "SELECT c.oid,c.*,t.relname as tabrelname,rt.relnamespace as refnamespace,d.description,case when c.contype='c' then substring(pg_get_constraintdef(c.oid),7) else null end consrc_copy FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid LEFT JOIN pg_class rt ON rt.oid=c.confrelid LEFT JOIN pg_description d ON d.objoid=c.oid AND d.objsubid=0 AND d.classoid='pg_constraint'::regclass WHERE c.conrelid=65540 ORDER BY c.oid";
        let plan = giggles_pg_catalog::plan(sql).unwrap().unwrap();
        let result = giggles_pg_catalog::execute(&plan, &snapshot, &[]).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.fields[28].data_type, PgType::PgNodeTree);
        assert!(retained_bytes(&result).unwrap() > 0);
        for format in [0, 1] {
            assert_eq!(encode(&result.rows[0][28], PgType::PgNodeTree, format), Ok(Some(b"(id >= 0)".to_vec())));
            assert_eq!(encode(&Value::Text("bad\0tree".into()), PgType::PgNodeTree, format), Err("22021"));
        }
    }

}

#[cfg(test)]
mod w1e_regressions {
    use super::*;
    #[test]
    fn task_sequences_and_schema_grants_are_hidden() {
        for owner in [10, 999] {
            let mut s = giggles_pg_catalog::CatalogSnapshot::default();
            s.sequences.push(giggles_pg_catalog::SequenceDef { oid: 90000, schema: "public".into(), name: "secret_sequence".into(), owner, acl: None });
            s.schema_privileges.insert((999, "secret_schema".into()), [true,true]);
            let s = task_filtered(s, &crate::service::TaskCatalogView { table: 7, schema: "public".into(), columns: Default::default(), hidden: vec!["secret_sequence".into()] });
            let plan = giggles_pg_catalog::plan("SELECT relname FROM pg_class").unwrap().unwrap();
            giggles_pg_catalog::execute(&plan, &s, &[]).unwrap();
            assert!(s.sequences.is_empty());
            assert!(s.schema_privileges.is_empty());
            for name in ["secret_sequence", "absent_sequence"] {
                let plan = giggles_pg_catalog::plan(&format!("SELECT has_sequence_privilege('{name}','USAGE') FROM pg_class LIMIT 1")).unwrap().unwrap();
                assert!(matches!(giggles_pg_catalog::execute(&plan, &s, &[]), Err(giggles_pg_catalog::Error::UnknownRelation(_))));
            }
        }
    }
}

#[cfg(test)]
mod task_hidden_owners {
    use super::*;
    use giggles_pg_catalog::{CatalogSnapshot, ExtensionDef};
    // Exercise registry identities before task_filtered sees presentation OIDs.
    #[test]
    fn stored_bootstrap_identity_policy() {
        use giggles_core::{Admin, Limits, roles::{NewRole, TablePrivilege}};
        let path = crate::test_support::fixture_root().join(format!("bootstrap-policy-{}-{}",
            std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        let admin = Admin::create(&path, Limits::default()).unwrap();
        admin.roles().create(NewRole { name: "r".into(), login: true, superuser: true, ..Default::default() }).unwrap();
        admin.sql_session().execute("CREATE TABLE t (id BIGINT PRIMARY KEY)", &[], None).unwrap();
        admin.roles().set_table_owner("t", "r").unwrap();
        let service = crate::Service::new(admin.clone(), vec![], crate::clock::Clock::new(0,3600).unwrap()).unwrap();
        let mut session = service.role_session("r").unwrap();
        let read = |session: &mut crate::Session, task, user: &str| {
            let crate::service::CatalogRead { metadata, tables, indexes, .. } =
                session.read_catalog().unwrap();
            let mut identity = identity(session);
            identity.task = task;
            identity.user = user.into();
            let owners = owner_keys(&metadata).into_iter().filter_map(|key|
                session.sql.table_owner(&key).unwrap().map(|owner| (key, owner))).collect();
            snapshot_owned(SnapshotContext { identity, metadata, owners, registry: admin.roles().image().unwrap() }, tables, indexes).unwrap()
        };
        let baseline = read(&mut session, true, "r");
        admin.roles().create(NewRole { name: "giggles".into(), ..Default::default() }).unwrap();
        admin.roles().grant_table("t", "giggles", &[TablePrivilege::Select]).unwrap();
        let ordinary = read(&mut session, false, "r");
        assert_eq!(ordinary.roles.iter().find(|r| r.name == "giggles").unwrap().oid, 10);
        assert!(!ordinary.roles[0].superuser);
        let id = ordinary.tables.iter().find(|t| t.name == "t").unwrap().id;
        assert_eq!(ordinary.table_acls[&id].len(), 2);
        assert_eq!(ordinary.table_acls[&id][1].grantee, 10);
        let hidden = read(&mut session, true, "r");
        assert_eq!(hidden.roles, baseline.roles);
        assert_eq!(hidden.table_acls, baseline.table_acls);
        // A retained session identity keeps the same presentation as ordinary.
        let retained = read(&mut session, true, "giggles");
        assert_eq!(retained.roles[0], ordinary.roles[0]);
        assert_eq!(retained.table_acls, ordinary.table_acls);
        admin.roles().set_table_owner("t", "giggles").unwrap();
        let owned = read(&mut session, true, "r");
        assert_eq!(owned.table_owners[&id], 10);
        assert_eq!(owned.table_acls[&id][1].grantee, 10);
        drop(session); drop(service); drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
    fn fixture() -> (CatalogSnapshot, crate::service::TaskCatalogView) {
        let mut s = CatalogSnapshot::default();
        for (oid, name) in [(42, "a"), (43, "b"), (99, "h")] {
            let mut role = s.roles[0].clone();
            role.oid = oid; role.name = name.into(); role.superuser = false;
            s.roles.push(role);
        }
        s.user = "a".into();
        s.tables.push(giggles_pg_catalog::TableDef { id: 7, name: "t".into(), primary_key: vec![], columns: vec![giggles_pg_catalog::ColumnDef { id: 11, name: "c".into(), data_type: giggles_pg_catalog::ScalarType::Text, not_null: false }] });
        s.table_schemas.insert(7, "s".into());
        s.table_owners.insert(7, 43);
        s.schemas.insert("s".into(), (80000, 99));
        (s, crate::service::TaskCatalogView { table: 7, schema: "s".into(), columns: [11].into(), hidden: vec![] })
    }
    fn run(s: &CatalogSnapshot, expr: &str) -> Value {
        let p = giggles_pg_catalog::plan_gateway(&format!("SELECT {expr}")).unwrap().unwrap();
        giggles_pg_catalog::execute(&p, s, &[]).unwrap().rows[0][0].clone()
    }
    #[test]
    fn schema_owner() {
        let (s, v) = fixture();
        assert_eq!(task_filtered(s, &v).schemas["s"].1, 10);
    }
    #[test]
    fn collations() {
        let (mut s, v) = fixture();
        for (oid, owner) in [(81000, 99), (81001, 99), (81002, 42)] {
            s.collations.push(vec![Value::Int(oid), Value::Text(format!("c{oid}")), Value::Int(80000), Value::Int(owner)]);
        }
        s.collation_oids.insert(11, vec![81000, 81002]);
        s.collation_oids.insert(12, vec![81001]);
        let s = task_filtered(s, &v);
        assert_eq!(s.collations.len(), 2);
        assert_eq!(s.collations[0][3], Value::Int(10));
        assert_eq!(s.collations[1][3], Value::Int(42));
        assert!(!s.collation_oids.contains_key(&12));
    }
    #[test]
    fn extensions() {
        let (mut s, v) = fixture();
        s.extensions.clear();
        for owner in [99, 42] {
            let mut e = ExtensionDef::uuid_ossp();
            e.installed = Some((82000 + owner, owner, 80000, "1.1".into()));
            s.extensions.push(e);
        }
        let s = task_filtered(s, &v);
        assert_eq!(s.extensions[0].installed.as_ref().unwrap().1, 10);
        assert_eq!(s.extensions[1].installed.as_ref().unwrap().1, 42);
    }
    fn edge(parent: i64, member: i64, inherit: bool, set: bool, admin: bool) -> Vec<Value> {
        vec![Value::Int(1), Value::Int(parent), Value::Int(member), Value::Int(10), Value::Bool(admin), Value::Bool(inherit), Value::Bool(set)]
    }
    #[test]
    fn membership_oids_ignore_hidden_edges() {
        let (mut s, v) = fixture();
        // Registry name order differs from retained OID order.
        s.role_memberships = vec![edge(43,42,true,false,true), edge(10,43,false,true,false)];
        for (n, edge) in s.role_memberships.iter_mut().enumerate() {
            edge[0] = Value::Int(100000 + n as i64);
        }
        let baseline = task_filtered(s.clone(), &v).role_memberships;
        s.role_memberships.insert(0, edge(99,42,true,true,true));
        for (n, edge) in s.role_memberships.iter_mut().enumerate() {
            edge[0] = Value::Int(100000 + n as i64);
        }
        assert_eq!(baseline, task_filtered(s, &v).role_memberships);
        assert_eq!(baseline[0][1], Value::Int(10));
        assert_eq!(baseline[0][0], Value::Int(100000));
        assert_eq!(baseline[1][0], Value::Int(100001));
    }
    #[test]
    fn hidden_chain_and_direct_options() {
        let (mut s, v) = fixture();
        s.role_memberships = vec![edge(99,42,true,true,true), edge(43,99,true,true,true)];
        s.role_privileges.insert(("a".into(), "b".into()), [true;4]);
        let filtered = task_filtered(s.clone(), &v);
        assert_eq!(filtered.role_privileges[&("a".into(),"b".into())], [false;4]);
        for p in ["MEMBER","USAGE","SET","MEMBER WITH ADMIN OPTION"] {
            assert_eq!(run(&filtered, &format!("pg_has_role('a','b','{p}')")), Value::Bool(false));
        }
        s.role_memberships.push(edge(43,42,false,true,true));
        let filtered = task_filtered(s, &v);
        assert_eq!(filtered.role_privileges[&("a".into(),"b".into())], [true,false,true,true]);
        assert_eq!(run(&filtered, "pg_has_role('a','b','MEMBER')"), Value::Bool(true));
        assert_eq!(filtered.role_privileges[&("giggles".into(),"b".into())], [true;4]);
    }
    #[test]
    fn hidden_schema_privilege_equals_absent() {
        let (s,v) = fixture();
        let s = task_filtered(s, &v);
        for p in ["CREATE","USAGE"] {
            assert_eq!(run(&s, &format!("has_schema_privilege(99::oid,'s','{p}')")), run(&s, &format!("has_schema_privilege(4294967000::oid,'s','{p}')")));
        }
    }
    #[test]
    fn hidden_acl_footprint() {
        use giggles_pg_catalog::{AclEntry, AclPrivilege as P};
        let (mut s, v) = fixture();
        let owner = AclEntry { grantee: 43, grantor: 43, privileges: vec![P::Insert, P::Select, P::Update, P::Delete, P::Truncate, P::References, P::Trigger, P::Maintain], grant_option: false };
        let hidden = AclEntry { grantee: 99, grantor: 43, privileges: vec![P::Select], grant_option: false };
        s.table_acls.insert(7, vec![owner.clone(), hidden.clone()]);
        s.column_acls.insert((7,11), vec![hidden]);
        let filtered = task_filtered(s.clone(), &v);
        assert!(!filtered.table_acls.contains_key(&7));
        assert!(!filtered.column_acls.contains_key(&(7,11)));
        // An explicit owner grant is a second entry, even after owner revocation.
        s.table_acls.get_mut(&7).unwrap().push(AclEntry { privileges: vec![P::Select], ..owner });
        assert_eq!(task_filtered(s.clone(), &v).table_acls[&7].len(), 2);
        s.table_acls.get_mut(&7).unwrap().push(AclEntry { grantee: 0, grantor: 43, privileges: vec![P::Select], grant_option: false });
        assert_eq!(task_filtered(s, &v).table_acls[&7].len(), 3);
    }

}
