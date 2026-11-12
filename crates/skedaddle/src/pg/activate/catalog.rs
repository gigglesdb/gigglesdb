//! Catalog adapter using the source fence's public role and ACL model.
use super::*;
use crate::pg::fence::{Grant, Membership, Role};

pub(super) fn read(
    connection: &mut impl Connection,
    tables: &[Table],
    timeout: Duration,
) -> Result<Catalog, Error> {
    let selected = selected(tables);
    let sql = format!(
        "SELECT 'role',rolname,rolcanlogin::text,rolsuper::text,rolbypassrls::text,'','','','' FROM pg_roles \
         UNION ALL SELECT 'member',pg_get_userbyid(member),pg_get_userbyid(roleid),'','','','','','' FROM pg_auth_members \
         UNION ALL SELECT 'member',pg_get_userbyid(datdba),'pg_database_owner','','','','','','' FROM pg_database WHERE datname=current_database() \
         UNION ALL SELECT 'table',n.nspname,c.relname,pg_get_userbyid(c.relowner),'','','','','' FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid IN ({selected}) \
         UNION ALL SELECT 'grant',n.nspname,c.relname,pg_get_userbyid(c.relowner),'',CASE WHEN x.grantee=0 THEN 'public' ELSE 'role' END,CASE WHEN x.grantee=0 THEN '' ELSE pg_get_userbyid(x.grantee) END,pg_get_userbyid(x.grantor),x.privilege_type||':'||x.is_grantable::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) x WHERE c.oid IN ({selected}) AND x.privilege_type IN ('INSERT','UPDATE','DELETE','TRUNCATE') \
         UNION ALL SELECT 'grant',n.nspname,c.relname,pg_get_userbyid(c.relowner),a.attname,CASE WHEN x.grantee=0 THEN 'public' ELSE 'role' END,CASE WHEN x.grantee=0 THEN '' ELSE pg_get_userbyid(x.grantee) END,pg_get_userbyid(x.grantor),x.privilege_type||':'||x.is_grantable::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped CROSS JOIN LATERAL aclexplode(a.attacl) x WHERE c.oid IN ({selected}) AND x.privilege_type IN ('INSERT','UPDATE')"
    );
    let mut catalog = Catalog::default();
    for row in query(connection, &sql, timeout)? {
        match cell(&row, 0)? {
            "role" => catalog.roles.push(Role {
                name: cell(&row, 1)?.into(),
                login: boolean(cell(&row, 2)?)?,
                superuser: boolean(cell(&row, 3)?)?,
                bypass_rls: boolean(cell(&row, 4)?)?,
            }),
            "member" => catalog.memberships.push(Membership {
                member: cell(&row, 1)?.into(),
                role: cell(&row, 2)?.into(),
            }),
            "table" => catalog.tables.push(table(&row)?),
            "grant" => {
                let (privilege, option) = cell(&row, 8)?
                    .split_once(':')
                    .ok_or_else(|| Error::Invalid("invalid ACL".into()))?;
                catalog.grants.push(Grant {
                    table: table(&row)?,
                    column: (!cell(&row, 4)?.is_empty()).then(|| row[4].clone()),
                    grantee: if cell(&row, 5)? == "public" {
                        Grantee::Public
                    } else {
                        Grantee::Role(cell(&row, 6)?.into())
                    },
                    grantor: cell(&row, 7)?.into(),
                    grant_option: boolean(option)?,
                    privilege: match privilege {
                        "INSERT" => Privilege::Insert,
                        "UPDATE" => Privilege::Update,
                        "DELETE" => Privilege::Delete,
                        "TRUNCATE" => Privilege::Truncate,
                        _ => return Err(Error::Invalid("unknown write privilege".into())),
                    },
                });
            }
            _ => return Err(Error::Invalid("unknown catalog row".into())),
        }
    }
    if catalog.tables.len() != tables.len() {
        return Err(Error::Invalid(
            "selected table missing or duplicated".into(),
        ));
    }
    Ok(catalog)
}

fn table(row: &[String]) -> Result<Table, Error> {
    Ok(Table {
        schema: cell(row, 1)?.into(),
        name: cell(row, 2)?.into(),
        owner: cell(row, 3)?.into(),
    })
}
