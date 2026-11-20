use super::*;

fn role(rt: &mut Runtime<'_>, value: &Value) -> R<i64> {
    rt.charge(rt.snapshot.roles.len())?;
    match value {
        Value::Int(oid) => Ok(*oid),
        Value::Text(name) => rt.catalogs["pg_authid"].rows.iter().find(|r| matches!(&r[1],Value::Text(n) if n==name))
            .and_then(|r| if let Value::Int(oid)=r[0] {Some(oid)} else {None}).ok_or_else(|| Error::UnknownRole(name.clone())),
        _ => Err(Error::Type),
    }
}
fn reachable(rt: &mut Runtime<'_>, member: i64, target: i64, option: usize) -> R<bool> {
    if member == target { return Ok(true); }
    let mut seen = std::collections::BTreeSet::from([member]);
    let mut pending = vec![member];
    while let Some(member) = pending.pop() {
        rt.charge(rt.snapshot.role_memberships.len())?;
        for edge in &rt.snapshot.role_memberships {
            if edge.len() < 7 { return Err(Error::InvalidSnapshot); }
            if edge[2] == Value::Int(member) && (option == 0 || edge[option] == Value::Bool(true)) {
                let Value::Int(parent) = edge[1] else { return Err(Error::InvalidSnapshot); };
                if parent == target { return Ok(true); }
                if seen.insert(parent) { pending.push(parent); }
            }
        }
    }
    Ok(false)
}
fn inherited(rt: &mut Runtime<'_>, member: i64, target: i64) -> R<bool> {
    rt.charge(rt.snapshot.roles.len().saturating_mul(2))?;
    let names = rt.snapshot.roles.iter().find(|r| i64::from(r.oid) == member)
        .zip(rt.snapshot.roles.iter().find(|r| i64::from(r.oid) == target));
    if names.is_some_and(|(m,t)| rt.snapshot.role_privileges.get(&(m.name.clone(),t.name.clone())).is_some_and(|p| p[1])) {
        return Ok(true);
    }
    reachable(rt, member, target, 5)
}
fn acl(rt: &mut Runtime<'_>, value: Value, member: i64, requested: &[(String,bool)]) -> R<bool> {
    if value == Value::Null { return Ok(false); }
    let Value::Array(entries) = rt.call("_aclexplode", vec![value], None)? else { return Err(Error::Type); };
    for entry in entries {
        let Value::Record(row) = entry else { return Err(Error::Type); };
        rt.charge(requested.len())?;
        if requested.iter().any(|(p,g)| row[2] == Value::Text(p.clone()) && (!g || row[3] == Value::Bool(true))) {
            let Value::Int(grantee) = row[1] else { return Err(Error::Type); };
            if grantee == 0 || inherited(rt, member, grantee)? { return Ok(true); }
        }
    }
    Ok(false)
}
pub(super) fn evaluate(rt: &mut Runtime<'_>, name: &str, values: &[Value], effective_role: &str) -> R<Value> {
    use Value::{Bool as B, Int as I, Null as N, Text as S};
    if values.contains(&N) { return Ok(N); }
    let column = name == "has_column_privilege";
    let explicit = values.len() == if column {4} else {3};
    let at = usize::from(explicit);
    let current = S(effective_role.to_owned());
    let who = if explicit { &values[0] } else { &current };
    let member = if name != "pg_has_role" && *who == S("public".into()) {0} else {role(rt,who)?};
    let superuser = rt.snapshot.roles.iter().any(|r| i64::from(r.oid) == member && r.superuser);
    let list = text(values.last().ok_or(Error::Type)?.clone())?;
    let mut requested = vec![];
    for item in list.split(',') {
        let normalized = item.trim().to_ascii_uppercase();
        let (base, grant) = normalized.strip_suffix(" WITH GRANT OPTION")
            .or_else(|| (name == "pg_has_role").then(|| normalized.strip_suffix(" WITH ADMIN OPTION")).flatten())
            .map_or((normalized.as_str(), false), |s| (s.trim_end(), true));
        let valid = match name {
            "pg_has_role" => matches!(base, "MEMBER" | "USAGE" | "SET"),
            "has_sequence_privilege" => matches!(base, "USAGE" | "SELECT" | "UPDATE"),
            "has_database_privilege" => matches!(base, "CREATE" | "CONNECT" | "TEMP" | "TEMPORARY"),
            "has_schema_privilege" => matches!(base, "CREATE" | "USAGE"),
            "has_type_privilege" => base == "USAGE",
            "has_column_privilege" | "has_any_column_privilege" => matches!(base, "SELECT" | "INSERT" | "UPDATE" | "REFERENCES"),
            _ => matches!(base, "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "TRUNCATE" | "REFERENCES" | "TRIGGER" | "MAINTAIN"),
        };
        if !valid { return Err(Error::InvalidPrivilege(format!("unrecognized privilege type: \"{}\"", item.trim()))); }
        requested.push((base.to_owned(),grant));
    }
    if name == "pg_has_role" {
        let target = role(rt, &values[at])?;
        if superuser { return Ok(B(true)); }
        for (p, admin) in requested {
            let index = if admin {3} else {match p.as_str() {"MEMBER"=>0,"USAGE"=>1,_=>2}};
            let names = rt.snapshot.roles.iter().find(|r| i64::from(r.oid) == member)
                .zip(rt.snapshot.roles.iter().find(|r| i64::from(r.oid) == target));
            if names.is_some_and(|(m,t)| rt.snapshot.role_privileges.get(&(m.name.clone(),t.name.clone())).is_some_and(|p| p[index])) { return Ok(B(true)); }
            if !admin && reachable(rt,member,target, match index {0=>0,1=>5,_=>6})? { return Ok(B(true)); }
            if admin {
                for edge in rt.snapshot.role_memberships.clone() {
                    rt.charge(1)?;
                    if edge.len() >= 7 && edge[1] == I(target) && edge[4] == B(true)
                        && let I(grantee) = edge[2] && reachable(rt,member,grantee,5)? { return Ok(B(true)); }
                }
            }
        }
        return Ok(B(false));
    }
    let object = &values[at];
    if matches!(name, "has_schema_privilege" | "has_database_privilege" | "has_type_privilege") {
        let (catalog, owner_field, missing) = match name {
            "has_schema_privilege" => ("pg_namespace","nspowner","3F000"),
            "has_database_privilege" => ("pg_database","datdba","3D000"),
            _ => ("pg_type","typowner","42704"),
        };
        let resolved = if name == "has_type_privilege" && let S(n) = object {Some(rt.regtype(n)?)} else {None};
        let lookup = resolved.as_ref().unwrap_or(object);
        let relation = &rt.catalogs[catalog];
        rt.charge(relation.rows.len())?;
        let Some(row) = relation.rows.iter().find(|r| &r[0] == lookup || &r[1] == lookup).cloned() else {
            return if matches!(object,S(_)) { Err(Error::Inquiry(missing)) } else { Ok(N) };
        };
        let owner = relation.fields.iter().position(|f| f.name == owner_field).and_then(|i| if let I(o)=row[i] {Some(o)} else {None});
        let owns = superuser || match owner {Some(o)=>inherited(rt,member,o)?,None=>false};
        if name == "has_schema_privilege" {
            for ((grantee,schema),grants) in rt.snapshot.schema_privileges.clone() {
                rt.charge(1)?;
                if row[1] == S(schema) && requested.iter().any(|(p,g)| !g && grants[usize::from(p == "USAGE")])
                    && (grantee == 0 || inherited(rt,member,i64::from(grantee))?) {return Ok(B(true));}
            }
        }
        return Ok(B(requested.iter().any(|(p,g)| owns || (!g && match name {
            "has_database_privilege" => p != "CREATE",
            "has_schema_privilege" => p == "USAGE" && (row[0] == I(11) || row[0] == I(2200) || row[0] == I(13293)),
            _ => true,
        }))));
    }
    if name == "has_sequence_privilege" {
        rt.charge(rt.snapshot.sequences.len())?;
        let reference = if let S(n) = object {
            let mut parser = Parser::new(&PostgreSqlDialect {}).with_recursion_limit(64).try_with_sql(n).map_err(|_| Error::Syntax)?;
            let parts = object_name(&mut parser)?;
            Some(parts)
        } else {None};
        let sequence = rt.snapshot.sequences.iter().filter(|s| match object {
            I(oid) => i64::from(s.oid) == *oid,
            S(_) => match reference.as_deref().unwrap_or_default() {
                [n] => *n == s.name && rt.snapshot.search_path.contains(&s.schema),
                [schema,n] => *n == s.name && *schema == s.schema,
                _ => false,
            },
            _ => false,
        }).min_by_key(|s| rt.snapshot.search_path.iter().position(|schema| schema == &s.schema).unwrap_or(0)).cloned();
        if let Some(s) = sequence {
            let owns = inherited(rt,member,i64::from(s.owner))?;
            if superuser || (owns && (s.acl.is_none() || requested.iter().any(|(_,g)| *g))) {return Ok(B(true));}
            for entry in s.acl.unwrap_or_default() {
                if (entry.grantee == 0 || inherited(rt,member,i64::from(entry.grantee))?)
                    && requested.iter().any(|(p,g)| (!g || entry.grant_option) && entry.privileges.iter().any(|v| match v {
                        crate::AclPrivilege::Usage=>p=="USAGE",crate::AclPrivilege::Select=>p=="SELECT",crate::AclPrivilege::Update=>p=="UPDATE",_=>false,
                    })) {return Ok(B(true));}
            }
            return Ok(B(false));
        }
        if let S(n) = object { regclass_oid(n,rt.snapshot).map_err(|_| Error::UnknownRelation(n.clone()))?; return Err(Error::Inquiry("42809")); }
        return if rt.catalogs["pg_class"].rows.iter().any(|r| &r[0]==object) { Err(Error::Inquiry("42809")) } else { Ok(N) };
    }
    let oid = match object { S(n)=>i64::from(regclass_oid(n,rt.snapshot).map_err(|e| if n.split('.').count()>2 && !n.contains('"') {e} else {Error::UnknownRelation(n.clone())})?), I(oid)=>*oid, _=>return Err(Error::Type) };
    let relation = &rt.catalogs["pg_class"];
    rt.charge(relation.rows.len())?;
    let Some(row) = relation.rows.iter().find(|r| r[0] == I(oid)).cloned() else { return Ok(N); };
    let field = |n| relation.fields.iter().position(|f| f.name == n).unwrap();
    let owner = row[field("relowner")].clone();
    let table_acl = row[field("relacl")].clone();
    let attributes = &rt.catalogs["pg_attribute"];
    let f = |n| attributes.fields.iter().position(|f| f.name == n).unwrap();
    rt.charge(attributes.rows.len())?;
    let mut column_acls = vec![];
    if column || name == "has_any_column_privilege" {
        for row in &attributes.rows {
            if row[0] == I(oid) && row[f("attisdropped")] == B(false) && (if column {match &values[at+1] {
                S(n)=>row[1]==S(n.clone()), I(num)=>row[f("attnum")]==I(*num), _=>false,
            }} else { matches!(row[f("attnum")], I(n) if n>0) }) { column_acls.push(row[f("attacl")].clone()); }
        }
        if column && column_acls.is_empty() { return if matches!(values[at+1], S(_)) { Err(Error::UnknownColumn) } else { Ok(N) }; }
    }
    if superuser || (requested.iter().any(|(_,g)| *g) && matches!(owner,I(o) if inherited(rt,member,o)?)) { return Ok(B(true)); }
    // PG18.6 oracle-w1c.out: reflected catalogs have PUBLIC SELECT except these restricted tables.
    if table_acl == N && let S(n) = &row[1]
        && catalog::relation_oid(n).is_some_and(|id| i64::from(id)==oid)
        && !matches!(n.as_str(), "pg_authid" | "pg_shadow" | "pg_user_mapping" | "pg_subscription")
        && requested.iter().any(|(p,g)| p=="SELECT" && !g) {return Ok(B(true));}
    let table_acl = if table_acl == N { rt.call("acldefault",vec![S("r".into()),owner],None)? } else {table_acl};
    if acl(rt,table_acl,member,&requested)? {return Ok(B(true));}
    for c in column_acls { if acl(rt,c,member,&requested)? {return Ok(B(true));} }
    Ok(B(false))
}

fn object_name(parser: &mut Parser<'_>) -> R<Vec<String>> {
    let name = parser.parse_object_name(false).map_err(|_| Error::Syntax)?;
    if parser.peek_token().token != Token::EOF {return Err(Error::Syntax);}
    Ok(object(&name))
}
