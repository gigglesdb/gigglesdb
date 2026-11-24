//! Expression collation metadata; never part of persisted values or keys.
use super::*;
use std::sync::{Arc, Mutex, OnceLock};
const PREFIX: &str = "\0collate:";
const IMPLICIT: &str = "\0implicit_collate:";
const PROPAGATED: &str = "\0implicit_collate:metadata";
const DATA: &str = include_str!("../../../../../giggles-icu/src/pg_collation.txt");
pub(in crate::relational::query) fn internal(name: &str) -> bool {
    name.starts_with(PREFIX) || name.starts_with(IMPLICIT)
}
pub(super) fn parse(p: &mut Parser, value: Scalar) -> R<Scalar> {
    let (mut name, _) = Scalar::routine_name(p)?;
    if p.sym('.') {
        let (last, _) = Scalar::routine_name(p)?;
        if name != "pg_catalog" {
            name = format!("{name}.{last}");
        } else {
            name = last;
        }
    }
    Ok(Scalar::Call(format!("{PREFIX}{name}"), vec![value]))
}
pub(in crate::relational::query) fn resolve_name(
    name: &mut String,
    state: &Relational,
    path: &[String],
) {
    let Some(n) = name.strip_prefix(PREFIX) else {
        return;
    };
    if let Some((_, c)) = super::super::super::collations::resolve(state, n, path) {
        *name = format!(
            "{PREFIX}{}.{}\0{}\0{}\0{}\0{}",
            c.schema,
            c.name,
            c.provider,
            c.deterministic,
            c.locale.as_deref().or(c.collate.as_deref()).unwrap_or(""),
            c.rules.as_deref().unwrap_or("")
        );
    }
}
fn user(n: &str) -> Option<(&str, bool, &str, &str)> {
    let mut parts = n.split('\0');
    parts.next()?;
    Some((
        parts.next()?,
        parts.next()? == "true",
        parts.next()?,
        parts.next()?,
    ))
}
pub(in crate::relational::query) fn is_collatable(ty: &DataType) -> bool {
    matches!(ty, DataType::Text | DataType::Varchar | DataType::Bpchar | DataType::TextArray)
}

pub(super) fn bind(name: &str, value: BoundScalar) -> R<BoundScalar> {
    let n = name
        .strip_prefix(PREFIX)
        .or_else(|| name.strip_prefix(IMPLICIT))
        .ok_or(SqlError::Type)?;
    if user(n).is_none() && !DATA.lines().any(|r| r.split('|').nth(1) == Some(n)) {
        user_types::view_replace_error(format!(
            "collation \"{n}\" for encoding \"UTF8\" does not exist"
        ));
        return Err(SqlError::UndefinedObject);
    }
    if !is_collatable(&value.ty) {
        user_types::view_replace_error(format!(
            "collations are not supported by type {}",
            match value.ty {
                DataType::Integer => "integer",
                DataType::BigInt => "bigint",
                DataType::Boolean => "boolean",
                _ => "non-collatable",
            }
        ));
        return Err(SqlError::Type);
    }
    Ok(BoundScalar {
        ty: value.ty.clone(),
        node: Node::Call(name.into(), vec![value]),
    })
}
#[derive(Clone, Copy, Default)]
struct Derivation<'a> {
    name: Option<&'a str>,
    explicit: bool,
    conflict: Option<(&'a str, &'a str)>,
}
fn merge<'a>(args: impl Iterator<Item = &'a BoundScalar>) -> R<Derivation<'a>> {
    let mut found = Derivation::default();
    for a in args {
        let d = a.collation_derivation()?;
        if d.explicit && !found.explicit {
            found = d;
            continue;
        }
        if found.explicit && !d.explicit {
            continue;
        }
        if let (Some(old), Some(n)) = (found.name, d.name) {
            if old != n && (d.explicit || old != "default" && n != "default") {
                if d.explicit {
                    mismatch(old, n, true)?;
                }
                found.conflict = Some((old, n));
            } else if old == "default" {
                found.name = Some(n);
            }
        } else if found.name.is_none() {
            found.name = d.name;
        }
        found.conflict = found.conflict.or(d.conflict);
    }
    Ok(found)
}
fn mismatch(a: &str, b: &str, explicit: bool) -> R<()> {
    let label = |n: &str| n.split('\0').next().unwrap_or(n).to_owned();
    user_types::view_replace_error(format!(
        "collation mismatch between {} collations \"{}\" and \"{}\"",
        if explicit { "explicit" } else { "implicit" },
        label(a),
        label(b)
    ));
    Err(SqlError::CollationMismatch)
}
impl BoundScalar {
    fn collation_derivation(&self) -> R<Derivation<'_>> {
        if !is_collatable(&self.ty) {
            return Ok(Derivation::default());
        }
        match &self.node {
            // The second child carries derivation only; internal calls evaluate
            // just the first child. Preserve indeterminate derivations across
            // materialization without evaluating the original expression again.
            Node::Call(n, args) if n == PROPAGATED => args[1].collation_derivation(),
            Node::Call(n, _) if n.starts_with(PREFIX) => Ok(Derivation {
                name: n.strip_prefix(PREFIX),
                explicit: true,
                conflict: None,
            }),
            Node::Call(n, _) if n.starts_with(IMPLICIT) => Ok(Derivation {
                name: n.strip_prefix(IMPLICIT),
                explicit: false,
                conflict: None,
            }),
            Node::Call(n, args) if aggregate_order::internal(n) => args[0].collation_derivation(),
            Node::Call(_, args) => merge(args.iter()),
            Node::Cast(v)
            | Node::TypmodCast(v, _)
            | Node::SessionCast(v, _)
            | Node::Domain(v, _) => v.collation_derivation(),
            Node::Case(arms, other) => merge(
                arms.iter()
                    .map(|(_, v)| v)
                    .chain(other.iter().map(|v| v.as_ref())),
            ),
            _ => Ok(Derivation::default()),
        }
    }
    pub(in crate::relational::query) fn explicit_collation(&self) -> R<Option<&str>> {
        let d = self.collation_derivation()?;
        Ok(if d.conflict.is_some() { None } else { d.name })
    }
    pub(in crate::relational::query) fn inherit_collation(self, original: &Self) -> R<Self> {
        self.inherit_derivation(original.collation_derivation()?)
    }
    fn inherit_derivation(self, d: Derivation<'_>) -> R<Self> {
        if let Some((a, b)) = d.conflict {
            let carrier = |n| {
                bind(
                    &format!("{IMPLICIT}{n}"),
                    BoundScalar {
                        ty: self.ty.clone(),
                        node: Node::Literal(Value::Null),
                    },
                )
            };
            let metadata = BoundScalar {
                ty: self.ty.clone(),
                node: Node::Call("coalesce".into(), vec![carrier(a)?, carrier(b)?]),
            };
            return Ok(Self {
                ty: self.ty.clone(),
                node: Node::Call(PROPAGATED.into(), vec![self, metadata]),
            });
        }
        if let Some(n) = d.name {
            return bind(
                &format!("{}{n}", if d.explicit { PREFIX } else { IMPLICIT }),
                self,
            );
        }
        Ok(self)
    }
    pub(in crate::relational::query) fn require_collation(&self) -> R<()> {
        let d = self.collation_derivation()?;
        if let Some((a, b)) = d.conflict {
            mismatch(a, b, false)?;
        }
        if let Some(n) = d.name {
            ready(n)?;
        }
        Ok(())
    }
    /// Comparison consumers defer an implicit conflict until two values really
    /// meet (a singleton MIN/MAX or GREATEST needs no comparator).
    pub(in crate::relational::query) fn check_comparison_collation(&self) -> R<()> {
        if self.collation_derivation()?.conflict.is_some() {
            return indeterminate();
        }
        Ok(())
    }
    pub(in crate::relational::query) fn try_compare_values(
        &self,
        a: &Value,
        b: &Value,
    ) -> R<Ordering> {
        self.check_comparison_collation()?;
        if self.ty.is_array() {
            return array_order::compare(&self.ty, self.explicit_collation()?, a, b);
        }
        Ok(self.compare_values(a, b))
    }
}
pub(in crate::relational::query) fn derive<'a>(
    args: impl Iterator<Item = &'a BoundScalar>,
) -> R<Option<&'a str>> {
    let d = merge(args)?;
    Ok(if d.conflict.is_some() { None } else { d.name })
}
std::thread_local! { static COLUMNS: std::cell::RefCell<BTreeMap<u64,String>> = const { std::cell::RefCell::new(BTreeMap::new()) }; }
pub(in crate::relational::query) struct Scope(BTreeMap<u64, String>);
impl Scope {
    pub(in crate::relational::query) fn enter(state: &Relational) -> Self {
        let columns = state
            .ext
            .column_collations
            .iter()
            .filter_map(|(id, b)| match b {
                crate::relational::column_collations::Binding::Column(r) => {
                    state.collation_name(r).ok().map(|n| (*id, n))
                }
                _ => None,
            })
            .collect();
        Self(COLUMNS.with(|c| c.replace(columns)))
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        COLUMNS.with(|c| c.replace(std::mem::take(&mut self.0)));
    }
}
// Binding-only overrides for one materialized input. Never catalog or wire data.
#[derive(Clone)]
struct Metadata {
    name: Option<String>,
    explicit: bool,
    conflict: Option<(String, String)>,
}
std::thread_local! { static MATERIALIZED: std::cell::RefCell<BTreeMap<u64, Metadata>> = const { std::cell::RefCell::new(BTreeMap::new()) }; }
pub(in crate::relational::query) fn has_materialized_columns() -> bool {
    MATERIALIZED.with(|columns| !columns.borrow().is_empty())
}
pub(in crate::relational::query) struct ColumnScope(BTreeMap<u64, Metadata>);
impl ColumnScope {
    pub(in crate::relational::query) fn enter(
        columns: &[ColumnDef],
        metadata: &[Option<BoundScalar>],
    ) -> R<Self> {
        Self::enter_refs(columns, metadata.iter().map(Option::as_ref))
    }
    pub(in crate::relational::query) fn enter_refs<'a>(
        columns: &[ColumnDef],
        metadata: impl IntoIterator<Item = Option<&'a BoundScalar>>,
    ) -> R<Self> {
        let mut values = BTreeMap::new();
        for (c, m) in columns
            .iter()
            .zip(metadata.into_iter().chain(std::iter::repeat(None)))
        {
            let d = m
                .map(BoundScalar::collation_derivation)
                .transpose()?
                .unwrap_or_default();
            values.insert(
                c.id,
                Metadata {
                    name: d.name.map(str::to_owned),
                    explicit: d.explicit,
                    conflict: d.conflict.map(|(a, b)| (a.to_owned(), b.to_owned())),
                },
            );
        }
        let old = MATERIALIZED.with(|c| {
            let old = c.borrow().clone();
            c.borrow_mut().extend(values);
            old
        });
        Ok(Self(old))
    }
}
impl Drop for ColumnScope {
    fn drop(&mut self) {
        MATERIALIZED.with(|c| c.replace(std::mem::take(&mut self.0)));
    }
}
pub(in crate::relational::query) fn column(value: BoundScalar, id: u64) -> R<BoundScalar> {
    if let Some(d) = MATERIALIZED.with(|c| c.borrow().get(&id).cloned()) {
        return value.inherit_derivation(Derivation {
            name: d.name.as_deref(),
            explicit: d.explicit,
            conflict: d.conflict.as_ref().map(|(a, b)| (a.as_str(), b.as_str())),
        });
    }
    let n = COLUMNS.with(|c| c.borrow().get(&id).cloned());
    if let Some(n) = n {
        bind(&format!("{IMPLICIT}{n}"), value)
    } else {
        Ok(value)
    }
}
pub(in crate::relational::query) fn bound_column(c: BoundColumn, d: &ColumnDef) -> R<BoundScalar> {
    column(
        BoundScalar {
            ty: d.data_type.clone(),
            node: Node::Column(
                c,
                if d.name.starts_with('\0') { "?column?".into() } else { d.name.clone() },
            ),
        },
        d.id,
    )
}
pub(in crate::relational::query) fn has_column(id: u64) -> bool {
    MATERIALIZED.with(|c| {
        c.borrow().get(&id).map(|m| m.name.is_some() || m.conflict.is_some())
    }).unwrap_or_else(|| COLUMNS.with(|c| c.borrow().contains_key(&id)))
}
type Cache = Mutex<std::collections::HashMap<String, Arc<giggles_icu::Collator>>>;
static CACHE: OnceLock<Cache> = OnceLock::new();
fn collator(n: &str) -> R<Arc<giggles_icu::Collator>> {
    let mut cache = CACHE.get_or_init(Default::default).lock().unwrap();
    if let Some(c) = cache.get(n) {
        return Ok(c.clone());
    }
    let locale = if let Some((_, _, locale, _)) = user(n) {
        locale
    } else if n == "unicode" {
        "und"
    } else {
        n.strip_suffix("-x-icu").ok_or(SqlError::Unsupported)?
    };
    let c = Arc::new(
        giggles_icu::Collator::open(
            locale,
            user(n).and_then(|(_, _, _, r)| (!r.is_empty()).then_some(r)),
        )
        .map_err(|_| SqlError::Unsupported)?,
    );
    cache.insert(n.into(), c.clone());
    Ok(c)
}
pub(in crate::relational::query) fn ready(n: &str) -> R<()> {
    if matches!(n, "en_US" | "en_US.utf8")
        || user(n).is_some_and(|(p, _, l, _)| p == "c" && l == "en_US.utf8")
    {
        return Err(SqlError::Unsupported);
    }
    if n == "unicode" || n.ends_with("-x-icu") || user(n).is_some_and(|(p, _, _, _)| p == "i") {
        collator(n)?;
    }
    Ok(())
}
pub(in crate::relational::query) fn compare(
    collation: Option<&str>,
    a: &Value,
    b: &Value,
    ty: &DataType,
) -> Ordering {
    if ty.is_array() {
        // Scalar inputs are validated before sort/map callbacks. Preserve the
        // diagnostic if an internal value nevertheless needs the fallback.
        return user_types::probe_preserving_error_message(|| {
            array_order::compare(ty, collation, a, b)
        })
        .unwrap_or_else(|_| cmp(a, b));
    }
    match (a, b) {
        (Value::Text(a), Value::Text(b)) => compare_text(collation, a, b),
        _ => cmp(a, b),
    }
}

pub(super) fn compare_text(collation: Option<&str>, a: &str, b: &str) -> Ordering {
    if let Some(n) = collation
        && (n == "unicode" || n.ends_with("-x-icu") || user(n).is_some_and(|(p, _, _, _)| p == "i"))
    {
        return giggles_icu::pg_compare(
            &collator(n).expect("collation validated before execution"),
            user(n).is_none_or(|(_, d, _, _)| d),
            a.as_bytes(),
            b.as_bytes(),
        );
    }
    a.cmp(b)
}
pub(in crate::relational::query) fn pair(a: &BoundScalar, b: &BoundScalar) -> R<()> {
    if let Some(n) = derive([a, b].into_iter())? {
        ready(n)?;
    }
    Ok(())
}
pub(in crate::relational::query) fn compare_pair(
    a: &BoundScalar,
    b: &BoundScalar,
    av: &Value,
    bv: &Value,
) -> R<Ordering> {
    if a.ty == DataType::TextArray {
        logical_types::validate_text_array(av)?;
        logical_types::validate_text_array(bv)?;
    }
    let d = merge([a, b].into_iter())?;
    if d.conflict.is_some() {
        return indeterminate();
    }
    if a.ty.is_array() {
        return array_order::compare(&a.ty, d.name, av, bv);
    }
    let trim = |v: &Value, ty: &DataType| {
        if *ty == DataType::Bpchar {
            if let Value::Text(t) = v {
                return Value::Text(t.trim_end_matches(' ').into());
            }
            v.clone()
        } else {
            v.clone()
        }
    };
    let (av, bv) = (trim(av, &a.ty), trim(bv, &b.ty));
    Ok(compare(d.name, &av, &bv, &a.ty))
}

fn indeterminate<T>() -> R<T> {
    user_types::view_replace_error(
        "could not determine which collation to use for string comparison".into(),
    );
    Err(SqlError::IndeterminateCollation)
}

pub(super) fn casing(
    name: &str,
    input: &BoundScalar,
    value: &Value,
    budget: &mut usize,
) -> R<Option<Value>> {
    let Some(n) = input.explicit_collation()? else {
        return Ok(None);
    };
    let Value::Text(text) = value else {
        return Ok(None);
    };
    ready(n)?;
    let locale_name = user(n).map(|(_, _, l, _)| l);
    let output = if n == "pg_unicode_fast" || locale_name == Some("PG_UNICODE_FAST") {
        match name {
            "upper" => text.to_uppercase(),
            "lower" => text.to_lowercase(),
            _ => {
                let mut start = true;
                text.chars()
                    .map(|c| {
                        let v: String = if start {
                            c.to_uppercase().collect()
                        } else {
                            c.to_lowercase().collect()
                        };
                        start = !c.is_alphanumeric();
                        v
                    })
                    .collect()
            }
        }
    } else if n == "unicode"
        || n.ends_with("-x-icu")
        || user(n).is_some_and(|(p, _, _, _)| p == "i")
    {
        let locale = if let Some((_, _, l, _)) = user(n) {
            l
        } else if n == "unicode" || n == "pg_unicode_fast" {
            "und"
        } else {
            n.strip_suffix("-x-icu").unwrap()
        };
        giggles_icu::Icu::load()
            .map_err(|_| SqlError::Unsupported)?
            .case_map(locale, name, text)
            .map_err(|_| SqlError::Unsupported)?
    } else if matches!(n, "C" | "POSIX" | "ucs_basic") || matches!(locale_name, Some("C" | "POSIX"))
    {
        match name {
            "upper" => text.to_ascii_uppercase(),
            "lower" => text.to_ascii_lowercase(),
            _ => {
                let mut start = true;
                text.chars()
                    .map(|c| {
                        let v = if start {
                            c.to_ascii_uppercase()
                        } else {
                            c.to_ascii_lowercase()
                        };
                        start = !c.is_ascii_alphanumeric();
                        v
                    })
                    .collect()
            }
        }
    } else {
        return unicode::casing(name, text, budget).map(Some);
    };
    let value = Value::Text(output);
    charge(budget, &value)?;
    Ok(Some(value))
}

fn comparison_locale<'a>(a: &'a BoundScalar, b: &'a BoundScalar) -> R<Option<&'a str>> {
    let d = merge([a, b].into_iter())?;
    if d.conflict.is_some() {
        return indeterminate();
    }
    if let Some(n) = d.name {
        ready(n)?;
    }
    Ok(d.name)
}

fn nondeterministic(n: &str) -> bool {
    user(n).is_some_and(|(_, deterministic, _, _)| !deterministic)
}

pub(super) fn starts_with(args: &[BoundScalar], values: &[Value], budget: &mut usize) -> R<Value> {
    let [Value::Text(value), Value::Text(prefix)] = values else {
        return Err(SqlError::UndefinedFunction);
    };
    if comparison_locale(&args[0], &args[1])?.is_some_and(nondeterministic) {
        user_types::view_replace_error(
            "nondeterministic collations are not supported for substring searches".into(),
        );
        return Err(SqlError::Unsupported);
    }
    let result = Value::Bool(value.starts_with(prefix));
    charge(budget, &result)?;
    Ok(result)
}

pub(in crate::relational::query) fn like(
    args: [&BoundScalar; 2],
    value: &Value,
    pattern: &Value,
    escape: crate::relational::like::Escape,
    flags: (bool, bool),
    work: &mut usize,
) -> R<Option<bool>> {
    let (insensitive, negated) = flags;
    let (Value::Text(value), Value::Text(pattern)) = (value, pattern) else {
        return if *value == Value::Null || *pattern == Value::Null {
            Ok(None)
        } else {
            Err(SqlError::UndefinedFunction)
        };
    };
    // LIKE has historically taken its matching collation from the value. In
    // particular a pattern-only collation must not select the costlier matcher.
    args[0].check_comparison_collation()?;
    let input_locale = args[0].explicit_collation()?;
    if let Some(n) = input_locale.filter(|n| nondeterministic(n)) {
        if insensitive {
            user_types::view_replace_error(
                "nondeterministic collations are not supported for ILIKE".into(),
            );
            return Err(SqlError::Unsupported);
        }
        ready(n)?;
        return Ok(Some(
            crate::relational::like::collated(value, pattern, escape, work, |a, b| {
                compare_text(Some(n), a, b).is_eq()
            })? != negated,
        ));
    }
    let locale = input_locale.or(args[1].explicit_collation()?);
    let fold = |c: char| {
        if !insensitive {
            return c;
        }
        match locale {
            None | Some("default") => unicode::simple_case(c, false),
            Some("C" | "POSIX") => c.to_ascii_lowercase(),
            Some(_) => crate::relational::like::legacy_fold(c),
        }
    };
    Ok(Some(crate::relational::like::like(value, pattern, escape, fold, work)? != negated))
}

pub(super) fn regex(
    op: &str,
    args: &[BoundScalar],
    value: &str,
    pattern: &str,
    budget: &mut usize,
) -> R<Value> {
    let locale = comparison_locale(&args[0], &args[1])?;
    if locale.is_some_and(nondeterministic) {
        user_types::view_replace_error(
            "nondeterministic collations are not supported for regular expressions".into(),
        );
        return Err(SqlError::Unsupported);
    }
    let expression = regex::RegexBuilder::new(pattern)
        .case_insensitive(op.ends_with('*'))
        .dot_matches_new_line(true)
        .size_limit(1 << 20)
        .dfa_size_limit(1 << 20)
        .build()
        .map_err(|_| SqlError::Unsupported)?;
    let result = Value::Bool(expression.is_match(value) != op.starts_with('!'));
    charge(budget, &result)?;
    Ok(result)
}

pub(in crate::relational::query) fn comparison_binding<'a>(
    args: impl Iterator<Item = &'a BoundScalar>,
) -> R<(Option<String>, bool)> {
    let d = merge(args)?;
    Ok((d.name.map(str::to_owned), d.conflict.is_some()))
}
impl BoundScalar {
    pub(in crate::relational::query) fn collation_column(&self) -> Option<BoundColumn> {
        match &self.node {
            Node::Column(c, _) => Some(*c),
            Node::Call(n, args) if internal(n) => args[0].collation_column(),
            _ => None,
        }
    }
}

pub(in crate::relational::query) fn display_name(n:&str)->String {
    let quote=|s:&str| {
        let simple=!s.is_empty() && s.chars().enumerate().all(|(i,c)|c=='_' || c.is_ascii_lowercase() || i>0 && c.is_ascii_digit());
        if simple && s!="default" {s.to_owned()} else {format!("\"{}\"",s.replace('"',"\"\""))}
    };
    let label=n.split('\0').next().unwrap_or(n);
    if user(n).is_some() && let Some((schema,name))=label.split_once('.') {
        if schema=="public" {quote(name)} else {format!("{}.{}",quote(schema),quote(name))}
    } else {quote(label)}
}

// Preserve binding derivations when a planner substitutes a materialized value.
pub(in crate::relational::query) fn scalar(
    value: Scalar,
    metadata: Option<&BoundScalar>,
) -> R<Scalar> {
    let Some(metadata) = metadata else {
        return Ok(value);
    };
    let d = metadata.collation_derivation()?;
    if let Some((a, b)) = d.conflict {
        let carrier =
            |n: &str| Scalar::Call(format!("{IMPLICIT}{n}"), vec![Scalar::Cast(Box::new(Scalar::Literal(Value::Null)),metadata.ty.clone())]);
        return Ok(Scalar::Call(
            "coalesce".into(),
            vec![value, carrier(a), carrier(b)],
        ));
    }
    Ok(if let Some(n) = d.name {
        Scalar::Call(
            format!("{}{n}", if d.explicit { PREFIX } else { IMPLICIT }),
            vec![value],
        )
    } else {
        value
    })
}
pub(in crate::relational::query) fn combined(
    a: Option<&BoundScalar>,
    b: Option<&BoundScalar>,
    ty: DataType,
) -> R<BoundScalar> {
    BoundScalar {
        ty,
        node: Node::Literal(Value::Null),
    }
    .inherit_derivation(merge(a.into_iter().chain(b))?)
}

pub(in crate::relational::query) fn register(
    columns: &[ColumnDef],
    metadata: &[Option<BoundScalar>],
) -> R<()> {
    for (c, m) in columns.iter().zip(metadata.iter()) {
        let d = m
            .as_ref()
            .map(BoundScalar::collation_derivation)
            .transpose()?
            .unwrap_or_default();
        MATERIALIZED.with(|values| {
            values.borrow_mut().insert(
                c.id,
                Metadata {
                    name: d.name.map(str::to_owned),
                    explicit: false,
                    conflict: d.conflict.map(|(a, b)| (a.to_owned(), b.to_owned())),
                },
            )
        });
    }
    Ok(())
}
pub(in crate::relational::query) fn equality_key(
    value: Value,
    metadata: Option<&BoundScalar>,
) -> R<Value> {
    let Some(n) = metadata
        .map(BoundScalar::explicit_collation)
        .transpose()?
        .flatten()
    else {
        return Ok(value);
    };
    if user(n).is_some_and(|(_, d, _, _)| !d) {
        let c=collator(n)?;
        return crate::relational::collation_keys::equality_value(&value,Some(c.as_ref()), metadata.is_some_and(|s| s.ty == DataType::TextArray));
    }
    Ok(value)
}

/// A relation/subquery output has an implicit collation at its consumer.
pub(in crate::relational::query) fn materialized(metadata: Option<&BoundScalar>, ty: DataType) -> R<BoundScalar> {
    let mut d = merge(metadata.into_iter())?;
    d.explicit = false;
    BoundScalar { ty, node: Node::Literal(Value::Null) }.inherit_derivation(d)
}

#[cfg(test)]
mod diagnostic_tests {
    use super::*;

    #[test]
    fn array_comparator_fallback_preserves_diagnostic() {
        for saved in [None, Some("division by zero")] {
            user_types::take_sql_error_message();
            if let Some(message) = saved {
                user_types::view_replace_error(message.into());
            }
            let a = Value::Text("{a,b".into());
            let b = Value::Text("{a}".into());
            assert_eq!(compare(Some("C"), &a, &b, &DataType::TextArray), cmp(&a, &b));
            assert_eq!(user_types::take_sql_error_message().as_deref(), saved);
        }
    }
}
