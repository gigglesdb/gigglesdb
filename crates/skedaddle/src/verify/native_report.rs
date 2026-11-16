//! Version 2 native report contract. No PG LSN or PG lineage interpretation.
use crate::coordinator::verification::{self as v1, Counts, DeadLetters, RetryPolicy, Sections};
use crate::coordinator::{self, SourceBinding, SourceBindingError, SourceConnector};
use crate::event::{Position, PositionOrder, PositionWireV1};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::BTreeSet};

pub const VERSION: u32 = 2;
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Version(Option<u32>),
    Binding(SourceBindingError),
    Position(crate::event::EventError),
    PositionEncoding,
    KeyScope,
    NativeReportKeyUnsupported {
        table: String,
        column: String,
        kind: String,
    },
    Invalid(&'static str),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "native verification report: {self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
fn invalid(s: &'static str) -> Error {
    Error::Invalid(s)
}
fn checked<T>(r: coordinator::Result<T>) -> Result<T> {
    r.map_err(|e| match e {
        coordinator::Error::SourceBinding(e) => Error::Binding(e),
        _ => invalid("coverage contract"),
    })
}
fn digest(value: &impl Serialize) -> Result<String> {
    checked(v1::digest(value))
}
fn required(s: &str) -> Result<()> {
    if s.is_empty() {
        Err(invalid("missing evidence/identity"))
    } else {
        Ok(())
    }
}
fn hash(s: &str) -> Result<()> {
    if s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Ok(())
    } else {
        Err(invalid("digest"))
    }
}
fn position(wire: &PositionWireV1, binding: &SourceBinding) -> Result<Position> {
    let p = Position::try_from(wire.clone()).map_err(|_| Error::PositionEncoding)?;
    if p.order_domain() != binding.order_domain {
        return Err(Error::Binding(SourceBindingError::OrderDomain));
    }
    if p.bytes().is_empty() {
        return Err(Error::PositionEncoding);
    }
    Ok(p)
}
fn compare(
    a: &PositionWireV1,
    b: &PositionWireV1,
    binding: &SourceBinding,
    order: &dyn PositionOrder,
) -> Result<Ordering> {
    position(a, binding)?
        .compare(&position(b, binding)?, order)
        .map_err(Error::Position)
}
/// The initial MySQL subset deliberately excludes text, floating point, temporal,
/// binary and enum keys. Type and mapping identities are catalog-qualified.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum KeyType {
    Signed { bits: u8 },
    Unsigned { bits: u8 },
    Decimal { precision: u8, scale: u8 },
}
/// Convert catalog-qualified MySQL metadata without widening the v2 wire scope.
pub fn mysql_key(key: &crate::mysql::chunk_key::MysqlTableKey) -> Result<Vec<KeyColumn>> {
    key.validate().map_err(|_| Error::KeyScope)?;
    key.columns
        .iter()
        .map(|c| {
            let unsupported = || Error::NativeReportKeyUnsupported {
                table: key.event_table.clone(),
                column: c.name.clone(),
                kind: c.column_type.clone(),
            };
            let data_type = c.data_type.to_ascii_lowercase();
            let column_type = c.column_type.to_ascii_lowercase();
            let base = column_type.split(['(', ' ']).next().unwrap_or_default();
            if base != data_type || c.character_set.is_some() || c.collation.is_some() {
                return Err(unsupported());
            }
            let bits = match data_type.as_str() {
                "tinyint" => Some(8),
                "smallint" => Some(16),
                "mediumint" => Some(24),
                "int" | "integer" => Some(32),
                "bigint" => Some(64),
                _ => None,
            };
            let kind = if let Some(bits) = bits {
                if column_type.split_whitespace().any(|s| s == "unsigned") {
                    KeyType::Unsigned { bits }
                } else {
                    KeyType::Signed { bits }
                }
            } else if matches!(data_type.as_str(), "decimal" | "numeric") {
                let params = column_type
                    .strip_prefix(&format!("{base}("))
                    .and_then(|s| s.split_once(')'))
                    .map(|(s, _)| s)
                    .ok_or_else(unsupported)?;
                let (precision, scale) = params.split_once(',').ok_or_else(unsupported)?;
                KeyType::Decimal {
                    precision: precision.trim().parse().map_err(|_| unsupported())?,
                    scale: scale.trim().parse().map_err(|_| unsupported())?,
                }
            } else {
                return Err(unsupported());
            };
            let column = KeyColumn {
                name: c.name.clone(),
                mapping_identity: c.type_id.clone(),
                kind,
            };
            column.validate()?;
            Ok(column)
        })
        .collect()
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyColumn {
    pub name: String,
    pub mapping_identity: String,
    pub kind: KeyType,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "kebab-case",
    deny_unknown_fields
)]
pub enum Scalar {
    Signed(i64),
    Unsigned(u64),
    Decimal(String),
}
impl KeyColumn {
    fn validate(&self) -> Result<()> {
        required(&self.name)?;
        required(&self.mapping_identity)?;
        match self.kind {
            KeyType::Signed { bits } | KeyType::Unsigned { bits }
                if matches!(bits, 8 | 16 | 24 | 32 | 64) =>
            {
                Ok(())
            }
            KeyType::Decimal { precision, scale }
                if (1..=65).contains(&precision) && scale <= 30 && scale <= precision =>
            {
                Ok(())
            }
            _ => Err(Error::KeyScope),
        }
    }
    fn decimal<'a>(&self, value: &'a str, precision: u8, scale: u8) -> Result<(bool, &'a str)> {
        let (negative, digits) = value
            .strip_prefix('-')
            .map_or((false, value), |v| (true, v));
        // Fixed-scale unscaled integer, with one canonical spelling per value.
        if digits.is_empty()
            || !digits.bytes().all(|b| b.is_ascii_digit())
            || (digits.len() > 1 && digits.starts_with('0'))
            || (negative && digits == "0")
            || digits.len() > usize::from(precision)
            || scale > precision
        {
            return Err(Error::KeyScope);
        }
        Ok((negative, digits))
    }
    fn compare(&self, a: &Scalar, b: &Scalar) -> Result<Ordering> {
        self.validate()?;
        match (&self.kind, a, b) {
            (KeyType::Signed { bits }, Scalar::Signed(a), Scalar::Signed(b)) => {
                let valid = |v: i64| {
                    *bits == 64 || (-(1i64 << (bits - 1))..(1i64 << (bits - 1))).contains(&v)
                };
                if !valid(*a) || !valid(*b) {
                    return Err(Error::KeyScope);
                }
                Ok(a.cmp(b))
            }
            (KeyType::Unsigned { bits }, Scalar::Unsigned(a), Scalar::Unsigned(b)) => {
                if *bits != 64 && (*a >= (1u64 << bits) || *b >= (1u64 << bits)) {
                    return Err(Error::KeyScope);
                }
                Ok(a.cmp(b))
            }
            (KeyType::Decimal { precision, scale }, Scalar::Decimal(a), Scalar::Decimal(b)) => {
                let (an, a) = self.decimal(a, *precision, *scale)?;
                let (bn, b) = self.decimal(b, *precision, *scale)?;
                Ok(match an.cmp(&bn) {
                    Ordering::Equal => {
                        let cmp = a.len().cmp(&b.len()).then_with(|| a.cmp(b));
                        if an { cmp.reverse() } else { cmp }
                    }
                    o => o.reverse(),
                })
            }
            _ => Err(Error::KeyScope),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableScope {
    pub schema: String,
    pub table: String,
    pub catalog_digest: String,
    pub plan_digest: String,
    pub key: Vec<KeyColumn>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "bound",
    content = "key",
    rename_all = "kebab-case",
    deny_unknown_fields
)]
pub enum Boundary {
    NegativeInfinity,
    Key(Vec<Scalar>),
    PositiveInfinity,
}
impl TableScope {
    fn validate(&self) -> Result<()> {
        required(&self.schema)?;
        required(&self.table)?;
        hash(&self.catalog_digest)?;
        hash(&self.plan_digest)?;
        let mut names = BTreeSet::new();
        if self.key.is_empty() {
            return Err(Error::KeyScope);
        }
        for c in &self.key {
            c.validate()?;
            if !names.insert(&c.name) {
                return Err(Error::KeyScope);
            }
        }
        Ok(())
    }
    pub fn compare(&self, a: &Boundary, b: &Boundary) -> Result<Ordering> {
        for bound in [a, b] {
            if let Boundary::Key(k) = bound {
                if k.len() != self.key.len() {
                    return Err(Error::KeyScope);
                }
                for (c, v) in self.key.iter().zip(k) {
                    c.compare(v, v)?;
                }
            }
        }
        Ok(match (a, b) {
            (Boundary::NegativeInfinity, Boundary::NegativeInfinity)
            | (Boundary::PositiveInfinity, Boundary::PositiveInfinity) => Ordering::Equal,
            (Boundary::NegativeInfinity, _) | (_, Boundary::PositiveInfinity) => Ordering::Less,
            (Boundary::PositiveInfinity, _) | (_, Boundary::NegativeInfinity) => Ordering::Greater,
            (Boundary::Key(a), Boundary::Key(b)) => {
                let mut cmp = Ordering::Equal;
                for ((c, a), b) in self.key.iter().zip(a).zip(b) {
                    cmp = c.compare(a, b)?;
                    if cmp != Ordering::Equal {
                        break;
                    }
                }
                cmp
            }
        })
    }
}
pub fn range_identity(scope: &TableScope, lower: &Boundary, upper: &Boundary) -> Result<String> {
    digest(&(VERSION, scope, lower, upper))
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Interval {
    pub armed: u64,
    pub source_read: u64,
    pub destination_read: u64,
    pub complete_through: PositionWireV1,
    pub evidence: String,
}
impl Interval {
    fn validate(&self, binding: &SourceBinding) -> Result<()> {
        required(&self.evidence)?;
        position(&self.complete_through, binding)?;
        if self.armed > self.source_read || self.source_read > self.destination_read {
            return Err(invalid("unordered interval"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Exclusion {
    pub key: Vec<Scalar>,
    pub reason: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangeResult {
    pub id: String,
    pub lower: Boundary,
    pub upper: Boundary,
    pub counts: Counts,
    pub exclusions: Vec<Exclusion>,
    pub interval: Interval,
    pub applied: PositionWireV1,
    pub applied_evidence: String,
    pub source_digest: String,
    pub destination_digest: String,
    pub retry_attempts: u32,
    pub exhausted_busy: u64,
    pub outstanding_keys: Vec<Vec<Scalar>>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableResult {
    pub scope: TableScope,
    pub counts: Counts,
    pub ranges: Vec<RangeResult>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Report {
    pub version: u32,
    pub coverage_version: u32,
    pub binding: SourceBinding,
    pub migration: String,
    pub definition_digest: String,
    pub run: String,
    pub epoch: u64,
    pub generation: u64,
    pub completed: bool,
    pub interval: Interval,
    pub tables: Vec<TableResult>,
    pub retry: RetryPolicy,
    pub dead_letters: DeadLetters,
    pub sections: Sections,
    pub coverage_digest: String,
}
impl Report {
    pub fn computed_coverage_digest(&self) -> Result<String> {
        digest(&(self.coverage_version, &self.binding, &self.tables))
    }
    pub fn validate(&self, binding: &SourceBinding, order: &dyn PositionOrder) -> Result<()> {
        if self.version != VERSION {
            return Err(Error::Version(Some(self.version)));
        }
        if self.coverage_version != VERSION || self.retry.version != VERSION {
            return Err(Error::Version(Some(if self.coverage_version != VERSION {
                self.coverage_version
            } else {
                self.retry.version
            })));
        }
        checked(binding.require_same(&self.binding))?;
        if binding.connector != SourceConnector::Mysql {
            return Err(Error::KeyScope);
        }
        if order.domain() != binding.order_domain {
            return Err(Error::Binding(SourceBindingError::OrderDomain));
        }
        if !self.completed || self.epoch == 0 || self.tables.is_empty() {
            return Err(invalid("incomplete scope/run"));
        }
        required(&self.migration)?;
        required(&self.run)?;
        required(&self.retry.policy)?;
        hash(&self.definition_digest)?;
        self.interval.validate(binding)?;
        compare(
            &self.interval.complete_through,
            &self.interval.complete_through,
            binding,
            order,
        )?;
        let mut tables = BTreeSet::new();
        let mut ranges = BTreeSet::new();
        let mut outstanding = BTreeSet::new();
        for table in &self.tables {
            table.scope.validate()?;
            checked(table.counts.validate())?;
            if !tables.insert((&table.scope.schema, &table.scope.table)) || table.ranges.is_empty()
            {
                return Err(invalid("duplicate/empty table"));
            }
            if table.ranges.first().map(|r| &r.lower) != Some(&Boundary::NegativeInfinity)
                || table.ranges.last().map(|r| &r.upper) != Some(&Boundary::PositiveInfinity)
            {
                return Err(invalid("incomplete keyspace"));
            }
            let mut counts = Counts::default();
            for (index, range) in table.ranges.iter().enumerate() {
                if range.id != range_identity(&table.scope, &range.lower, &range.upper)?
                    || !ranges.insert(&range.id)
                {
                    return Err(invalid("range identity"));
                }
                if table.scope.compare(&range.lower, &range.upper)? != Ordering::Less
                    || (index > 0 && table.ranges[index - 1].upper != range.lower)
                {
                    return Err(invalid("gap/overlap/reversed range"));
                }
                checked(range.counts.validate())?;
                checked(counts.add(&range.counts))?;
                range.interval.validate(binding)?;
                if range.interval.armed < self.interval.armed
                    || range.interval.destination_read > self.interval.destination_read
                    || compare(
                        &range.interval.complete_through,
                        &self.interval.complete_through,
                        binding,
                        order,
                    )? == Ordering::Less
                    || compare(
                        &range.applied,
                        &range.interval.complete_through,
                        binding,
                        order,
                    )? == Ordering::Less
                {
                    return Err(invalid("range/applied interval"));
                }
                required(&range.applied_evidence)?;
                hash(&range.source_digest)?;
                hash(&range.destination_digest)?;
                if range.counts.mismatched == 0
                    && range.counts.busy == 0
                    && range.source_digest != range.destination_digest
                {
                    return Err(invalid("unreported mismatch"));
                }
                if range.retry_attempts > self.retry.budget
                    || range.exhausted_busy != range.counts.busy
                    || (range.exhausted_busy > 0 && range.retry_attempts != self.retry.budget)
                    || range.outstanding_keys.len() as u64 != range.counts.busy
                    || range.exclusions.len() as u64 != range.counts.excluded
                {
                    return Err(invalid("retry/exclusion counts"));
                }
                let mut keys = BTreeSet::new();
                for key in range
                    .outstanding_keys
                    .iter()
                    .chain(range.exclusions.iter().map(|e| &e.key))
                {
                    let bound = Boundary::Key(key.clone());
                    if table.scope.compare(&range.lower, &bound)? == Ordering::Greater
                        || table.scope.compare(&bound, &range.upper)? != Ordering::Less
                        || !keys.insert(digest(key)?)
                    {
                        return Err(invalid("outstanding/excluded key"));
                    }
                }
                for e in &range.exclusions {
                    required(&e.reason)?;
                }
                if range.counts.busy > 0 {
                    outstanding.insert(range.id.clone());
                }
            }
            if counts != table.counts {
                return Err(invalid("table/range counts"));
            }
        }
        if ranges.len() > v1::MAX_RANGES
            || self
                .retry
                .outstanding_ranges
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
                != outstanding
            || self.retry.outstanding_ranges.len() != outstanding.len()
        {
            return Err(invalid("outstanding range manifest/budget"));
        }
        let dead = &self.dead_letters;
        required(&dead.collection)?;
        required(&dead.evidence)?;
        if !dead.completed
            || dead.count != dead.identities.len() as u64
            || dead.identities.iter().collect::<BTreeSet<_>>().len() != dead.identities.len()
        {
            return Err(invalid("dead-letter completion/count"));
        }
        for id in &dead.identities {
            required(id)?;
        }
        for section in self.sections.all() {
            checked(section.validate())?;
        }
        if self.coverage_digest != self.computed_coverage_digest()? {
            return Err(invalid("coverage digest mismatch"));
        }
        if serde_json::to_vec(self)
            .map_err(|_| invalid("report encoding"))?
            .len()
            > v1::REPORT_BYTES
        {
            return Err(invalid("report size cap"));
        }
        Ok(())
    }
    /// Decode only the explicitly requested v2 format; never infer a legacy shape.
    pub fn decode(
        bytes: &[u8],
        binding: &SourceBinding,
        order: &dyn PositionOrder,
    ) -> Result<Self> {
        if bytes.len() > v1::REPORT_BYTES {
            return Err(invalid("report size cap"));
        }
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|_| invalid("report JSON"))?;
        let version = value
            .get("version")
            .and_then(|v| v.as_u64())
            .and_then(|v| u32::try_from(v).ok());
        if version != Some(VERSION) {
            return Err(Error::Version(version));
        }
        let report: Self =
            serde_json::from_slice(bytes).map_err(|_| invalid("report shape/key scope"))?;
        report.validate(binding, order)?;
        Ok(report)
    }
}

/// Exact report identity: metadata is authenticated by the report digest, never
/// sufficient by itself to recreate verification readiness.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportRef {
    pub version: u32,
    pub binding: SourceBinding,
    pub run: String,
    pub digest: String,
    pub bytes: usize,
    pub through: PositionWireV1,
    pub coverage_digest: String,
    pub migration: String,
    pub definition_digest: String,
    pub epoch: u64,
    pub generation: u64,
    pub scope: Vec<TableScope>,
}
impl Report {
    pub fn reference(
        &self,
        binding: &SourceBinding,
        order: &dyn PositionOrder,
    ) -> Result<ReportRef> {
        self.validate(binding, order)?;
        let bytes = serde_json::to_vec(self).map_err(|_| invalid("report encoding"))?;
        use sha2::{Digest, Sha256};
        Ok(ReportRef {
            version: VERSION,
            binding: self.binding.clone(),
            run: self.run.clone(),
            digest: format!("{:x}", Sha256::digest(&bytes)),
            bytes: bytes.len(),
            through: self.interval.complete_through.clone(),
            coverage_digest: self.coverage_digest.clone(),
            migration: self.migration.clone(),
            definition_digest: self.definition_digest.clone(),
            epoch: self.epoch,
            generation: self.generation,
            scope: self.tables.iter().map(|t| t.scope.clone()).collect(),
        })
    }
}
impl ReportRef {
    /// An absent or corrupt object cannot be replaced by retained summary data.
    pub fn authenticate(
        &self,
        bytes: Option<&[u8]>,
        binding: &SourceBinding,
        order: &dyn PositionOrder,
    ) -> Result<Report> {
        if self.version != VERSION {
            return Err(Error::Version(Some(self.version)));
        }
        checked(binding.require_same(&self.binding))?;
        required(&self.run)?;
        hash(&self.digest)?;
        hash(&self.coverage_digest)?;
        position(&self.through, binding)?;
        let bytes = bytes.ok_or_else(|| invalid("missing durable report"))?;
        if self.bytes == 0 || self.bytes != bytes.len() || self.bytes > v1::REPORT_BYTES {
            return Err(invalid("report reference size"));
        }
        use sha2::{Digest, Sha256};
        if format!("{:x}", Sha256::digest(bytes)) != self.digest {
            return Err(invalid("report digest differs"));
        }
        let report = Report::decode(bytes, binding, order)?;
        if report.reference(binding, order)? != *self {
            return Err(invalid("report digest/reference differs"));
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct DecimalOrder;
    impl PositionOrder for DecimalOrder {
        fn domain(&self) -> &str {
            "native:test"
        }
        fn compare(
            &self,
            a: &[u8],
            b: &[u8],
        ) -> std::result::Result<Ordering, crate::event::EventError> {
            let parse = |v: &[u8]| {
                std::str::from_utf8(v)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .ok_or(crate::event::EventError::PositionDomainMismatch)
            };
            Ok(parse(a)?.cmp(&parse(b)?))
        }
    }
    fn pos(s: &str) -> PositionWireV1 {
        (&Position::new("native:test", s.as_bytes().to_vec())).into()
    }
    fn fixture() -> Report {
        let scope = TableScope {
            schema: "s".into(),
            table: "t".into(),
            catalog_digest: "a".repeat(64),
            plan_digest: "b".repeat(64),
            key: vec![KeyColumn {
                name: "id".into(),
                mapping_identity: "mysql:bigint unsigned->numeric(20,0)".into(),
                kind: KeyType::Unsigned { bits: 64 },
            }],
        };
        let counts = Counts {
            total: 1,
            verified: 1,
            ..Default::default()
        };
        let interval = Interval {
            armed: 1,
            source_read: 2,
            destination_read: 3,
            complete_through: pos("9"),
            evidence: "armed complete evidence".into(),
        };
        let lower = Boundary::NegativeInfinity;
        let upper = Boundary::PositiveInfinity;
        let range = RangeResult {
            id: range_identity(&scope, &lower, &upper).unwrap(),
            lower,
            upper,
            counts: counts.clone(),
            exclusions: vec![],
            interval: Interval {
                complete_through: pos("10"),
                ..interval.clone()
            },
            applied: pos("11"),
            applied_evidence: "authenticated source checkpoint".into(),
            source_digest: "c".repeat(64),
            destination_digest: "c".repeat(64),
            retry_attempts: 0,
            exhausted_busy: 0,
            outstanding_keys: vec![],
        };
        let section = || v1::Section {
            completed: true,
            checked: 1,
            findings: vec![],
            evidence: "collected evidence".into(),
        };
        let mut report = Report {
            version: VERSION,
            coverage_version: VERSION,
            binding: SourceBinding {
                version: 1,
                connector: SourceConnector::Mysql,
                order_domain: "native:test".into(),
                stream: "stream".into(),
            },
            migration: "m".into(),
            definition_digest: "d".repeat(64),
            run: "r".into(),
            epoch: 1,
            generation: 0,
            completed: true,
            interval,
            tables: vec![TableResult {
                scope,
                counts,
                ranges: vec![range],
            }],
            retry: RetryPolicy {
                version: VERSION,
                policy: "bounded".into(),
                budget: 2,
                outstanding_ranges: vec![],
            },
            dead_letters: DeadLetters {
                collection: "dead".into(),
                completed: true,
                count: 0,
                identities: vec![],
                evidence: "complete scan".into(),
            },
            sections: Sections {
                schema: section(),
                sequences: section(),
                constraints: section(),
                indexes: section(),
                destination_foreign_keys: section(),
            },
            coverage_digest: String::new(),
        };
        report.coverage_digest = report.computed_coverage_digest().unwrap();
        report
    }
    #[test]
    fn native_report_roundtrip_uses_connector_order_and_no_lsn_fields() {
        let r = fixture();
        r.validate(&r.binding, &DecimalOrder).unwrap();
        let bytes = serde_json::to_vec(&r).unwrap();
        let text = String::from_utf8(bytes.clone()).unwrap();
        assert!(!text.contains("lsn"));
        assert!(!text.contains("lineage"));
        assert_eq!(
            Report::decode(&bytes, &r.binding, &DecimalOrder).unwrap(),
            r
        );
        let reference = r.reference(&r.binding, &DecimalOrder).unwrap();
        assert_eq!(
            reference
                .authenticate(Some(&bytes), &r.binding, &DecimalOrder)
                .unwrap(),
            r
        );
        assert!(
            reference
                .authenticate(None, &r.binding, &DecimalOrder)
                .is_err()
        );
        assert!(
            reference
                .authenticate(Some(b"{}"), &r.binding, &DecimalOrder)
                .is_err()
        );
        let reordered =
            serde_json::to_vec(&serde_json::from_slice::<serde_json::Value>(&bytes).unwrap())
                .unwrap();
        assert_eq!(reordered.len(), bytes.len());
        assert_ne!(reordered, bytes);
        assert!(
            reference
                .authenticate(Some(&reordered), &r.binding, &DecimalOrder)
                .is_err()
        );
        let mut bad = bytes;
        bad.push(b' ');
        assert!(
            reference
                .authenticate(Some(&bad), &r.binding, &DecimalOrder)
                .is_err()
        );
    }
    #[test]
    fn explicit_versions_connector_stream_domain_and_progress_fail_closed() {
        let r = fixture();
        for version in [None, Some(1), Some(3)] {
            let mut value = serde_json::to_value(&r).unwrap();
            if let Some(v) = version {
                value["version"] = v.into();
            } else {
                value.as_object_mut().unwrap().remove("version");
            }
            assert_eq!(
                Report::decode(
                    &serde_json::to_vec(&value).unwrap(),
                    &r.binding,
                    &DecimalOrder
                )
                .unwrap_err(),
                Error::Version(version)
            );
        }
        let mut binding = r.binding.clone();
        binding.connector = SourceConnector::Spanner;
        assert_eq!(
            r.validate(&binding, &DecimalOrder),
            Err(Error::Binding(SourceBindingError::Connector))
        );
        binding = r.binding.clone();
        binding.stream = "other".into();
        assert_eq!(
            r.validate(&binding, &DecimalOrder),
            Err(Error::Binding(SourceBindingError::Stream))
        );
        binding = r.binding.clone();
        binding.order_domain = "other".into();
        assert_eq!(
            r.validate(&binding, &DecimalOrder),
            Err(Error::Binding(SourceBindingError::OrderDomain))
        );
        let mut bad = r.clone();
        bad.tables[0].ranges[0].applied = pos("9");
        assert!(bad.validate(&r.binding, &DecimalOrder).is_err());
        bad = r.clone();
        bad.tables[0].ranges[0].interval.complete_through = pos("8");
        assert!(bad.validate(&r.binding, &DecimalOrder).is_err());
        bad = r.clone();
        bad.interval.complete_through = (&Position::new("wrong", vec![0; 8])).into();
        assert_eq!(
            bad.validate(&r.binding, &DecimalOrder),
            Err(Error::Binding(SourceBindingError::OrderDomain))
        );
    }
    #[test]
    fn restricted_numeric_keys_are_exact_and_non_numeric_scope_is_rejected() {
        let signed = KeyColumn {
            name: "s".into(),
            mapping_identity: "signed".into(),
            kind: KeyType::Signed { bits: 24 },
        };
        assert_eq!(
            signed
                .compare(&Scalar::Signed(-8388608), &Scalar::Signed(8388607))
                .unwrap(),
            Ordering::Less
        );
        assert_eq!(
            signed.compare(&Scalar::Signed(8388608), &Scalar::Signed(0)),
            Err(Error::KeyScope)
        );
        let unsigned = KeyColumn {
            kind: KeyType::Unsigned { bits: 64 },
            ..signed.clone()
        };
        assert_eq!(
            unsigned
                .compare(
                    &Scalar::Unsigned(u64::MAX),
                    &Scalar::Unsigned(i64::MAX as u64)
                )
                .unwrap(),
            Ordering::Greater
        );
        let decimal = KeyColumn {
            kind: KeyType::Decimal {
                precision: 65,
                scale: 30,
            },
            ..signed
        };
        let d = |v: &str| Scalar::Decimal(v.into());
        assert_eq!(
            decimal.compare(&d("-10"), &d("-9")).unwrap(),
            Ordering::Less
        );
        assert_eq!(decimal.compare(&d("9"), &d("10")).unwrap(), Ordering::Less);
        for bad in ["01", "-0", "+1", "1.0", "", "1e2"] {
            assert_eq!(decimal.compare(&d(bad), &d("0")), Err(Error::KeyScope));
        }
        let mut value = serde_json::to_value(fixture()).unwrap();
        value["tables"][0]["scope"]["key"][0]["kind"] =
            serde_json::json!({"text":{"collation":"guessed"}});
        assert!(
            Report::decode(
                &serde_json::to_vec(&value).unwrap(),
                &fixture().binding,
                &DecimalOrder
            )
            .is_err()
        );
    }
    #[test]
    fn partitions_composite_ties_and_counts_remain_exact() {
        let mut r = fixture();
        let scope = &mut r.tables[0].scope;
        scope.key.push(KeyColumn {
            name: "second".into(),
            mapping_identity: "signed".into(),
            kind: KeyType::Signed { bits: 8 },
        });
        let k = |u, s| Boundary::Key(vec![Scalar::Unsigned(u), Scalar::Signed(s)]);
        assert_eq!(scope.compare(&k(1, -1), &k(1, 1)).unwrap(), Ordering::Less);
        assert_eq!(scope.compare(&k(1, 1), &k(2, -1)).unwrap(), Ordering::Less);
        assert_eq!(
            scope.compare(&Boundary::Key(vec![Scalar::Unsigned(1)]), &k(1, 0)),
            Err(Error::KeyScope)
        );
        r = fixture();
        r.tables[0].ranges[0].counts.verified = 0;
        assert!(r.validate(&r.binding, &DecimalOrder).is_err());
        r = fixture();
        r.tables[0].ranges[0].lower = Boundary::Key(vec![Scalar::Unsigned(0)]);
        assert!(r.validate(&r.binding, &DecimalOrder).is_err());
    }
}

#[cfg(test)]
mod mysql_key_tests {
    use super::*;
    use crate::mysql::chunk_key::*;
    #[test]
    fn qualified_numeric_metadata_preserves_width_unsigned_scale_and_mapping() {
        for (data_type, column_type, expected) in [
            ("tinyint", "tinyint", KeyType::Signed { bits: 8 }),
            (
                "mediumint",
                "mediumint unsigned",
                KeyType::Unsigned { bits: 24 },
            ),
            ("bigint", "bigint unsigned", KeyType::Unsigned { bits: 64 }),
            (
                "decimal",
                "decimal(65,30)",
                KeyType::Decimal {
                    precision: 65,
                    scale: 30,
                },
            ),
        ] {
            let key = MysqlTableKey {
                event_table: "s.t".into(),
                schema: "s".into(),
                table: "t".into(),
                columns: vec![MysqlKeyColumn {
                    name: "id".into(),
                    data_type: data_type.into(),
                    column_type: column_type.into(),
                    character_set: None,
                    collation: None,
                    type_id: "qualified-mapping".into(),
                    direction: MysqlKeyDirection::Asc,
                }],
            };
            let converted = mysql_key(&key).unwrap();
            assert_eq!(converted[0].kind, expected);
            assert_eq!(converted[0].mapping_identity, "qualified-mapping");
        }
    }
}
