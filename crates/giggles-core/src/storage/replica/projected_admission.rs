//! Restricted projected-source admission. No runtime integration is released.
//! Bookkeeping is stack-only; the original serde visitors construct the values.
//! Allocation units are cumulative requested layouts, not resident memory.
//!
//! Qualification pin: rustc 1.99.1 (58cf8f9c3), x86_64-pc-windows-msvc,
//! serde/core 1.0.229 and bincode 1.3.3. Requalify on dependency/toolchain
//! changes. `layouts` refuses other targets or changed actual type layouts.
//! No independent wire schema is declared: struct names/field lists are context
//! guards, and the actual derived visitors and seeds still perform construction.
//!
//! Restricted source: exactly namespaces 0,5, metadata format3, relational
//! format1, one table/row map, 1..8 columns and PK offsets, <=64 row occurrences,
//! <=256-byte value leaves, <=63-byte names, <=4120-byte row keys. The raw ns5
//! copy is <=403402 bytes and the whole input <=403505. Duplicate occurrences
//! consume quota even when the original BTreeMap visitor replaces an entry.
//!
//! Before any construction, `pipeline` reserves all four phases plus failure
//! handling. T=2*63+8*63+40*8+8*8=1014; H=263680+40*64*8+131072=415232.
//! Outer=2*648+2+61+403402. SQL=1264+472+T+736*64+H. PK validation clones
//! <=64*(40*8+2048+G(1,4120)); validation sets<=208*(1+8+64+8+8).
//! Records/reencode=Outer+403505. Total=2904081 including Failure1184.
//! These bounds include simultaneous/failed/duplicate construction requests;
//! nothing is refunded. G(s,n)=4*s*max(n,min_nonzero(s)) bounds ALL geometric
//! allocation/reallocation requests, not only final capacity. Actual Vec serde
//! cautious capacities equal the gated counts (all backing buffers<1MiB), so
//! no push growth is needed. Borrowed String callbacks allocate exactly len.
//! The skipped default Sidecar consists of empty maps/vectors and allocates none.
//!
//! `post_work` separately prepays validation, TWO size/emit serialization walks,
//! copy/equality and possible partial-drop work. Map visitor work prepays linear
//! node search, shifts/splits and drop walks via n*11*h(n)*(1+keybytes) and
//! n*2*NODE*h(n)+n*NODE, h(n)=1+floor(log2(n+1)). Recursive entry/access/callback
//! events are additionally gated at runtime. Every underlying UTF8 scan prepays
//! the full checked source slice, including invalid/overlength strings. These
//! are logical quotas, not a CPU, allocator-internal or resident-RSS bound.
//!
//! Existing storage validate can format only a fixed SqlError in this profile;
//! its <64-byte diagnostic is covered by the failure allowance. All authority,
//! application/native ledgers and index branches are unreachable. Empty index
//! sidecar also excludes the validator's sole ambient cancellation branch.
//! Existing records/encode cannot return a serializer/format/size error after
//! these gates: fixed known fields, finite collections, primitive variants,
//! no indexes, output<=403505<MAX_BYTES. Their general formatting error paths
//! therefore never receive untrusted data here. OOM and cancellation callback
//! internals are outside the requested-layout model. No general apply proof.
//! The consuming certificate method MOVES its already validated State once;
//! there is no plain decoder call, second validation, refund or publication.
use super::{CoreError, Image, Metadata, State, empty, encode, records, validate};
use crate::relational::{self, ColumnDef, SqlRow, TableDef, Value};
use bincode::Options;
use serde::{
    Deserialize, Deserializer,
    de::{self, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor},
};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    fmt,
};

const INPUT: usize = 403505;
const SQL: usize = 403402;
const DEPTH: usize = 16;
// At most two serde/bincode error bridges per depth plus two terminal bridges.
// Each bridge uses empty custom text: one ErrorKind box<=32, no string payload.
// EOF may additionally allocate<=96. Validator diagnostics<=G(1,64)=256 are
// an alternative terminal path, covered by this larger reserve.
const FAILURE_BYTES: usize = 96 + (2 * DEPTH + 2) * 32;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Failure {
    Invalid,
    Unsupported,
    Limit,
    Cancelled,
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Invalid => "invalid",
            Self::Unsupported => "unsupported",
            Self::Limit => "limit",
            Self::Cancelled => "cancelled",
        })
    }
}
impl std::error::Error for Failure {}
impl de::Error for Failure {
    fn custom<T: fmt::Display>(_: T) -> Self {
        Self::Invalid
    }
}
impl From<Failure> for CoreError {
    fn from(e: Failure) -> Self {
        match e {
            Failure::Limit => Self::Limit,
            Failure::Cancelled => Self::Cancelled,
            _ => Self::Unsupported,
        }
    }
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
struct Work {
    events: usize,
    bytes: usize,
    comparisons: usize,
    moves: usize,
    values: usize,
}
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
struct SourceCosts {
    allocations: usize,
    phases: [usize; 4],
    work: Work,
    declared: usize,
    visited: usize,
    max_depth: usize,
    payloads: usize,
}
/// Limits may only tighten the pinned restricted profile.
pub(super) struct SourceAdmissionLimits {
    allocation_bytes: usize,
    phases: [usize; 4],
    work: Work,
    declared: usize,
    visited: usize,
}
impl Default for SourceAdmissionLimits {
    fn default() -> Self {
        Self {
            allocation_bytes: 2904081,
            phases: [404761, 465086, 1224784, 808266],
            work: Work {
                events: 8_000_000,
                bytes: 2_000_000_000,
                comparisons: 50_000_000,
                moves: 50_000_000,
                values: 100_000,
            },
            declared: 1_000_000,
            visited: 1_000_000,
        }
    }
}
/// Monotone across success AND failure; no reset/refund API.
pub(super) struct SourceBudget {
    limits: SourceAdmissionLimits,
    used: SourceCosts,
}
impl SourceBudget {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Source admission budget construction awaits the separately released replica consumer."
        )
    )]
    pub(super) fn new(limits: SourceAdmissionLimits) -> Self {
        Self {
            limits,
            used: SourceCosts::default(),
        }
    }
}
fn add(a: usize, b: usize) -> Result<usize, Failure> {
    a.checked_add(b).ok_or(Failure::Limit)
}
fn mul(a: usize, b: usize) -> Result<usize, Failure> {
    a.checked_mul(b).ok_or(Failure::Limit)
}
fn charge(slot: &mut usize, n: usize, max: usize) -> Result<(), Failure> {
    let next = add(*slot, n)?;
    if next > max {
        return Err(Failure::Limit);
    }
    *slot = next;
    Ok(())
}
impl SourceBudget {
    fn work(&mut self, w: Work) -> Result<(), Failure> {
        // A refusal retains charges already made, including earlier components.
        charge(
            &mut self.used.work.events,
            w.events,
            self.limits.work.events,
        )?;
        charge(&mut self.used.work.bytes, w.bytes, self.limits.work.bytes)?;
        charge(
            &mut self.used.work.comparisons,
            w.comparisons,
            self.limits.work.comparisons,
        )?;
        charge(&mut self.used.work.moves, w.moves, self.limits.work.moves)?;
        charge(
            &mut self.used.work.values,
            w.values,
            self.limits.work.values,
        )
    }
    fn reserve(&mut self, phases: [usize; 4]) -> Result<(), Failure> {
        let total = phases.into_iter().try_fold(FAILURE_BYTES, add)?;
        charge(
            &mut self.used.allocations,
            total,
            self.limits.allocation_bytes,
        )?;
        for (i, n) in phases.into_iter().enumerate() {
            charge(&mut self.used.phases[i], n, self.limits.phases[i])?;
        }
        Ok(())
    }
}
fn node(k: usize, v: usize) -> Result<usize, Failure> {
    add(120, mul(11, add(k, v)?)?)
}
fn growth(s: usize, n: usize) -> Result<usize, Failure> {
    if n == 0 {
        return Ok(0);
    }
    mul(
        mul(4, s)?,
        n.max(if s == 1 {
            8
        } else if s <= 1024 {
            4
        } else {
            1
        }),
    )
}
/// Rust 1.99.1, Windows x64: B=6,CAPACITY=11; header<=24, edges<=96.
/// Insertion-only tree nodes<=entry occurrences (duplicates included). Each
/// key resides in one nonempty node. No bulk/clone trees occur in admission.
fn pipeline() -> Result<[usize; 4], Failure> {
    let c = 8;
    let p = 8;
    let rows = 64;
    let t = add(add(2 * 63, 8 * 63)?, add(mul(40, c)?, mul(8, p)?)?)?;
    let h = add(263680, add(mul(40, rows * c)?, 131072)?)?;
    let outer = add(2 * node(24, 24)? + 2, 61 + SQL)?;
    let sql = add(
        node(24, 80)? + node(8, 24)?,
        add(t, add(mul(node(24, 32)?, rows)?, h)?)?,
    )?;
    let pk = mul(rows, add(mul(40, p)?, add(8 * 256, growth(1, 4120)?)?)?)?;
    let validation = add(mul(node(8, 0)?, 1 + c + rows + c + p)?, pk)?;
    let output = add(outer, INPUT)?;
    Ok([outer, sql, validation, output])
}
fn tree_work(n: usize, node: usize, key: usize) -> Result<Work, Failure> {
    let h = 1 + (usize::BITS - 1 - (n + 1).leading_zeros()) as usize;
    Ok(Work {
        comparisons: mul(mul(mul(n, 11)?, h)?, add(1, key)?)?,
        moves: add(mul(mul(mul(n, 2)?, node)?, h)?, mul(n, node)?)?,
        ..Work::default()
    })
}
fn post_work() -> Result<Work, Failure> {
    let mut w = Work {
        events: 2 * (7 + SQL + 61)
            + 2 * (20 + 5 * 8 + 8 + 64 * (4 + 4120 + 8 * (2 + 256)))
            + 2 * (SQL + 61)
            + 4096,
        bytes: SQL + INPUT + INPUT + 64 * (2048 + 2 * 4120 + growth(1, 4120)?) + 63 + 504,
        values: 64 * (2 * 8 + 3 * 8) + 2 * (2 * 8 + 64 * 8) + 2 * 512 + 8 + 2,
        moves: 64 * 40 * 8,
        ..Work::default()
    };
    for (n, node, key) in [(9, 208, 1), (64, 208, 1), (8, 208, 63), (8, 208, 1)] {
        let t = tree_work(n, node, key)?;
        w.comparisons = add(w.comparisons, t.comparisons)?;
        w.moves = add(w.moves, t.moves)?;
    }
    Ok(w)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Outer,
    OuterKey(u8),
    Namespace(u8),
    Raw(u8),
    Metadata,
    Format,
    U128,
    U64,
    U8,
    Bool,
    Int,
    Origin,
    Relational,
    SqlFormat,
    Tables,
    Table,
    Columns,
    Column,
    Name,
    Types,
    Primary,
    Rows,
    RowMap,
    RowKey,
    Row,
    Values,
    Value,
    Text,
    Bytes,
    Tag(u8),
    #[cfg(test)]
    Probe,
}
struct Context<'a> {
    budget: RefCell<&'a mut SourceBudget>,
    cancel: RefCell<&'a mut dyn FnMut() -> bool>,
    fault: Cell<Option<Failure>>,
    stack: RefCell<[Role; DEPTH]>,
    columns: Cell<usize>,
    input: Cell<usize>,
    allocated: Cell<usize>,
}
impl Context<'_> {
    fn tick(&self) -> Result<(), Failure> {
        if let Some(f) = self.fault.get() {
            return Err(f);
        }
        if (self.cancel.borrow_mut())() {
            return Err(Failure::Cancelled);
        }
        self.budget.borrow_mut().work(Work {
            events: 1,
            ..Work::default()
        })
    }
    fn enter(&self, role: Role, depth: usize) -> Result<(), Failure> {
        self.tick()?;
        if depth >= DEPTH {
            return Err(Failure::Limit);
        }
        self.stack.borrow_mut()[depth] = role;
        let mut b = self.budget.borrow_mut();
        b.used.max_depth = b.used.max_depth.max(depth + 1);
        Ok(())
    }
    fn bridge<E: de::Error>(&self, f: Failure) -> E {
        self.fault.set(Some(f));
        E::custom("")
    }
    fn result<T, E>(&self, r: Result<T, E>) -> Result<T, Failure> {
        r.map_err(|_| self.fault.get().unwrap_or(Failure::Invalid))
    }
    fn bytes(&self, n: usize) -> Result<(), Failure> {
        self.budget.borrow_mut().work(Work {
            bytes: n,
            ..Work::default()
        })
    }
    fn alloc(&self, n: usize) -> Result<(), Failure> {
        let next = add(self.allocated.get(), n)?;
        let p = pipeline()?;
        let cap = add(p[0], p[1])?;
        if next > cap {
            return Err(Failure::Limit);
        }
        self.allocated.set(next);
        Ok(())
    }
    fn declared(&self, n: usize) -> Result<(), Failure> {
        let mut b = self.budget.borrow_mut();
        let cap = b.limits.declared;
        charge(&mut b.used.declared, n, cap)
    }
    fn visited(&self) -> Result<(), Failure> {
        self.tick()?;
        let mut b = self.budget.borrow_mut();
        let cap = b.limits.visited;
        charge(&mut b.used.visited, 1, cap)
    }
}
struct Gated<'a, 'b, D> {
    inner: D,
    cx: &'a Context<'b>,
    role: Role,
    depth: usize,
}
struct Seed<'a, 'b, S> {
    inner: S,
    cx: &'a Context<'b>,
    role: Role,
    depth: usize,
}
impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for Seed<'_, '_, S> {
    type Value = S::Value;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        self.cx
            .enter(self.role, self.depth)
            .map_err(|f| self.cx.bridge(f))?;
        self.inner
            .deserialize(Gated {
                inner: d,
                cx: self.cx,
                role: self.role,
                depth: self.depth,
            })
            .map_err(|f| self.cx.bridge(f))
    }
}
struct Wrapped<'a, 'b, V> {
    inner: V,
    cx: &'a Context<'b>,
    role: Role,
    depth: usize,
    arity: Option<usize>,
}
impl<'a, 'b, D> Gated<'a, 'b, D> {
    fn wrap<V>(&self, inner: V, arity: Option<usize>) -> Wrapped<'a, 'b, V> {
        Wrapped {
            inner,
            cx: self.cx,
            role: self.role,
            depth: self.depth,
            arity,
        }
    }
}
fn is_probe(role: Role) -> bool {
    #[cfg(test)]
    {
        if role == Role::Probe {
            return true;
        }
    }
    let _ = role;
    false
}
macro_rules! primitive {
    ($method:ident,$visit:ident,$ty:ty,$($role:pat_param)|+) => {
        fn $method<V:Visitor<'de>>(self,v:V)->Result<V::Value,Failure>{
            self.cx.tick()?;if !matches!(self.role,$($role)|+) && !is_probe(self.role){return Err(Failure::Unsupported)}
            let w=self.wrap(v,None);self.cx.result(self.inner.$method(w))
        }
    }
}
macro_rules! denied {($($name:ident),*)=>{$(fn $name<V:Visitor<'de>>(self,_:V)->Result<V::Value,Failure>{self.cx.tick()?;Err(Failure::Unsupported)})*}}
impl<'de, D: Deserializer<'de>> Deserializer<'de> for Gated<'_, '_, D> {
    type Error = Failure;
    primitive!(deserialize_bool, visit_bool, bool, Role::Bool);
    primitive!(deserialize_u8, visit_u8, u8, Role::U8 | Role::Namespace(_));
    primitive!(
        deserialize_u32,
        visit_u32,
        u32,
        Role::Format | Role::SqlFormat | Role::Tag(_)
    );
    primitive!(deserialize_u64, visit_u64, u64, Role::U64 | Role::Tag(_));
    primitive!(deserialize_u128, visit_u128, u128, Role::U128);
    primitive!(deserialize_i64, visit_i64, i64, Role::Int);
    denied!(
        deserialize_i8,
        deserialize_i16,
        deserialize_i32,
        deserialize_i128,
        deserialize_u16,
        deserialize_f32,
        deserialize_f64,
        deserialize_char,
        deserialize_unit,
        deserialize_ignored_any
    );
    fn deserialize_any<V: Visitor<'de>>(self, v: V) -> Result<V::Value, Failure> {
        self.cx.tick()?;
        if !matches!(self.role, Role::Tag(_)) {
            return Err(Failure::Unsupported);
        }
        let w = self.wrap(v, None);
        self.cx.result(self.inner.deserialize_any(w))
    }
    fn deserialize_identifier<V: Visitor<'de>>(self, v: V) -> Result<V::Value, Failure> {
        if !matches!(self.role, Role::Tag(_)) {
            return Err(Failure::Unsupported);
        }
        self.deserialize_any(v)
    }
    fn deserialize_str<V: Visitor<'de>>(self, v: V) -> Result<V::Value, Failure> {
        self.cx.tick()?;
        if !matches!(self.role, Role::Name | Role::Text) && !is_probe(self.role) {
            return Err(Failure::Unsupported);
        }
        // Reserve the ENTIRE checked input before bincode scans UTF8, even on error.
        self.cx.bytes(self.cx.input.get())?;
        let w = self.wrap(v, None);
        self.cx.result(self.inner.deserialize_str(w))
    }
    fn deserialize_string<V: Visitor<'de>>(self, v: V) -> Result<V::Value, Failure> {
        self.deserialize_str(v)
    }
    fn deserialize_bytes<V: Visitor<'de>>(self, v: V) -> Result<V::Value, Failure> {
        self.cx.tick()?;
        if !matches!(self.role, Role::Bytes) && !is_probe(self.role) {
            return Err(Failure::Unsupported);
        }
        self.cx.bytes(self.cx.input.get())?;
        let w = self.wrap(v, None);
        self.cx.result(self.inner.deserialize_bytes(w))
    }
    fn deserialize_byte_buf<V: Visitor<'de>>(self, v: V) -> Result<V::Value, Failure> {
        self.deserialize_bytes(v)
    }
    fn deserialize_option<V: Visitor<'de>>(self, v: V) -> Result<V::Value, Failure> {
        self.cx.tick()?;
        if self.role != Role::Origin && !is_probe(self.role) {
            return Err(Failure::Unsupported);
        }
        let w = self.wrap(v, None);
        self.cx.result(self.inner.deserialize_option(w))
    }
    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: V,
    ) -> Result<V::Value, Failure> {
        self.cx.tick()?;
        Err(Failure::Unsupported)
    }
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        v: V,
    ) -> Result<V::Value, Failure> {
        self.cx.tick()?;
        if !is_probe(self.role) {
            return Err(Failure::Unsupported);
        }
        let w = self.wrap(v, None);
        self.cx
            .result(self.inner.deserialize_newtype_struct(name, w))
    }
    fn deserialize_seq<V: Visitor<'de>>(self, v: V) -> Result<V::Value, Failure> {
        self.cx.tick()?;
        if !matches!(
            self.role,
            Role::OuterKey(_)
                | Role::Raw(_)
                | Role::Columns
                | Role::Primary
                | Role::RowKey
                | Role::Values
                | Role::Bytes
        ) && !is_probe(self.role)
        {
            return Err(Failure::Unsupported);
        }
        let w = self.wrap(v, None);
        self.cx.result(self.inner.deserialize_seq(w))
    }
    fn deserialize_tuple<V: Visitor<'de>>(self, n: usize, v: V) -> Result<V::Value, Failure> {
        self.cx.tick()?;
        if !is_probe(self.role) || n > 8 {
            return Err(Failure::Unsupported);
        }
        let w = self.wrap(v, Some(n));
        self.cx.result(self.inner.deserialize_tuple(n, w))
    }
    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        n: usize,
        v: V,
    ) -> Result<V::Value, Failure> {
        self.cx.tick()?;
        if !is_probe(self.role) || n > 8 {
            return Err(Failure::Unsupported);
        }
        let w = self.wrap(v, Some(n));
        self.cx
            .result(self.inner.deserialize_tuple_struct(name, n, w))
    }
    fn deserialize_map<V: Visitor<'de>>(self, v: V) -> Result<V::Value, Failure> {
        self.cx.tick()?;
        if !matches!(
            self.role,
            Role::Outer | Role::Tables | Role::Rows | Role::RowMap
        ) && !is_probe(self.role)
        {
            return Err(Failure::Unsupported);
        }
        let w = self.wrap(v, None);
        self.cx.result(self.inner.deserialize_map(w))
    }
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        v: V,
    ) -> Result<V::Value, Failure> {
        self.cx.tick()?;
        let expected: &[&str] = match (self.role, name) {
            (Role::Metadata, "Metadata") => &[
                "format",
                "domain",
                "schema",
                "last_time",
                "next_approval",
                "origin",
            ],
            (Role::Relational, "Relational") => &[
                "format",
                "generation",
                "epoch",
                "next_id",
                "next_version",
                "tables",
                "rows",
                "ext",
            ],
            (Role::Table, "TableDef") => &["id", "name", "columns", "primary_key"],
            (Role::Column, "ColumnDef") => &["id", "name", "data_type", "not_null"],
            (Role::Row, "SqlRow") => &["values", "version"],
            _ => return Err(Failure::Unsupported),
        };
        if fields != expected {
            return Err(Failure::Unsupported);
        }
        let w = self.wrap(v, Some(fields.len()));
        self.cx
            .result(self.inner.deserialize_struct(name, fields, w))
    }
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        v: V,
    ) -> Result<V::Value, Failure> {
        self.cx.tick()?;
        match (self.role, name) {
            (Role::Value, "Value")
                if variants
                    == [
                        "Null",
                        "Bool",
                        "Int",
                        "Text",
                        "Bytes",
                        "Numeric",
                        "Float4",
                        "Float8",
                        "Date",
                        "Time",
                        "TimeTz",
                        "Timestamp",
                        "TimestampTz",
                        "Interval",
                    ] => {}
            (Role::Types, "DataType")
                if variants
                    == [
                        "Boolean",
                        "SmallInt",
                        "Integer",
                        "BigInt",
                        "Text",
                        "Bytes",
                        "Numeric",
                        "Real",
                        "DoublePrecision",
                        "Date",
                        "Time",
                        "TimeTz",
                        "Timestamp",
                        "TimestampTz",
                        "Interval",
                    ] => {}
            _ if is_probe(self.role) => {}
            _ => return Err(Failure::Unsupported),
        }
        let w = self.wrap(v, None);
        self.cx
            .result(self.inner.deserialize_enum(name, variants, w))
    }
    fn is_human_readable(&self) -> bool {
        false
    }
}

fn seq_shape(role: Role, cx: &Context<'_>) -> Result<(usize, usize, usize), Failure> {
    Ok(match role {
        Role::OuterKey(_) => (1, 1, 1),
        Role::Raw(0) => (0, 61, 1),
        Role::Raw(5) => (0, SQL, 1),
        Role::Columns => (1, 8, 40),
        Role::Primary => (1, cx.columns.get(), 8),
        Role::RowKey => (0, 4120, 1),
        Role::Values => (cx.columns.get(), cx.columns.get(), 40),
        Role::Bytes => (0, 256, 1),
        _ if is_probe(role) => (0, 8, 40),
        _ => return Err(Failure::Unsupported),
    })
}
fn child(role: Role, i: usize) -> Result<Role, Failure> {
    Ok(match role {
        Role::Metadata => match i {
            0 => Role::Format,
            1 => Role::U128,
            2..=4 => Role::U64,
            5 => Role::Origin,
            _ => return Err(Failure::Invalid),
        },
        Role::Relational => match i {
            0 => Role::SqlFormat,
            1..=4 => Role::U64,
            5 => Role::Tables,
            6 => Role::Rows,
            _ => return Err(Failure::Invalid),
        },
        Role::Table => match i {
            0 => Role::U64,
            1 => Role::Name,
            2 => Role::Columns,
            3 => Role::Primary,
            _ => return Err(Failure::Invalid),
        },
        Role::Column => match i {
            0 => Role::U64,
            1 => Role::Name,
            2 => Role::Types,
            3 => Role::Bool,
            _ => return Err(Failure::Invalid),
        },
        Role::Row => match i {
            0 => Role::Values,
            1 => Role::U64,
            _ => return Err(Failure::Invalid),
        },
        Role::OuterKey(ns) => Role::Namespace(ns),
        Role::Raw(_) | Role::Bytes | Role::RowKey => Role::U8,
        Role::Columns => Role::Column,
        Role::Primary => Role::U64,
        Role::Values => Role::Value,
        _ if is_probe(role) => role,
        _ => return Err(Failure::Unsupported),
    })
}
struct Sequence<'a, 'b, A> {
    inner: A,
    cx: &'a Context<'b>,
    role: Role,
    depth: usize,
    n: usize,
    index: usize,
}
impl<'de, A: SeqAccess<'de>> SeqAccess<'de> for &mut Sequence<'_, '_, A> {
    type Error = Failure;
    fn next_element_seed<S: DeserializeSeed<'de>>(
        &mut self,
        s: S,
    ) -> Result<Option<S::Value>, Failure> {
        self.cx.tick()?;
        if self.index == self.n {
            return Ok(None);
        }
        let role = child(self.role, self.index)?;
        self.index += 1;
        self.cx.visited()?; // attempted children survive failures.
        let r = self.inner.next_element_seed(Seed {
            inner: s,
            cx: self.cx,
            role,
            depth: self.depth + 1,
        });
        let value = self.cx.result(r)?;
        if value.is_none() {
            return Err(Failure::Invalid);
        }
        Ok(value)
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.n - self.index)
    }
}
struct Mapping<'a, 'b, A> {
    inner: A,
    cx: &'a Context<'b>,
    role: Role,
    depth: usize,
    n: usize,
    index: usize,
    pending: bool,
}
impl<'de, A: MapAccess<'de>> MapAccess<'de> for &mut Mapping<'_, '_, A> {
    type Error = Failure;
    fn next_key_seed<S: DeserializeSeed<'de>>(
        &mut self,
        s: S,
    ) -> Result<Option<S::Value>, Failure> {
        self.cx.tick()?;
        if self.pending {
            return Err(Failure::Invalid);
        }
        if self.index == self.n {
            return Ok(None);
        }
        let role = match self.role {
            Role::Outer => Role::OuterKey(if self.index == 0 { 0 } else { 5 }),
            Role::Tables => Role::Name,
            Role::Rows => Role::U64,
            Role::RowMap => Role::RowKey,
            _ if is_probe(self.role) => self.role,
            _ => return Err(Failure::Unsupported),
        };
        self.pending = true;
        self.cx.visited()?;
        let r = self.inner.next_key_seed(Seed {
            inner: s,
            cx: self.cx,
            role,
            depth: self.depth + 1,
        });
        let v = self.cx.result(r)?;
        if v.is_none() {
            return Err(Failure::Invalid);
        }
        Ok(v)
    }
    fn next_value_seed<S: DeserializeSeed<'de>>(&mut self, s: S) -> Result<S::Value, Failure> {
        self.cx.tick()?;
        if !self.pending {
            return Err(Failure::Invalid);
        }
        let role = match self.role {
            Role::Outer => Role::Raw(if self.index == 0 { 0 } else { 5 }),
            Role::Tables => Role::Table,
            Role::Rows => Role::RowMap,
            Role::RowMap => Role::Row,
            _ if is_probe(self.role) => self.role,
            _ => return Err(Failure::Unsupported),
        };
        self.pending = false;
        self.index += 1;
        self.cx.visited()?;
        self.cx.result(self.inner.next_value_seed(Seed {
            inner: s,
            cx: self.cx,
            role,
            depth: self.depth + 1,
        }))
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.n - self.index)
    }
}
macro_rules! scalar_visit {
    ($name:ident,$ty:ty) => {
        fn $name<E: de::Error>(self, value: $ty) -> Result<Self::Value, E> {
            self.cx.tick().map_err(|f| self.cx.bridge(f))?;
            self.inner
                .$name::<Failure>(value)
                .map_err(|f| self.cx.bridge(f))
        }
    };
}
impl<'de, V: Visitor<'de>> Visitor<'de> for Wrapped<'_, '_, V> {
    type Value = V::Value;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("restricted source")
    }
    scalar_visit!(visit_bool, bool);
    scalar_visit!(visit_i64, i64);
    scalar_visit!(visit_u128, u128);
    fn visit_u8<E: de::Error>(self, v: u8) -> Result<V::Value, E> {
        self.cx.tick().map_err(|f| self.cx.bridge(f))?;
        if let Role::Namespace(expected) = self.role
            && v != expected
        {
            return Err(self.cx.bridge(Failure::Unsupported));
        }
        self.inner
            .visit_u8::<Failure>(v)
            .map_err(|f| self.cx.bridge(f))
    }
    fn visit_u32<E: de::Error>(self, v: u32) -> Result<V::Value, E> {
        self.cx.tick().map_err(|f| self.cx.bridge(f))?;
        if matches!(self.role, Role::Format) && v != 3
            || matches!(self.role, Role::SqlFormat) && v != 1
        {
            return Err(self.cx.bridge(Failure::Unsupported));
        }
        if let Role::Tag(max) = self.role
            && v >= u32::from(max)
        {
            return Err(self.cx.bridge(Failure::Unsupported));
        }
        self.inner
            .visit_u32::<Failure>(v)
            .map_err(|f| self.cx.bridge(f))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<V::Value, E> {
        self.cx.tick().map_err(|f| self.cx.bridge(f))?;
        if let Role::Tag(max) = self.role
            && v >= u64::from(max)
        {
            return Err(self.cx.bridge(Failure::Unsupported));
        }
        self.inner
            .visit_u64::<Failure>(v)
            .map_err(|f| self.cx.bridge(f))
    }
    fn visit_str<E: de::Error>(self, v: &str) -> Result<V::Value, E> {
        self.cx
            .tick()
            .and_then(|()| leaf(self.role, v.len(), self.cx))
            .map_err(|f| self.cx.bridge(f))?;
        self.inner
            .visit_str::<Failure>(v)
            .map_err(|f| self.cx.bridge(f))
    }
    fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<V::Value, E> {
        self.cx
            .tick()
            .and_then(|()| leaf(self.role, v.len(), self.cx))
            .map_err(|f| self.cx.bridge(f))?;
        self.inner
            .visit_borrowed_str::<Failure>(v)
            .map_err(|f| self.cx.bridge(f))
    }
    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<V::Value, E> {
        self.cx
            .tick()
            .and_then(|()| leaf(self.role, v.len(), self.cx))
            .map_err(|f| self.cx.bridge(f))?;
        self.inner
            .visit_bytes::<Failure>(v)
            .map_err(|f| self.cx.bridge(f))
    }
    fn visit_borrowed_bytes<E: de::Error>(self, v: &'de [u8]) -> Result<V::Value, E> {
        self.cx
            .tick()
            .and_then(|()| leaf(self.role, v.len(), self.cx))
            .map_err(|f| self.cx.bridge(f))?;
        self.inner
            .visit_borrowed_bytes::<Failure>(v)
            .map_err(|f| self.cx.bridge(f))
    }
    // Owned callbacks indicate an unqualified reader. Never forward them.
    fn visit_string<E: de::Error>(self, _: String) -> Result<V::Value, E> {
        Err(self.cx.bridge(Failure::Unsupported))
    }
    fn visit_byte_buf<E: de::Error>(self, _: Vec<u8>) -> Result<V::Value, E> {
        Err(self.cx.bridge(Failure::Unsupported))
    }
    fn visit_none<E: de::Error>(self) -> Result<V::Value, E> {
        self.cx.tick().map_err(|f| self.cx.bridge(f))?;
        self.inner
            .visit_none::<Failure>()
            .map_err(|f| self.cx.bridge(f))
    }
    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<V::Value, D::Error> {
        self.cx.tick().map_err(|f| self.cx.bridge(f))?;
        let role = if self.role == Role::Origin {
            Role::U128
        } else if is_probe(self.role) {
            self.role
        } else {
            return Err(self.cx.bridge(Failure::Unsupported));
        };
        self.cx
            .enter(role, self.depth + 1)
            .map_err(|f| self.cx.bridge(f))?;
        self.inner
            .visit_some(Gated {
                inner: d,
                cx: self.cx,
                role,
                depth: self.depth + 1,
            })
            .map_err(|f| self.cx.bridge(f))
    }
    fn visit_newtype_struct<D: Deserializer<'de>>(self, d: D) -> Result<V::Value, D::Error> {
        self.cx.tick().map_err(|f| self.cx.bridge(f))?;
        if !is_probe(self.role) {
            return Err(self.cx.bridge(Failure::Unsupported));
        }
        self.cx
            .enter(self.role, self.depth + 1)
            .map_err(|f| self.cx.bridge(f))?;
        self.inner
            .visit_newtype_struct(Gated {
                inner: d,
                cx: self.cx,
                role: self.role,
                depth: self.depth + 1,
            })
            .map_err(|f| self.cx.bridge(f))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, a: A) -> Result<V::Value, A::Error> {
        let result = (|| {
            self.cx.tick()?;
            let n = a.size_hint().ok_or(Failure::Unsupported)?;
            if let Some(arity) = self.arity {
                if n != arity {
                    return Err(Failure::Invalid);
                }
            } else {
                let (min, max, size) = seq_shape(self.role, self.cx)?;
                if n < min || n > max {
                    return Err(Failure::Limit);
                }
                self.cx.declared(n)?;
                self.cx.alloc(mul(n, size)?)?;
                self.cx.bytes(mul(n, size)?)?;
                if self.role == Role::Columns {
                    self.cx.columns.set(n)
                }
            }
            let mut seq = Sequence {
                inner: a,
                cx: self.cx,
                role: self.role,
                depth: self.depth,
                n,
                index: 0,
            };
            let out = self.inner.visit_seq(&mut seq)?;
            // Relational declares an 8th `ext` field that only format 2 carries; replica
            // images admit format 1 alone, so its visitor stops after the 7 legacy fields.
            let legacy_relational = self.role == Role::Relational && n == 8 && seq.index == 7;
            if seq.index != n && !legacy_relational {
                return Err(Failure::Invalid);
            }
            Ok(out)
        })();
        result.map_err(|f| self.cx.bridge(f))
    }
    fn visit_map<A: MapAccess<'de>>(self, a: A) -> Result<V::Value, A::Error> {
        let result = (|| {
            self.cx.tick()?;
            let n = a.size_hint().ok_or(Failure::Unsupported)?;
            let (min, max, node, key) = match self.role {
                Role::Outer => (2, 2, 648, 1),
                Role::Tables => (1, 1, 1264, 63),
                Role::Rows => (1, 1, 472, 1),
                Role::RowMap => (0, 64, 736, 4120),
                _ if is_probe(self.role) => (0, 8, 1000, 256),
                _ => return Err(Failure::Unsupported),
            };
            if n < min || n > max {
                return Err(Failure::Limit);
            }
            self.cx.declared(n)?;
            self.cx.alloc(mul(n, node)?)?;
            self.cx.budget.borrow_mut().work(tree_work(n, node, key)?)?;
            let mut map = Mapping {
                inner: a,
                cx: self.cx,
                role: self.role,
                depth: self.depth,
                n,
                index: 0,
                pending: false,
            };
            let out = self.inner.visit_map(&mut map)?;
            if map.index != n || map.pending {
                return Err(Failure::Invalid);
            }
            Ok(out)
        })();
        result.map_err(|f| self.cx.bridge(f))
    }
    fn visit_enum<A: EnumAccess<'de>>(self, a: A) -> Result<V::Value, A::Error> {
        self.cx.tick().map_err(|f| self.cx.bridge(f))?;
        let max = match self.role {
            Role::Value => 5,
            Role::Types => 6,
            _ if is_probe(self.role) => 4,
            _ => return Err(self.cx.bridge(Failure::Unsupported)),
        };
        self.inner
            .visit_enum(Enumeration {
                inner: a,
                cx: self.cx,
                role: self.role,
                depth: self.depth,
                max,
            })
            .map_err(|f| self.cx.bridge(f))
    }
}
fn leaf(role: Role, n: usize, cx: &Context<'_>) -> Result<(), Failure> {
    let max = match role {
        Role::Name => 63,
        Role::Text | Role::Bytes => 256,
        _ if is_probe(role) => 8,
        _ => return Err(Failure::Unsupported),
    };
    if n > max {
        return Err(Failure::Limit);
    }
    cx.bytes(n)?;
    cx.alloc(n)
}
struct Enumeration<'a, 'b, A> {
    inner: A,
    cx: &'a Context<'b>,
    role: Role,
    depth: usize,
    max: u8,
}
struct Variant<'a, 'b, A> {
    inner: A,
    cx: &'a Context<'b>,
    role: Role,
    depth: usize,
    tag: u8,
}
struct TagSeed<'a, 'b, S> {
    inner: S,
    cx: &'a Context<'b>,
    depth: usize,
    max: u8,
    tag: &'a Cell<Option<u8>>,
}
struct TagVisitor<'a, V> {
    inner: V,
    tag: &'a Cell<Option<u8>>,
}
impl<'de, V: Visitor<'de>> Visitor<'de> for TagVisitor<'_, V> {
    type Value = V::Value;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("tag")
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<V::Value, E> {
        self.tag.set(u8::try_from(v).ok());
        self.inner.visit_u64(v)
    }
    fn visit_u32<E: de::Error>(self, v: u32) -> Result<V::Value, E> {
        self.tag.set(u8::try_from(v).ok());
        self.inner.visit_u32(v)
    }
}
// The derived identifier seed invokes deserialize_identifier. TagOnly changes
// only that call to already-decoded scalar deserialize_any, never bincode any.
struct TagOnly<'a, 'b, D> {
    inner: D,
    cx: &'a Context<'b>,
    depth: usize,
    max: u8,
    tag: &'a Cell<Option<u8>>,
}
impl<'de, D: Deserializer<'de>> Deserializer<'de> for TagOnly<'_, '_, D> {
    type Error = Failure;
    fn deserialize_any<V: Visitor<'de>>(self, v: V) -> Result<V::Value, Failure> {
        Gated {
            inner: self.inner,
            cx: self.cx,
            role: Role::Tag(self.max),
            depth: self.depth,
        }
        .deserialize_any(TagVisitor {
            inner: v,
            tag: self.tag,
        })
    }
    fn deserialize_identifier<V: Visitor<'de>>(self, v: V) -> Result<V::Value, Failure> {
        self.deserialize_any(v)
    }
    denied!(
        deserialize_bool,
        deserialize_i8,
        deserialize_i16,
        deserialize_i32,
        deserialize_i64,
        deserialize_i128,
        deserialize_u8,
        deserialize_u16,
        deserialize_u128,
        deserialize_f32,
        deserialize_f64,
        deserialize_char,
        deserialize_str,
        deserialize_string,
        deserialize_bytes,
        deserialize_byte_buf,
        deserialize_option,
        deserialize_unit,
        deserialize_ignored_any
    );
    fn deserialize_u32<V: Visitor<'de>>(self, v: V) -> Result<V::Value, Failure> {
        self.deserialize_any(v)
    }
    fn deserialize_u64<V: Visitor<'de>>(self, v: V) -> Result<V::Value, Failure> {
        self.deserialize_any(v)
    }
    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: V,
    ) -> Result<V::Value, Failure> {
        Err(Failure::Unsupported)
    }
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: V,
    ) -> Result<V::Value, Failure> {
        Err(Failure::Unsupported)
    }
    fn deserialize_tuple<V: Visitor<'de>>(self, _: usize, _: V) -> Result<V::Value, Failure> {
        Err(Failure::Unsupported)
    }
    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: usize,
        _: V,
    ) -> Result<V::Value, Failure> {
        Err(Failure::Unsupported)
    }
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        _: V,
    ) -> Result<V::Value, Failure> {
        Err(Failure::Unsupported)
    }
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        _: V,
    ) -> Result<V::Value, Failure> {
        Err(Failure::Unsupported)
    }
    fn deserialize_seq<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Failure> {
        Err(Failure::Unsupported)
    }
    fn deserialize_map<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Failure> {
        Err(Failure::Unsupported)
    }
    fn is_human_readable(&self) -> bool {
        false
    }
}
impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for TagSeed<'_, '_, S> {
    type Value = S::Value;
    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<S::Value, D::Error> {
        self.cx
            .enter(Role::Tag(self.max), self.depth)
            .map_err(|f| self.cx.bridge(f))?;
        self.inner
            .deserialize(TagOnly {
                inner: d,
                cx: self.cx,
                depth: self.depth,
                max: self.max,
                tag: self.tag,
            })
            .map_err(|f| self.cx.bridge(f))
    }
}

impl<'de, 'a, 'b, A: EnumAccess<'de>> EnumAccess<'de> for Enumeration<'a, 'b, A> {
    type Error = Failure;
    type Variant = Variant<'a, 'b, A::Variant>;
    fn variant_seed<S: DeserializeSeed<'de>>(
        self,
        s: S,
    ) -> Result<(S::Value, Self::Variant), Failure> {
        self.cx.tick()?;
        let tag = Cell::new(None);
        let (value, variant) = self.cx.result(self.inner.variant_seed(TagSeed {
            inner: s,
            cx: self.cx,
            depth: self.depth + 1,
            max: self.max,
            tag: &tag,
        }))?;
        let tag = tag.get().ok_or(Failure::Invalid)?;
        if tag >= self.max {
            return Err(Failure::Unsupported);
        }
        Ok((
            value,
            Variant {
                inner: variant,
                cx: self.cx,
                role: self.role,
                depth: self.depth,
                tag,
            },
        ))
    }
}
impl<'de, A: VariantAccess<'de>> VariantAccess<'de> for Variant<'_, '_, A> {
    type Error = Failure;
    fn unit_variant(self) -> Result<(), Failure> {
        self.cx.tick()?;
        if !(self.role == Role::Types
            || self.role == Role::Value && self.tag == 0
            || is_probe(self.role))
        {
            return Err(Failure::Unsupported);
        }
        self.cx.result(self.inner.unit_variant())
    }
    fn newtype_variant_seed<S: DeserializeSeed<'de>>(self, s: S) -> Result<S::Value, Failure> {
        self.cx.tick()?;
        {
            let mut b = self.cx.budget.borrow_mut();
            b.used.payloads = add(b.used.payloads, 1)?;
        }
        let role = match (self.role, self.tag) {
            (Role::Value, 1) => Role::Bool,
            (Role::Value, 2) => Role::Int,
            (Role::Value, 3) => Role::Text,
            (Role::Value, 4) => Role::Bytes,
            _ if is_probe(self.role) => self.role,
            _ => return Err(Failure::Unsupported),
        };
        self.cx.result(self.inner.newtype_variant_seed(Seed {
            inner: s,
            cx: self.cx,
            role,
            depth: self.depth + 1,
        }))
    }
    fn tuple_variant<V: Visitor<'de>>(self, n: usize, v: V) -> Result<V::Value, Failure> {
        self.cx.tick()?;
        if !is_probe(self.role) || n > 8 {
            return Err(Failure::Unsupported);
        }
        self.cx.result(self.inner.tuple_variant(
            n,
            Wrapped {
                inner: v,
                cx: self.cx,
                role: self.role,
                depth: self.depth,
                arity: Some(n),
            },
        ))
    }
    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        v: V,
    ) -> Result<V::Value, Failure> {
        self.cx.tick()?;
        if !is_probe(self.role) || fields.len() > 8 {
            return Err(Failure::Unsupported);
        }
        self.cx.result(self.inner.struct_variant(
            fields,
            Wrapped {
                inner: v,
                cx: self.cx,
                role: self.role,
                depth: self.depth,
                arity: Some(fields.len()),
            },
        ))
    }
}
fn typed<'de, T: Deserialize<'de>>(
    bytes: &'de [u8],
    cx: &Context<'_>,
    role: Role,
) -> Result<T, Failure> {
    cx.input.set(bytes.len());
    cx.enter(role, 0)?;
    let options = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .with_limit(INPUT as u64);
    let mut d = bincode::Deserializer::from_slice(bytes, options);
    let out = T::deserialize(Gated {
        inner: &mut d,
        cx,
        role,
        depth: 0,
    })?;
    // Deserializer::from_slice does not itself enforce reject_trailing_bytes.
    // SliceReader position is private; count exact consumed bytes with a second
    // reader is forbidden. This is enforced by the final canonical comparison.
    cx.tick()?;
    Ok(out)
}
/// Private, non-cloneable, non-serde certificate tied to the exact immutable Image.
pub(super) struct AdmittedProjectedSource<'a> {
    image: &'a Image,
    state: State,
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Retained source cost evidence awaits the separately released projector handoff; no getter is released."
        )
    )]
    costs: SourceCosts,
}
impl AdmittedProjectedSource<'_> {
    /// Exact borrowed object identity only, never byte equivalence/authentication.
    pub(super) fn matches_source(&self, source: &Image) -> bool {
        std::ptr::eq(self.image, source)
    }

    pub(super) fn decode_canonical(
        self,
        budget: &mut SourceBudget,
        cancel: &mut impl FnMut() -> bool,
    ) -> Result<State, CoreError> {
        if cancel() {
            return Err(CoreError::Cancelled);
        }
        // One move, no decode, no allocations, no budget reset or refund.
        budget.work(Work {
            events: 1,
            ..Work::default()
        })?;
        if cancel() {
            return Err(CoreError::Cancelled);
        }
        Ok(self.state)
    }
}
fn layouts() -> Result<(), Failure> {
    use std::mem::{align_of, size_of};
    if !cfg!(all(
        target_os = "windows",
        target_arch = "x86_64",
        target_env = "msvc"
    )) || [
        size_of::<Value>(),
        size_of::<ColumnDef>(),
        size_of::<TableDef>(),
        size_of::<SqlRow>(),
        size_of::<relational::Relational>(),
    ] != [40, 40, 80, 32, 264]
        || [
            align_of::<Value>(),
            align_of::<ColumnDef>(),
            align_of::<TableDef>(),
            align_of::<SqlRow>(),
            align_of::<relational::Relational>(),
        ] != [8; 5]
        || [
            size_of::<String>(),
            size_of::<Vec<u8>>(),
            size_of::<BTreeMap<u64, u64>>(),
        ] != [24; 3]
        || size_of::<State>() != 608
        || size_of::<bincode::ErrorKind>() > 32
    {
        return Err(Failure::Unsupported);
    }
    Ok(())
}
impl SourceBudget {
    fn tighten(&mut self, lim: &SourceAdmissionLimits) -> Result<(), Failure> {
        let hard = SourceAdmissionLimits::default();
        if lim.allocation_bytes > hard.allocation_bytes
            || lim.phases.iter().zip(hard.phases).any(|(a, b)| *a > b)
        {
            return Err(Failure::Limit);
        }
        self.limits.allocation_bytes = self.limits.allocation_bytes.min(lim.allocation_bytes);
        for (a, b) in self.limits.phases.iter_mut().zip(lim.phases) {
            *a = (*a).min(b)
        }
        macro_rules! tight {($($f:ident),*)=>{$(self.limits.work.$f=self.limits.work.$f.min(lim.work.$f).min(hard.work.$f);)*}}
        tight!(events, bytes, comparisons, moves, values);
        self.limits.declared = self.limits.declared.min(lim.declared).min(hard.declared);
        self.limits.visited = self.limits.visited.min(lim.visited).min(hard.visited);
        Ok(())
    }
}
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Source admission entry awaits the separately released replica integration; tests exercise it."
    )
)]
pub(super) fn admit_projected_source<'a>(
    image: &'a Image,
    limits: &SourceAdmissionLimits,
    budget: &mut SourceBudget,
    cancel: &mut impl FnMut() -> bool,
) -> Result<AdmittedProjectedSource<'a>, CoreError> {
    if cancel() {
        return Err(CoreError::Cancelled);
    }
    if image.0.len() > INPUT {
        return Err(CoreError::Limit);
    }
    layouts()?;
    budget.tighten(limits)?;
    budget.reserve(pipeline()?)?;
    budget.work(post_work()?)?;
    let cx = Context {
        budget: RefCell::new(budget),
        cancel: RefCell::new(cancel),
        fault: Cell::new(None),
        stack: RefCell::new([Role::Outer; DEPTH]),
        columns: Cell::new(0),
        input: Cell::new(image.0.len()),
        allocated: Cell::new(0),
    };
    let outer: BTreeMap<Vec<u8>, Vec<u8>> = typed(&image.0, &cx, Role::Outer)?;
    let metadata: Metadata = typed(
        outer.get(&[0][..]).ok_or(Failure::Invalid)?,
        &cx,
        Role::Metadata,
    )?;
    let sql: relational::Relational = typed(
        outer.get(&[5][..]).ok_or(Failure::Invalid)?,
        &cx,
        Role::Relational,
    )?;
    let mut state = empty(metadata.domain);
    state.view.schema = metadata.schema;
    state.last_time = metadata.last_time;
    state.next_approval = metadata.next_approval;
    state.origin = metadata.origin;
    state.sql = sql;
    cx.tick()?;
    validate(&state).map_err(|_| CoreError::Unsupported)?;
    cx.tick()?;
    let canonical = records(&state).map_err(|_| CoreError::Unsupported)?;
    cx.tick()?;
    let bytes = encode(&canonical).map_err(|_| CoreError::Unsupported)?;
    cx.tick()?;
    if bytes != image.0 {
        return Err(CoreError::Unsupported);
    }
    cx.tick()?;
    let costs = cx.budget.borrow().used;
    Ok(AdmittedProjectedSource {
        image,
        state,
        costs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relational::DataType;
    use serde::Serialize;
    pub(super) fn fixture() -> Image {
        Image::new(7)
            .unwrap()
            .with_admin(|a| {
                let mut s = a.sql_session();
                s.execute(
                    "CREATE TABLE items (id BIGINT PRIMARY KEY, note TEXT, data BYTEA)",
                    &[],
                    None,
                )?;
                s.execute(
                    "INSERT INTO items (id,note,data) VALUES ($1,$2,$3)",
                    &[
                        Value::Int(1),
                        Value::Text("hello".into()),
                        Value::Bytes(vec![0, 1, 255]),
                    ],
                    None,
                )?;
                Ok(())
            })
            .unwrap()
            .0
    }
    pub(super) fn run<T: for<'de> Deserialize<'de>>(
        bytes: &[u8],
        role: Role,
    ) -> (Result<T, Failure>, SourceCosts) {
        let mut b = SourceBudget::new(SourceAdmissionLimits::default());
        let mut cancel = || false;
        let cx = Context {
            budget: RefCell::new(&mut b),
            cancel: RefCell::new(&mut cancel),
            fault: Cell::new(None),
            stack: RefCell::new([Role::Outer; DEPTH]),
            columns: Cell::new(8),
            input: Cell::new(bytes.len()),
            allocated: Cell::new(0),
        };
        let result = typed(bytes, &cx, role);
        let costs = cx.budget.borrow().used;
        (result, costs)
    }
    fn wire<T: Serialize>(v: &T) -> Vec<u8> {
        super::super::encode(v).unwrap()
    }
    #[test]
    fn actual_layouts() {
        use std::mem::{align_of, size_of};
        assert_eq!(
            [
                size_of::<Value>(),
                size_of::<ColumnDef>(),
                size_of::<TableDef>(),
                size_of::<SqlRow>()
            ],
            [40, 40, 80, 32]
        );
        assert_eq!(
            [
                align_of::<Value>(),
                align_of::<ColumnDef>(),
                align_of::<TableDef>(),
                align_of::<SqlRow>()
            ],
            [8; 4]
        );
        assert_eq!(size_of::<relational::Relational>(), 264);
        assert_eq!(size_of::<Metadata>(), 80);
        assert_eq!(size_of::<State>(), 608);
        assert_eq!(size_of::<DataType>(), 1);
        layouts().unwrap();
        println!(
            "Relational={} align={} Metadata={} State={}",
            size_of::<relational::Relational>(),
            align_of::<relational::Relational>(),
            size_of::<Metadata>(),
            size_of::<State>()
        );
        assert_eq!(pipeline().unwrap(), [404761, 465086, 1224784, 808266]);
    }
    #[test]
    fn canonical_trusted_admin_roundtrip() {
        let image = fixture();
        let original = image.0.clone();
        let mut b = SourceBudget::new(SourceAdmissionLimits::default());
        let c = admit_projected_source(
            &image,
            &SourceAdmissionLimits::default(),
            &mut b,
            &mut || false,
        )
        .unwrap();
        assert!(std::ptr::eq(c.image, &image));
        assert_eq!(c.costs.allocations, 2904081);
        let s = c.decode_canonical(&mut b, &mut || false).unwrap();
        assert_eq!(wire(&records(&s).unwrap()), original);
        assert_eq!(image.0, original);
    }
    #[test]
    fn format_two_catalog_extension_rejected_but_base_admitted() {
        let image = fixture();
        let mut map: BTreeMap<Vec<u8>, Vec<u8>> = super::super::decode(&image.0).unwrap();
        let mut sql: relational::Relational = super::super::decode(&map[&vec![5]]).unwrap();
        let column = sql.tables["items"].columns[0].id;
        sql.set_column_default(column, Some("1".into())).unwrap();
        assert!(!sql.catalog_ext().is_empty());
        let extended = wire(&sql);
        assert_eq!(&extended[..4], &2u32.to_be_bytes());
        let decoded: relational::Relational = super::super::decode(&extended).unwrap();
        decoded.assert_legacy_format_for_catalog_ext_test();
        assert_eq!(decoded.catalog_ext(), sql.catalog_ext());
        map.insert(vec![5], extended);
        let extended_image = Image(wire(&map));
        let limits = SourceAdmissionLimits::default();
        let mut budget = SourceBudget::new(SourceAdmissionLimits::default());
        assert!(matches!(
            admit_projected_source(&extended_image, &limits, &mut budget, &mut || false),
            Err(CoreError::Unsupported)
        ));

        sql.set_column_default(column, None).unwrap();
        assert!(sql.catalog_ext().is_empty());
        map.insert(vec![5], wire(&sql));
        let base_image = Image(wire(&map));
        assert_eq!(base_image.0, image.0);
        let mut budget = SourceBudget::new(SourceAdmissionLimits::default());
        assert!(admit_projected_source(&base_image, &limits, &mut budget, &mut || false).is_ok());
    }
    #[test]
    fn actual_public_nested_types() {
        let table = TableDef {
            id: 1,
            name: "items".into(),
            columns: vec![ColumnDef {
                id: 2,
                name: "id".into(),
                data_type: DataType::BigInt,
                not_null: true,
            }],
            primary_key: vec![0],
        };
        assert_eq!(
            run::<TableDef>(&wire(&table), Role::Table).0.unwrap(),
            table
        );
        let row = SqlRow {
            values: vec![
                Value::Null,
                Value::Bool(true),
                Value::Int(-2),
                Value::Text("a".into()),
                Value::Bytes(vec![0, 255]),
                Value::Null,
                Value::Null,
                Value::Null,
            ],
            version: 1,
        };
        assert_eq!(run::<SqlRow>(&wire(&row), Role::Row).0.unwrap(), row);
        for dt in [
            DataType::Boolean,
            DataType::SmallInt,
            DataType::Integer,
            DataType::BigInt,
            DataType::Text,
            DataType::Bytes,
        ] {
            assert_eq!(run::<DataType>(&wire(&dt), Role::Types).0.unwrap(), dt);
        }
    }
    #[test]
    fn huge_vec_declared_rejected() {
        let (r, c) = run::<Vec<u8>>(&[255; 8], Role::Bytes);
        assert_eq!(r, Err(Failure::Limit));
        assert_eq!(c.visited, 0);
        assert_eq!(c.declared, 0);
    }
    #[test]
    fn sequence_exact_boundary() {
        for n in [256, 257] {
            let (r, c) = run::<Vec<u8>>(&wire(&vec![1u8; n]), Role::Bytes);
            assert_eq!(r.is_ok(), n == 256);
            assert_eq!(c.visited, if n == 256 { 256 } else { 0 });
        }
    }
    #[test]
    fn columns_exact_boundary() {
        for n in [8, 9] {
            let cols = vec![
                ColumnDef {
                    id: 1,
                    name: "x".into(),
                    data_type: DataType::Text,
                    not_null: true
                };
                n
            ];
            assert_eq!(
                run::<Vec<ColumnDef>>(&wire(&cols), Role::Columns).0.is_ok(),
                n == 8
            );
        }
    }
    #[test]
    fn map_exact_boundary() {
        for n in [64, 65] {
            let map: BTreeMap<Vec<u8>, SqlRow> = (0..n)
                .map(|i| {
                    (
                        vec![i as u8],
                        SqlRow {
                            values: vec![Value::Null; 8],
                            version: 1,
                        },
                    )
                })
                .collect();
            let (r, c) = run::<BTreeMap<Vec<u8>, SqlRow>>(&wire(&map), Role::RowMap);
            assert_eq!(r.is_ok(), n == 64);
            if n == 65 {
                assert_eq!(c.visited, 0)
            }
        }
    }
    #[test]
    fn leaf_exact_boundary() {
        for n in [256, 257] {
            assert_eq!(
                run::<Value>(&wire(&Value::Text("x".repeat(n))), Role::Value)
                    .0
                    .is_ok(),
                n == 256
            );
        }
        for n in [63, 64] {
            assert_eq!(
                run::<String>(&wire(&"x".repeat(n)), Role::Name).0.is_ok(),
                n == 63
            );
        }
    }
    #[test]
    fn duplicates_count_occurrences() {
        let mut bytes = wire(&2u64);
        for _ in 0..2 {
            bytes.extend(wire(&7u64));
            bytes.extend(wire(&9u64));
        }
        let (r, c) = run::<BTreeMap<u64, u64>>(&bytes, Role::Probe);
        assert_eq!(r.unwrap().len(), 1);
        assert_eq!(c.declared, 2);
        assert_eq!(c.visited, 4);
    }
    #[test]
    fn late_failed_child_counted() {
        let mut bytes = wire(&2u64);
        bytes.extend(wire(&1u64));
        let (r, c) = run::<Vec<u64>>(&bytes, Role::Probe);
        assert!(r.is_err());
        assert_eq!(c.visited, 2);
        assert_eq!(c.declared, 2);
    }
    #[test]
    fn utf8_bool_option_errors() {
        let mut bytes = wire(&1u64);
        bytes.push(255);
        assert!(run::<String>(&bytes, Role::Text).0.is_err());
        assert!(run::<bool>(&[2], Role::Bool).0.is_err());
        assert!(run::<Option<u128>>(&[2], Role::Origin).0.is_err());
        assert_eq!(
            run::<Option<u128>>(&wire(&Some(4u128)), Role::Origin)
                .0
                .unwrap(),
            Some(4)
        );
    }
    #[test]
    fn forbidden_actual_tags_before_payload() {
        for tag in 5u32..14 {
            let (r, c) = run::<Value>(&wire(&tag), Role::Value);
            assert_eq!(r, Err(Failure::Unsupported));
            assert_eq!(c.visited, 0);
            assert_eq!(c.payloads, 0);
        }
        for tag in 6u32..15 {
            assert_eq!(
                run::<DataType>(&wire(&tag), Role::Types).0,
                Err(Failure::Unsupported)
            );
        }
    }
    #[test]
    fn optional_namespaces_rejected() {
        let image = fixture();
        let base: BTreeMap<Vec<u8>, Vec<u8>> = super::super::decode(&image.0).unwrap();
        for ns in [1, 2, 3, 4, 6, 7, 8, 9] {
            let mut map = base.clone();
            map.remove(&vec![5]);
            map.insert(vec![ns], vec![]);
            let bad = Image(wire(&map));
            let mut b = SourceBudget::new(SourceAdmissionLimits::default());
            assert!(
                admit_projected_source(
                    &bad,
                    &SourceAdmissionLimits::default(),
                    &mut b,
                    &mut || false
                )
                .is_err()
            );
        }
    }
    #[test]
    fn reversed_duplicate_namespace() {
        for keys in [[5u8, 0], [0, 0], [5, 5]] {
            let mut bytes = wire(&2u64);
            for k in keys {
                bytes.extend(wire(&vec![k]));
                bytes.extend(wire(&Vec::<u8>::new()));
            }
            let (r, c) = run::<BTreeMap<Vec<u8>, Vec<u8>>>(&bytes, Role::Outer);
            assert_eq!(r, Err(Failure::Unsupported));
            assert!(c.visited < 8);
        }
    }
    #[test]
    fn canonical_mismatch_trailing() {
        let mut image = fixture();
        image.0.push(0);
        let original = image.0.clone();
        let mut b = SourceBudget::new(SourceAdmissionLimits::default());
        assert!(
            admit_projected_source(
                &image,
                &SourceAdmissionLimits::default(),
                &mut b,
                &mut || false
            )
            .is_err()
        );
        assert_eq!(image.0, original);
    }
    #[test]
    fn allocation_phase_boundaries() {
        let p = pipeline().unwrap();
        for i in 0..4 {
            let mut lim = SourceAdmissionLimits::default();
            lim.phases[i] = p[i] - 1;
            let mut b = SourceBudget::new(lim);
            assert_eq!(b.reserve(p), Err(Failure::Limit));
            assert_eq!(b.used.allocations, 2904081);
        }
        let lim = SourceAdmissionLimits {
            allocation_bytes: 2904080,
            ..SourceAdmissionLimits::default()
        };
        assert_eq!(SourceBudget::new(lim).reserve(p), Err(Failure::Limit));
        assert!(
            SourceBudget::new(SourceAdmissionLimits::default())
                .reserve(p)
                .is_ok()
        );
    }
    #[test]
    fn cancellation_initial_mid_final() {
        let image = fixture();
        let mut calls = 0;
        let mut b = SourceBudget::new(SourceAdmissionLimits::default());
        admit_projected_source(
            &image,
            &SourceAdmissionLimits::default(),
            &mut b,
            &mut || {
                calls += 1;
                false
            },
        )
        .unwrap();
        for at in [1, 10, calls] {
            let mut i = 0;
            let mut b = SourceBudget::new(SourceAdmissionLimits::default());
            assert!(matches!(
                admit_projected_source(
                    &image,
                    &SourceAdmissionLimits::default(),
                    &mut b,
                    &mut || {
                        i += 1;
                        i == at
                    }
                ),
                Err(CoreError::Cancelled)
            ));
        }
    }
    #[test]
    fn explicit_false_ignores_cancelled_ambient() {
        let image = fixture();
        let ambient = crate::Cancellation::default();
        ambient.cancel();
        ambient.scope(|| {
            let mut b = SourceBudget::new(SourceAdmissionLimits::default());
            let c = admit_projected_source(
                &image,
                &SourceAdmissionLimits::default(),
                &mut b,
                &mut || false,
            )
            .unwrap();
            c.decode_canonical(&mut b, &mut || false).unwrap();
            assert!(crate::cancellation::check().is_err());
        });
    }
    #[test]
    fn ledger_never_reset() {
        let image = Image(vec![255; 8]);
        let mut b = SourceBudget::new(SourceAdmissionLimits::default());
        assert!(
            admit_projected_source(
                &image,
                &SourceAdmissionLimits::default(),
                &mut b,
                &mut || false
            )
            .is_err()
        );
        let used = b.used;
        assert_eq!(used.allocations, 2904081);
        assert!(
            admit_projected_source(
                &image,
                &SourceAdmissionLimits::default(),
                &mut b,
                &mut || false
            )
            .is_err()
        );
        assert_eq!(b.used, used);
    }
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    enum AccessProbe {
        Unit,
        New(u64),
        Tuple(u64, u64),
        Struct { v: u64 },
    }
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Newtype(u64);
    #[test]
    fn all_variant_accesses_wrapped() {
        for v in [
            AccessProbe::Unit,
            AccessProbe::New(3),
            AccessProbe::Tuple(4, 5),
            AccessProbe::Struct { v: 6 },
        ] {
            assert_eq!(run::<AccessProbe>(&wire(&v), Role::Probe).0.unwrap(), v);
        }
        assert_eq!(
            run::<Newtype>(&wire(&Newtype(9)), Role::Probe).0.unwrap(),
            Newtype(9)
        );
    }
    #[derive(Debug, PartialEq)]
    struct BorrowedDispatch;
    impl<'de> Deserialize<'de> for BorrowedDispatch {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            struct V;
            impl<'de> Visitor<'de> for V {
                type Value = BorrowedDispatch;
                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("borrowed")
                }
                fn visit_borrowed_str<E: de::Error>(self, _: &'de str) -> Result<Self::Value, E> {
                    Ok(BorrowedDispatch)
                }
            }
            d.deserialize_string(V)
        }
    }
    #[test]
    fn string_owned_request_uses_borrowed_dispatch() {
        assert_eq!(
            run::<BorrowedDispatch>(&wire(&"abc"), Role::Text)
                .0
                .unwrap(),
            BorrowedDispatch
        );
    }
    #[derive(Debug, PartialEq)]
    struct BorrowedBytes;
    impl<'de> Deserialize<'de> for BorrowedBytes {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            struct V;
            impl<'de> Visitor<'de> for V {
                type Value = BorrowedBytes;
                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("borrowed")
                }
                fn visit_borrowed_bytes<E: de::Error>(
                    self,
                    _: &'de [u8],
                ) -> Result<Self::Value, E> {
                    Ok(BorrowedBytes)
                }
            }
            d.deserialize_byte_buf(V)
        }
    }
    #[test]
    fn bytes_owned_request_uses_borrowed_dispatch() {
        assert_eq!(
            run::<BorrowedBytes>(&wire(&"abc"), Role::Bytes).0.unwrap(),
            BorrowedBytes
        );
    }
    #[test]
    fn unsupported_generic_methods_failclosed() {
        assert!(run::<f64>(&wire(&1f64), Role::Probe).0.is_err());
        assert!(run::<serde::de::IgnoredAny>(&[], Role::Probe).0.is_err());
    }
    #[test]
    fn huge_map_and_vec_visitors_never_called() {
        struct Spy<'a>(&'a Cell<bool>);
        impl<'de> Visitor<'de> for Spy<'_> {
            type Value = ();
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("spy")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, _: A) -> Result<(), A::Error> {
                self.0.set(true);
                Ok(())
            }
            fn visit_map<A: MapAccess<'de>>(self, _: A) -> Result<(), A::Error> {
                self.0.set(true);
                Ok(())
            }
        }
        for map in [false, true] {
            let mut b = SourceBudget::new(SourceAdmissionLimits::default());
            let mut no = || false;
            let cx = Context {
                budget: RefCell::new(&mut b),
                cancel: RefCell::new(&mut no),
                fault: Cell::new(None),
                stack: RefCell::new([Role::Outer; DEPTH]),
                columns: Cell::new(8),
                input: Cell::new(8),
                allocated: Cell::new(0),
            };
            let bytes = [255; 8];
            let mut d = bincode::Deserializer::from_slice(
                &bytes,
                bincode::DefaultOptions::new()
                    .with_fixint_encoding()
                    .with_big_endian(),
            );
            let called = Cell::new(false);
            let g = Gated {
                inner: &mut d,
                cx: &cx,
                role: if map { Role::RowMap } else { Role::Bytes },
                depth: 0,
            };
            let result = if map {
                g.deserialize_map(Spy(&called))
            } else {
                g.deserialize_seq(Spy(&called))
            };
            assert_eq!(result, Err(Failure::Limit));
            assert!(!called.get());
            assert_eq!(cx.allocated.get(), 0);
        }
    }
    #[derive(Deserialize, Serialize)]
    enum ForbiddenAccess {
        New(f64),
        Tuple(f64, f64),
        Struct { v: f64 },
    }
    #[derive(Deserialize, Serialize)]
    struct ForbiddenNew(f64);
    #[derive(Deserialize, Serialize)]
    struct ForbiddenTuple(f64, f64);
    #[test]
    fn every_payload_access_rejects_raw_escape() {
        for v in [
            ForbiddenAccess::New(1.0),
            ForbiddenAccess::Tuple(1.0, 2.0),
            ForbiddenAccess::Struct { v: 3.0 },
        ] {
            assert!(run::<ForbiddenAccess>(&wire(&v), Role::Probe).0.is_err());
        }
        assert!(
            run::<ForbiddenNew>(&wire(&ForbiddenNew(1.0)), Role::Probe)
                .0
                .is_err()
        );
        assert!(
            run::<ForbiddenTuple>(&wire(&ForbiddenTuple(1.0, 2.0)), Role::Probe)
                .0
                .is_err()
        );
        assert!(
            run::<Option<f64>>(&wire(&Some(1.0)), Role::Probe)
                .0
                .is_err()
        );
        assert!(run::<Vec<f64>>(&wire(&vec![1.0]), Role::Probe).0.is_err());
        assert!(
            run::<BTreeMap<u64, f64>>(&wire(&BTreeMap::from([(1u64, 1.0)])), Role::Probe)
                .0
                .is_err()
        );
    }
    #[test]
    fn origin_and_framing_caps() {
        for (role, n, ok) in [
            (Role::Raw(0), 61, true),
            (Role::Raw(0), 62, false),
            (Role::Raw(5), SQL, true),
            (Role::Raw(5), SQL + 1, false),
            (Role::RowKey, 4120, true),
            (Role::RowKey, 4121, false),
        ] {
            assert_eq!(run::<Vec<u8>>(&wire(&vec![0u8; n]), role).0.is_ok(), ok);
        }
        let mut b = SourceBudget::new(SourceAdmissionLimits::default());
        let oversized = Image(vec![0; INPUT + 1]);
        assert!(matches!(
            admit_projected_source(
                &oversized,
                &SourceAdmissionLimits::default(),
                &mut b,
                &mut || false
            ),
            Err(CoreError::Limit)
        ));
        assert_eq!(b.used.allocations, 0);
    }
    #[test]
    fn count_and_work_exact_limits() {
        let image = fixture();
        let lim = SourceAdmissionLimits::default();
        let mut b = SourceBudget::new(SourceAdmissionLimits::default());
        admit_projected_source(&image, &lim, &mut b, &mut || false).unwrap();
        let exact = b.used;
        for which in 0..7 {
            for delta in [0, 1] {
                let mut l = SourceAdmissionLimits::default();
                match which {
                    0 => l.declared = exact.declared - delta,
                    1 => l.visited = exact.visited - delta,
                    2 => l.work.events = exact.work.events - delta,
                    3 => l.work.bytes = exact.work.bytes - delta,
                    4 => l.work.comparisons = exact.work.comparisons - delta,
                    5 => l.work.moves = exact.work.moves - delta,
                    _ => l.work.values = exact.work.values - delta,
                }
                let mut b = SourceBudget::new(l);
                let r = admit_projected_source(&image, &lim, &mut b, &mut || false);
                assert_eq!(r.is_ok(), delta == 0, "component{which}");
            }
        }
    }
    #[test]
    fn malformed_whole_source_truncations() {
        let image = fixture();
        for n in [0, 1, 7, 8, 20, image.0.len() / 2, image.0.len() - 1] {
            let bad = Image(image.0[..n].to_vec());
            let saved = bad.0.clone();
            let mut b = SourceBudget::new(SourceAdmissionLimits::default());
            assert!(
                admit_projected_source(
                    &bad,
                    &SourceAdmissionLimits::default(),
                    &mut b,
                    &mut || false
                )
                .is_err()
            );
            assert_eq!(bad.0, saved);
        }
    }
    #[test]
    fn metadata_format_and_noncanonical_inner() {
        let image = fixture();
        let original: BTreeMap<Vec<u8>, Vec<u8>> = super::super::decode(&image.0).unwrap();
        for version in [1u32, 2, 4, 14] {
            let mut map = original.clone();
            map.get_mut(&vec![0]).unwrap()[..4].copy_from_slice(&version.to_be_bytes());
            let mut b = SourceBudget::new(SourceAdmissionLimits::default());
            assert!(
                admit_projected_source(
                    &Image(wire(&map)),
                    &SourceAdmissionLimits::default(),
                    &mut b,
                    &mut || false
                )
                .is_err()
            );
        }
        for ns in [0u8, 5] {
            let mut map = original.clone();
            map.get_mut(&vec![ns]).unwrap().push(0);
            let mut b = SourceBudget::new(SourceAdmissionLimits::default());
            assert!(
                admit_projected_source(
                    &Image(wire(&map)),
                    &SourceAdmissionLimits::default(),
                    &mut b,
                    &mut || false
                )
                .is_err()
            );
        }
    }
    #[test]
    fn fixed_stack_refuses_recursion() {
        #[derive(Deserialize, Serialize)]
        struct Nested(Option<Box<Nested>>);
        let v = Nested(Some(Box::new(Nested(None))));
        assert!(run::<Nested>(&wire(&v), Role::Probe).0.is_ok());
        let mut v = Nested(None);
        for _ in 0..20 {
            v = Nested(Some(Box::new(v)))
        }
        assert!(matches!(
            run::<Nested>(&wire(&v), Role::Probe).0,
            Err(Failure::Limit)
        ));
    }
    pub(super) fn wide_fixture() -> Image {
        Image::new(9).unwrap().with_admin(|a|{let mut s=a.sql_session();s.execute("CREATE TABLE wide (a BYTEA NOT NULL,b BYTEA NOT NULL,c BYTEA NOT NULL,d BYTEA NOT NULL,e BYTEA NOT NULL,f BYTEA NOT NULL,g BYTEA NOT NULL,h BYTEA NOT NULL,PRIMARY KEY (a,b,c,d,e,f,g,h))",&[],None)?;for i in 0u8..64{let mut vs=vec![Value::Bytes(vec![0;256]);8];if let Value::Bytes(v)=&mut vs[0]{v[0]=i;}s.execute("INSERT INTO wide (a,b,c,d,e,f,g,h) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",&vs,None)?;}Ok(())}).unwrap().0
    }
    #[test]
    fn maximum_profile_trusted_admin() {
        let image = wide_fixture();
        assert!(image.0.len() <= INPUT);
        let saved = image.0.clone();
        let mut b = SourceBudget::new(SourceAdmissionLimits::default());
        let c = admit_projected_source(
            &image,
            &SourceAdmissionLimits::default(),
            &mut b,
            &mut || false,
        )
        .unwrap();
        assert!(c.costs.max_depth <= 12);
        assert_eq!(c.costs.allocations, 2904081);
        c.decode_canonical(&mut b, &mut || false).unwrap();
        assert_eq!(image.0, saved);
        println!(
            "max input={} events={} bytes={} depth={}",
            image.0.len(),
            b.used.work.events,
            b.used.work.bytes,
            b.used.max_depth
        );
    }
    #[test]
    fn consuming_move_cancellation() {
        let image = fixture();
        for at in [1, 2] {
            let mut b = SourceBudget::new(SourceAdmissionLimits::default());
            let c = admit_projected_source(
                &image,
                &SourceAdmissionLimits::default(),
                &mut b,
                &mut || false,
            )
            .unwrap();
            let used = b.used.allocations;
            let mut n = 0;
            assert!(matches!(
                c.decode_canonical(&mut b, &mut || {
                    n += 1;
                    n == at
                }),
                Err(CoreError::Cancelled)
            ));
            assert_eq!(b.used.allocations, used);
        }
    }
    #[test]
    fn custom_errors_never_format_display() {
        struct Bad;
        impl fmt::Display for Bad {
            fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
                panic!("untrusted formatting")
            }
        }
        assert_eq!(<Failure as de::Error>::custom(Bad), Failure::Invalid);
    }
    struct EnumSpy<'a> {
        tag: u32,
        calls: &'a Cell<usize>,
    }
    struct VariantSpy<'a>(&'a Cell<usize>);
    impl<'de> Deserializer<'de> for EnumSpy<'_> {
        type Error = Failure;
        fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Failure> {
            Err(Failure::Unsupported)
        }
        fn deserialize_enum<V: Visitor<'de>>(
            self,
            _: &'static str,
            _: &'static [&'static str],
            v: V,
        ) -> Result<V::Value, Failure> {
            v.visit_enum(self)
        }
        // Test backend only: every non-enum method goes to its rejecting any.
        serde::forward_to_deserialize_any! {bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct map struct identifier ignored_any}
    }
    impl<'de, 'a> EnumAccess<'de> for EnumSpy<'a> {
        type Error = Failure;
        type Variant = VariantSpy<'a>;
        fn variant_seed<S: DeserializeSeed<'de>>(
            self,
            s: S,
        ) -> Result<(S::Value, Self::Variant), Failure> {
            let d = serde::de::value::U32Deserializer::<Failure>::new(self.tag);
            Ok((s.deserialize(d)?, VariantSpy(self.calls)))
        }
    }
    impl<'de> VariantAccess<'de> for VariantSpy<'_> {
        type Error = Failure;
        fn unit_variant(self) -> Result<(), Failure> {
            self.0.set(self.0.get() + 1);
            Err(Failure::Invalid)
        }
        fn newtype_variant_seed<S: DeserializeSeed<'de>>(self, _: S) -> Result<S::Value, Failure> {
            self.0.set(self.0.get() + 1);
            Err(Failure::Invalid)
        }
        fn tuple_variant<V: Visitor<'de>>(self, _: usize, _: V) -> Result<V::Value, Failure> {
            self.0.set(self.0.get() + 1);
            Err(Failure::Invalid)
        }
        fn struct_variant<V: Visitor<'de>>(
            self,
            _: &'static [&'static str],
            _: V,
        ) -> Result<V::Value, Failure> {
            self.0.set(self.0.get() + 1);
            Err(Failure::Invalid)
        }
    }
    #[test]
    fn actual_derived_forbidden_tags_never_reach_variant_spy() {
        for role in [Role::Value, Role::Types] {
            for tag in 0..15 {
                let mut b = SourceBudget::new(SourceAdmissionLimits::default());
                let mut no = || false;
                let cx = Context {
                    budget: RefCell::new(&mut b),
                    cancel: RefCell::new(&mut no),
                    fault: Cell::new(None),
                    stack: RefCell::new([Role::Outer; DEPTH]),
                    columns: Cell::new(8),
                    input: Cell::new(4),
                    allocated: Cell::new(0),
                };
                let calls = Cell::new(0);
                let g = Gated {
                    inner: EnumSpy { tag, calls: &calls },
                    cx: &cx,
                    role,
                    depth: 0,
                };
                let failed = if role == Role::Value {
                    Value::deserialize(g).is_err()
                } else {
                    DataType::deserialize(g).is_err()
                };
                assert!(failed);
                let max = if role == Role::Value { 5 } else { 6 };
                assert_eq!(calls.get(), usize::from(tag < max));
            }
        }
    }
    #[test]
    fn owned_callbacks_failclosed() {
        let mut b = SourceBudget::new(SourceAdmissionLimits::default());
        let mut no = || false;
        let cx = Context {
            budget: RefCell::new(&mut b),
            cancel: RefCell::new(&mut no),
            fault: Cell::new(None),
            stack: RefCell::new([Role::Outer; DEPTH]),
            columns: Cell::new(8),
            input: Cell::new(4),
            allocated: Cell::new(0),
        };
        struct Never;
        impl Visitor<'_> for Never {
            type Value = ();
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("never")
            }
        }
        let w = Wrapped {
            inner: Never,
            cx: &cx,
            role: Role::Text,
            depth: 0,
            arity: None,
        };
        assert!(w.visit_string::<Failure>("a".into()).is_err());
        assert_eq!(cx.fault.get(), Some(Failure::Unsupported));
        let w = Wrapped {
            inner: Never,
            cx: &cx,
            role: Role::Bytes,
            depth: 0,
            arity: None,
        };
        assert!(w.visit_byte_buf::<Failure>(vec![1]).is_err());
        assert_eq!(cx.fault.get(), Some(Failure::Unsupported));
    }
    #[test]
    fn extra_optional_empty_namespace_refused_before_map_visitor() {
        let image = fixture();
        let mut map: BTreeMap<Vec<u8>, Vec<u8>> = super::super::decode(&image.0).unwrap();
        map.insert(vec![9], vec![]);
        let (r, c) = run::<BTreeMap<Vec<u8>, Vec<u8>>>(&wire(&map), Role::Outer);
        assert_eq!(r, Err(Failure::Limit));
        assert_eq!(c.visited, 0);
    }
    #[test]
    fn admitted_actual_serializer_matches_checked_wire_formula() {
        let image = fixture();
        let mut b = SourceBudget::new(SourceAdmissionLimits::default());
        let admitted = admit_projected_source(
            &image,
            &SourceAdmissionLimits::default(),
            &mut b,
            &mut || false,
        )
        .unwrap();
        // AFTER admission only: actual unchanged Relational serializer. Known
        // trusted fixture has table-name5, names2/4/4, C3/P1/R1, Ki9 and
        // Value wire widths12/17/15. No sizing allocation is used by admission.
        let table = 8 + (8 + 5) + 8 + ((21 + 2) + (21 + 4) + (21 + 4)) + 8 + 8;
        let sql = 36 + 8 + (8 + 5) + table + 8 + 8 + 8 + (24 + 9 + 12 + 17 + 15);
        assert_eq!(sql, 276);
        assert_eq!(
            super::super::codec()
                .serialized_size(&admitted.state.sql)
                .unwrap(),
            sql
        );
        assert_eq!(image.0.len() as u64, 42 + 45 + sql);
        for (v, n) in [
            (Value::Null, 4),
            (Value::Bool(true), 5),
            (Value::Int(4), 12),
            (Value::Text("abc".into()), 15),
            (Value::Bytes(vec![0; 3]), 15),
        ] {
            assert_eq!(super::super::codec().serialized_size(&v).unwrap(), n);
        }
    }
    #[test]
    fn primary_and_row_arity_boundaries() {
        for n in [8, 9] {
            assert_eq!(
                run::<Vec<usize>>(&wire(&vec![0usize; n]), Role::Primary)
                    .0
                    .is_ok(),
                n == 8
            );
            assert_eq!(
                run::<Vec<Value>>(&wire(&vec![Value::Null; n]), Role::Values)
                    .0
                    .is_ok(),
                n == 8
            );
        }
        let t = TableDef {
            id: 1,
            name: "items".into(),
            columns: vec![ColumnDef {
                id: 2,
                name: "id".into(),
                data_type: DataType::BigInt,
                not_null: true,
            }],
            primary_key: vec![0, 0],
        };
        assert!(matches!(
            run::<TableDef>(&wire(&t), Role::Table).0,
            Err(Failure::Limit)
        ));
    }
}
