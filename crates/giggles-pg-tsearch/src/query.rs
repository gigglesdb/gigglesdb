// Port of PostgreSQL 18.6 tsquery.c, tsquery_op.c, tsquery_cleanup.c,
// tsquery_util.c and the query construction in to_tsany.c.
// Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group.
use std::{cmp::Ordering, collections::BTreeSet, fmt, str::FromStr};

use crate::{
    Config, Corrupt, SearchResult, TsError, Wire, config, locale, quote,
    vector::{MAX_DATA, MAX_WORD, literal_word},
};

mod rewrite;

const MAX_DISTANCE: i16 = 16384;
const MAX_NODES: usize = (0x3fffffff - 8) / 12;
// PostgreSQL checks max_stack_depth on recursive parenthesis parsing. The
// pure API uses the depth observed on the AM-6 PostgreSQL 18.6 oracle
// with max_stack_depth = 2 MB and a nested SQL function call.
const MAX_SCOPES: usize = 7671;
const STOP_NOTICE: &str =
    "text-search query contains only stop words or doesn't contain lexemes, ignored";

/// A query value as returned by PostgreSQL's full-text search functions.
///
/// The PostgreSQL btree comparison ignores operand weight masks and prefixes.
#[derive(Clone, Debug, Default)]
pub struct TsQuery {
    pub(crate) root: Option<Branch>,
}

#[derive(Clone, Debug)]
pub(crate) struct Operand {
    pub word: String,
    pub weights: u8,
    pub prefix: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Operator {
    Not,
    And,
    Or,
    Phrase(i16),
}

impl Operator {
    fn id(self) -> u8 {
        match self {
            Self::Not => 1,
            Self::And => 2,
            Self::Or => 3,
            Self::Phrase(_) => 4,
        }
    }

    fn priority(self) -> u8 {
        match self {
            Self::Or => 1,
            Self::And => 2,
            Self::Phrase(_) => 3,
            Self::Not => 4,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Node {
    Value(Operand),
    Stop,
    Not(Branch),
    Binary(Operator, Branch, Branch),
}

// A parsed query can have an arbitrarily long associative spine. Keep
// ownership and traversal of the tree off the caller's stack.
#[derive(Debug)]
pub(crate) struct Branch(Option<Box<Node>>);

impl Branch {
    fn new(node: Node) -> Self {
        Self(Some(Box::new(node)))
    }
}

impl std::ops::Deref for Branch {
    type Target = Node;

    fn deref(&self) -> &Node {
        self.0.as_deref().expect("live branch has a node")
    }
}

impl Clone for Branch {
    fn clone(&self) -> Self {
        self.fold(
            |node| match node {
                Node::Value(value) => Self::new(Node::Value(value.clone())),
                Node::Stop => Self::new(Node::Stop),
                _ => unreachable!(),
            },
            |op, left, right| {
                Self::new(match left {
                    Some(left) => Node::Binary(op, left, right),
                    None => Node::Not(right),
                })
            },
        )
    }
}

impl Drop for Branch {
    fn drop(&mut self) {
        let Some(root) = self.0.take() else { return };
        let mut pending = vec![root];
        while let Some(node) = pending.pop() {
            match *node {
                Node::Not(mut child) => pending.extend(child.0.take()),
                Node::Binary(_, mut left, mut right) => {
                    pending.extend(right.0.take());
                    pending.extend(left.0.take());
                }
                _ => (),
            }
        }
    }
}

enum Fold<'a> {
    Visit(&'a Node),
    AfterLeft(Operator, &'a Node),
    Join(Operator),
}

impl Node {
    fn preorder<'a>(&'a self, mut visit: impl FnMut(&'a Self)) {
        let mut pending = vec![self];
        while let Some(node) = pending.pop() {
            visit(node);
            match node {
                Self::Not(child) => pending.push(child),
                Self::Binary(_, left, right) => {
                    pending.push(left);
                    pending.push(right);
                }
                _ => (),
            }
        }
    }

    pub(crate) fn visit_operands<'a>(&'a self, visit: &mut impl FnMut(&'a Operand)) {
        self.preorder(|node| {
            if let Self::Value(value) = node {
                visit(value);
            }
        });
    }

    fn fold<'a, T>(
        &'a self,
        mut leaf: impl FnMut(&'a Node) -> T,
        join: impl FnMut(Operator, Option<T>, T) -> T,
    ) -> T {
        self.fold_with(
            |node| match node {
                Self::Value(_) | Self::Stop => Some(leaf(node)),
                _ => None,
            },
            |_, _| false,
            join,
        )
    }

    pub(crate) fn fold_with<'a, T>(
        &'a self,
        mut leaf: impl FnMut(&'a Node) -> Option<T>,
        mut skip_right: impl FnMut(Operator, &T) -> bool,
        mut join: impl FnMut(Operator, Option<T>, T) -> T,
    ) -> T {
        let mut pending = vec![Fold::Visit(self)];
        let mut values = Vec::new();
        while let Some(task) = pending.pop() {
            match task {
                Fold::Visit(node) => {
                    if let Some(value) = leaf(node) {
                        values.push(value);
                    } else {
                        match node {
                            Self::Not(child) => {
                                pending.push(Fold::Join(Operator::Not));
                                pending.push(Fold::Visit(child));
                            }
                            Self::Binary(op, left, right) => {
                                pending.push(Fold::AfterLeft(*op, right));
                                pending.push(Fold::Visit(left));
                            }
                            _ => unreachable!("every leaf must produce a value"),
                        }
                    }
                }
                Fold::AfterLeft(op, right) => {
                    if !skip_right(op, values.last().unwrap()) {
                        pending.push(Fold::Join(op));
                        pending.push(Fold::Visit(right));
                    }
                }
                Fold::Join(op) => {
                    let right = values.pop().unwrap();
                    let left = (op != Operator::Not).then(|| values.pop().unwrap());
                    values.push(join(op, left, right));
                }
            }
        }
        values.pop().unwrap()
    }

    fn size(&self) -> (usize, usize) {
        let (mut count, mut bytes) = (0, 0);
        self.preorder(|node| {
            count += 1;
            if let Self::Value(value) = node {
                bytes += value.word.len() + 1;
            }
        });
        (count, bytes)
    }

    fn operator(&self) -> Option<Operator> {
        match self {
            Self::Not(_) => Some(Operator::Not),
            Self::Binary(op, ..) => Some(*op),
            _ => None,
        }
    }

    fn bt_cmp(&self, other: &Self) -> Ordering {
        enum Compare<'a> {
            Pair(&'a Node, &'a Node),
            Distance(i16, i16),
        }

        let mut pending = vec![Compare::Pair(self, other)];
        while let Some(task) = pending.pop() {
            let order = match task {
                Compare::Pair(Self::Value(a), Self::Value(b)) => crc(&b.word)
                    .cmp(&crc(&a.word))
                    .then_with(|| a.word.cmp(&b.word)),
                Compare::Pair(Self::Not(a), Self::Not(b)) => {
                    pending.push(Compare::Pair(a, b));
                    Ordering::Equal
                }
                Compare::Pair(Self::Binary(a, al, ar), Self::Binary(b, bl, br)) => {
                    let order = b.id().cmp(&a.id());
                    if order.is_eq() {
                        if let (Operator::Phrase(a), Operator::Phrase(b)) = (a, b) {
                            pending.push(Compare::Distance(*a, *b));
                        }
                        pending.push(Compare::Pair(al, bl));
                        pending.push(Compare::Pair(ar, br));
                    }
                    order
                }
                Compare::Pair(a, b) => b
                    .operator()
                    .map(Operator::id)
                    .cmp(&a.operator().map(Operator::id)),
                Compare::Distance(a, b) => b.cmp(&a),
            };
            if !order.is_eq() {
                return order;
            }
        }
        Ordering::Equal
    }

    fn display(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        enum Emit<'a> {
            Node(&'a Node, u8, bool),
            Op(Operator),
            Close,
        }
        let mut pending = vec![Emit::Node(self, 0, false)];
        while let Some(task) = pending.pop() {
            match task {
                Emit::Node(Self::Value(operand), ..) => {
                    quote(f, &operand.word)?;
                    if operand.prefix || operand.weights != 0 {
                        f.write_str(":")?;
                        if operand.prefix {
                            f.write_str("*")?;
                        }
                        for (bit, letter) in [(8, "A"), (4, "B"), (2, "C"), (1, "D")] {
                            if operand.weights & bit != 0 {
                                f.write_str(letter)?;
                            }
                        }
                    }
                }
                Emit::Node(Self::Not(child), ..) => {
                    f.write_str("!")?;
                    pending.push(Emit::Node(child, Operator::Not.priority(), false));
                }
                Emit::Node(Self::Binary(op, left, right), parent, right_phrase) => {
                    let priority = op.priority();
                    let phrase = matches!(op, Operator::Phrase(_));
                    if priority < parent || (phrase && right_phrase) {
                        f.write_str("( ")?;
                        pending.push(Emit::Close);
                    }
                    pending.push(Emit::Node(right, priority, phrase));
                    pending.push(Emit::Op(*op));
                    pending.push(Emit::Node(left, priority, false));
                }
                Emit::Op(Operator::And) => f.write_str(" & ")?,
                Emit::Op(Operator::Or) => f.write_str(" | ")?,
                Emit::Op(Operator::Phrase(1)) => f.write_str(" <-> ")?,
                Emit::Op(Operator::Phrase(distance)) => write!(f, " <{distance}> ")?,
                Emit::Close => f.write_str(" )")?,
                _ => unreachable!(),
            }
        }
        Ok(())
    }

    fn send(&self, buf: &mut Vec<u8>) {
        self.preorder(|node| match node {
            Self::Value(value) => {
                buf.extend_from_slice(&[1, value.weights, u8::from(value.prefix)]);
                buf.extend_from_slice(value.word.as_bytes());
                buf.push(0);
            }
            Self::Not(_) => buf.extend_from_slice(&[2, Operator::Not.id()]),
            Self::Binary(op, ..) => {
                buf.extend_from_slice(&[2, op.id()]);
                if let Operator::Phrase(distance) = op {
                    buf.extend_from_slice(&distance.to_be_bytes());
                }
            }
            Self::Stop => unreachable!(),
        });
    }
}

// PostgreSQL's legacy CRC uses the Ethernet lookup table and shifts left.
// Based on PostgreSQL 18.6 utils/pg_crc.h and pg_crc.c.
fn crc(word: &str) -> i32 {
    const TABLE: [u32; 256] = {
        let mut table = [0; 256];
        let mut index = 0;
        while index < table.len() {
            let mut value = index as u32;
            let mut bit = 0;
            while bit < 8 {
                value = (value >> 1) ^ (0xedb8_8320 * (value & 1));
                bit += 1;
            }
            table[index] = value;
            index += 1;
        }
        table
    };
    let mut value = u32::MAX;
    for byte in word.bytes() {
        let index = ((value >> 24) as u8) ^ byte;
        value = (value << 8) ^ TABLE[index as usize];
    }
    !value as i32
}

fn depth_error() -> TsError {
    TsError {
        sqlstate: "54001",
        message: "stack depth limit exceeded".into(),
        detail: None,
        hint: Some("Increase the configuration parameter \"max_stack_depth\" (currently 2048kB), after ensuring the platform's stack depth limit is adequate.".into()),
    }
}

fn distance_error() -> TsError {
    TsError::new(
        "22023",
        format!(
            "distance in phrase operator must be an integer value between zero and {MAX_DISTANCE} inclusive"
        ),
    )
}

fn size_error() -> TsError {
    TsError::new("54000", "tsquery is too large")
}

impl TsQuery {
    fn new(root: Option<Branch>) -> Result<Self, TsError> {
        if let Some(node) = &root {
            let (nodes, bytes) = node.size();
            if nodes > MAX_NODES || nodes * 12 + bytes + 8 > 0x3fffffff {
                return Err(size_error());
            }
        }
        Ok(Self { root })
    }

    pub fn numnode(&self) -> usize {
        self.root.as_ref().map_or(0, |node| node.size().0)
    }

    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    fn rebuild(root: Branch) -> Result<Self, TsError> {
        // QTN2QT checks each operand's start offset as it repacks the tree.
        // The final operand itself can extend beyond MAXSTRPOS.
        let mut end = 0;
        let mut start = 0;
        root.visit_operands(&mut |operand| {
            start = end;
            end += operand.word.len() + 1;
        });
        if start > MAX_DATA {
            return Err(size_error());
        }
        Self::new(Some(root))
    }

    fn join(&self, other: &Self, op: Operator) -> Result<Self, TsError> {
        match (&self.root, &other.root) {
            (Some(left), Some(right)) => {
                Self::rebuild(Branch::new(Node::Binary(op, left.clone(), right.clone())))
            }
            _ => Ok(Self {
                root: self.root.clone().or_else(|| other.root.clone()),
            }),
        }
    }

    pub fn and(&self, other: &Self) -> Result<Self, TsError> {
        self.join(other, Operator::And)
    }

    pub fn or(&self, other: &Self) -> Result<Self, TsError> {
        self.join(other, Operator::Or)
    }

    pub fn negate(&self) -> Result<Self, TsError> {
        match &self.root {
            Some(child) => Self::rebuild(Branch::new(Node::Not(child.clone()))),
            None => Ok(Self::default()),
        }
    }

    pub fn phrase(&self, other: &Self, distance: Option<i32>) -> Result<Self, TsError> {
        let distance = distance.unwrap_or(1);
        if !(0..=i32::from(MAX_DISTANCE)).contains(&distance) {
            return Err(distance_error());
        }
        self.join(other, Operator::Phrase(distance as i16))
    }

    pub fn contains(&self, other: &Self) -> bool {
        fn terms(node: Option<&Node>) -> BTreeSet<&str> {
            let mut words = BTreeSet::new();
            if let Some(node) = node {
                node.visit_operands(&mut |operand| {
                    words.insert(operand.word.as_str());
                });
            }
            words
        }
        terms(other.root.as_deref()).is_subset(&terms(self.root.as_deref()))
    }

    pub fn contained_by(&self, other: &Self) -> bool {
        other.contains(self)
    }

    pub fn querytree(&self) -> String {
        let Some(root) = &self.root else {
            return String::new();
        };
        let clean = root.fold(
            |node| match node {
                Node::Value(value) => Some(Branch::new(Node::Value(value.clone()))),
                _ => None,
            },
            |op, left, right| {
                if op == Operator::Not {
                    return None;
                }
                match (left.flatten(), right) {
                    (Some(left), Some(right)) => Some(Branch::new(Node::Binary(op, left, right))),
                    (left, right) if op != Operator::Or => left.or(right),
                    _ => None,
                }
            },
        );
        clean.map_or_else(
            || "T".to_owned(),
            |root| Self { root: Some(root) }.to_string(),
        )
    }

    pub fn send(&self) -> Vec<u8> {
        let mut buf = (self.numnode() as u32).to_be_bytes().to_vec();
        if let Some(root) = &self.root {
            root.send(&mut buf);
        }
        buf
    }

    /// Storage version 1: the send payload, including zero-node queries.
    pub fn to_storage(&self) -> Vec<u8> {
        let mut bytes = vec![1];
        bytes.extend(self.send());
        bytes
    }

    /// Decode the canonical payload without applying wire-only rejections.
    pub fn from_storage(bytes: &[u8]) -> Result<Self, Corrupt> {
        let Some((1, payload)) = bytes.split_first() else {
            return Err(Corrupt);
        };
        if payload == [0; 4] {
            return Ok(Self::default());
        }
        Self::decode(payload, true).map_err(|_| Corrupt)
    }

    pub fn recv(bytes: &[u8]) -> Result<Self, TsError> {
        Self::decode(bytes, false)
    }

    fn decode(bytes: &[u8], storage: bool) -> Result<Self, TsError> {
        enum Item {
            Value(Operand),
            Op(Operator),
        }

        let mut wire = Wire(bytes);
        let count = u32::from_be_bytes(wire.read()?) as usize;
        if count == 0 {
            return Err(TsError::internal("malformed tsquery: operand not found"));
        }
        if count > MAX_NODES {
            return Err(TsError::internal("invalid size of tsquery"));
        }
        let mut items = Vec::with_capacity(count.min(wire.0.len() / 2));
        let mut data_len = 0;
        for i in 0..count {
            let [kind] = wire.read()?;
            let item = match kind {
                1 => {
                    let [weights, prefix] = wire.read()?;
                    if storage && prefix > 1 {
                        return Err(TsError::internal("invalid stored tsquery prefix flag"));
                    }
                    let word = wire.string()?;
                    if weights > 15 {
                        return Err(TsError::internal("invalid tsquery: invalid weight bitmap"));
                    }
                    if word.len() > MAX_WORD {
                        return Err(TsError::internal("invalid tsquery: operand too long"));
                    }
                    if data_len > MAX_DATA {
                        return Err(TsError::internal(
                            "invalid tsquery: total operand length exceeded",
                        ));
                    }
                    data_len += word.len() + 1;
                    Item::Value(Operand {
                        word: word.to_owned(),
                        weights,
                        prefix: prefix != 0,
                    })
                }
                2 => {
                    let [op] = wire.read()?;
                    if !(1..=4).contains(&op) {
                        return Err(TsError::internal(format!(
                            "invalid tsquery: unrecognized operator type {}",
                            op as i8
                        )));
                    }
                    if i + 1 == count {
                        return Err(TsError::internal("invalid pointer to right operand"));
                    }
                    Item::Op(match op {
                        1 => Operator::Not,
                        2 => Operator::And,
                        3 => Operator::Or,
                        _ => Operator::Phrase(i16::from_be_bytes(wire.read()?)),
                    })
                }
                _ => {
                    return Err(TsError::internal(format!(
                        "unrecognized tsquery node type: {}",
                        kind as i8
                    )));
                }
            };
            items.push(item);
        }
        let mut stack = Vec::new();
        for item in items.into_iter().rev() {
            let missing = || TsError::internal("malformed tsquery: operand not found");
            let node = match item {
                Item::Value(value) => Node::Value(value),
                Item::Op(Operator::Not) => Node::Not(stack.pop().ok_or_else(missing)?),
                Item::Op(op) => {
                    let right = stack.pop().ok_or_else(missing)?;
                    let left = stack.pop().ok_or_else(missing)?;
                    Node::Binary(op, left, right)
                }
            };
            stack.push(Branch::new(node));
        }
        if stack.len() > 1 {
            return Err(TsError::internal("malformed tsquery: extra nodes"));
        }
        wire.finish()?;
        Ok(Self { root: stack.pop() })
    }
}

impl fmt::Display for TsQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.root.as_ref().map_or(Ok(()), |node| node.display(f))
    }
}

impl Ord for TsQuery {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.root, &other.root) {
            (Some(a), Some(b)) => a.size().cmp(&b.size()).then_with(|| a.bt_cmp(b)),
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => Ordering::Equal,
        }
    }
}

impl PartialOrd for TsQuery {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TsQuery {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for TsQuery {}

pub(crate) fn is_operator(c: char) -> bool {
    matches!(c, '!' | '&' | '|' | '(' | ')' | '<')
}

fn phrase_operator(input: &str) -> Result<Option<(Operator, &str)>, TsError> {
    let Some(tail) = input.strip_prefix('<') else {
        return Ok(None);
    };
    let (distance, tail) = if let Some(tail) = tail.strip_prefix('-') {
        (1, tail)
    } else {
        let end = tail
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(tail.len());
        if end == 0 {
            return Ok(None);
        }
        let distance = tail[..end].parse::<i16>().map_err(|_| distance_error())?;
        if distance > MAX_DISTANCE {
            return Err(distance_error());
        }
        (distance, &tail[end..])
    };
    Ok(tail
        .strip_prefix('>')
        .filter(|s| !s.is_empty())
        .map(|tail| (Operator::Phrase(distance), tail)))
}

fn web_or(input: &str) -> bool {
    let Some(head) = input.as_bytes().get(..2) else {
        return false;
    };
    if !head.eq_ignore_ascii_case(b"or") {
        return false;
    }
    let mut tail = input[2..].chars();
    tail.next()
        .is_some_and(|c| !matches!(c, '-' | '_') && !c.is_ascii_digit() && !locale::alpha(c))
        && tail.any(|c| !locale::ascii_space(c))
}

enum Token {
    Value(String, u8, bool),
    Op(Operator),
    Open,
    Close,
    Stop,
    End,
}

struct Scanner<'a> {
    full: &'a str,
    rest: &'a str,
    expect_value: bool,
    first: bool,
    nesting: usize,
    web: bool,
}

impl<'a> Scanner<'a> {
    fn syntax(&self) -> TsError {
        TsError::syntax("tsquery", self.full)
    }

    fn next(&mut self) -> Result<Token, TsError> {
        loop {
            self.rest = self.rest.trim_start_matches(locale::ascii_space);
            let Some(c) = self.rest.chars().next() else {
                return if self.web && self.expect_value && !self.first {
                    self.expect_value = false;
                    Ok(Token::Stop)
                } else if !self.web && self.expect_value && !self.first {
                    Err(TsError::new(
                        "42601",
                        format!("no operand in tsquery: \"{}\"", self.full),
                    ))
                } else if self.nesting > 0 {
                    Err(self.syntax())
                } else {
                    Ok(Token::End)
                };
            };

            if self.expect_value {
                self.first = false;
                if (!self.web && c == '!') || (self.web && c == '-') {
                    self.rest = &self.rest[1..];
                    return Ok(Token::Op(Operator::Not));
                }
                if self.web && c == '"' {
                    let tail = &self.rest[1..];
                    let (word, rest) = tail.split_once('"').unwrap_or((tail, ""));
                    self.rest = rest;
                    self.expect_value = false;
                    return Ok(Token::Value(word.to_owned(), 0, false));
                }
                if self.web && is_operator(c) {
                    self.rest = &self.rest[1..];
                    continue;
                }
                if !self.web && c == '(' {
                    self.rest = &self.rest[1..];
                    self.nesting += 1;
                    return Ok(Token::Open);
                }
                if c == ':' && !self.web {
                    return Err(self.syntax());
                }
                let (word, rest) = literal_word(self.rest, true, self.web).map_err(|err| {
                    if err.message.starts_with("there is no escaped") {
                        TsError::new(
                            "42601",
                            format!("there is no escaped character: \"{}\"", self.full),
                        )
                    } else {
                        self.syntax()
                    }
                })?;
                self.rest = rest;
                self.expect_value = false;
                let mut weights = 0;
                let mut prefix = false;
                if !self.web
                    && let Some(tail) = rest.strip_prefix(':')
                {
                    self.rest = tail;
                    while let Some(c) = self.rest.as_bytes().first().map(u8::to_ascii_uppercase) {
                        match c {
                            b'A'..=b'D' => weights |= 8 >> (c - b'A'),
                            b'*' => prefix = true,
                            _ => break,
                        }
                        self.rest = &self.rest[1..];
                    }
                }
                return Ok(Token::Value(word, weights, prefix));
            }
            if self.web {
                if web_or(self.rest) {
                    self.rest = &self.rest[2..];
                    self.expect_value = true;
                    return Ok(Token::Op(Operator::Or));
                }
                if is_operator(c) {
                    self.rest = &self.rest[1..];
                    continue;
                }
                self.expect_value = true;
                return Ok(Token::Op(Operator::And));
            }
            match c {
                '|' | '&' => {
                    self.rest = &self.rest[1..];
                    self.expect_value = true;
                    return Ok(Token::Op(if c == '|' {
                        Operator::Or
                    } else {
                        Operator::And
                    }));
                }
                ')' if self.nesting > 0 => {
                    self.rest = &self.rest[1..];
                    self.nesting -= 1;
                    return Ok(Token::Close);
                }
                '<' => {
                    let Some((op, tail)) = phrase_operator(self.rest)? else {
                        return Err(self.syntax());
                    };
                    self.rest = tail;
                    self.expect_value = true;
                    return Ok(Token::Op(op));
                }
                _ => return Err(self.syntax()),
            }
        }
    }
}

struct QueryParser<'a> {
    scanner: Scanner<'a>,
    config: Option<Config>,
    notices: Vec<TsError>,
    operand_bytes: usize,
    has_stops: bool,
}

#[derive(Default)]
struct Scope {
    nodes: Vec<Branch>,
    operators: Vec<Operator>,
}

impl Scope {
    fn push_operator(&mut self, op: Operator) -> Result<(), TsError> {
        while self.operators.last().is_some_and(|last| {
            last.priority() > op.priority()
                || (*last != Operator::Not && last.priority() == op.priority())
        }) {
            combine(&mut self.nodes, self.operators.pop().unwrap())?;
        }
        if self.operators.len() == 32 {
            return Err(TsError::internal("tsquery stack too small"));
        }
        self.operators.push(op);
        Ok(())
    }

    fn finish(mut self) -> Result<Option<Branch>, TsError> {
        for op in self.operators.into_iter().rev() {
            combine(&mut self.nodes, op)?;
        }
        Ok(self.nodes.pop())
    }
}

fn combine(nodes: &mut Vec<Branch>, op: Operator) -> Result<(), TsError> {
    let missing = || TsError::internal("malformed tsquery: operand not found");
    let right = nodes.pop().ok_or_else(missing)?;
    let node = if op == Operator::Not {
        Node::Not(right)
    } else {
        let left = nodes.pop().ok_or_else(missing)?;
        Node::Binary(op, left, right)
    };
    nodes.push(Branch::new(node));
    Ok(())
}

impl QueryParser<'_> {
    fn value(&mut self, word: String, weights: u8, prefix: bool) -> Result<Branch, TsError> {
        if word.len() >= MAX_WORD {
            return Err(TsError::new(
                "54000",
                format!("word is too long in tsquery: \"{}\"", self.scanner.full),
            ));
        }
        if self.operand_bytes >= MAX_DATA {
            return Err(TsError::new(
                "54000",
                format!("value is too big in tsquery: \"{}\"", self.scanner.full),
            ));
        }
        self.operand_bytes += word.len() + 1;
        Ok(Branch::new(Node::Value(Operand {
            word,
            weights,
            prefix,
        })))
    }

    fn morph(
        &mut self,
        word: String,
        weights: u8,
        prefix: bool,
        op: Operator,
    ) -> Result<Branch, TsError> {
        let Some(config) = self.config else {
            return self.value(word, weights, prefix);
        };
        let words = config::lexemes(config, &word, &mut self.notices);
        let mut nodes = Vec::new();
        let mut previous = 0;
        let mut words = words.into_iter().peekable();
        while let Some((position, word)) = words.next() {
            while previous > 0 && previous + 1 < position {
                nodes.push(Branch::new(Node::Stop));
                self.has_stops = true;
                combine(&mut nodes, op)?;
                previous += 1;
            }
            let mut group = vec![self.value(word, weights, prefix)?];
            while words.peek().is_some_and(|(next, _)| *next == position) {
                let (_, word) = words.next().unwrap();
                group.push(self.value(word, weights, prefix)?);
                combine(&mut group, Operator::And)?;
            }
            nodes.push(group.pop().unwrap());
            if nodes.len() > 1 {
                combine(&mut nodes, op)?;
            }
            previous = position;
        }
        Ok(nodes.pop().unwrap_or_else(|| {
            self.has_stops = true;
            Branch::new(Node::Stop)
        }))
    }

    fn parse_scope(&mut self) -> Result<Option<Branch>, TsError> {
        let mut scope = Scope::default();
        let mut parents = Vec::new();
        loop {
            match self.scanner.next()? {
                Token::End => return scope.finish(),
                Token::Open => {
                    if parents.len() == MAX_SCOPES {
                        return Err(depth_error());
                    }
                    parents.push(std::mem::take(&mut scope));
                }
                Token::Close => {
                    let branch = scope.finish()?.ok_or_else(|| self.scanner.syntax())?;
                    scope = parents.pop().expect("close is preceded by open");
                    scope.nodes.push(branch);
                }
                Token::Stop => {
                    self.has_stops = true;
                    scope.nodes.push(Branch::new(Node::Stop));
                }
                Token::Value(word, weights, prefix) => {
                    scope
                        .nodes
                        .push(self.morph(word, weights, prefix, Operator::Phrase(1))?);
                }
                Token::Op(op) => scope.push_operator(op)?,
            }
        }
    }
}

type StopCleanup = (Option<Branch>, i32, i32);

fn remove_stops(node: Branch) -> StopCleanup {
    node.fold(
        |leaf| match leaf {
            Node::Stop => (None, 0, 0),
            Node::Value(value) => (Some(Branch::new(Node::Value(value.clone()))), 0, 0),
            _ => unreachable!(),
        },
        |op, left, right| {
            let (right, rl, rr) = right;
            let Some((left, ll, lr)) = left else {
                return (right.map(|child| Branch::new(Node::Not(child))), rl, rr);
            };
            if let Operator::Phrase(n) = op {
                let n = i32::from(n);
                match (left, right) {
                    (None, None) => {
                        let adjustment = ll + n + rl;
                        (None, adjustment, adjustment)
                    }
                    (None, right) => (right, ll + n + rl, rr),
                    (left, None) => (left, ll, lr + n + rr),
                    (Some(left), Some(right)) => {
                        let distance = (n + lr + rl) as i16;
                        (
                            Some(Branch::new(Node::Binary(
                                Operator::Phrase(distance),
                                left,
                                right,
                            ))),
                            ll,
                            rr,
                        )
                    }
                }
            } else {
                match (left, right) {
                    (None, None) => (None, ll.max(rl), ll.max(rl)),
                    (None, right) => (right, rl, rr),
                    (left, None) => (left, ll, lr),
                    (Some(left), Some(right)) => {
                        (Some(Branch::new(Node::Binary(op, left, right))), 0, 0)
                    }
                }
            }
        },
    )
}

fn parse(
    input: &str,
    config: Option<Config>,
    web: bool,
    plain: Option<Operator>,
) -> Result<SearchResult<TsQuery>, TsError> {
    let mut parser = QueryParser {
        scanner: Scanner {
            full: input,
            rest: input,
            expect_value: true,
            first: true,
            nesting: 0,
            web,
        },
        config,
        notices: Vec::new(),
        operand_bytes: 0,
        has_stops: false,
    };
    let result = match plain {
        Some(op) if !input.is_empty() => Some(parser.morph(input.to_owned(), 0, false, op)?),
        Some(_) => None,
        None => parser.parse_scope()?,
    };
    let root = if let Some(node) = result {
        if parser.has_stops {
            let (node, _, _) = remove_stops(node);
            if node.is_none() {
                parser.notices.push(TsError::new("00000", STOP_NOTICE));
            }
            node
        } else {
            Some(node)
        }
    } else {
        parser.notices.push(TsError::new(
            "00000",
            format!("text-search query doesn't contain lexemes: \"{input}\""),
        ));
        None
    };
    Ok(SearchResult {
        value: TsQuery::new(root)?,
        notices: parser.notices,
    })
}

impl TsQuery {
    pub fn parse(input: &str) -> Result<SearchResult<Self>, TsError> {
        parse(input, None, false, None)
    }
}

impl FromStr for TsQuery {
    type Err = TsError;

    fn from_str(s: &str) -> Result<Self, TsError> {
        Ok(Self::parse(s)?.value)
    }
}

pub fn to_tsquery(config: Config, input: &str) -> Result<SearchResult<TsQuery>, TsError> {
    parse(input, Some(config), false, None)
}

pub fn plainto_tsquery(config: Config, input: &str) -> Result<SearchResult<TsQuery>, TsError> {
    parse(input, Some(config), false, Some(Operator::And))
}

pub fn phraseto_tsquery(config: Config, input: &str) -> Result<SearchResult<TsQuery>, TsError> {
    parse(input, Some(config), false, Some(Operator::Phrase(1)))
}

pub fn websearch_to_tsquery(config: Config, input: &str) -> Result<SearchResult<TsQuery>, TsError> {
    parse(input, Some(config), true, None)
}
