use super::*;

pub(super) fn scalar(n: &Node) -> bool {
    !matches!(n.kind, Kind::Array(_) | Kind::Object(_))
}
fn equal(a: &Node, b: &Node) -> bool {
    match (&a.kind, &b.kind) {
        (Kind::Null, Kind::Null) => true,
        (Kind::Bool(a), Kind::Bool(b)) => a == b,
        (Kind::String(a), Kind::String(b)) => a == b,
        (Kind::Number(a), Kind::Number(b)) => Numeric::parse(a).ok() == Numeric::parse(b).ok(),
        (Kind::Array(a), Kind::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| equal(a, b))
        }
        (Kind::Object(a), Kind::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|((ka, a), (kb, b))| ka == kb && equal(a, b))
        }
        _ => false,
    }
}
fn contains(a: &Node, b: &Node) -> bool {
    match (&a.kind, &b.kind) {
        (Kind::Object(_), Kind::Object(b)) => b.iter().all(|(k, b)| {
            a.field(k).is_some_and(|a| {
                if scalar(b) {
                    equal(a, b)
                } else {
                    !scalar(a) && contains(a, b)
                }
            })
        }),
        (Kind::Array(a), Kind::Array(b)) => b.iter().all(|b| {
            a.iter().any(|a| {
                if scalar(b) {
                    equal(a, b)
                } else {
                    !scalar(a) && contains(a, b)
                }
            })
        }),
        (Kind::Array(a), _) if scalar(b) => a.iter().any(|a| equal(a, b)),
        _ => equal(a, b),
    }
}
pub(super) fn index(s: &str) -> Option<i32> {
    // PostgreSQL strtol accepts leading ASCII whitespace and a sign, not trailing space.
    s.trim_start_matches([' ', '\t', '\n', '\r', '\x0b', '\x0c'])
        .parse()
        .ok()
}
pub(super) fn position(i: i32, len: usize) -> Option<usize> {
    usize::try_from(if i < 0 {
        len as i64 + i as i64
    } else {
        i as i64
    })
    .ok()
    .filter(|i| *i < len)
}
fn strip(n: &mut Node, arrays: bool) {
    match &mut n.kind {
        Kind::Object(v) => {
            v.retain(|(_, n)| !matches!(n.kind, Kind::Null));
            for (_, n) in v {
                strip(n, arrays)
            }
        }
        Kind::Array(v) => {
            if arrays {
                v.retain(|n| !matches!(n.kind, Kind::Null));
            }
            for n in v {
                strip(n, arrays)
            }
        }
        _ => {}
    }
}
impl Jsonb {
    /// SQL equality: exact numeric equality ignores display scale.
    pub fn sql_eq(&self, other: &Self) -> bool {
        equal(&self.node, &other.node)
    }
    pub fn contains(&self, other: &Self) -> bool {
        contains(&self.node, &other.node)
    }
    pub fn contained_by(&self, other: &Self) -> bool {
        other.contains(self)
    }
    /// Top-level object key or string array element (including raw scalar strings).
    pub fn exists(&self, key: &str) -> bool {
        match &self.node.kind {
            Kind::Object(_) => self.node.field(key).is_some(),
            Kind::Array(v) => v
                .iter()
                .any(|n| matches!(&n.kind,Kind::String(s) if s==key)),
            Kind::String(s) => s == key,
            _ => false,
        }
    }
    /// NULL elements are ignored, as for PostgreSQL text[].
    pub fn exists_any(&self, keys: &[Option<&str>]) -> bool {
        keys.iter().flatten().any(|k| self.exists(k))
    }
    pub fn exists_all(&self, keys: &[Option<&str>]) -> bool {
        keys.iter().flatten().all(|k| self.exists(k))
    }
    pub(super) fn check_path(&self, path: &[Option<&str>]) -> Result<()> {
        if path.len() > self.limits.nodes || path.len() > self.limits.depth.min(128) {
            return Err(Error::Limit);
        }
        let bytes = path
            .iter()
            .flatten()
            .try_fold(0usize, |n, s| n.checked_add(s.len()).ok_or(Error::Limit))?;
        if bytes > self.limits.input_bytes {
            return Err(Error::Limit);
        }
        Ok(())
    }
    /// Explicit one-dimensional text[]; None elements yield SQL NULL. A SQL NULL
    /// array itself is represented by the caller's Option, outside this strict API.
    pub fn path(&self, path: &[Option<&str>]) -> Result<Option<JsonbRef<'_>>> {
        self.check_path(path)?;
        let mut n = &self.node;
        for p in path {
            let Some(p) = p else { return Ok(None) };
            let next = match &n.kind {
                Kind::Object(_) => n.field(p),
                Kind::Array(_) => index(p).and_then(|i| n.element(i)),
                _ => None,
            };
            let Some(next) = next else { return Ok(None) };
            n = next;
        }
        Ok(Some(JsonbRef {
            node: n,
            limits: self.limits,
        }))
    }
    pub fn path_text(&self, path: &[Option<&str>]) -> Result<Option<String>> {
        self.selected_text(self.path(path)?.map(|v| v.node))
    }
    // Shrinking transforms preflight the entire source before cloning. Source
    // parse already checked nodes/depth/input; canonical output is checked here.
    pub(super) fn shrinking(&self, f: impl FnOnce(&mut Node)) -> Result<Self> {
        self.text()?;
        let mut node = self.node.clone();
        f(&mut node);
        Ok(Self {
            node,
            limits: self.limits,
        })
    }
    pub fn strip_nulls(&self, strip_arrays: bool) -> Result<Self> {
        self.shrinking(|n| strip(n, strip_arrays))
    }
    pub fn delete_key(&self, key: &str) -> Result<Self> {
        if scalar(&self.node) {
            return Err(Error::Shape);
        }
        self.shrinking(|n| match &mut n.kind {
            Kind::Object(v) => v.retain(|(k, _)| k != key),
            Kind::Array(v) => v.retain(|n| !matches!(&n.kind,Kind::String(s) if s==key)),
            _ => {}
        })
    }
    pub fn delete_index(&self, i: i32) -> Result<Self> {
        let Kind::Array(v) = &self.node.kind else {
            return Err(Error::Shape);
        };
        let i = position(i, v.len());
        self.shrinking(|n| {
            if let (Some(i), Kind::Array(v)) = (i, &mut n.kind) {
                v.remove(i);
            }
        })
    }
}
impl Jsonb {
    /// Nonrecursive concatenation. Rendering is bounded before the result tree
    /// is parsed; the parser charges every node before insertion.
    pub fn concat(&self, other: &Self) -> Result<Self> {
        let mut count = 1usize;
        if count > self.limits.nodes {
            return Err(Error::Limit);
        }
        for n in [&self.node, &other.node] {
            match (&self.node.kind, &other.node.kind, &n.kind) {
                (Kind::Object(_), Kind::Object(_), Kind::Object(v)) => {
                    for (_, child) in v {
                        if count >= self.limits.nodes {
                            return Err(Error::Limit);
                        }
                        count += 1;
                        super::mutation::admit(child, 1, &mut count, self.limits)?;
                    }
                }
                (_, _, Kind::Array(v)) => {
                    for child in v {
                        super::mutation::admit(child, 1, &mut count, self.limits)?;
                    }
                }
                _ => super::mutation::admit(n, 1, &mut count, self.limits)?,
            }
        }
        let mut out = String::new();
        match (&self.node.kind, &other.node.kind) {
            (Kind::Object(a), Kind::Object(b)) => {
                push(&mut out, "{", self.limits)?;
                for (i, (k, n)) in a.iter().chain(b).enumerate() {
                    if i > 0 {
                        push(&mut out, ",", self.limits)?;
                    }
                    quote(&mut out, k, self.limits)?;
                    push(&mut out, ":", self.limits)?;
                    render(n, &mut out, self.limits)?;
                }
                push(&mut out, "}", self.limits)?;
            }
            _ => {
                push(&mut out, "[", self.limits)?;
                let mut first = true;
                for n in [&self.node, &other.node] {
                    let nodes = match &n.kind {
                        Kind::Array(v) => v.as_slice(),
                        _ => std::slice::from_ref(n),
                    };
                    for n in nodes {
                        if !first {
                            push(&mut out, ",", self.limits)?;
                        }
                        first = false;
                        render(n, &mut out, self.limits)?;
                    }
                }
                push(&mut out, "]", self.limits)?;
            }
        }
        let result = Self::parse(&out, self.limits)?;
        result.text()?;
        Ok(result)
    }
}
fn compare(a: &Node, b: &Node) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    fn rank(n: &Node) -> u8 {
        match n.kind {
            Kind::Null => 0,
            Kind::String(_) => 1,
            Kind::Number(_) => 2,
            Kind::Bool(_) => 3,
            Kind::Array(_) => 4,
            Kind::Object(_) => 5,
        }
    }
    match (&a.kind, &b.kind) {
        (Kind::Null, Kind::Null) => Ordering::Equal,
        (Kind::String(a), Kind::String(b)) => a.as_bytes().cmp(b.as_bytes()),
        (Kind::Number(a), Kind::Number(b)) => Numeric::parse(a)
            .expect("validated Numeric")
            .cmp(&Numeric::parse(b).expect("validated Numeric")),
        (Kind::Bool(a), Kind::Bool(b)) => a.cmp(b),
        (Kind::Array(a), Kind::Array(b)) => a.len().cmp(&b.len()).then_with(|| {
            a.iter()
                .zip(b)
                .map(|(a, b)| compare(a, b))
                .find(|o| *o != Ordering::Equal)
                .unwrap_or(Ordering::Equal)
        }),
        (Kind::Object(a), Kind::Object(b)) => a.len().cmp(&b.len()).then_with(|| {
            a.iter()
                .zip(b)
                .map(|((ka, a), (kb, b))| {
                    ka.as_bytes().cmp(kb.as_bytes()).then_with(|| compare(a, b))
                })
                .find(|o| *o != Ordering::Equal)
                .unwrap_or(Ordering::Equal)
        }),
        _ => rank(a).cmp(&rank(b)),
    }
}
impl Jsonb {
    /// PostgreSQL ordering under C collation (UTF-8 bytes). Includes PostgreSQL's
    /// historical root empty-array/raw-scalar anomaly. No host locale is read.
    pub fn compare_c(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (&self.node.kind, &other.node.kind) {
            (Kind::Array(a), _) if a.is_empty() && scalar(&other.node) => Ordering::Less,
            (_, Kind::Array(b)) if b.is_empty() && scalar(&self.node) => Ordering::Greater,
            _ => compare(&self.node, &other.node),
        }
    }
}
impl Jsonb {
    /// Delete at a text[] path; NULL elements error only when reached.
    pub fn delete_path(&self, path: &[Option<&str>]) -> Result<Self> {
        self.check_path(path)?;
        if scalar(&self.node) {
            return Err(Error::Shape);
        }
        fn walk(n: &Node, p: &[Option<&str>]) -> Result<()> {
            if p.is_empty() {
                return Ok(());
            }
            let key = p[0].ok_or(Error::NullPath)?;
            match &n.kind {
                Kind::Array(v) => {
                    let i = index(key).ok_or(Error::Syntax)?;
                    if let Some(i) = position(i, v.len())
                        && p.len() > 1
                    {
                        walk(&v[i], &p[1..])?;
                    }
                }
                Kind::Object(_) if p.len() > 1 => {
                    if let Some(n) = n.field(key) {
                        walk(n, &p[1..])?;
                    }
                }
                _ => {}
            }
            Ok(())
        }
        fn remove(n: &mut Node, p: &[Option<&str>]) {
            if p.is_empty() {
                return;
            }
            let Some(key) = p[0] else { return };
            match &mut n.kind {
                Kind::Array(v) => {
                    if let Some(i) = index(key).and_then(|i| position(i, v.len())) {
                        if p.len() == 1 {
                            v.remove(i);
                        } else {
                            remove(&mut v[i], &p[1..]);
                        }
                    }
                }
                Kind::Object(v) => {
                    if p.len() == 1 {
                        v.retain(|(k, _)| k != key)
                    } else if let Some((_, n)) = v.iter_mut().find(|(k, _)| k == key) {
                        remove(n, &p[1..]);
                    }
                }
                _ => {}
            }
        }
        let empty = matches!(&self.node.kind,Kind::Array(v) if v.is_empty())
            || matches!(&self.node.kind,Kind::Object(v) if v.is_empty());
        if !empty {
            walk(&self.node, path)?;
        }
        self.shrinking(|n| {
            if !empty {
                remove(n, path)
            }
        })
    }
}
