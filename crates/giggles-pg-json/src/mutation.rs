use super::*;

// A plan borrows operands: no cloned subtree or enlarged tree exists during admission.
enum Edit<'a> {
    Replace(&'a Node, &'a Node),
    Array(&'a Node, usize, &'a Node),
    Object(&'a Node, &'a str, &'a Node),
    None,
}
fn plan<'a>(
    n: &'a Node,
    p: &'a [Option<&'a str>],
    value: &'a Node,
    create: bool,
    insert: Option<bool>,
) -> Result<Edit<'a>> {
    if p.is_empty() {
        return Ok(Edit::None);
    }
    let key = p[0].ok_or(Error::NullPath)?;
    let last = p.len() == 1;
    match &n.kind {
        Kind::Object(v) => {
            if let Some((_, child)) = v.iter().find(|(k, _)| k == key) {
                if last {
                    if insert.is_some() {
                        return Err(Error::Shape);
                    }
                    Ok(Edit::Replace(child, value))
                } else {
                    plan(child, &p[1..], value, create, insert)
                }
            } else if last && (create || insert.is_some()) {
                Ok(Edit::Object(n, key, value))
            } else {
                Ok(Edit::None)
            }
        }
        Kind::Array(v) => {
            let i = super::operators::index(key).ok_or(Error::Syntax)?;
            let pos = super::operators::position(i, v.len());
            if last {
                if let Some(j) = pos {
                    if let Some(after) = insert {
                        Ok(Edit::Array(n, j + usize::from(after), value))
                    } else {
                        Ok(Edit::Replace(&v[j], value))
                    }
                } else if create || insert.is_some() {
                    Ok(Edit::Array(n, if i < 0 { 0 } else { v.len() }, value))
                } else {
                    Ok(Edit::None)
                }
            } else if let Some(j) = pos {
                plan(&v[j], &p[1..], value, create, insert)
            } else {
                Ok(Edit::None)
            }
        }
        _ => Ok(Edit::None),
    }
}
struct Writer {
    out: String,
    count: usize,
    limits: Limits,
}
impl Writer {
    fn charge(&mut self, depth: usize) -> Result<()> {
        if depth > self.limits.depth.min(128) || self.count >= self.limits.nodes {
            return Err(Error::Limit);
        }
        self.count += 1;
        Ok(())
    }
    fn node(&mut self, n: &Node, edit: &Edit<'_>, depth: usize) -> Result<()> {
        if let Edit::Replace(old, new) = edit
            && std::ptr::eq(n, *old)
        {
            return self.node(new, &Edit::None, depth);
        }
        self.charge(depth)?;
        match &n.kind {
            Kind::Array(v) => {
                push(&mut self.out, "[", self.limits)?;
                let add = match edit {
                    Edit::Array(old, i, new) if std::ptr::eq(n, *old) => Some((*i, *new)),
                    _ => None,
                };
                let mut first = true;
                for i in 0..=v.len() {
                    if let Some((j, new)) = add
                        && i == j
                    {
                        if !first {
                            push(&mut self.out, ", ", self.limits)?;
                        }
                        first = false;
                        self.node(new, &Edit::None, depth + 1)?;
                    }
                    if let Some(child) = v.get(i) {
                        if !first {
                            push(&mut self.out, ", ", self.limits)?;
                        }
                        first = false;
                        self.node(child, edit, depth + 1)?;
                    }
                }
                push(&mut self.out, "]", self.limits)
            }
            Kind::Object(v) => {
                push(&mut self.out, "{", self.limits)?;
                let add = match edit {
                    Edit::Object(old, k, new) if std::ptr::eq(n, *old) => Some((*k, *new)),
                    _ => None,
                };
                for (i, (k, child)) in v
                    .iter()
                    .map(|(k, n)| (k.as_str(), n))
                    .chain(add)
                    .enumerate()
                {
                    self.charge(depth + 1)?;
                    if i > 0 {
                        push(&mut self.out, ", ", self.limits)?;
                    }
                    quote(&mut self.out, k, self.limits)?;
                    push(&mut self.out, ": ", self.limits)?;
                    self.node(child, edit, depth + 1)?;
                }
                push(&mut self.out, "}", self.limits)
            }
            _ => render(n, &mut self.out, self.limits),
        }
    }
}
impl Jsonb {
    fn mutate(
        &self,
        path: &[Option<&str>],
        value: &Self,
        create: bool,
        insert: Option<bool>,
    ) -> Result<Self> {
        self.check_path(path)?;
        if super::operators::scalar(&self.node) {
            return Err(Error::Shape);
        }
        let empty = matches!(&self.node.kind, Kind::Array(v) if v.is_empty())
            || matches!(&self.node.kind, Kind::Object(v) if v.is_empty());
        let edit = if empty && !create && insert.is_none() {
            Edit::None
        } else {
            plan(&self.node, path, &value.node, create, insert)?
        };
        let mut w = Writer {
            out: String::new(),
            count: 0,
            limits: self.limits,
        };
        w.node(&self.node, &edit, 0)?;
        // All output/node/depth admission precedes result tree construction.
        Self::parse(&w.out, self.limits)
    }
    /// PG18 jsonb_set; a one-dimensional text[] path, with SQL NULL elements.
    pub fn set(&self, path: &[Option<&str>], value: &Self, create_missing: bool) -> Result<Self> {
        self.mutate(path, value, create_missing, None)
    }
    /// PG18 jsonb_insert. Existing object keys error; array entries are retained.
    pub fn insert(&self, path: &[Option<&str>], value: &Self, insert_after: bool) -> Result<Self> {
        self.mutate(path, value, true, Some(insert_after))
    }
    /// PG18 lax SQL argument order. None is SQL NULL, including for the path array;
    /// Some(&[None]) is a non-NULL array containing a NULL element.
    pub fn set_lax(
        target: Option<&Self>,
        path: Option<&[Option<&str>]>,
        value: Option<&Self>,
        create_missing: Option<bool>,
        treatment: Option<&str>,
    ) -> Result<Option<Self>> {
        let (Some(target), Some(path), Some(create)) = (target, path, create_missing) else {
            return Ok(None);
        };
        let treatment = treatment.ok_or(Error::Shape)?;
        if let Some(value) = value {
            return target.set(path, value, create).map(Some);
        }
        match treatment {
            "raise_exception" => Err(Error::NullPath),
            "use_json_null" => target
                .set(path, &Self::parse("null", Limits::default())?, create)
                .map(Some),
            "delete_key" => target.delete_path(path).map(Some),
            "return_target" => target.shrinking(|_| {}).map(Some),
            _ => Err(Error::Shape),
        }
    }
}

// Admission for concat must precede even the temporary rendered expansion.
// Count duplicate object keys too, matching the parser's admission contract.
pub(super) fn admit(n: &Node, depth: usize, count: &mut usize, l: Limits) -> Result<()> {
    if depth > l.depth.min(128) || *count >= l.nodes {
        return Err(Error::Limit);
    }
    *count += 1;
    match &n.kind {
        Kind::Array(v) => {
            for child in v {
                admit(child, depth + 1, count, l)?;
            }
        }
        Kind::Object(v) => {
            for (_, child) in v {
                if *count >= l.nodes {
                    return Err(Error::Limit);
                }
                *count += 1;
                admit(child, depth + 1, count, l)?;
            }
        }
        _ => {}
    }
    Ok(())
}
