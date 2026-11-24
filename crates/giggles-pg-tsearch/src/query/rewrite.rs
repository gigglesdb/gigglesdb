// Based on PostgreSQL 18.6 tsquery_rewrite.c and tsquery_util.c.
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group.
use super::*;

// QueryItem visits the right subtree first. Preserve that order for PHRASE
// and for rebuilding the binary tree after rewriting a flattened AND or OR.
#[derive(Clone)]
enum Tree {
    Word(Operand),
    Op(Operator, Vec<Self>),
}

impl Tree {
    fn from_node(node: &Node, flatten: bool, depth: usize) -> Result<Self, TsError> {
        if depth == 512 {
            return Err(TsError::new("54001", "stack depth limit exceeded"));
        }
        let child = |node| Self::from_node(node, flatten, depth + 1);
        Ok(match node {
            Node::Value(value) => Self::Word(value.clone()),
            Node::Not(node) => Self::Op(Operator::Not, vec![child(node)?]),
            Node::Binary(op, left, right) => {
                let mut children = Vec::new();
                for node in [right, left] {
                    match child(node)? {
                        Self::Op(inner, parts) if flatten && *op == inner && associative(*op) => {
                            children.extend(parts);
                        }
                        node => children.push(node),
                    }
                }
                Self::Op(*op, children)
            }
            Node::Stop => unreachable!("stop words removed before rewrite"),
        })
    }

    fn compare(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Word(a), Self::Word(b)) => crc(&b.word)
                .cmp(&crc(&a.word))
                .then_with(|| a.word.cmp(&b.word)),
            (Self::Word(_), _) => Ordering::Greater,
            (_, Self::Word(_)) => Ordering::Less,
            (Self::Op(a, ac), Self::Op(b, bc)) => b
                .id()
                .cmp(&a.id())
                .then_with(|| bc.len().cmp(&ac.len()))
                .then_with(|| {
                    ac.iter()
                        .zip(bc)
                        .find_map(|(a, b)| {
                            let order = a.compare(b);
                            (!order.is_eq()).then_some(order)
                        })
                        .unwrap_or(Ordering::Equal)
                })
                .then_with(|| match (a, b) {
                    (Operator::Phrase(a), Operator::Phrase(b)) => b.cmp(a),
                    _ => Ordering::Equal,
                }),
        }
    }

    fn sort(&mut self) {
        if let Self::Op(op, children) = self {
            children.iter_mut().for_each(Self::sort);
            if associative(*op) {
                crate::pg_sort::sort(children, &Self::compare);
            }
        }
    }

    fn replace(self, pattern: &Self, replacement: Option<&Self>) -> Option<Self> {
        if self.compare(pattern).is_eq() {
            return replacement.cloned();
        }
        let Self::Op(op, mut children) = self else {
            return Some(self);
        };
        let mut inserted = None;
        if let Self::Op(other, parts) = pattern
            && op == *other
            && associative(op)
            && children.len() > parts.len()
            && let Some(matched) = subset(&children, parts)
        {
            let mut index = 0;
            children.retain(|_| {
                let keep = !matched[index];
                index += 1;
                keep
            });
            inserted = replacement.cloned().map(|mut replacement| {
                replacement.sort();
                replacement
            });
        }
        let mut children = children
            .into_iter()
            .filter_map(|child| child.replace(pattern, replacement))
            .collect::<Vec<_>>();
        if let Some(inserted) = inserted {
            children.push(inserted);
            crate::pg_sort::sort(&mut children, &Self::compare);
        }
        match children.len() {
            0 => None,
            1 if op != Operator::Not => children.pop(),
            _ => Some(Self::Op(op, children)),
        }
    }

    fn into_branch(self) -> Branch {
        match self {
            Self::Word(value) => Branch::new(Node::Value(value)),
            Self::Op(Operator::Not, mut children) => {
                Branch::new(Node::Not(children.pop().unwrap().into_branch()))
            }
            Self::Op(op, children) => {
                let mut children = children
                    .into_iter()
                    .map(Self::into_branch)
                    .collect::<std::collections::VecDeque<_>>();
                while children.len() > 2 {
                    let right = children.pop_front().unwrap();
                    let left = children.pop_front().unwrap();
                    let tail = children.pop_back().unwrap();
                    children.push_front(tail);
                    children.push_front(Branch::new(Node::Binary(op, left, right)));
                }
                let left = children.pop_back().unwrap();
                let right = children.pop_back().unwrap();
                Branch::new(Node::Binary(op, left, right))
            }
        }
    }
}

fn associative(op: Operator) -> bool {
    matches!(op, Operator::And | Operator::Or)
}

fn subset(children: &[Tree], parts: &[Tree]) -> Option<Vec<bool>> {
    let mut matched = vec![false; children.len()];
    let mut required = parts.iter();
    let mut next = required.next()?;
    for (i, child) in children.iter().enumerate() {
        match child.compare(next) {
            Ordering::Less => {}
            Ordering::Greater => return None,
            Ordering::Equal => {
                matched[i] = true;
                let Some(part) = required.next() else {
                    return Some(matched);
                };
                next = part;
            }
        }
    }
    None
}

impl TsQuery {
    /// Replace a subquery throughout this query using PostgreSQL's tree order.
    pub fn rewrite(&self, pattern: &Self, replacement: &Self) -> Result<Self, TsError> {
        let (Some(root), Some(pattern)) = (self.root.as_deref(), pattern.root.as_deref()) else {
            return Ok(self.clone());
        };
        let mut root = Tree::from_node(root, true, 0)?;
        root.sort();
        let mut pattern = Tree::from_node(pattern, true, 0)?;
        pattern.sort();
        let replacement = replacement
            .root
            .as_deref()
            .map(|node| Tree::from_node(node, false, 0))
            .transpose()?;
        Ok(Self {
            root: root
                .replace(&pattern, replacement.as_ref())
                .map(Tree::into_branch),
        })
    }
}
