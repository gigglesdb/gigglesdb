// Port of PostgreSQL 18.6 tsvector_op.c matching and phrase execution.
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group.
use crate::{
    Config, SearchResult, TsError, TsQuery, TsVector, plainto_tsquery,
    query::{Node, Operand, Operator},
    to_tsvector,
    vector::MAX_POS,
};

#[derive(Default)]
pub(crate) struct Phrase {
    pub(crate) positions: Vec<u16>,
    pub(crate) width: i32,
    pub(crate) negate: bool,
}

pub(crate) enum Hit {
    No,
    Maybe,
    Yes(Phrase),
}

impl Hit {
    pub(crate) fn present() -> Self {
        Self::Yes(Phrase::default())
    }
}

pub(crate) fn execute(node: &Node, check: &impl Fn(&Operand, bool) -> Hit) -> bool {
    node.fold_with(
        |node| match node {
            Node::Value(operand) => Some(matches!(check(operand, false), Hit::Yes(_))),
            Node::Binary(Operator::Phrase(_), ..) => {
                Some(matches!(phrase_execute(node, check), Hit::Yes(_)))
            }
            _ => None,
        },
        |op, left| match op {
            Operator::And => !*left,
            Operator::Or => *left,
            _ => unreachable!(),
        },
        |op, _, right| {
            // If the left arm did not short-circuit, the right arm decides
            // the AND or OR result.
            if op == Operator::Not { !right } else { right }
        },
    )
}

const LEFT: u8 = 1;
const RIGHT: u8 = 2;
const BOTH: u8 = 4;

fn merge_positions(left: &Phrase, right: &Phrase, offsets: (i32, i32), emit: u8) -> Vec<u16> {
    let mut output = Vec::new();
    let mut l = left
        .positions
        .iter()
        .map(|p| i32::from(p & MAX_POS) + offsets.0)
        .peekable();
    let mut r = right
        .positions
        .iter()
        .map(|p| i32::from(p & MAX_POS) + offsets.1)
        .peekable();
    while l.peek().is_some() || r.peek().is_some() {
        let a = l.peek().copied().unwrap_or(i32::MAX);
        let b = r.peek().copied().unwrap_or(i32::MAX);
        let (position, category) = match a.cmp(&b) {
            std::cmp::Ordering::Less => {
                l.next();
                (a, LEFT)
            }
            std::cmp::Ordering::Greater => {
                r.next();
                (b, RIGHT)
            }
            std::cmp::Ordering::Equal => {
                l.next();
                r.next();
                (a, BOTH)
            }
        };
        if position > 0 && emit & category != 0 {
            output.push(position as u16);
        }
    }
    output
}

fn phrase_execute(node: &Node, check: &impl Fn(&Operand, bool) -> Hit) -> Hit {
    node.fold_with(
        |node| match node {
            Node::Value(operand) => Some(check(operand, true)),
            _ => None,
        },
        |op, left| op != Operator::Or && matches!(left, Hit::No),
        |op, left, right| {
            if let Some(left) = left {
                phrase_join(op, left, right)
            } else {
                match right {
                    Hit::No => Hit::Yes(Phrase {
                        negate: true,
                        ..Phrase::default()
                    }),
                    Hit::Maybe => Hit::Maybe,
                    Hit::Yes(mut phrase) => {
                        phrase.negate = !phrase.negate;
                        if phrase.negate || !phrase.positions.is_empty() {
                            Hit::Yes(phrase)
                        } else {
                            Hit::No
                        }
                    }
                }
            }
        },
    )
}

fn phrase_join(op: Operator, left: Hit, right: Hit) -> Hit {
    let union = op == Operator::Or;
    if matches!(right, Hit::No) && (!union || matches!(left, Hit::No)) {
        return Hit::No;
    }
    if matches!(left, Hit::Maybe) || matches!(right, Hit::Maybe) {
        return Hit::Maybe;
    }
    let left = if let Hit::Yes(value) = left {
        value
    } else {
        Phrase::default()
    };
    let right = if let Hit::Yes(value) = right {
        value
    } else {
        Phrase::default()
    };
    let (width, offsets) = if let Operator::Phrase(distance) = op {
        let distance = i32::from(distance);
        (
            distance + left.width + right.width,
            (distance + right.width, 0),
        )
    } else {
        let width = left.width.max(right.width);
        (width, (width - left.width, width - right.width))
    };
    let (emit, negate) = match (union, left.negate, right.negate) {
        (false, false, false) | (true, true, true) => (BOTH, union),
        (false, true, true) | (true, false, false) => (BOTH | LEFT | RIGHT, !union),
        (false, true, false) | (true, false, true) => (RIGHT, union),
        (false, false, true) | (true, true, false) => (LEFT, union),
    };
    let positions = merge_positions(&left, &right, offsets, emit);
    if negate || !positions.is_empty() {
        Hit::Yes(Phrase {
            positions,
            width,
            negate,
        })
    } else {
        Hit::No
    }
}

pub(crate) fn locations(
    node: &Node,
    check: &impl Fn(&Operand, bool) -> Hit,
) -> Option<Vec<Phrase>> {
    node.fold_with(
        |node| match node {
            Node::Value(_) | Node::Binary(Operator::Phrase(_), ..) => {
                Some(match phrase_execute(node, check) {
                    Hit::Yes(phrase) if phrase.negate => Some(Vec::new()),
                    Hit::Yes(phrase) => Some(vec![phrase]),
                    Hit::No | Hit::Maybe => None,
                })
            }
            _ => None,
        },
        |op, left| op == Operator::And && left.is_none(),
        |op, left, right| match op {
            Operator::Not => right.is_none().then(Vec::new),
            Operator::And => {
                let mut left = left.flatten()?;
                left.extend(right?);
                Some(left)
            }
            Operator::Or => match (left.flatten(), right) {
                (None, None) => None,
                (Some(only), None) | (None, Some(only)) => Some(only),
                (Some(left), Some(right)) if left.is_empty() => Some(right),
                (Some(left), Some(right)) if right.is_empty() => Some(left),
                (Some(left), Some(right)) => {
                    let mut result = Vec::new();
                    for a in &left {
                        for b in &right {
                            result.push(Phrase {
                                positions: merge_positions(a, b, (0, 0), LEFT | RIGHT | BOTH),
                                width: a.width.max(b.width),
                                negate: false,
                            });
                        }
                    }
                    Some(result)
                }
            },
            Operator::Phrase(_) => unreachable!(),
        },
    )
}

pub fn ts_match(vector: &TsVector, query: &TsQuery) -> bool {
    query.root.as_deref().is_some_and(|root| {
        execute(root, &|operand, positions| {
            let mut hits = Vec::new();
            for lexeme in vector.matching(&operand.word, operand.prefix) {
                if lexeme.positions.is_empty() {
                    return if positions {
                        Hit::Maybe
                    } else {
                        Hit::present()
                    };
                }
                let matches = lexeme
                    .positions
                    .iter()
                    .filter(|pos| operand.weights == 0 || operand.weights & pos.weight.mask() != 0);
                if positions {
                    hits.extend(matches.map(|pos| pos.index));
                } else if matches.count() != 0 {
                    return Hit::present();
                }
            }
            if hits.is_empty() {
                Hit::No
            } else {
                hits.sort_unstable();
                hits.dedup();
                Hit::Yes(Phrase {
                    positions: hits,
                    ..Phrase::default()
                })
            }
        })
    })
}

pub fn ts_match_text(
    config: Config,
    text: &str,
    query: &TsQuery,
) -> Result<SearchResult<bool>, TsError> {
    let vector = to_tsvector(config, text)?;
    Ok(SearchResult {
        value: ts_match(&vector.value, query),
        notices: vector.notices,
    })
}

pub fn ts_match_text_text(
    config: Config,
    text: &str,
    query: &str,
) -> Result<SearchResult<bool>, TsError> {
    let query = plainto_tsquery(config, query)?;
    let mut result = ts_match_text(config, text, &query.value)?;
    let mut notices = query.notices;
    notices.append(&mut result.notices);
    Ok(SearchResult {
        value: result.value,
        notices,
    })
}
