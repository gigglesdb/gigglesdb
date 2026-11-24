// Port of PostgreSQL 18.6 tsrank.c.
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group.
use std::collections::HashMap;

use crate::{
    Position, TsError, TsQuery, TsVector, Weight,
    matching::{Hit, Phrase, execute},
    query::{Node, Operand, Operator},
    vector::MAX_POS,
};

const DEFAULT_WEIGHTS: [f32; 4] = [0.1, 0.2, 0.4, 1.0];
const LOG_LENGTH: i32 = 1;
const LENGTH: i32 = 2;
const EXTENT_DISTANCE: i32 = 4;
const UNIQUE: i32 = 8;
const LOG_UNIQUE: i32 = 16;
const DIVIDE_BY_RANK_PLUS_ONE: i32 = 32;

fn weights(input: Option<&[Option<f32>]>) -> Result<[f32; 4], TsError> {
    let Some(input) = input else {
        return Ok(DEFAULT_WEIGHTS);
    };
    if input.is_empty() {
        return Err(TsError::new(
            "2202E",
            "array of weight must be one-dimensional",
        ));
    }
    if input.len() < 4 {
        return Err(TsError::new("2202E", "array of weight is too short"));
    }
    if input.iter().any(Option::is_none) {
        return Err(TsError::new(
            "22004",
            "array of weight must not contain nulls",
        ));
    }
    let mut weights = DEFAULT_WEIGHTS;
    for (i, value) in input[..4].iter().flatten().copied().enumerate() {
        if value >= 0.0 {
            weights[i] = value;
        }
        if weights[i] > 1.0 {
            return Err(TsError::new("22023", "weight out of range"));
        }
    }
    Ok(weights)
}

fn document_length(vector: &TsVector) -> usize {
    vector
        .lexemes()
        .iter()
        .map(|entry| entry.positions.len().max(1))
        .sum()
}

fn word_distance(distance: u16) -> f32 {
    if distance > 100 {
        1e-30
    } else {
        (1.0 / (1.005 + 0.05 * (f64::from(distance) / 1.5 - 2.0).exp())) as f32
    }
}

fn rank_or(weights: [f32; 4], vector: &TsVector, operands: &[&Operand]) -> f32 {
    let mut result = 0.0_f32;
    let stripped = [Position {
        index: 0,
        weight: Weight::D,
    }];
    for operand in operands {
        for entry in vector.matching(&operand.word, operand.prefix) {
            let positions = if entry.positions.is_empty() {
                &stripped
            } else {
                entry.positions.as_slice()
            };
            let mut sum = 0.0_f32;
            let mut maximum = -1.0_f32;
            let mut max_index = 1;
            for (index, position) in positions.iter().enumerate() {
                let j = index + 1;
                let w = weights[position.weight as usize];
                sum += w / (j * j) as f32;
                if w > maximum {
                    maximum = w;
                    max_index = j;
                }
            }
            let contribution = maximum + sum - maximum / (max_index * max_index) as f32;
            result = (f64::from(result) + f64::from(contribution) / 1.644_934_066_85) as f32;
        }
    }
    result / operands.len() as f32
}

fn rank_and(weights: [f32; 4], vector: &TsVector, operands: &[&Operand]) -> f32 {
    let mut result = -1.0_f32;
    let stripped = [Position {
        index: MAX_POS,
        weight: Weight::D,
    }];
    let mut last_positions: Vec<(&[Position], bool)> = Vec::with_capacity(operands.len());
    for operand in operands {
        let mut last = (&[][..], false);
        for entry in vector.matching(&operand.word, operand.prefix) {
            let no_positions = entry.positions.is_empty();
            let positions = if no_positions {
                &stripped
            } else {
                entry.positions.as_slice()
            };
            last = (positions, no_positions);
            for &(previous, previous_stripped) in &last_positions {
                for pos in positions {
                    for prior in previous {
                        let distance = pos.index.abs_diff(prior.index);
                        if distance == 0 && !(no_positions || previous_stripped) {
                            continue;
                        }
                        let distance = if distance == 0 { MAX_POS + 1 } else { distance };
                        let product = weights[pos.weight as usize]
                            * weights[prior.weight as usize]
                            * word_distance(distance);
                        let current = f64::from(product).sqrt() as f32;
                        result = if result < 0.0 {
                            current
                        } else {
                            (1.0 - (1.0 - f64::from(result)) * (1.0 - f64::from(current))) as f32
                        };
                    }
                }
            }
        }
        last_positions.push(last);
    }
    result
}

/// PostgreSQL rank, with optional float4 weights in D, C, B, A order and a
/// normalization bitmask (use 0 for overloads without normalization).
pub fn ts_rank(
    vector: &TsVector,
    query: &TsQuery,
    input_weights: Option<&[Option<f32>]>,
    normalization: i32,
) -> Result<f32, TsError> {
    let weights = weights(input_weights)?;
    let Some(root) = query.root.as_deref().filter(|_| !vector.is_empty()) else {
        return Ok(0.0);
    };
    let mut operands = Vec::new();
    root.visit_operands(&mut |operand| operands.push(operand));
    crate::pg_sort::sort(&mut operands, &|a, b| a.word.cmp(&b.word));
    operands.dedup_by(|a, b| a.word == b.word);
    let mut result = if operands.len() >= 2
        && matches!(root, Node::Binary(Operator::And | Operator::Phrase(_), ..))
    {
        rank_and(weights, vector, &operands)
    } else {
        rank_or(weights, vector, &operands)
    };
    if result < 0.0 {
        result = 1e-20;
    }
    if normalization & LOG_LENGTH != 0 {
        result = (f64::from(result)
            / (((document_length(vector) + 1) as f64).ln() / std::f64::consts::LN_2))
            as f32;
    }
    if normalization & LENGTH != 0 {
        result /= document_length(vector) as f32;
    }
    if normalization & UNIQUE != 0 {
        result /= vector.len() as f32;
    }
    if normalization & LOG_UNIQUE != 0 {
        result = (f64::from(result) / (((vector.len() + 1) as f64).ln() / std::f64::consts::LN_2))
            as f32;
    }
    if normalization & DIVIDE_BY_RANK_PLUS_ONE != 0 {
        result /= result + 1.0;
    }
    Ok(result)
}

struct Occurrence {
    position: Position,
    lexeme: usize,
    operands: Vec<usize>,
}

fn doc_occurrences(vector: &TsVector, operands: &[&Operand]) -> Vec<Occurrence> {
    let mut mapping = Vec::new();
    for (index, operand) in operands.iter().enumerate() {
        for (lexeme, entry) in vector.matching_entries(&operand.word, operand.prefix) {
            for &position in &entry.positions {
                if operand.weights == 0 || operand.weights & position.weight.mask() != 0 {
                    mapping.push((position.index, position.weight, lexeme, index));
                }
            }
        }
    }
    mapping.sort_unstable();
    let mut document: Vec<Occurrence> = Vec::new();
    for (index, weight, lexeme, operand) in mapping {
        let position = Position { index, weight };
        if let Some(end) = document.last_mut()
            && end.position == position
            && end.lexeme == lexeme
        {
            end.operands.push(operand);
        } else {
            document.push(Occurrence {
                position,
                lexeme,
                operands: vec![operand],
            });
        }
    }
    document
}

fn add_occurrence(matches: &mut [Vec<u16>], entry: &Occurrence) {
    for &operand in &entry.operands {
        let positions = &mut matches[operand];
        if positions.last() != Some(&entry.position.index) {
            positions.push(entry.position.index);
        }
    }
}

/// PostgreSQL cover-density rank. Weights and normalization use the same
/// arguments as `ts_rank`.
pub fn ts_rank_cd(
    vector: &TsVector,
    query: &TsQuery,
    input_weights: Option<&[Option<f32>]>,
    normalization: i32,
) -> Result<f32, TsError> {
    let weights = weights(input_weights)?;
    let Some(root) = query.root.as_deref() else {
        return Ok(0.0);
    };
    let mut operands = Vec::new();
    root.visit_operands(&mut |operand| operands.push(operand));
    let ids: HashMap<*const Operand, usize> = operands
        .iter()
        .enumerate()
        .map(|(id, &operand)| (std::ptr::from_ref(operand), id))
        .collect();
    let doc = doc_occurrences(vector, &operands);
    let inverse = weights.map(|w| 1.0 / f64::from(w));
    let mut matches = vec![Vec::new(); operands.len()];
    let mut start = 0;
    let mut score = 0.0;
    let mut extents = 0;
    let mut distance_sum = 0.0;
    let mut previous_midpoint = 0.0;
    let test_cover = |matches: &[Vec<u16>], reversed: bool| {
        execute(root, &|operand, phrase| {
            let data = &matches[ids[&std::ptr::from_ref(operand)]];
            if data.is_empty() {
                Hit::No
            } else if phrase {
                let mut positions = data.clone();
                if reversed {
                    positions.reverse();
                }
                Hit::Yes(Phrase {
                    positions,
                    ..Phrase::default()
                })
            } else {
                Hit::present()
            }
        })
    };
    while start < doc.len() {
        matches.iter_mut().for_each(Vec::clear);
        let Some(end) = (start..doc.len()).find(|&i| {
            add_occurrence(&mut matches, &doc[i]);
            test_cover(&matches, false)
        }) else {
            break;
        };
        let high = i32::from(doc[end].position.index);
        if high == 0 {
            break;
        }
        matches.iter_mut().for_each(Vec::clear);
        let Some(begin) = (start..=end).rev().find(|&i| {
            add_occurrence(&mut matches, &doc[i]);
            test_cover(&matches, true)
        }) else {
            start += 1;
            continue;
        };
        let low = i32::from(doc[begin].position.index);
        let count = (end - begin) as i32;
        let inv_sum: f64 = doc[begin..=end]
            .iter()
            .map(|item| inverse[item.position.weight as usize])
            .sum();
        let noise = high - low - count;
        let noise = if noise < 0 { count / 2 } else { noise };
        score += f64::from(count + 1) / inv_sum / f64::from(noise + 1);
        let midpoint = f64::from(high + low) / 2.0;
        if extents > 0 && midpoint > previous_midpoint {
            distance_sum += 1.0 / (midpoint - previous_midpoint);
        }
        previous_midpoint = midpoint;
        extents += 1;
        start = begin + 1;
    }
    if normalization & LOG_LENGTH != 0 && !vector.is_empty() {
        score /= ((document_length(vector) + 1) as f64).ln();
    }
    if normalization & LENGTH != 0 && !vector.is_empty() {
        score /= document_length(vector) as f64;
    }
    if normalization & EXTENT_DISTANCE != 0 && extents > 0 && distance_sum > 0.0 {
        score /= f64::from(extents) / distance_sum;
    }
    if normalization & UNIQUE != 0 && !vector.is_empty() {
        score /= vector.len() as f64;
    }
    if normalization & LOG_UNIQUE != 0 && !vector.is_empty() {
        score /= ((vector.len() + 1) as f64).ln() / std::f64::consts::LN_2;
    }
    if normalization & DIVIDE_BY_RANK_PLUS_ONE != 0 {
        score /= score + 1.0;
    }
    Ok(score as f32)
}
