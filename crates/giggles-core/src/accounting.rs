//! Versioned extension to the original task/mutation model. No delivery refunds.
use crate::{CoreError, Error};
use serde::{Deserialize, Serialize};

/// Independent lifetime units. Bytes are logical encoded application bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskResources {
    pub read_rows: u64,
    pub disclosed_rows: u64,
    pub encoded_bytes: u64,
}
impl TaskResources {
    pub(crate) fn fits(self, cap: Self) -> bool {
        self.read_rows <= cap.read_rows
            && self.disclosed_rows <= cap.disclosed_rows
            && self.encoded_bytes <= cap.encoded_bytes
    }
    pub(crate) fn add(self, debit: Self, cap: Self) -> Result<Self, CoreError> {
        let fail = || CoreError::Policy(Error::Budget);
        let next = Self {
            read_rows: self
                .read_rows
                .checked_add(debit.read_rows)
                .ok_or_else(fail)?,
            disclosed_rows: self
                .disclosed_rows
                .checked_add(debit.disclosed_rows)
                .ok_or_else(fail)?,
            encoded_bytes: self
                .encoded_bytes
                .checked_add(debit.encoded_bytes)
                .ok_or_else(fail)?,
        };
        if !next.fits(cap) {
            return Err(fail());
        }
        Ok(next)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskAccounting {
    pub limits: TaskResources,
    pub used: TaskResources,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn independent_wide_oracle_for_each_resource_at_all_integer_edges() {
        let edges = [0, 1, 2, 17, u64::MAX - 1, u64::MAX];
        for axis in 0..3 {
            for used in edges {
                for limit in edges {
                    for debit in edges {
                        if used > limit {
                            continue;
                        }
                        let make = |n| match axis {
                            0 => TaskResources {
                                read_rows: n,
                                ..Default::default()
                            },
                            1 => TaskResources {
                                disclosed_rows: n,
                                ..Default::default()
                            },
                            _ => TaskResources {
                                encoded_bytes: n,
                                ..Default::default()
                            },
                        };
                        let expected = u128::from(used) + u128::from(debit);
                        let result = make(used).add(make(debit), make(limit));
                        if expected <= u128::from(limit) {
                            assert_eq!(result.unwrap(), make(expected as u64));
                        } else {
                            assert_eq!(result, Err(CoreError::Policy(Error::Budget)));
                        }
                    }
                }
            }
        }
    }
}
