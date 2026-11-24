use super::{DataType, Description, EnumParameters, Prepared, R, Relational, clock, searched};
use std::{borrow::Cow, cell::OnceCell};

/// A lazy description for one immutable statement input and binding catalog.
/// Paged callers may share it only across equivalent binding catalogs. Each
/// execution still binds and validates its own statement before using it.
pub(crate) struct StatementDescription<'a> {
    prepared: &'a Prepared,
    full: OnceCell<(Description, EnumParameters)>,
    searched: [OnceCell<Description>; 3],
}

impl<'a> StatementDescription<'a> {
    pub(super) fn new(prepared: &'a Prepared) -> Self {
        Self {
            prepared,
            full: OnceCell::new(),
            searched: Default::default(),
        }
    }

    pub(crate) fn prepared(&self) -> &'a Prepared {
        self.prepared
    }

    pub(super) fn input(&self) -> StatementDescriptionInput<'_> {
        StatementDescriptionInput {
            memo: self,
            prepared: Cow::Borrowed(self.prepared),
        }
    }

    pub(super) fn bind_schemas(&self, state: &Relational) -> R<StatementDescriptionInput<'_>> {
        Ok(StatementDescriptionInput {
            memo: self,
            prepared: Cow::Owned(self.prepared.bind_schemas(state)?),
        })
    }

    fn searched_slot(
        &self,
        order: giggles_pg_types::temporal::DateOrder,
    ) -> &OnceCell<Description> {
        use giggles_pg_types::temporal::DateOrder::*;
        &self.searched[match order {
            Ymd => 0,
            Mdy => 1,
            Dmy => 2,
        }]
    }
}

/// Pairs the memo with its original input or a schema binding of that input.
pub(super) struct StatementDescriptionInput<'a> {
    memo: &'a StatementDescription<'a>,
    prepared: Cow<'a, Prepared>,
}

impl StatementDescriptionInput<'_> {
    pub(super) fn prepared(&self) -> &Prepared {
        &self.prepared
    }

    // The full description binds at the prepared date order and adds result
    // OIDs. The searched caller uses the ambient order and only needs types.
    pub(super) fn describe(&self, state: &Relational) -> R<&Description> {
        if let Some((description, _)) = self.memo.full.get() {
            return Ok(description);
        }
        let searched_description = self
            .memo
            .searched_slot(self.prepared.literal_date_order)
            .get()
            .filter(|_| self.can_share_searched(state));
        let description = self.prepared.describe_with_searched(state, searched_description)?;
        Ok(&self.memo.full.get_or_init(|| description).0)
    }

    pub(super) fn searched_parameters(&self, state: &Relational) -> R<Vec<Option<DataType>>> {
        if !self.can_share_searched(state) {
            let description = searched::describe(&self.prepared, state)?;
            return Ok(description.parameters.into_iter().map(Some).collect());
        }
        let order = clock::LiteralDateOrder::current();
        let slot = self.memo.searched_slot(order);
        let description = if order == self.prepared.literal_date_order
            && let Some((description, _)) = self.memo.full.get()
        {
            description
        } else if let Some(description) = slot.get() {
            description
        } else {
            let description = searched::describe(&self.prepared, state)?;
            slot.get_or_init(|| description)
        };
        Ok(description.parameters.iter().cloned().map(Some).collect())
    }

    fn can_share_searched(&self, state: &Relational) -> bool {
        // Full describe uses its own enum and type-display scopes. Searched
        // binding uses the caller's scopes and can record enum inference there.
        state.ext.types.is_empty()
            && self
                .prepared
                .declared_enum_parameters
                .as_deref()
                .is_none_or(EnumParameters::is_empty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relational::{enum_parameters, type_oid};

    #[test]
    fn searched_and_full_describe_preserve_enum_parameter_scopes() {
        let mut state = Relational::default();
        for sql in [
            "CREATE TYPE mood AS ENUM ('ok')",
            "CREATE TABLE feelings (id int PRIMARY KEY, mood mood, moods mood[])",
        ] {
            let statement = Prepared::parse(sql, 1, state.epoch).unwrap();
            state.execute(&statement, &[], None).unwrap();
        }
        let prepared = Prepared::parse(
            "UPDATE feelings SET mood = $1, moods = $2 WHERE id >= 0 RETURNING mood, moods",
            1,
            state.epoch,
        )
        .unwrap();
        let memo = StatementDescription::new(&prepared);
        let input = memo.bind_schemas(&state).unwrap();
        let oid = type_oid(state.type_by_name("mood").unwrap().0).unwrap();
        let expected_oids = [oid, oid + 1];
        let parameter_oids = |parameters: &EnumParameters| {
            parameters.values().map(|parameter| parameter.oid()).collect::<Vec<_>>()
        };

        input.searched_parameters(&state).unwrap();
        assert_eq!(input.describe(&state).unwrap().type_oids, expected_oids);
        assert_eq!(parameter_oids(&memo.full.get().unwrap().1), expected_oids);

        let _scope = enum_parameters::Scope::enter(None);
        input.searched_parameters(&state).unwrap();
        assert_eq!(
            parameter_oids(&enum_parameters::describe(&state).unwrap()),
            expected_oids
        );
    }
}
