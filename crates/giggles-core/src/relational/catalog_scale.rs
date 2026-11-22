//! Historical count thresholds for the forward-reader gate.
use super::{MAX_COLUMNS, secondary_index::Descriptor};
use std::collections::BTreeMap;

pub(crate) struct Counts {
    pub tables: usize,
    pub schemas: usize,
    pub sequences: usize,
    pub types: usize,
    pub collations: usize,
    pub column_collations: usize,
    pub constraint_tables: usize,
    pub constraints: usize,
}

impl Counts {
    pub(crate) fn needs_feature<'a>(
        &self,
        indexes: impl ExactSizeIterator<Item = &'a Descriptor>,
    ) -> bool {
        const OLD_CATALOG_COUNT: usize = 128;
        const OLD_EXTENSION_COUNT: usize = OLD_CATALOG_COUNT * MAX_COLUMNS;
        if self.tables > OLD_CATALOG_COUNT
            || self.schemas > OLD_CATALOG_COUNT
            || self.sequences > OLD_EXTENSION_COUNT
            || self.types > OLD_EXTENSION_COUNT
            || self.collations > OLD_CATALOG_COUNT
            || self.column_collations > OLD_EXTENSION_COUNT
            || self.constraint_tables > OLD_CATALOG_COUNT
            || self.constraints > OLD_EXTENSION_COUNT
            || indexes.len() > OLD_CATALOG_COUNT
        {
            return true;
        }
        let mut counts = BTreeMap::new();
        for index in indexes {
            let count = counts.entry(index.table).or_insert(0);
            *count += 1;
            if *count > 8 {
                return true;
            }
        }
        false
    }
}
