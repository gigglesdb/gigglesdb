use super::{Admin, CoreError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexInfo {
    pub id: u64,
    pub table: u64,
    pub name: String,
    pub columns: Vec<u64>,
    pub unique: bool,
}

impl Admin {
    /// Returns the committed catalog epoch and secondary indexes from one snapshot,
    /// using the same committed snapshot semantics as [`Admin::catalog`].
    pub fn secondary_index_catalog(&self) -> Result<(u64, Vec<IndexInfo>), CoreError> {
        self.inner.inspect_metadata(|s| {
            crate::cancellation::check()?;
            let mut indexes = s
                .sql
                .secondary_descriptors_engine()
                .iter()
                .map(|descriptor| IndexInfo {
                    id: descriptor.id,
                    table: descriptor.table,
                    name: crate::relational::split_key(&descriptor.name).1.into(),
                    columns: descriptor.columns.clone(),
                    unique: descriptor.unique,
                })
                .collect::<Vec<_>>();
            indexes.sort_by_key(|index| (index.table, index.id));
            crate::cancellation::check()?;
            Ok((s.sql.epoch, indexes))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Limits;

    #[test]
    fn secondary_index_public_reports_committed_descriptors() {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!(
            "gd42-secondary-index-public-{:032x}",
            u128::from_be_bytes(nonce)
        ));
        let admin = Admin::create(&path, Limits::default()).unwrap();
        let mut session = admin.sql_session();
        for sql in [
            "CREATE TABLE items (id BIGINT, email TEXT, other BIGINT, PRIMARY KEY (id))",
            "CREATE UNIQUE INDEX unique_items ON items (other, email)",
            "CREATE INDEX ordinary_items ON items (email, other)",
        ] {
            session.execute(sql, &[], None).unwrap();
        }
        let (epoch, tables) = admin.catalog().unwrap();
        let table = &tables[0];
        let column = |name: &str| {
            table
                .columns
                .iter()
                .find(|column| column.name == name)
                .unwrap()
                .id
        };
        let (index_epoch, indexes) = admin.secondary_index_catalog().unwrap();
        assert_eq!(index_epoch, epoch);
        assert_eq!(indexes.len(), 2);
        assert!(
            indexes
                .windows(2)
                .all(|pair| { (pair[0].table, pair[0].id) < (pair[1].table, pair[1].id) })
        );
        let unique = indexes.iter().find(|index| index.unique).unwrap();
        assert_eq!(unique.table, table.id);
        assert_eq!(unique.name, "unique_items");
        assert_eq!(unique.columns, [column("other"), column("email")]);
        let ordinary = indexes.iter().find(|index| !index.unique).unwrap();
        assert_eq!(ordinary.table, table.id);
        assert_eq!(ordinary.name, "ordinary_items");
        assert_eq!(ordinary.columns, [column("email"), column("other")]);
        drop(session);
        drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}
