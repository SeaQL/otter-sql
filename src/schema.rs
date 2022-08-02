use crate::{vm::TableIndex, BoundedString};

/// A namespace in a database.
///
/// The schema only holds a reference to the actual tables, which are owned by the VM.
pub struct Schema {
    name: BoundedString,
    tables: Vec<TableIndex>,
}

impl Schema {
    pub fn new(name: BoundedString) -> Self {
        Self {
            name,
            tables: Vec::new(),
        }
    }

    /// The name of the schema.
    pub fn name(&self) -> &BoundedString {
        &self.name
    }

    /// All the tables in the schema.
    pub fn tables(&self) -> &Vec<TableIndex> {
        &self.tables
    }

    /// Add a new table to the schema.
    pub fn add_table(&mut self, table: TableIndex) -> &mut Self {
        self.tables.push(table);
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::vm::TableIndex;

    use super::Schema;

    #[test]
    fn create_schema() {
        let mut schema = Schema::new("test".into());
        assert_eq!(schema.name(), "test");
        assert_eq!(schema.tables().len(), 0);

        schema.add_table(TableIndex::default());
        assert_eq!(schema.tables().len(), 1);
        assert_eq!(schema.tables()[0], TableIndex::default());
        assert_ne!(schema.tables()[0], TableIndex::default().next_index());
    }
}
