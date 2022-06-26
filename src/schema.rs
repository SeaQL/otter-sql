use crate::table::Table;

/// A namespace in a database.
pub struct Schema {
    name: String,
    tables: Vec<Table>,
}

impl Schema {
    pub fn new(name: String) -> Self {
        Self {
            name,
            tables: Vec::new(),
        }
    }

    /// The name of the schema.
    pub fn name(&self) -> &String {
        &self.name
    }

    /// All the tables in the schema.
    pub fn tables(&self) -> &Vec<Table> {
        &self.tables
    }

    /// Add a new table to the schema.
    pub fn add_table(&mut self, table: Table) -> &mut Self {
        self.tables.push(table);
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::table::Table;

    use super::Schema;

    #[test]
    fn create_schema() {
        let mut schema = Schema::new("test".to_owned());
        assert_eq!(schema.name(), "test");
        assert_eq!(schema.tables().len(), 0);

        schema.add_table(Table::new("test".to_owned(), vec![]));
        assert_eq!(schema.tables().len(), 1);
        assert_eq!(schema.tables()[0].name(), "test");
        assert_eq!(schema.tables()[0].columns().len(), 0);
    }
}
