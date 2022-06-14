use crate::table::Table;

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

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn tables(&self) -> &Vec<Table> {
        &self.tables
    }

    pub fn with_table(mut self, table: Table) -> Self {
        self.tables.push(table);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::Schema;

    #[test]
    fn create_schema() {
        let _ = Schema::new("test".to_owned());
    }
}
