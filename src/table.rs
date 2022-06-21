use crate::column::{Column, Value};

pub struct Table {
    name: String,
    columns: Vec<Column>,
    pub data: Vec<Row>,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self
    {
        Self {
            name,
            columns,
            data: Vec::new(),
        }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn with_column(mut self, column: Column) -> Self {
        self.columns.push(column);
        self
    }
}

pub struct Row {
    pub data: Vec<Value>,
}

#[cfg(test)]
mod tests {
    use super::Table;

    #[test]
    fn create_table() {
        let _ = Table::new("test".to_owned(), vec![]);
    }
}
