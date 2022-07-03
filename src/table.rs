use crate::{column::Column, value::Value, BoundedString};

/// A table in a database.
///
/// Contains both the metadata and the actual data.
pub struct Table {
    name: BoundedString,
    columns: Vec<Column>,
    /// The table's data.
    // TODO: provide methods that verify the data while adding
    pub data: Vec<Row>,
}

impl Table {
    pub fn new(name: BoundedString, columns: Vec<Column>) -> Self {
        Self {
            name,
            columns,
            data: Vec::new(),
        }
    }

    /// The table's name.
    pub fn name(&self) -> &BoundedString {
        &self.name
    }

    /// The table's columns.
    pub fn columns(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter().filter(|c| !c.is_internal())
    }

    /// Add a new column to the table.
    // TODO: does not add the column data to the rows.
    pub fn with_column(mut self, column: Column) -> Self {
        self.columns.push(column);
        self
    }
}

/// A row in a table. Represents a relation in relational algebra terms.
pub struct Row {
    /// Values for each column in the row.
    pub data: Vec<Value>,
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::DataType;

    use super::Table;
    use crate::column::Column;

    #[test]
    fn create_table() {
        let mut table = Table::new("test".into(), vec![]);
        assert_eq!(table.name(), "test");

        table = table.with_column(Column::new(
            "col1".into(),
            DataType::Int(None),
            vec![],
            false,
        ));

        table = table.with_column(Column::new(
            "col2".into(),
            DataType::Int(None),
            vec![],
            true,
        ));

        assert_eq!(table.columns().collect::<Vec<_>>().len(), 1);
        assert_eq!(table.columns().next().unwrap().name(), "col1");
    }
}
