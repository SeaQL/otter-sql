use sqlparser::ast::{ColumnOption, ColumnOptionDef, DataType};

use crate::{column::Column, value::Value, BoundedString};

const TABLE_UNIQUE_KEY_NAME: &str = "__unique_key";

/// A table in a database.
///
/// Contains both the metadata and the actual data.
pub struct Table {
    name: BoundedString,
    columns: Vec<Column>,
    /// The table's data.
    // TODO: provide methods that verify the data while adding
    data: Vec<Row>,
    row_id: u64,
}

impl Table {
    pub fn new(name: BoundedString, mut columns: Vec<Column>) -> Self {
        // every table has a default unique key
        columns.push(Column::new(
            TABLE_UNIQUE_KEY_NAME.into(),
            DataType::UnsignedInt(None),
            vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::Unique { is_primary: false },
            }],
            true,
        ));

        Self {
            name,
            columns,
            data: Vec::new(),
            row_id: 0,
        }
    }

    pub fn new_row(&mut self, mut data: Vec<Value>) {
        data.insert(0, Value::UInt64(self.row_id));
        self.data.push(Row { data });
        self.row_id += 1;
    }

    pub fn all_data(&self) -> Vec<Row> {
        self.data
            .iter()
            .map(|row| Row {
                data: row
                    .data
                    .iter()
                    .enumerate()
                    .filter_map(|(i, value)| {
                        if self.columns[i].is_internal() {
                            None
                        } else {
                            Some(value)
                        }
                    })
                    .cloned()
                    .collect(),
            })
            .collect()
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
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// Values for each column in the row.
    pub data: Vec<Value>,
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::DataType;

    use super::Table;
    use crate::{column::Column, table::Row, value::Value};

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

        table.new_row(vec![Value::Int64(1), Value::Int64(2)]);

        assert_eq!(
            table.all_data(),
            vec![Row {
                data: vec![Value::Int64(1)],
            }]
        );
    }
}
