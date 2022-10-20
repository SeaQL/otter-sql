use sqlparser::ast::{ColumnOption, ColumnOptionDef, DataType};

use crate::{column::Column, identifier::ColumnRef, value::Value, vm::RuntimeError, BoundedString};

pub const TABLE_UNIQUE_KEY_NAME: &str = "__otter_unique_key";

pub const TABLE_TEMPORARY_NAME: &str = "__otter_temporary_table";

#[derive(Debug, Clone)]
/// A table in a database.
///
/// Contains both the metadata and the actual data.
pub struct Table {
    name: BoundedString,
    columns: Vec<Column>,
    /// The table's data.
    // TODO: provide methods that verify the data while adding
    pub(crate) raw_data: Vec<Row>,
    row_id: u64,
}

impl Table {
    pub fn new(name: BoundedString, mut columns: Vec<Column>) -> Self {
        // every table has a default unique key
        columns.insert(
            0,
            Column::new(
                TABLE_UNIQUE_KEY_NAME.into(),
                DataType::UnsignedInt(None),
                vec![ColumnOptionDef {
                    name: None,
                    option: ColumnOption::Unique { is_primary: false },
                }],
                true,
            ),
        );

        Self {
            name,
            columns,
            raw_data: Vec::new(),
            row_id: 0,
        }
    }

    pub fn new_temp(num: usize) -> Self {
        Self::new(
            format!("{}_{}", TABLE_TEMPORARY_NAME, num).as_str().into(),
            Vec::new(),
        )
    }

    pub fn new_row(&mut self, mut data: Vec<Value>) -> &mut Self {
        data.insert(0, Value::Int64(self.row_id as i64));
        self.raw_data.push(Row { data });
        self.row_id += 1;
        self
    }

    pub fn all_data(&self) -> Vec<Row> {
        self.raw_data
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

    /// Number of actual columns.
    pub fn num_columns(&self) -> usize {
        // TODO: keep track of this count instead of calculating every time
        self.columns().count()
    }

    /// Add a new column to the table.
    // TODO: does not add the column data to the rows.
    pub fn add_column(&mut self, column: Column) -> &mut Self {
        self.columns.push(column);
        self
    }

    /// Add data for a new column to all rows.
    pub fn add_column_data(
        &mut self,
        col_name: &BoundedString,
        data: Vec<Value>,
    ) -> Result<&mut Self, RuntimeError> {
        let (col_index, _) = self.get_column(col_name)?;

        if self.raw_data.len() != data.len() {
            return Err(RuntimeError::TableNewColumnSizeMismatch {
                table_name: *self.name(),
                table_len: self.raw_data.len(),
                col_name: *col_name,
                col_len: data.len(),
            });
        }

        if !self.is_empty() {
            let first_row_size = self.raw_data[0].data.len();
            if first_row_size == col_index {
                // column is the last one. just push it at the end.
                for (row, new_data) in self.raw_data.iter_mut().zip(data.into_iter()) {
                    row.data.push(new_data);
                }
            } else if first_row_size == self.columns.len() {
                // column data is already added. we replace it.
                for (row, new_data) in self.raw_data.iter_mut().zip(data.into_iter()) {
                    row.data[col_index] = new_data;
                }
            } else {
                // when the column is somewhere in the middle or beginning.
                // perhaps an expensive operation!
                for (row, new_data) in self.raw_data.iter_mut().zip(data.into_iter()) {
                    row.data.insert(col_index, new_data)
                }
            }
        }

        Ok(self)
    }

    /// Map column name to its index and definition.
    pub fn get_column(&self, col_name: &BoundedString) -> Result<(usize, &Column), RuntimeError> {
        let idx = self.columns.iter().position(|c| c.name() == col_name);
        if let Some(idx) = idx {
            Ok((idx, &self.columns[idx]))
        } else {
            return Err(RuntimeError::ColumnNotFound(ColumnRef {
                schema_name: None,
                table_name: Some(*self.name()),
                col_name: *col_name,
            }));
        }
    }

    /// Retrieve all data of a column.
    pub fn get_column_data(&self, col_name: &BoundedString) -> Result<Vec<Value>, RuntimeError> {
        let (col_index, _) = self.get_column(col_name)?;

        Ok(self
            .raw_data
            .iter()
            .map(|row| row.data[col_index].clone())
            .collect())
    }

    pub fn rename(&mut self, new_name: BoundedString) {
        self.name = new_name;
    }

    /// Whether the table has no rows.
    pub fn is_empty(&self) -> bool {
        self.raw_data.is_empty()
    }

    /// Whether the table has no defined columns.
    pub fn has_no_columns(&self) -> bool {
        self.columns().next().is_none()
    }

    /// Create a new row filled with sentinel values for the data type.
    ///
    /// Note: does not add the row to the table.
    pub(crate) fn sentinel_row(&self) -> Result<Row, RuntimeError> {
        let data = self
            .columns
            .iter()
            .map(|c| Value::sentinel_value(c.data_type()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Row { data })
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

        table.add_column(Column::new(
            "col1".into(),
            DataType::Int(None),
            vec![],
            false,
        ));

        table.add_column(Column::new(
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
