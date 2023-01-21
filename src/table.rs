//! Tables and rows.

use sqlparser::ast::{ColumnOption, ColumnOptionDef, DataType};

use crate::{column::Column, identifier::ColumnRef, value::Value, vm::RuntimeError, BoundedString};

pub(super) const TABLE_UNIQUE_KEY_NAME: &str = "__otter_unique_key";

pub(super) const TABLE_TEMPORARY_NAME: &str = "__otter_temporary_table";

#[derive(Debug, Clone)]
/// A table in a database.
///
/// Contains both the metadata and the actual data.
pub struct Table {
    name: BoundedString,
    pub(super) raw_columns: Vec<Column>,
    /// The table's data.
    // TODO: provide methods that verify the data while adding
    pub(super) raw_data: Vec<RawRow>,
    row_id: u64,
}

impl Table {
    pub(super) fn new(name: BoundedString, mut columns: Vec<Column>) -> Self {
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
            raw_columns: columns,
            raw_data: Vec::new(),
            row_id: 0,
        }
    }

    pub(super) fn new_temp(num: usize) -> Self {
        Self::new(
            format!("{}_{}", TABLE_TEMPORARY_NAME, num).as_str().into(),
            Vec::new(),
        )
    }

    pub(super) fn new_from(table: &Self) -> Self {
        Self {
            name: table.name,
            raw_columns: table.raw_columns.clone(),
            raw_data: Vec::new(),
            row_id: 0,
        }
    }

    /// Add a new row of data to the table.
    ///
    /// **Note**: validation is not implemented yet and the return type is subject to change.
    pub fn new_row(&mut self, mut data: Vec<Value>) -> &mut Self {
        data.insert(0, Value::Int64(self.row_id as i64));
        self.raw_data.push(RawRow { raw_data: data });
        self.row_id += 1;
        self
    }

    /// Retrieve a copy of all of the table's non-internal data.
    pub fn all_data(&self) -> Vec<Row> {
        self.raw_data
            .iter()
            .cloned()
            .map(|row| Row::from_raw(row, self))
            .collect()
    }

    /// The table's name.
    pub fn name(&self) -> &BoundedString {
        &self.name
    }

    /// The table's (non-internal) columns.
    pub fn columns(&self) -> impl Iterator<Item = &Column> {
        self.raw_columns.iter().filter(|c| !c.is_internal())
    }

    /// Number of non-internal columns.
    pub fn num_columns(&self) -> usize {
        // TODO: keep track of this count instead of calculating every time
        self.columns().count()
    }

    /// Add a new column to the table.
    ///
    /// **Note**: this does not yet modify any of the rows. They must be kept consistent
    /// externally using [`add_column_data`](`Self::add_column_data`).
    // TODO: does not add the column data to the rows.
    pub fn add_column(&mut self, column: Column) -> &mut Self {
        self.raw_columns.push(column);
        self
    }

    /// Add data for a new column to all rows.
    pub fn add_column_data(
        &mut self,
        col_name: &BoundedString,
        data: Vec<Value>,
    ) -> Result<&mut Self, RuntimeError> {
        let (col_index, _) = self.get_column(col_name)?;

        if !self.is_empty() && self.raw_data.len() != data.len() {
            return Err(RuntimeError::TableNewColumnSizeMismatch {
                table_name: *self.name(),
                table_len: self.raw_data.len(),
                col_name: *col_name,
                col_len: data.len(),
            });
        }

        if !self.is_empty() {
            let first_row_size = self.raw_data[0].raw_data.len();
            if first_row_size == col_index {
                // column is the last one. just push it at the end.
                for (row, new_data) in self.raw_data.iter_mut().zip(data.into_iter()) {
                    row.raw_data.push(new_data);
                }
            } else if first_row_size == self.raw_columns.len() {
                // column data is already added. we replace it.
                for (row, new_data) in self.raw_data.iter_mut().zip(data.into_iter()) {
                    row.raw_data[col_index] = new_data;
                }
            } else {
                // when the column is somewhere in the middle or beginning.
                // perhaps an expensive operation!
                for (row, new_data) in self.raw_data.iter_mut().zip(data.into_iter()) {
                    row.raw_data.insert(col_index, new_data)
                }
            }
        } else {
            for value in data {
                self.new_row(vec![value]);
            }
        }

        Ok(self)
    }

    /// Map column name to its index and definition.
    pub(super) fn get_column(
        &self,
        col_name: &BoundedString,
    ) -> Result<(usize, &Column), RuntimeError> {
        let idx = self.raw_columns.iter().position(|c| c.name() == col_name);
        if let Some(idx) = idx {
            Ok((idx, &self.raw_columns[idx]))
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
            .map(|row| row.raw_data[col_index].clone())
            .collect())
    }

    /// Rename the table.
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
    pub(super) fn sentinel_row(&self) -> Result<Row, RuntimeError> {
        let data = self
            .raw_columns
            .iter()
            .map(|c| Value::sentinel_value(c.data_type()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Row { data })
    }
}

#[cfg(feature = "terminal-output")]
#[cfg_attr(docsrs, doc(cfg(feature = "terminal-output")))]
impl std::fmt::Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use tabled::{builder::Builder, Style};

        let mut builder = Builder::default();

        for row in self.all_data() {
            builder.add_record(row.data().into_iter().map(|v| v.to_string()));
        }

        builder.set_columns(self.columns().map(|c| c.name().to_string()));

        let mut table = builder.build();

        table.with(Style::rounded());

        write!(f, "{}", table)
    }
}

/// Trait to retrieve data from something that looks like a row in a table.
pub trait RowLike {
    /// Copy or move of the data contained in the row.
    ///
    /// Whether the operation is a copy or move depends on whether the row is a reference to a row
    /// in a table or is an actual row in a table.
    fn data(self) -> Vec<Value>;

    /// References to all values contained in the row.
    fn data_shared(&self) -> Vec<&Value>;
}

/// A row stored in a table. Represents a relation in relational algebra terms.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    /// Values for each column in the row.
    data: Vec<Value>,
}

impl RowLike for Row {
    fn data(self) -> Vec<Value> {
        self.data
    }

    fn data_shared(&self) -> Vec<&Value> {
        self.data.iter().collect()
    }
}

impl Row {
    pub fn new(data: Vec<Value>) -> Self {
        Self { data }
    }

    pub(super) fn from_raw(raw: RawRow, table: &Table) -> Row {
        Row {
            data: raw
                .raw_data
                .into_iter()
                .enumerate()
                .filter_map(|(i, value)| {
                    if table.raw_columns[i].is_internal() {
                        None
                    } else {
                        Some(value)
                    }
                })
                .collect(),
        }
    }

    pub fn to_shared(&self) -> RowShared {
        RowShared::from_row(self)
    }
}

/// A reference to a row in a table. Does not contain internal columns.
#[derive(Debug, Clone, PartialEq)]
pub struct RowShared<'a> {
    data: Vec<&'a Value>,
}

impl<'a> RowLike for RowShared<'a> {
    fn data_shared(&self) -> Vec<&Value> {
        self.data.clone()
    }

    fn data(self) -> Vec<Value> {
        self.data.into_iter().cloned().collect()
    }
}

impl<'a> RowShared<'a> {
    pub(super) fn from_raw<'b>(raw: &'a RawRow, table: &'b Table) -> Self {
        Self {
            data: raw
                .raw_data
                .iter()
                .enumerate()
                .filter_map(|(i, value)| {
                    if table.raw_columns[i].is_internal() {
                        None
                    } else {
                        Some(value)
                    }
                })
                .collect(),
        }
    }

    pub fn from_row(row: &'a Row) -> Self {
        Self {
            data: row.data_shared(),
        }
    }
}

impl<'a> From<&'a Row> for RowShared<'a> {
    fn from(row: &'a Row) -> Self {
        Self::from_row(row)
    }
}

/// A row in a table, including internal columns.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct RawRow {
    /// Values for each column in the row.
    pub(crate) raw_data: Vec<Value>,
}

impl RowLike for RawRow {
    fn data(self) -> Vec<Value> {
        self.raw_data
    }

    fn data_shared(&self) -> Vec<&Value> {
        self.raw_data.iter().collect()
    }
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
