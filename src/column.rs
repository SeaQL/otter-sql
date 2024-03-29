//! Columns in a table.
use sqlparser::ast::{ColumnOptionDef, DataType};

use crate::BoundedString;

/// A column's metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Column {
    name: BoundedString,
    data_type: DataType,
    options: Vec<ColumnOptionDef>,
    /// Whether this is a hidden, internal column.
    internal: bool,
}

impl Column {
    pub fn new(
        name: BoundedString,
        data_type: DataType,
        options: Vec<ColumnOptionDef>,
        internal: bool,
    ) -> Self {
        Self {
            name,
            data_type,
            options,
            internal,
        }
    }

    /// Name of the column.
    pub fn name(&self) -> &BoundedString {
        &self.name
    }

    /// Data type of the column.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Column's options (attributes, constraints, etc.).
    pub fn options(&self) -> &Vec<ColumnOptionDef> {
        &self.options
    }

    /// Add a new column option.
    pub fn add_column_option(&mut self, option: ColumnOptionDef) {
        self.options.push(option)
    }

    /// Whether the column is a hidden, internal-only column.
    pub fn is_internal(&self) -> bool {
        self.internal
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{ColumnOption, ColumnOptionDef, DataType};

    use super::Column;

    #[test]
    fn create_column() {
        let column = Column::new(
            "test".into(),
            DataType::Int(None),
            vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::NotNull,
            }],
            false,
        );

        assert_eq!(column.name(), "test");
        assert_eq!(column.data_type(), &DataType::Int(None));
        assert_eq!(
            column.options(),
            &vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::NotNull,
            }],
        );
    }
}
