use sqlparser::ast::{ColumnOptionDef, DataType};

/// A column's metadata.
pub struct Column {
    name: String,
    data_type: DataType,
    options: Vec<ColumnOptionDef>,
}

impl Column {
    pub fn new(name: String, data_type: DataType, options: Vec<ColumnOptionDef>) -> Self {
        Self {
            name,
            data_type,
            options,
        }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn options(&self) -> &Vec<ColumnOptionDef> {
        &self.options
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{ColumnOption, ColumnOptionDef, DataType};

    use super::Column;

    #[test]
    fn create_column() {
        let column = Column::new(
            "test".to_owned(),
            DataType::Int(None),
            vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::NotNull,
            }],
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
