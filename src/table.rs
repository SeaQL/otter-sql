use crate::column::{Column, ColumnKind};

pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
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
}

pub struct Row {
    pub data: Vec<ColumnKind>,
}

#[cfg(test)]
mod tests {
    use super::Table;

    #[test]
    fn create_table() {
        let _ = Table::new("test".to_owned(), vec![]);
    }
}
