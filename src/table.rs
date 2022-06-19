use std::borrow::Borrow;

use crate::column::{Column, ColumnKind};

// TODO: break into specific traits?
pub trait TableLike {}

pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub data: Vec<Row>,
}

impl Table {
    pub fn new<S>(name: S, columns: Vec<Column>) -> Self
    where
        S: Borrow<str>,
    {
        Self {
            name: name.borrow().to_string(),
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
        let _ = Table::new("test", vec![]);
    }
}
