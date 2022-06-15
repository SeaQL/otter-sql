use std::{any::Any, borrow::Borrow};

use crate::column::Column;

// TODO: break into specific traits?
pub trait TableLike {}

pub struct Table {
    name: String,
    columns: Vec<Column>,
    data: Vec<Row>,
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

pub struct Row(Vec<Box<dyn Any>>);

#[cfg(test)]
mod tests {
    use super::Table;

    #[test]
    fn create_table() {
        let _ = Table::new("test", vec![]);
    }
}
