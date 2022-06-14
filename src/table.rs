use std::any::Any;

// TODO: break into specific traits?
pub trait TableLike {}

pub struct Table {
    columns: Vec<Column>,
}

pub struct Column {}

pub struct Row(Vec<Box<dyn Any>>);

pub struct ActiveTable {
    meta: Table,
    data: Vec<Row>,
}

#[cfg(test)]
mod tests {
    use super::{ActiveTable, Table};

    #[test]
    fn create_table() {
        let _ = Table {};
    }

    #[test]
    fn create_active_table() {
        let _ = ActiveTable {};
    }
}
