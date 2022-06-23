use sqlparser::ast::ColumnDef;

/// A column's metadata.
pub struct Column {
    name: String,
    meta: ColumnDef,
}

impl Column {
    pub fn new(name: String, meta: ColumnDef) -> Self {
        Self { name, meta }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    // TODO: check if this is really needed
    pub fn meta(&self) -> &ColumnDef {
        &self.meta
    }
}
