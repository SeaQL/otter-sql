use std::fmt::Display;

use arraystring::{typenum::U63, ArrayString};

use sqlparser::ast::Ident;

/// A fixed capacity copy-able string.
pub type BoundedString = ArrayString<U63>;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SchemaRef(pub BoundedString);

impl TryFrom<Vec<Ident>> for SchemaRef {
    type Error = IdentifierError;

    fn try_from(value: Vec<Ident>) -> Result<Self, Self::Error> {
        match value.as_slice() {
            [] => Err(IdentifierError {
                idents: value,
                reason: "Empty schema name",
            }),
            [schema_name] => Ok(SchemaRef(schema_name.value.as_str().into())),
            _ => Err(IdentifierError {
                idents: value,
                reason: "More than 1 part in schema name.",
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TableRef {
    pub schema_name: Option<BoundedString>,
    pub table_name: BoundedString,
}

impl TryFrom<Vec<Ident>> for TableRef {
    type Error = IdentifierError;

    fn try_from(value: Vec<Ident>) -> Result<Self, Self::Error> {
        match value.as_slice() {
            [] => Err(IdentifierError {
                idents: value,
                reason: "Empty table name",
            }),
            [table_name] => Ok(TableRef {
                schema_name: None,
                table_name: table_name.value.as_str().into(),
            }),
            [schema_name, table_name] => Ok(TableRef {
                schema_name: Some(schema_name.value.as_str().into()),
                table_name: table_name.value.as_str().into(),
            }),
            _ => Err(IdentifierError {
                idents: value,
                reason: "More than 2 parts in table name.",
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ColumnRef {
    pub schema_name: Option<BoundedString>,
    pub table_name: Option<BoundedString>,
    pub col_name: BoundedString,
}

impl TryFrom<Vec<Ident>> for ColumnRef {
    type Error = IdentifierError;

    fn try_from(value: Vec<Ident>) -> Result<Self, Self::Error> {
        match value.as_slice() {
            [] => Err(IdentifierError {
                idents: value,
                reason: "Empty column name",
            }),
            [col_name] => Ok(ColumnRef {
                schema_name: None,
                table_name: None,
                col_name: col_name.value.as_str().into(),
            }),
            [table_name, col_name] => Ok(ColumnRef {
                schema_name: None,
                table_name: Some(table_name.value.as_str().into()),
                col_name: col_name.value.as_str().into(),
            }),
            [schema_name, table_name, col_name] => Ok(ColumnRef {
                schema_name: Some(schema_name.value.as_str().into()),
                table_name: Some(table_name.value.as_str().into()),
                col_name: col_name.value.as_str().into(),
            }),
            _ => Err(IdentifierError {
                idents: value,
                reason: "More than 3 parts in column name.",
            }),
        }
    }
}

#[derive(Debug)]
pub struct IdentifierError {
    idents: Vec<Ident>,
    reason: &'static str,
}

impl Display for IdentifierError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IdentifierError: {} (Got: '{:?}')",
            self.reason, self.idents
        )
    }
}
