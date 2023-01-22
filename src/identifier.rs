//! Names used for tables, columns, schemas, DBs, etc.
use std::fmt::Display;

use arraystring::{typenum::U63, ArrayString};

use sqlparser::ast::Ident;

/// A fixed capacity copy-able string.
pub type BoundedString = ArrayString<U63>;

/// A name given to a schema. Uniquely identifies a single schema in a database.
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

/// Uniquely identifies a table in a database. The schema will be assumed to be the
/// [`default_schema`](`crate::Database::default_schema`) if not specified.
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

impl Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self {
                schema_name: None,
                table_name,
            } => write!(f, "{}", table_name),
            Self {
                schema_name: Some(schema_name),
                table_name,
            } => write!(f, "{}.{}", schema_name, table_name),
        }
    }
}

/// Uniquely identifies a column in a given table in a database.
///
/// The schema will be assumed to be the
/// [`default_schema`](`crate::Database::default_schema`) if not specified.
///
/// The table will be the one specified in the `WHERE` clause of the query if not specified
/// explicitly.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ColumnRef {
    pub schema_name: Option<BoundedString>,
    pub table_name: Option<BoundedString>,
    pub col_name: BoundedString,
}

impl Display for ColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self {
                schema_name: None,
                table_name: None,
                col_name,
            } => write!(f, "{}", col_name),
            Self {
                schema_name: None,
                table_name: Some(table_name),
                col_name,
            } => write!(f, "{}.{}", table_name, col_name),
            Self {
                schema_name: Some(schema_name),
                table_name: Some(table_name),
                col_name,
            } => write!(f, "{}.{}.{}", schema_name, table_name, col_name),
            // the below case does not occur
            Self {
                schema_name: Some(schema_name),
                table_name: None,
                col_name,
            } => write!(f, "{}.{}", schema_name, col_name),
        }
    }
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

/// Invalid identifier.
#[derive(Debug, PartialEq)]
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
