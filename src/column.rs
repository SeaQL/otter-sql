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

/// A value contained within a table's cell.
///
/// One or more column types may be mapped to a single variant of [`Value`].
pub enum Value {
    // integer types
    // reference: https://dev.mysql.com/doc/refman/8.0/en/integer-types.html
    UInt8Null(Option<u8>),
    UInt8(u8),

    Int8Null(Option<i8>),
    Int8(i8),

    UInt16Null(Option<u16>),
    UInt16(u16),

    Int16Null(Option<i16>),
    Int16(i16),

    UInt32Null(Option<u32>),
    UInt32(u32),

    Int32Null(Option<i32>),
    Int32(i32),

    UInt64Null(Option<u64>),
    UInt64(u64),

    Int64Null(Option<i64>),
    Int64(i64),

    // TODO: exact value fixed point types - Decimal and Numeric

    // floating point types
    // reference: https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html
    // note: specifying exact precision and digits is not supported yet
    Float32Null(Option<f32>),
    Float32(f32),

    Float64Null(Option<f64>),
    Float64(f64),

    // TODO: date and timestamp

    // string types
    // reference: https://dev.mysql.com/doc/refman/8.0/en/string-types.html
    StringNull(Option<String>),
    String(String),

    BinaryNull(Option<Vec<u8>>),
    Binary(Vec<u8>),
}
