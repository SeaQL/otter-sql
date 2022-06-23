/// A value contained within a table's cell.
///
/// One or more column types may be mapped to a single variant of [`Value`].
#[derive(Debug, PartialEq)]
pub enum Value {
    Null,

    // integer types
    // reference: https://dev.mysql.com/doc/refman/8.0/en/integer-types.html
    UInt8(u8),

    Int8(i8),

    UInt16(u16),

    Int16(i16),

    UInt32(u32),

    Int32(i32),

    UInt64(u64),

    Int64(i64),

    // TODO: exact value fixed point types - Decimal and Numeric

    // floating point types
    // reference: https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html
    // note: specifying exact precision and digits is not supported yet
    Float32(f32),

    Float64(f64),

    // TODO: date and timestamp

    // string types
    // reference: https://dev.mysql.com/doc/refman/8.0/en/string-types.html
    String(String),

    Binary(Vec<u8>),
}

#[cfg(test)]
mod tests {
    use super::Value;

    #[test]
    fn create_value() {
        let value = Value::Null;
        assert_eq!(value, Value::Null);
        assert!(value != Value::String("test".to_owned()));
    }
}
