use std::fmt::Display;

use sqlparser::ast;

/// A value contained within a table's cell.
///
/// One or more column types may be mapped to a single variant of [`Value`].
#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    Null,

    Bool(bool),

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

impl TryFrom<ast::Value> for Value {
    type Error = ValueError;

    fn try_from(val: ast::Value) -> Result<Self, Self::Error> {
        match val {
            ast::Value::Null => Ok(Value::Null),
            ast::Value::Boolean(b) => Ok(Value::Bool(b)),
            ast::Value::SingleQuotedString(s) => Ok(Value::String(s)),
            ast::Value::DoubleQuotedString(s) => Ok(Value::String(s)),
            ast::Value::Number(ref s, _long) => {
                if let Ok(int) = s.parse::<i64>() {
                    Ok(Value::Int64(int))
                } else {
                    if let Ok(float) = s.parse::<f64>() {
                        Ok(Value::Float64(float))
                    } else {
                        Err(ValueError {
                            reason: "Unsupported number format",
                            value: val.clone(),
                        })
                    }
                }
            }
            _ => Err(ValueError {
                reason: "Unsupported value format",
                value: val,
            }),
        }
    }
}

pub struct ValueError {
    pub reason: &'static str,
    pub value: ast::Value,
}

impl Display for ValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}: {}", self.reason, self.value)
    }
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
