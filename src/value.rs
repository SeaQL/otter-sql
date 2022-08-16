use std::{
    fmt::Display,
    ops::{Add, Div, Mul, Neg, Not, Rem, Sub},
};

use sqlparser::ast;

use crate::expr::{BinOp, UnOp};

/// A value contained within a table's cell.
///
/// One or more column types may be mapped to a single variant of [`Value`].
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Value {
    Null,

    Bool(bool),

    // integer types
    // reference: https://dev.mysql.com/doc/refman/8.0/en/integer-types.html
    // TODO: other integer types. Currently, all integers are casted to Int64.
    Int64(i64),

    // TODO: exact value fixed point types - Decimal and Numeric

    // floating point types
    // reference: https://dev.mysql.com/doc/refman/8.0/en/floating-point-types.html
    // note: specifying exact precision and digits is not supported yet
    // TODO: Float32
    Float64(f64),

    // TODO: date and timestamp

    // string types
    // reference: https://dev.mysql.com/doc/refman/8.0/en/string-types.html
    String(String),

    Binary(Vec<u8>),
}

impl Value {
    pub fn is_true(self) -> Result<Value, ValueUnaryOpError> {
        match self {
            Value::Bool(lhs) => Ok(Value::Bool(lhs)),
            _ => Err(ValueUnaryOpError {
                operator: UnOp::Not,
                value: self,
            }),
        }
    }

    pub fn is_false(self) -> Result<Value, ValueUnaryOpError> {
        match self {
            Value::Bool(lhs) => Ok(Value::Bool(!lhs)),
            _ => Err(ValueUnaryOpError {
                operator: UnOp::Not,
                value: self,
            }),
        }
    }

    pub fn is_null(self) -> Result<Value, ValueUnaryOpError> {
        match self {
            Value::Null => Ok(Value::Bool(true)),
            _ => Ok(Value::Bool(false)),
        }
    }

    pub fn is_not_null(self) -> Result<Value, ValueUnaryOpError> {
        match self {
            Value::Null => Ok(Value::Bool(false)),
            _ => Ok(Value::Bool(true)),
        }
    }

    pub fn like(self, rhs: Value) -> Result<Value, ValueBinaryOpError> {
        match (&self, &rhs) {
            // TODO: implement proper pattern matching
            (Value::String(lhs), Value::String(rhs)) => Ok(Value::Bool(lhs.contains(rhs))),
            _ => Err(ValueBinaryOpError {
                operator: BinOp::Like,
                values: (self, rhs),
            }),
        }
    }

    pub fn ilike(self, rhs: Value) -> Result<Value, ValueBinaryOpError> {
        match (&self, &rhs) {
            // TODO: implement proper pattern matching
            (Value::String(lhs), Value::String(rhs)) => Ok(Value::Bool(
                lhs.to_lowercase().contains(&rhs.to_lowercase()),
            )),
            _ => Err(ValueBinaryOpError {
                operator: BinOp::Like,
                values: (self, rhs),
            }),
        }
    }
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

impl Add for Value {
    type Output = Result<Value, ValueBinaryOpError>;

    fn add(self, rhs: Self) -> Self::Output {
        match self {
            Value::Null => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
            Value::Bool(_) => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
            Value::Int64(lhs) => match rhs {
                Value::Int64(rhs) => Ok(Value::Int64(lhs + rhs)),
                _ => Err(ValueBinaryOpError {
                    operator: BinOp::Plus,
                    values: (self, rhs),
                }),
            },
            Value::Float64(lhs) => match rhs {
                Value::Float64(rhs) => Ok(Value::Float64(lhs + rhs)),
                _ => Err(ValueBinaryOpError {
                    operator: BinOp::Plus,
                    values: (self, rhs),
                }),
            },
            Value::String(_) => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
            Value::Binary(_) => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
        }
    }
}

impl Sub for Value {
    type Output = Result<Value, ValueBinaryOpError>;

    fn sub(self, rhs: Self) -> Self::Output {
        match self {
            Value::Null => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
            Value::Bool(_) => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
            Value::Int64(lhs) => match rhs {
                Value::Int64(rhs) => Ok(Value::Int64(lhs - rhs)),
                _ => Err(ValueBinaryOpError {
                    operator: BinOp::Plus,
                    values: (self, rhs),
                }),
            },
            Value::Float64(lhs) => match rhs {
                Value::Float64(rhs) => Ok(Value::Float64(lhs - rhs)),
                _ => Err(ValueBinaryOpError {
                    operator: BinOp::Plus,
                    values: (self, rhs),
                }),
            },
            Value::String(_) => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
            Value::Binary(_) => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
        }
    }
}

impl Mul for Value {
    type Output = Result<Value, ValueBinaryOpError>;

    fn mul(self, rhs: Self) -> Self::Output {
        match self {
            Value::Null => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
            Value::Bool(_) => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
            Value::Int64(lhs) => match rhs {
                Value::Int64(rhs) => Ok(Value::Int64(lhs * rhs)),
                _ => Err(ValueBinaryOpError {
                    operator: BinOp::Plus,
                    values: (self, rhs),
                }),
            },
            Value::Float64(lhs) => match rhs {
                Value::Float64(rhs) => Ok(Value::Float64(lhs * rhs)),
                _ => Err(ValueBinaryOpError {
                    operator: BinOp::Plus,
                    values: (self, rhs),
                }),
            },
            Value::String(_) => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
            Value::Binary(_) => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
        }
    }
}

impl Div for Value {
    type Output = Result<Value, ValueBinaryOpError>;

    fn div(self, rhs: Self) -> Self::Output {
        match self {
            Value::Null => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
            Value::Bool(_) => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
            Value::Int64(lhs) => match rhs {
                Value::Int64(rhs) => Ok(Value::Int64(lhs / rhs)),
                _ => Err(ValueBinaryOpError {
                    operator: BinOp::Plus,
                    values: (self, rhs),
                }),
            },
            Value::Float64(lhs) => match rhs {
                Value::Float64(rhs) => Ok(Value::Float64(lhs / rhs)),
                _ => Err(ValueBinaryOpError {
                    operator: BinOp::Plus,
                    values: (self, rhs),
                }),
            },
            Value::String(_) => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
            Value::Binary(_) => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
        }
    }
}

impl Rem for Value {
    type Output = Result<Value, ValueBinaryOpError>;

    fn rem(self, rhs: Self) -> Self::Output {
        match self {
            Value::Null => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
            Value::Bool(_) => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
            Value::Int64(lhs) => match rhs {
                Value::Int64(rhs) => Ok(Value::Int64(lhs % rhs)),
                _ => Err(ValueBinaryOpError {
                    operator: BinOp::Plus,
                    values: (self, rhs),
                }),
            },
            Value::Float64(lhs) => match rhs {
                Value::Float64(rhs) => Ok(Value::Float64(lhs % rhs)),
                _ => Err(ValueBinaryOpError {
                    operator: BinOp::Plus,
                    values: (self, rhs),
                }),
            },
            Value::String(_) => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
            Value::Binary(_) => Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (self, rhs),
            }),
        }
    }
}

impl Neg for Value {
    type Output = Result<Value, ValueUnaryOpError>;

    fn neg(self) -> Self::Output {
        match self {
            Value::Null => Err(ValueUnaryOpError {
                operator: UnOp::Minus,
                value: self,
            }),
            Value::Bool(_) => Err(ValueUnaryOpError {
                operator: UnOp::Minus,
                value: self,
            }),
            Value::Int64(lhs) => Ok(Value::Int64(-lhs)),
            Value::Float64(lhs) => Ok(Value::Float64(-lhs)),
            Value::String(_) => Err(ValueUnaryOpError {
                operator: UnOp::Minus,
                value: self,
            }),
            Value::Binary(_) => Err(ValueUnaryOpError {
                operator: UnOp::Minus,
                value: self,
            }),
        }
    }
}

impl Not for Value {
    type Output = Result<Value, ValueUnaryOpError>;

    fn not(self) -> Self::Output {
        match self {
            Value::Bool(lhs) => Ok(Value::Bool(!lhs)),
            _ => Err(ValueUnaryOpError {
                operator: UnOp::Not,
                value: self,
            }),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ValueBinaryOpError {
    pub operator: BinOp,
    pub values: (Value, Value),
}

impl Display for ValueBinaryOpError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "ValueBinaryOpError: unsupported operation '{}' between '{:?}' and '{:?}'",
            self.operator, self.values.0, self.values.1
        )
    }
}

#[derive(Debug, PartialEq)]
pub struct ValueUnaryOpError {
    pub operator: UnOp,
    pub value: Value,
}

impl Display for ValueUnaryOpError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "ValueUnaryOpError: unsupported operation '{}' for '{:?}'",
            self.operator, self.value
        )
    }
}

#[derive(Debug, PartialEq)]
pub struct ValueError {
    pub reason: &'static str,
    pub value: ast::Value,
}

impl Display for ValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ValueError: {}: {}", self.reason, self.value)
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast;

    use crate::value::ValueError;

    use super::Value;

    #[test]
    fn create_value() {
        let value = Value::Null;
        assert_eq!(value, Value::Null);
        assert!(value != Value::String("test".to_owned()));
    }

    #[test]
    fn conversion_from_ast() {
        assert_eq!(Value::try_from(ast::Value::Null), Ok(Value::Null));

        assert_eq!(
            Value::try_from(ast::Value::Number("1000".to_owned(), false)),
            Ok(Value::Int64(1000))
        );

        assert_eq!(
            Value::try_from(ast::Value::Number("1000".to_owned(), true)),
            Ok(Value::Int64(1000))
        );

        assert_eq!(
            Value::try_from(ast::Value::Number("1000.0".to_owned(), false)),
            Ok(Value::Float64(1000.0))
        );

        assert_eq!(
            Value::try_from(ast::Value::Number("0.300000000000000004".to_owned(), false)),
            Ok(Value::Float64(0.300000000000000004))
        );

        assert_eq!(
            Value::try_from(ast::Value::Number("-1".to_owned(), false)),
            Ok(Value::Int64(-1))
        );

        assert_eq!(
            Value::try_from(ast::Value::Number("9223372036854775807".to_owned(), false)),
            Ok(Value::Int64(9223372036854775807))
        );

        assert_eq!(
            Value::try_from(ast::Value::HexStringLiteral("brr".to_owned())),
            Err(ValueError {
                reason: "Unsupported value format",
                value: ast::Value::HexStringLiteral("brr".to_owned())
            })
        )
    }
}
