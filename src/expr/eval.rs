//! Evaluator of expressions.

use std::{error::Error, fmt::Display};

use crate::{
    expr::{BinOp, Expr, ExprError},
    value::{Value, ValueBinaryOpError, ValueUnaryOpError},
    VirtualMachine,
};

use super::UnOp;

impl Expr {
    pub fn execute(expr: Expr, context: &VirtualMachine) -> Result<Value, ExprExecError> {
        match expr {
            Expr::Value(v) => Ok(v),
            Expr::Binary {
                left,
                op: BinOp::And,
                right,
            } => {
                // we handle short circuit evaluation here
                let left = Expr::execute(*left, context)?;
                if let Value::Bool(b) = left {
                    if !b {
                        return Ok(Value::Bool(false));
                    }
                } else {
                    let right = Expr::execute(*right, context)?;
                    return Err(ExprExecError::ValueBinaryOpError(ValueBinaryOpError {
                        operator: BinOp::And,
                        values: (left, right),
                    }));
                };

                let right = Expr::execute(*right, context)?;
                if let Value::Bool(b) = right {
                    Ok(Value::Bool(b))
                } else {
                    Err(ExprExecError::ValueBinaryOpError(ValueBinaryOpError {
                        operator: BinOp::And,
                        values: (left, right),
                    }))
                }
            }
            Expr::Binary {
                left,
                op: BinOp::Or,
                right,
            } => {
                // we handle short circuit evaluation here
                let left = Expr::execute(*left, context)?;
                if let Value::Bool(b) = left {
                    if b {
                        return Ok(Value::Bool(true));
                    }
                } else {
                    let right = Expr::execute(*right, context)?;
                    return Err(ExprExecError::ValueBinaryOpError(ValueBinaryOpError {
                        operator: BinOp::Or,
                        values: (left, right),
                    }));
                };

                let right = Expr::execute(*right, context)?;
                if let Value::Bool(b) = right {
                    Ok(Value::Bool(b))
                } else {
                    Err(ExprExecError::ValueBinaryOpError(ValueBinaryOpError {
                        operator: BinOp::Or,
                        values: (left, right),
                    }))
                }
            }
            Expr::Binary { left, op, right } => {
                let left = Expr::execute(*left, context)?;
                let right = Expr::execute(*right, context)?;
                Ok(match op {
                    BinOp::Plus => left + right,
                    BinOp::Minus => left - right,
                    BinOp::Multiply => left * right,
                    BinOp::Divide => left / right,
                    BinOp::Modulo => left % right,
                    BinOp::Equal => Ok(Value::Bool(left == right)),
                    BinOp::NotEqual => Ok(Value::Bool(left != right)),
                    BinOp::LessThan => Ok(Value::Bool(left < right)),
                    BinOp::LessThanOrEqual => Ok(Value::Bool(left <= right)),
                    BinOp::GreaterThan => Ok(Value::Bool(left > right)),
                    BinOp::GreaterThanOrEqual => Ok(Value::Bool(left >= right)),
                    BinOp::Like => left.like(right),
                    BinOp::ILike => left.ilike(right),
                    BinOp::And | BinOp::Or => {
                        unreachable!("AND and OR should be handled separately")
                    }
                }?)
            }
            Expr::Unary { op, operand } => {
                let operand = Expr::execute(*operand, context)?;
                Ok(match op {
                    UnOp::Plus => Ok(operand),
                    UnOp::Minus => -operand,
                    UnOp::Not => !operand,
                    UnOp::IsFalse => operand.is_false(),
                    UnOp::IsTrue => operand.is_true(),
                    UnOp::IsNull => operand.is_null(),
                    UnOp::IsNotNull => operand.is_not_null(),
                }?)
            }
            Expr::Wildcard => Err(ExprExecError::CannotExecute(expr)),
            Expr::ColumnRef(c) => todo!(),
            Expr::Function { name, args } => todo!(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ExprExecError {
    CannotExecute(Expr),
    ValueBinaryOpError(ValueBinaryOpError),
    ValueUnaryOpError(ValueUnaryOpError),
}

impl From<ValueBinaryOpError> for ExprExecError {
    fn from(e: ValueBinaryOpError) -> Self {
        Self::ValueBinaryOpError(e)
    }
}

impl From<ValueUnaryOpError> for ExprExecError {
    fn from(e: ValueUnaryOpError) -> Self {
        Self::ValueUnaryOpError(e)
    }
}

impl Display for ExprExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CannotExecute(expr) => write!(f, "ExprExecError: cannot execute '{}'", expr),
            Self::ValueBinaryOpError(e) => write!(f, "ExprExecError: {}", e),
            Self::ValueUnaryOpError(e) => write!(f, "ExprExecError: {}", e),
        }
    }
}

impl Error for ExprExecError {}

#[cfg(test)]
mod test {
    use sqlparser::{dialect::GenericDialect, parser::Parser, tokenizer::Tokenizer};

    use crate::{
        expr::{BinOp, Expr, UnOp},
        value::{Value, ValueBinaryOpError, ValueUnaryOpError},
        VirtualMachine,
    };

    use super::ExprExecError;

    fn exec_expr(expr: Expr) -> Result<Value, ExprExecError> {
        let vm = VirtualMachine::new("test".into());
        Expr::execute(expr, &vm)
    }

    fn exec_str(s: &str) -> Result<Value, ExprExecError> {
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, s);
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(tokens, &dialect);
        let expr = parser.parse_expr().unwrap().try_into().unwrap();
        exec_expr(expr)
    }

    #[test]
    fn exec_value() {
        assert_eq!(exec_str("NULL"), Ok(Value::Null));

        assert_eq!(exec_str("true"), Ok(Value::Bool(true)));

        assert_eq!(exec_str("1"), Ok(Value::Int64(1)));

        assert_eq!(exec_str("1.1"), Ok(Value::Float64(1.1)));

        assert_eq!(exec_str(".1"), Ok(Value::Float64(0.1)));

        assert_eq!(exec_str("'str'"), Ok(Value::String("str".to_owned())));
    }

    #[test]
    fn exec_logical() {
        assert_eq!(exec_str("true and true"), Ok(Value::Bool(true)));
        assert_eq!(exec_str("true and false"), Ok(Value::Bool(false)));
        assert_eq!(exec_str("false and true"), Ok(Value::Bool(false)));
        assert_eq!(exec_str("false and false"), Ok(Value::Bool(false)));
        assert_eq!(exec_str("false and 10"), Ok(Value::Bool(false)));
        assert_eq!(
            exec_str("10 and false"),
            Err(ValueBinaryOpError {
                operator: BinOp::And,
                values: (Value::Int64(10), Value::Bool(false))
            }
            .into())
        );

        assert_eq!(exec_str("true or true"), Ok(Value::Bool(true)));
        assert_eq!(exec_str("true or false"), Ok(Value::Bool(true)));
        assert_eq!(exec_str("false or true"), Ok(Value::Bool(true)));
        assert_eq!(exec_str("false or false"), Ok(Value::Bool(false)));
        assert_eq!(exec_str("true or 10"), Ok(Value::Bool(true)));
        assert_eq!(
            exec_str("10 or true"),
            Err(ValueBinaryOpError {
                operator: BinOp::Or,
                values: (Value::Int64(10), Value::Bool(true))
            }
            .into())
        );
    }

    #[test]
    fn exec_arithmetic() {
        assert_eq!(exec_str("1 + 1"), Ok(Value::Int64(2)));
        assert_eq!(exec_str("1.1 + 1.1"), Ok(Value::Float64(2.2)));

        // this applies to all binary ops
        assert_eq!(
            exec_str("1 + 1.1"),
            Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (Value::Int64(1), Value::Float64(1.1))
            }
            .into())
        );

        assert_eq!(exec_str("4 - 2"), Ok(Value::Int64(2)));
        assert_eq!(exec_str("4 - 6"), Ok(Value::Int64(-2)));
        assert_eq!(exec_str("4.5 - 2.2"), Ok(Value::Float64(2.3)));

        assert_eq!(exec_str("4 * 2"), Ok(Value::Int64(8)));
        assert_eq!(exec_str("0.5 * 2.2"), Ok(Value::Float64(1.1)));

        assert_eq!(exec_str("4 / 2"), Ok(Value::Int64(2)));
        assert_eq!(exec_str("4 / 3"), Ok(Value::Int64(1)));
        assert_eq!(exec_str("4.0 / 2.0"), Ok(Value::Float64(2.0)));
        assert_eq!(exec_str("5.1 / 2.5"), Ok(Value::Float64(2.04)));

        assert_eq!(exec_str("5 % 2"), Ok(Value::Int64(1)));
        assert_eq!(exec_str("5.5 % 2.5"), Ok(Value::Float64(0.5)));
    }

    #[test]
    fn exec_comparison() {
        assert_eq!(exec_str("1 = 1"), Ok(Value::Bool(true)));
        assert_eq!(exec_str("1 = 2"), Ok(Value::Bool(false)));
        assert_eq!(exec_str("1 != 2"), Ok(Value::Bool(true)));
        assert_eq!(exec_str("1.1 = 1.1"), Ok(Value::Bool(true)));
        assert_eq!(exec_str("1.2 = 1.22"), Ok(Value::Bool(false)));
        assert_eq!(exec_str("1.2 != 1.22"), Ok(Value::Bool(true)));

        assert_eq!(exec_str("1 < 2"), Ok(Value::Bool(true)));
        assert_eq!(exec_str("1 < 1"), Ok(Value::Bool(false)));
        assert_eq!(exec_str("1 <= 2"), Ok(Value::Bool(true)));
        assert_eq!(exec_str("1 <= 1"), Ok(Value::Bool(true)));
        assert_eq!(exec_str("3 > 2"), Ok(Value::Bool(true)));
        assert_eq!(exec_str("3 > 3"), Ok(Value::Bool(false)));
        assert_eq!(exec_str("3 >= 2"), Ok(Value::Bool(true)));
        assert_eq!(exec_str("3 >= 3"), Ok(Value::Bool(true)));
    }

    #[test]
    fn exec_pattern_match() {
        assert_eq!(
            exec_str("'my name is yoshikage kira' LIKE 'kira'"),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            exec_str("'my name is yoshikage kira' LIKE 'KIRA'"),
            Ok(Value::Bool(false))
        );
        assert_eq!(
            exec_str("'my name is yoshikage kira' LIKE 'kira yoshikage'"),
            Ok(Value::Bool(false))
        );

        assert_eq!(
            exec_str("'my name is Yoshikage Kira' ILIKE 'kira'"),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            exec_str("'my name is Yoshikage Kira' ILIKE 'KIRA'"),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            exec_str("'my name is Yoshikage Kira' ILIKE 'KIRAA'"),
            Ok(Value::Bool(false))
        );
    }

    #[test]
    fn exec_unary() {
        assert_eq!(exec_str("+1"), Ok(Value::Int64(1)));
        assert_eq!(exec_str("+ -1"), Ok(Value::Int64(-1)));
        assert_eq!(exec_str("-1"), Ok(Value::Int64(-1)));
        assert_eq!(exec_str("- -1"), Ok(Value::Int64(1)));
        assert_eq!(exec_str("not true"), Ok(Value::Bool(false)));
        assert_eq!(exec_str("not false"), Ok(Value::Bool(true)));

        assert_eq!(exec_str("true is true"), Ok(Value::Bool(true)));
        assert_eq!(exec_str("false is false"), Ok(Value::Bool(true)));
        assert_eq!(exec_str("false is true"), Ok(Value::Bool(false)));
        assert_eq!(exec_str("true is false"), Ok(Value::Bool(false)));
        assert_eq!(
            exec_str("1 is true"),
            Err(ValueUnaryOpError {
                operator: UnOp::Not,
                value: Value::Int64(1)
            }
            .into())
        );

        assert_eq!(exec_str("NULL is NULL"), Ok(Value::Bool(true)));
        assert_eq!(exec_str("NULL is not NULL"), Ok(Value::Bool(false)));
        assert_eq!(exec_str("1 is NULL"), Ok(Value::Bool(false)));
        assert_eq!(exec_str("1 is not NULL"), Ok(Value::Bool(true)));
        assert_eq!(exec_str("0 is not NULL"), Ok(Value::Bool(true)));
        assert_eq!(exec_str("'' is not NULL"), Ok(Value::Bool(true)));
    }

    #[test]
    fn exec_wildcard() {
        assert_eq!(
            exec_expr(Expr::Wildcard),
            Err(ExprExecError::CannotExecute(Expr::Wildcard))
        );
    }

    // #[test]
    // fn exec_column_ref() {
    //     todo!()
    // }

    // #[test]
    // fn exec_function() {
    //     todo!()
    // }
}
