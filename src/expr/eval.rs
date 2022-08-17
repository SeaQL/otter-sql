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
