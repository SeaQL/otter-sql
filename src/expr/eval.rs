//! Evaluator of expressions.

use std::{error::Error, fmt::Display};

use crate::{
    expr::{BinOp, Expr, UnOp},
    identifier::BoundedString,
    table::{RowLike, RowShared, Table},
    value::{Value, ValueBinaryOpError, ValueUnaryOpError},
};

impl Expr {
    pub fn execute(expr: &Expr, table: &Table, row: RowShared) -> Result<Value, ExprExecError> {
        match expr {
            Expr::Value(v) => Ok(v.to_owned()),
            Expr::Binary {
                left,
                op: BinOp::And,
                right,
            } => {
                let left = Expr::execute(left, table, row.clone())?;
                let right = Expr::execute(right, table, row)?;

                match (&left, &right) {
                    (Value::Bool(left), Value::Bool(right)) => Ok(Value::Bool(*left && *right)),
                    _ => Err(ExprExecError::ValueBinaryOpError(ValueBinaryOpError {
                        operator: BinOp::And,
                        values: (left, right),
                    })),
                }
            }
            Expr::Binary {
                left,
                op: BinOp::Or,
                right,
            } => {
                let left = Expr::execute(left, table, row.clone())?;
                let right = Expr::execute(right, table, row)?;

                match (&left, &right) {
                    (Value::Bool(left), Value::Bool(right)) => Ok(Value::Bool(*left || *right)),
                    _ => Err(ExprExecError::ValueBinaryOpError(ValueBinaryOpError {
                        operator: BinOp::Or,
                        values: (left, right),
                    })),
                }
            }
            Expr::Binary { left, op, right } => {
                let left = Expr::execute(left, table, row.clone())?;
                let right = Expr::execute(right, table, row)?;
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
                let operand = Expr::execute(operand, table, row)?;
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
            Expr::Wildcard => Err(ExprExecError::CannotExecute(expr.to_owned())),
            Expr::ColumnRef(col_ref) => {
                let col_index = if let Some(col_index) =
                    table.columns().position(|c| c.name() == &col_ref.col_name)
                {
                    col_index
                } else {
                    // TODO: show table name here too
                    // and think of how it will work for JOINs and temp tables
                    return Err(ExprExecError::NoSuchColumn(col_ref.col_name));
                };
                if let Some(val) = row.data().get(col_index) {
                    Ok(val.clone())
                } else {
                    // TODO: show the row here too
                    return Err(ExprExecError::CorruptedData {
                        col_name: col_ref.col_name,
                        table_name: *table.name(),
                    });
                }
            }
            // TODO: functions
            Expr::Function { name: _, args: _ } => todo!(),
        }
    }
}

/// Error in execution of an expression.
#[derive(Debug, PartialEq)]
pub enum ExprExecError {
    CannotExecute(Expr),
    ValueBinaryOpError(ValueBinaryOpError),
    ValueUnaryOpError(ValueUnaryOpError),
    NoSuchColumn(BoundedString),
    CorruptedData {
        col_name: BoundedString,
        table_name: BoundedString,
    },
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
            Self::NoSuchColumn(col_name) => {
                write!(f, "ExprExecError: no such column '{}'", col_name)
            }
            Self::CorruptedData {
                col_name,
                table_name,
            } => write!(
                f,
                "ExprExecError: data is corrupted for column '{}' of table '{}'",
                col_name, table_name
            ),
        }
    }
}

impl Error for ExprExecError {}
