//! SQL expressions and their evaluation.

use crate::{value::Value, BoundedString};

/// An expression
#[derive(Debug, Clone)]
pub enum Expr {
    Value(Value),
    ColumnRef(BoundedString),
    Binary {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    Unary {
        op: UnOp,
        operand: Box<Expr>,
    },
    Function {
        name: BoundedString,
        args: Vec<Expr>,
    },
}

/// A binary operator
#[derive(Debug, Copy, Clone)]
pub enum BinOp {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Like,
    ILike,
    Between,
    NotBetween,
    And,
    Or,
}

/// A unary operator
#[derive(Debug, Copy, Clone)]
pub enum UnOp {
    Plus,
    Minus,
    IsFalse,
    IsTrue,
    IsNull,
    IsNotNull,
}
