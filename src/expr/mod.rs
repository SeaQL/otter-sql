//! SQL expressions and their evaluation.

use std::{error::Error, fmt::Display};

use sqlparser::ast;

use crate::{
    identifier::{ColumnRef, IdentifierError},
    value::{Value, ValueError},
    BoundedString,
};

pub mod agg;
pub mod eval;

/// An expression
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Value(Value),
    ColumnRef(ColumnRef),
    Wildcard,
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

impl Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Value(v) => write!(f, "{}", v),
            Self::ColumnRef(c) => write!(f, "column '{}'", c),
            Self::Wildcard => write!(f, "*"),
            Self::Binary { left, op, right } => write!(f, "({} {} {})", left, op, right),
            Self::Unary { op, operand } => write!(f, "{}{}", op, operand),
            Self::Function { name, args } => write!(
                f,
                "{}({})",
                name,
                args.iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
        }
    }
}

/// A binary operator
#[derive(Debug, Copy, Clone, PartialEq)]
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
    And,
    Or,
}

impl Display for BinOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                BinOp::Plus => "+",
                BinOp::Minus => "-",
                BinOp::Multiply => "*",
                BinOp::Divide => "/",
                BinOp::Modulo => "%",
                BinOp::Equal => "=",
                BinOp::NotEqual => "!=",
                BinOp::LessThan => "<",
                BinOp::LessThanOrEqual => "<=",
                BinOp::GreaterThan => ">",
                BinOp::GreaterThanOrEqual => ">=",
                BinOp::Like => "LIKE",
                BinOp::ILike => "ILIKE",
                BinOp::And => "AND",
                BinOp::Or => "OR",
            }
        )
    }
}

/// A unary operator
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum UnOp {
    Plus,
    Minus,
    Not,
    IsFalse,
    IsTrue,
    IsNull,
    IsNotNull,
}

impl Display for UnOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                UnOp::Plus => "+",
                UnOp::Minus => "-",
                UnOp::Not => "NOT",
                UnOp::IsFalse => "IS FALSE",
                UnOp::IsTrue => "IS TRUE",
                UnOp::IsNull => "IS NULL",
                UnOp::IsNotNull => "IS NOT NULL",
            }
        )
    }
}

impl TryFrom<ast::BinaryOperator> for BinOp {
    type Error = ExprError;
    fn try_from(op: ast::BinaryOperator) -> Result<Self, Self::Error> {
        match op {
            ast::BinaryOperator::Plus => Ok(BinOp::Plus),
            ast::BinaryOperator::Minus => Ok(BinOp::Minus),
            ast::BinaryOperator::Multiply => Ok(BinOp::Multiply),
            ast::BinaryOperator::Divide => Ok(BinOp::Divide),
            ast::BinaryOperator::Modulo => Ok(BinOp::Modulo),
            ast::BinaryOperator::Eq => Ok(BinOp::Equal),
            ast::BinaryOperator::NotEq => Ok(BinOp::NotEqual),
            ast::BinaryOperator::Lt => Ok(BinOp::LessThan),
            ast::BinaryOperator::LtEq => Ok(BinOp::LessThanOrEqual),
            ast::BinaryOperator::Gt => Ok(BinOp::GreaterThan),
            ast::BinaryOperator::GtEq => Ok(BinOp::GreaterThanOrEqual),
            ast::BinaryOperator::Like => Ok(BinOp::Like),
            ast::BinaryOperator::ILike => Ok(BinOp::ILike),
            ast::BinaryOperator::And => Ok(BinOp::And),
            ast::BinaryOperator::Or => Ok(BinOp::Or),
            // TODO: xor?
            _ => Err(ExprError::Binary {
                reason: "Unknown binary operator",
                op,
            }),
        }
    }
}

impl TryFrom<ast::UnaryOperator> for UnOp {
    type Error = ExprError;
    fn try_from(op: ast::UnaryOperator) -> Result<Self, Self::Error> {
        match op {
            ast::UnaryOperator::Plus => Ok(UnOp::Plus),
            ast::UnaryOperator::Minus => Ok(UnOp::Minus),
            ast::UnaryOperator::Not => Ok(UnOp::Not),
            // IsFalse, IsTrue, etc. are handled in TryFrom<ast::Expr> for Expr
            // since `sqlparser` does not consider them unary operators for some reason.
            _ => Err(ExprError::Unary {
                reason: "Unkown unary operator",
                op,
            }),
        }
    }
}

/// Error in parsing an expression.
#[derive(Debug, PartialEq)]
pub enum ExprError {
    Expr {
        reason: &'static str,
        expr: ast::Expr,
    },
    Binary {
        reason: &'static str,
        op: ast::BinaryOperator,
    },
    Unary {
        reason: &'static str,
        op: ast::UnaryOperator,
    },
    Value(ValueError),
    Identifier(IdentifierError),
    UnknownAggregateFunction(String),
}

impl Display for ExprError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ExprError::Expr { reason, expr } => {
                write!(f, "ExprError: {}: {}", reason, expr)
            }
            ExprError::Binary { reason, op } => {
                write!(f, "ExprError: {}: {}", reason, op)
            }
            ExprError::Unary { reason, op } => {
                write!(f, "ExprError: {}: {}", reason, op)
            }
            ExprError::Value(v) => write!(f, "{}", v),
            ExprError::Identifier(v) => write!(f, "{}", v),
            ExprError::UnknownAggregateFunction(agg) => {
                write!(f, "Unsupported Aggregate Function: {}", agg)
            }
        }
    }
}

impl From<ValueError> for ExprError {
    fn from(v: ValueError) -> Self {
        Self::Value(v)
    }
}

impl From<IdentifierError> for ExprError {
    fn from(i: IdentifierError) -> Self {
        Self::Identifier(i)
    }
}

impl Error for ExprError {}
