//! SQL expressions and their evaluation.

use std::{error::Error, fmt::Display};

use sqlparser::ast;

use crate::{
    identifier::{ColumnRef, IdentifierError},
    value::{Value, ValueError},
    BoundedString,
};

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

impl TryFrom<ast::Expr> for Expr {
    type Error = ExprError;
    fn try_from(expr_ast: ast::Expr) -> Result<Self, Self::Error> {
        match expr_ast {
            ast::Expr::Identifier(i) => Ok(Expr::ColumnRef(vec![i].try_into()?)),
            ast::Expr::CompoundIdentifier(i) => Ok(Expr::ColumnRef(i.try_into()?)),
            ast::Expr::IsFalse(e) => Ok(Expr::Unary {
                op: UnOp::IsFalse,
                operand: Box::new((*e).try_into()?),
            }),
            ast::Expr::IsTrue(e) => Ok(Expr::Unary {
                op: UnOp::IsTrue,
                operand: Box::new((*e).try_into()?),
            }),
            ast::Expr::IsNull(e) => Ok(Expr::Unary {
                op: UnOp::IsNull,
                operand: Box::new((*e).try_into()?),
            }),
            ast::Expr::IsNotNull(e) => Ok(Expr::Unary {
                op: UnOp::IsNotNull,
                operand: Box::new((*e).try_into()?),
            }),
            ast::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr: Box<Expr> = Box::new((*expr).try_into()?);
                let left = Box::new((*low).try_into()?);
                let right = Box::new((*high).try_into()?);
                let between = Expr::Binary {
                    left: Box::new(Expr::Binary {
                        left,
                        op: BinOp::LessThanOrEqual,
                        right: expr.clone(),
                    }),
                    op: BinOp::And,
                    right: Box::new(Expr::Binary {
                        left: expr,
                        op: BinOp::LessThanOrEqual,
                        right,
                    }),
                };
                if negated {
                    Ok(Expr::Unary {
                        op: UnOp::Not,
                        operand: Box::new(between),
                    })
                } else {
                    Ok(between)
                }
            }
            ast::Expr::BinaryOp { left, op, right } => Ok(Expr::Binary {
                left: Box::new((*left).try_into()?),
                op: op.try_into()?,
                right: Box::new((*right).try_into()?),
            }),
            ast::Expr::UnaryOp { op, expr } => Ok(Expr::Unary {
                op: op.try_into()?,
                operand: Box::new((*expr).try_into()?),
            }),
            ast::Expr::Value(v) => Ok(Expr::Value(v.try_into()?)),
            ast::Expr::Function(ref f) => Ok(Expr::Function {
                name: f.name.to_string().as_str().into(),
                args: f
                    .args
                    .iter()
                    .map(|arg| match arg {
                        ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                            ast::FunctionArgExpr::Expr(e) => Ok(e.clone().try_into()?),
                            ast::FunctionArgExpr::Wildcard => Ok(Expr::Wildcard),
                            ast::FunctionArgExpr::QualifiedWildcard(_) => Err(ExprError::Expr {
                                reason: "Qualified wildcards are not supported yet",
                                expr: expr_ast.clone(),
                            }),
                        },
                        ast::FunctionArg::Named { .. } => Err(ExprError::Expr {
                            reason: "Named function arguments are not supported",
                            expr: expr_ast.clone(),
                        }),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            }),
            _ => Err(ExprError::Expr {
                reason: "Unsupported expression",
                expr: expr_ast,
            }),
        }
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

#[cfg(test)]
mod tests {
    use sqlparser::{ast, dialect::GenericDialect, parser::Parser, tokenizer::Tokenizer};

    use crate::{
        expr::{BinOp, Expr, UnOp},
        identifier::ColumnRef,
        value::Value,
    };

    #[test]
    fn conversion_from_ast() {
        fn parse_expr(s: &str) -> ast::Expr {
            let dialect = GenericDialect {};
            let mut tokenizer = Tokenizer::new(&dialect, s);
            let tokens = tokenizer.tokenize().unwrap();
            let mut parser = Parser::new(tokens, &dialect);
            parser.parse_expr().unwrap()
        }

        assert_eq!(
            parse_expr("abc").try_into(),
            Ok(Expr::ColumnRef(ColumnRef {
                schema_name: None,
                table_name: None,
                col_name: "abc".into()
            }))
        );

        assert_ne!(
            parse_expr("abc").try_into(),
            Ok(Expr::ColumnRef(ColumnRef {
                schema_name: None,
                table_name: None,
                col_name: "cab".into()
            }))
        );

        assert_eq!(
            parse_expr("table1.col1").try_into(),
            Ok(Expr::ColumnRef(ColumnRef {
                schema_name: None,
                table_name: Some("table1".into()),
                col_name: "col1".into()
            }))
        );

        assert_eq!(
            parse_expr("schema1.table1.col1").try_into(),
            Ok(Expr::ColumnRef(ColumnRef {
                schema_name: Some("schema1".into()),
                table_name: Some("table1".into()),
                col_name: "col1".into()
            }))
        );

        assert_eq!(
            parse_expr("5 IS NULL").try_into(),
            Ok(Expr::Unary {
                op: UnOp::IsNull,
                operand: Box::new(Expr::Value(Value::Int64(5)))
            })
        );

        assert_eq!(
            parse_expr("1 IS TRUE").try_into(),
            Ok(Expr::Unary {
                op: UnOp::IsTrue,
                operand: Box::new(Expr::Value(Value::Int64(1)))
            })
        );

        assert_eq!(
            parse_expr("4 BETWEEN 3 AND 5").try_into(),
            Ok(Expr::Binary {
                left: Box::new(Expr::Binary {
                    left: Box::new(Expr::Value(Value::Int64(3))),
                    op: BinOp::LessThanOrEqual,
                    right: Box::new(Expr::Value(Value::Int64(4)))
                }),
                op: BinOp::And,
                right: Box::new(Expr::Binary {
                    left: Box::new(Expr::Value(Value::Int64(4))),
                    op: BinOp::LessThanOrEqual,
                    right: Box::new(Expr::Value(Value::Int64(5)))
                })
            })
        );

        assert_eq!(
            parse_expr("4 NOT BETWEEN 3 AND 5").try_into(),
            Ok(Expr::Unary {
                op: UnOp::Not,
                operand: Box::new(Expr::Binary {
                    left: Box::new(Expr::Binary {
                        left: Box::new(Expr::Value(Value::Int64(3))),
                        op: BinOp::LessThanOrEqual,
                        right: Box::new(Expr::Value(Value::Int64(4)))
                    }),
                    op: BinOp::And,
                    right: Box::new(Expr::Binary {
                        left: Box::new(Expr::Value(Value::Int64(4))),
                        op: BinOp::LessThanOrEqual,
                        right: Box::new(Expr::Value(Value::Int64(5)))
                    })
                })
            })
        );

        assert_eq!(
            parse_expr("MAX(col1)").try_into(),
            Ok(Expr::Function {
                name: "MAX".into(),
                args: vec![Expr::ColumnRef(ColumnRef {
                    schema_name: None,
                    table_name: None,
                    col_name: "col1".into()
                })]
            })
        );

        assert_eq!(
            parse_expr("some_func(col1, 1, 'abc')").try_into(),
            Ok(Expr::Function {
                name: "some_func".into(),
                args: vec![
                    Expr::ColumnRef(ColumnRef {
                        schema_name: None,
                        table_name: None,
                        col_name: "col1".into()
                    }),
                    Expr::Value(Value::Int64(1)),
                    Expr::Value(Value::String("abc".to_owned()))
                ]
            })
        );

        assert_eq!(
            parse_expr("COUNT(*)").try_into(),
            Ok(Expr::Function {
                name: "COUNT".into(),
                args: vec![Expr::Wildcard]
            })
        );
    }
}
