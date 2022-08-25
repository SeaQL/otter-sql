//! Evaluator of expressions.

use std::{error::Error, fmt::Display};

use crate::{
    expr::{BinOp, Expr},
    identifier::BoundedString,
    table::{Row, Table},
    value::{Value, ValueBinaryOpError, ValueUnaryOpError},
    VirtualMachine,
};

use super::UnOp;

impl Expr {
    pub fn execute(
        expr: Expr,
        vm: &VirtualMachine,
        table: &Table,
        row: &Row,
    ) -> Result<Value, ExprExecError> {
        match expr {
            Expr::Value(v) => Ok(v),
            Expr::Binary {
                left,
                op: BinOp::And,
                right,
            } => {
                let left = Expr::execute(*left, vm, table, row)?;
                let right = Expr::execute(*right, vm, table, row)?;

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
                let left = Expr::execute(*left, vm, table, row)?;
                let right = Expr::execute(*right, vm, table, row)?;

                match (&left, &right) {
                    (Value::Bool(left), Value::Bool(right)) => Ok(Value::Bool(*left || *right)),
                    _ => Err(ExprExecError::ValueBinaryOpError(ValueBinaryOpError {
                        operator: BinOp::Or,
                        values: (left, right),
                    })),
                }
            }
            Expr::Binary { left, op, right } => {
                let left = Expr::execute(*left, vm, table, row)?;
                let right = Expr::execute(*right, vm, table, row)?;
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
                let operand = Expr::execute(*operand, vm, table, row)?;
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
                if let Some(val) = row.data.get(col_index) {
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
            Expr::Function { name, args } => todo!(),
        }
    }
}

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

#[cfg(test)]
mod test {
    use sqlparser::{
        ast::{ColumnOption, ColumnOptionDef, DataType},
        dialect::GenericDialect,
        parser::Parser,
        tokenizer::Tokenizer,
    };

    use crate::{
        column::Column,
        expr::{BinOp, Expr, UnOp},
        table::{Row, Table},
        value::{Value, ValueBinaryOpError, ValueUnaryOpError},
        VirtualMachine,
    };

    use super::ExprExecError;

    fn str_to_expr(s: &str) -> Expr {
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, s);
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(tokens, &dialect);
        parser.parse_expr().unwrap().try_into().unwrap()
    }

    fn exec_expr_no_context(expr: Expr) -> Result<Value, ExprExecError> {
        let vm = VirtualMachine::new("test".into());
        let mut table = Table::new_temp(0);
        table.new_row(vec![]);
        Expr::execute(expr, &vm, &table, &table.all_data()[0])
    }

    fn exec_str_no_context(s: &str) -> Result<Value, ExprExecError> {
        let expr = str_to_expr(s);
        exec_expr_no_context(expr)
    }

    fn exec_str_with_context(s: &str, table: &Table, row: &Row) -> Result<Value, ExprExecError> {
        let expr = str_to_expr(s);
        let vm = VirtualMachine::new("test".into());
        Expr::execute(expr, &vm, table, row)
    }

    #[test]
    fn exec_value() {
        assert_eq!(exec_str_no_context("NULL"), Ok(Value::Null));

        assert_eq!(exec_str_no_context("true"), Ok(Value::Bool(true)));

        assert_eq!(exec_str_no_context("1"), Ok(Value::Int64(1)));

        assert_eq!(exec_str_no_context("1.1"), Ok(Value::Float64(1.1)));

        assert_eq!(exec_str_no_context(".1"), Ok(Value::Float64(0.1)));

        assert_eq!(
            exec_str_no_context("'str'"),
            Ok(Value::String("str".to_owned()))
        );
    }

    #[test]
    fn exec_logical() {
        assert_eq!(exec_str_no_context("true and true"), Ok(Value::Bool(true)));
        assert_eq!(
            exec_str_no_context("true and false"),
            Ok(Value::Bool(false))
        );
        assert_eq!(
            exec_str_no_context("false and true"),
            Ok(Value::Bool(false))
        );
        assert_eq!(
            exec_str_no_context("false and false"),
            Ok(Value::Bool(false))
        );
        assert_eq!(
            exec_str_no_context("false and 10"),
            Err(ValueBinaryOpError {
                operator: BinOp::And,
                values: (Value::Bool(false), Value::Int64(10))
            }
            .into())
        );
        assert_eq!(
            exec_str_no_context("10 and false"),
            Err(ValueBinaryOpError {
                operator: BinOp::And,
                values: (Value::Int64(10), Value::Bool(false))
            }
            .into())
        );

        assert_eq!(exec_str_no_context("true or true"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("true or false"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("false or true"), Ok(Value::Bool(true)));
        assert_eq!(
            exec_str_no_context("false or false"),
            Ok(Value::Bool(false))
        );
        assert_eq!(
            exec_str_no_context("true or 10"),
            Err(ValueBinaryOpError {
                operator: BinOp::Or,
                values: (Value::Bool(true), Value::Int64(10))
            }
            .into())
        );
        assert_eq!(
            exec_str_no_context("10 or true"),
            Err(ValueBinaryOpError {
                operator: BinOp::Or,
                values: (Value::Int64(10), Value::Bool(true))
            }
            .into())
        );
    }

    #[test]
    fn exec_arithmetic() {
        assert_eq!(exec_str_no_context("1 + 1"), Ok(Value::Int64(2)));
        assert_eq!(exec_str_no_context("1.1 + 1.1"), Ok(Value::Float64(2.2)));

        // this applies to all binary ops
        assert_eq!(
            exec_str_no_context("1 + 1.1"),
            Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (Value::Int64(1), Value::Float64(1.1))
            }
            .into())
        );

        assert_eq!(exec_str_no_context("4 - 2"), Ok(Value::Int64(2)));
        assert_eq!(exec_str_no_context("4 - 6"), Ok(Value::Int64(-2)));
        assert_eq!(exec_str_no_context("4.5 - 2.2"), Ok(Value::Float64(2.3)));

        assert_eq!(exec_str_no_context("4 * 2"), Ok(Value::Int64(8)));
        assert_eq!(exec_str_no_context("0.5 * 2.2"), Ok(Value::Float64(1.1)));

        assert_eq!(exec_str_no_context("4 / 2"), Ok(Value::Int64(2)));
        assert_eq!(exec_str_no_context("4 / 3"), Ok(Value::Int64(1)));
        assert_eq!(exec_str_no_context("4.0 / 2.0"), Ok(Value::Float64(2.0)));
        assert_eq!(exec_str_no_context("5.1 / 2.5"), Ok(Value::Float64(2.04)));

        assert_eq!(exec_str_no_context("5 % 2"), Ok(Value::Int64(1)));
        assert_eq!(exec_str_no_context("5.5 % 2.5"), Ok(Value::Float64(0.5)));
    }

    #[test]
    fn exec_comparison() {
        assert_eq!(exec_str_no_context("1 = 1"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("1 = 2"), Ok(Value::Bool(false)));
        assert_eq!(exec_str_no_context("1 != 2"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("1.1 = 1.1"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("1.2 = 1.22"), Ok(Value::Bool(false)));
        assert_eq!(exec_str_no_context("1.2 != 1.22"), Ok(Value::Bool(true)));

        assert_eq!(exec_str_no_context("1 < 2"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("1 < 1"), Ok(Value::Bool(false)));
        assert_eq!(exec_str_no_context("1 <= 2"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("1 <= 1"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("3 > 2"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("3 > 3"), Ok(Value::Bool(false)));
        assert_eq!(exec_str_no_context("3 >= 2"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("3 >= 3"), Ok(Value::Bool(true)));
    }

    #[test]
    fn exec_pattern_match() {
        assert_eq!(
            exec_str_no_context("'my name is yoshikage kira' LIKE 'kira'"),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            exec_str_no_context("'my name is yoshikage kira' LIKE 'KIRA'"),
            Ok(Value::Bool(false))
        );
        assert_eq!(
            exec_str_no_context("'my name is yoshikage kira' LIKE 'kira yoshikage'"),
            Ok(Value::Bool(false))
        );

        assert_eq!(
            exec_str_no_context("'my name is Yoshikage Kira' ILIKE 'kira'"),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            exec_str_no_context("'my name is Yoshikage Kira' ILIKE 'KIRA'"),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            exec_str_no_context("'my name is Yoshikage Kira' ILIKE 'KIRAA'"),
            Ok(Value::Bool(false))
        );
    }

    #[test]
    fn exec_unary() {
        assert_eq!(exec_str_no_context("+1"), Ok(Value::Int64(1)));
        assert_eq!(exec_str_no_context("+ -1"), Ok(Value::Int64(-1)));
        assert_eq!(exec_str_no_context("-1"), Ok(Value::Int64(-1)));
        assert_eq!(exec_str_no_context("- -1"), Ok(Value::Int64(1)));
        assert_eq!(exec_str_no_context("not true"), Ok(Value::Bool(false)));
        assert_eq!(exec_str_no_context("not false"), Ok(Value::Bool(true)));

        assert_eq!(exec_str_no_context("true is true"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("false is false"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("false is true"), Ok(Value::Bool(false)));
        assert_eq!(exec_str_no_context("true is false"), Ok(Value::Bool(false)));
        assert_eq!(
            exec_str_no_context("1 is true"),
            Err(ValueUnaryOpError {
                operator: UnOp::Not,
                value: Value::Int64(1)
            }
            .into())
        );

        assert_eq!(exec_str_no_context("NULL is NULL"), Ok(Value::Bool(true)));
        assert_eq!(
            exec_str_no_context("NULL is not NULL"),
            Ok(Value::Bool(false))
        );
        assert_eq!(exec_str_no_context("1 is NULL"), Ok(Value::Bool(false)));
        assert_eq!(exec_str_no_context("1 is not NULL"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("0 is not NULL"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("'' is not NULL"), Ok(Value::Bool(true)));
    }

    #[test]
    fn exec_wildcard() {
        assert_eq!(
            exec_expr_no_context(Expr::Wildcard),
            Err(ExprExecError::CannotExecute(Expr::Wildcard))
        );
    }

    #[test]
    fn exec_column_ref() {
        let mut table = Table::new(
            "table1".into(),
            vec![
                Column::new(
                    "col1".into(),
                    DataType::Int(None),
                    vec![ColumnOptionDef {
                        name: None,
                        option: ColumnOption::Unique { is_primary: true },
                    }],
                    false,
                ),
                Column::new("col2".into(), DataType::String, vec![], false),
            ],
        );
        table.new_row(vec![Value::Int64(1), Value::String("brr".to_owned())]);

        assert_eq!(
            table.all_data(),
            vec![Row {
                data: vec![Value::Int64(1), Value::String("brr".to_owned())]
            }]
        );

        assert_eq!(
            exec_str_with_context("col1", &table, &table.all_data()[0]),
            Ok(Value::Int64(1))
        );
    }

    // #[test]
    // fn exec_function() {
    //     todo!()
    // }
}
