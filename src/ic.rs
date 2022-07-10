use sqlparser::ast::{ColumnOptionDef, DataType};

use crate::{expr::Expr, value::Value, vm::RegisterIndex, BoundedString};

/// The intermediate representation of a query.
pub struct IntermediateCode {
    pub instrs: Vec<Instruction>,
}

/// The instruction set.
#[derive(Debug, Clone)]
pub enum Instruction {
    /// Load a [`Value`] into a register.
    Value { index: RegisterIndex, value: Value },

    /// Load an *existing* table given by `name`.
    ///
    /// This will result in a [`Register::TableRef](`crate::vm::Register::TableRef) being stored at the
    /// given register.
    Source {
        index: RegisterIndex,
        name: BoundedString,
    },

    /// Load an *existing* table given by `name` from the schema `schema_name`.
    ///
    /// This will result in a [`Register::TableRef](`crate::vm::Register::TableRef) being stored at the
    /// given register.
    SourceFromSchema {
        index: RegisterIndex,
        schema_name: BoundedString,
        name: BoundedString,
    },

    /// Create a new empty [`Register::TableRef](`crate::vm::Register::TableRef).
    Empty { index: RegisterIndex },

    /// Filter the [`Register::TableRef](`crate::vm::Register::TableRef) at `index` using the given expression.
    ///
    /// This represents a `WHERE` clause of a `SELECT` statement in SQL.
    Filter { index: RegisterIndex, expr: Expr },

    /// Create a projection of the columns of the [`Register::TableRef](`crate::vm::Register::TableRef) at `input`.
    ///
    /// The resultant column is added to the [`Register::TableRef](`crate::vm::Register::TableRef)
    /// at `output`. It must be either an empty table or a table with the same number of rows.
    ///
    /// This represents the column list of the `SELECT` statement in SQL.
    Project {
        input: RegisterIndex,
        output: RegisterIndex,
        expr: Expr,
        alias: Option<BoundedString>,
    },

    /// Group the [`Register::TableRef](`crate::vm::Register::TableRef) at `index` by the given expression.
    ///
    /// This will result in a [`Register::GroupedTable`](`crate::vm::Register::GroupedTable`) being stored at the `index` register.
    ///
    /// Must be added before any projections so as to catch errors in column selections.
    GroupBy { index: RegisterIndex, expr: Expr },

    /// Order the [`Register::TableRef](`crate::vm::Register::TableRef) at `index` by the given expression.
    ///
    /// This represents the `ORDER BY` clause in SQL.
    Order {
        index: RegisterIndex,
        expr: Expr,
        ascending: bool,
    },

    /// Truncate the [`Register::TableRef](`crate::vm::Register::TableRef) at `index` to the given number of rows.
    ///
    /// This represents the `LIMIT` clause in SQL.
    Limit { index: RegisterIndex, limit: u64 },

    /// Return from register at `index`.
    ///
    /// Some values stored in a register may be intermediate values and cannot be returned.
    /// See [`Register`](`crate::vm::Register`) for more information.
    Return { index: RegisterIndex },

    /// Create a new database.
    ///
    /// This represents a `CREATE DATABASE [IF NOT EXISTS]` statement.
    NewDatabase {
        name: BoundedString,
        /// If `true`, the database is not created if it exists and no error is returned.
        exists_ok: bool,
    },

    /// Create a new schema.
    ///
    /// This represents a `CREATE SCHEMA [IF NOT EXISTS]` statement.
    NewSchema {
        name: BoundedString,
        /// If `true`, the schema is not created if it exists and no error is returned.
        exists_ok: bool,
    },

    /// Start defining a  new column and store the temporary metadata in register `index`.
    ///
    /// The value stored in the register will be of type [`Register::Column`](`crate::vm::Register::Column`).
    ColumnDef {
        index: RegisterIndex,
        /// The column name.
        name: BoundedString,
        data_type: DataType,
    },

    /// Add an option or constraint to the [`Column`](`crate::vm::Register::Column`) definition in register `index`.
    AddColumnOption {
        index: RegisterIndex,
        option: ColumnOptionDef,
    },

    /// Add column in register `col_index` to the [`Register::TableRef](`crate::vm::Register::TableRef) in `table_reg_index`.
    AddColumn {
        table_reg_index: RegisterIndex,
        col_index: RegisterIndex,
    },

    /// Create table from the [`Register::TableRef](`crate::vm::Register::TableRef) in register `index`.
    ///
    /// This represents a `CREATE TABLE [IF NOT EXISTS]` statement.
    NewTable {
        index: RegisterIndex,
        name: BoundedString,
        /// If `true`, the table is not created if it exists and no error is returned.
        exists_ok: bool,
    },

    /// Remove the given column from the [`Register::TableRef](`crate::vm::Register::TableRef) in register `index`.
    RemoveColumn {
        index: RegisterIndex,
        col_name: BoundedString,
    },

    /// Rename an existing column from the [`Register::TableRef](`crate::vm::Register::TableRef) in register `index`.
    RenameColumn {
        index: RegisterIndex,
        old_name: BoundedString,
        new_name: BoundedString,
    },

    /// Start a new insertion into the [`Register::TableRef](`crate::vm::Register::TableRef) in register `view_index`.
    ///
    /// A [`Register::InsertDef`](`crate::vm::Register::InsertDef`) is stored in register `index`.
    InsertDef {
        table_reg_index: RegisterIndex,
        index: RegisterIndex,
    },

    /// Add a column to the [`Register::InsertDef`](`crate::vm::Register::InsertDef`) in register `index`.
    ColumnInsertDef {
        insert_index: RegisterIndex,
        col_name: BoundedString,
    },

    /// Start defining a new row of data to be inserted into the [`Register::InsertDef`](`crate::vm::Register::InsertDef`) in register `insert_index`.
    ///
    /// The value stored in the register `index` will be of type [`Register::InsertRow`](`crate::vm::Register::InsertRow`).
    RowDef {
        insert_index: RegisterIndex,
        row_index: RegisterIndex,
    },

    /// Add a value to the [`Register::InsertRow`](`crate::vm::Register::InsertRow`) in register `index`.
    AddValue {
        row_index: RegisterIndex,
        expr: Expr,
    },

    /// Perform insertion defined in the [`Register::InsertRow`](`crate::vm::Register::InsertRow`) in register `index`.
    ///
    /// This represents an `INSERT INTO` statement.
    Insert { index: RegisterIndex },

    /// Update values of the [`Register::TableRef](`crate::vm::Register::TableRef) in register `index`.
    ///
    /// This represents an `UPDATE` statement.
    Update {
        index: RegisterIndex,
        col: Expr,
        expr: Expr,
    },

    /// Perform a union of the [`Register::TableRef](`crate::vm::Register::TableRef) in register `input1` and the [`Register::TableRef](`crate::vm::Register::TableRef) in register `input2`.
    ///
    /// The output is stored as a [`Register::TableRef](`crate::vm::Register::TableRef) in register
    /// `output`.
    Union {
        input1: RegisterIndex,
        input2: RegisterIndex,
        output: RegisterIndex,
    },

    /// Perform a cartesian join of the [`Register::TableRef](`crate::vm::Register::TableRef) in register `input1` and the [`Register::TableRef](`crate::vm::Register::TableRef) in register `input2`.
    ///
    /// The output is stored as a [`Register::TableRef](`crate::vm::Register::TableRef) in register `output`.
    CrossJoin {
        input1: RegisterIndex,
        input2: RegisterIndex,
        output: RegisterIndex,
    },

    /// Perform a natural join of the [`Register::TableRef](`crate::vm::Register::TableRef) in register `input1` and the [`Register::TableRef](`crate::vm::Register::TableRef) in register `input2`.
    ///
    /// The output is stored as a [`Register::TableRef](`crate::vm::Register::TableRef) in register `output`.
    ///
    /// Note: this is both a left and a right join i.e., there will be `NULL`s where the common
    /// columns do not match. The result must be filtered at a later stage.
    NaturalJoin {
        input1: RegisterIndex,
        input2: RegisterIndex,
        output: RegisterIndex,
    },
}

#[cfg(test)]
mod test {
    use sqlparser::ast::{ColumnOption, ColumnOptionDef, DataType};

    use crate::{
        expr::{BinOp, Expr, UnOp},
        table::TABLE_UNIQUE_KEY_NAME,
        value,
        vm::RegisterIndex,
    };

    use super::{Instruction::*, IntermediateCode};

    // TODO: placeholder tests. Test actual AST -> IC conversion once that is implemented.
    #[test]
    fn select_statements() {
        // `SELECT 1`
        let _ = IntermediateCode {
            instrs: vec![Value {
                index: RegisterIndex::default(),
                // NOTE: All numbers from the AST will be assumed to be Int64.
                value: value::Value::Int64(1),
            }],
        };

        // `SELECT * FROM table1`
        let table_reg_index = RegisterIndex::default();
        let _ = IntermediateCode {
            instrs: vec![
                Source {
                    index: table_reg_index,
                    name: "table1".into(),
                },
                Return {
                    index: table_reg_index,
                },
            ],
        };

        // `SELECT * FROM table1 WHERE col1 = 1`
        let table_reg_index = RegisterIndex::default();
        let _ = IntermediateCode {
            instrs: vec![
                Source {
                    index: table_reg_index,
                    name: "table1".into(),
                },
                Filter {
                    index: table_reg_index,
                    expr: Expr::Binary {
                        left: Box::new(Expr::ColumnRef("col1".into())),
                        op: BinOp::Equal,
                        right: Box::new(Expr::Value(value::Value::Int64(1))),
                    },
                },
                Return {
                    index: table_reg_index,
                },
            ],
        };

        // `SELECT col2, col3 FROM table1 WHERE col1 = 1`
        let table_reg_index = RegisterIndex::default();
        let table_reg_index_2 = table_reg_index.next_index();
        let _ = IntermediateCode {
            instrs: vec![
                Source {
                    index: table_reg_index,
                    name: "table1".into(),
                },
                Filter {
                    index: table_reg_index,
                    expr: Expr::Binary {
                        left: Box::new(Expr::ColumnRef("col1".into())),
                        op: BinOp::Equal,
                        right: Box::new(Expr::Value(value::Value::Int64(1))),
                    },
                },
                Empty {
                    index: table_reg_index_2,
                },
                Project {
                    input: table_reg_index,
                    output: table_reg_index_2,
                    expr: Expr::ColumnRef("col2".into()),
                    alias: None,
                },
                Project {
                    input: table_reg_index,
                    output: table_reg_index_2,
                    expr: Expr::ColumnRef("col3".into()),
                    alias: None,
                },
                Return {
                    index: table_reg_index_2,
                },
            ],
        };

        // `SELECT col2, col3 FROM main.table1 WHERE col1 = 1 ORDER BY col2 LIMIT 100`
        let table_reg_index = RegisterIndex::default();
        let table_reg_index_2 = table_reg_index.next_index();
        let _ = IntermediateCode {
            instrs: vec![
                Source {
                    index: table_reg_index,
                    name: "table1".into(),
                },
                Filter {
                    index: table_reg_index,
                    expr: Expr::Binary {
                        left: Box::new(Expr::ColumnRef("col1".into())),
                        op: BinOp::Equal,
                        right: Box::new(Expr::Value(value::Value::Int64(1))),
                    },
                },
                Empty {
                    index: table_reg_index_2,
                },
                Project {
                    input: table_reg_index,
                    output: table_reg_index_2,
                    expr: Expr::ColumnRef("col2".into()),
                    alias: None,
                },
                Project {
                    input: table_reg_index,
                    output: table_reg_index_2,
                    expr: Expr::ColumnRef("col3".into()),
                    alias: None,
                },
                Order {
                    index: table_reg_index_2,
                    expr: Expr::ColumnRef("col2".into()),
                    ascending: true,
                },
                Limit {
                    index: table_reg_index_2,
                    limit: 100,
                },
                Return {
                    index: table_reg_index_2,
                },
            ],
        };

        // `SELECT col2, MAX(col3) AS max_col3 FROM table1 WHERE col1 = 1 GROUP BY col2 HAVING MAX(col3) > 10`
        let table_reg_index = RegisterIndex::default();
        let table_reg_index_2 = table_reg_index.next_index();
        let _ = IntermediateCode {
            instrs: vec![
                Source {
                    index: table_reg_index,
                    name: "table1".into(),
                },
                Filter {
                    index: table_reg_index,
                    expr: Expr::Binary {
                        left: Box::new(Expr::ColumnRef("col1".into())),
                        op: BinOp::Equal,
                        right: Box::new(Expr::Value(value::Value::Int64(1))),
                    },
                },
                GroupBy {
                    index: table_reg_index,
                    expr: Expr::ColumnRef("col2".into()),
                },
                Filter {
                    index: table_reg_index,
                    expr: Expr::Binary {
                        left: Box::new(Expr::Function {
                            name: "MAX".into(),
                            args: vec![Expr::ColumnRef("col3".into())],
                        }),
                        op: BinOp::GreaterThan,
                        right: Box::new(Expr::Value(value::Value::Int64(10))),
                    },
                },
                Empty {
                    index: table_reg_index_2,
                },
                Project {
                    input: table_reg_index,
                    output: table_reg_index_2,
                    expr: Expr::ColumnRef("col2".into()),
                    alias: None,
                },
                Project {
                    input: table_reg_index,
                    output: table_reg_index_2,
                    expr: Expr::Function {
                        name: "MAX".into(),
                        args: vec![Expr::ColumnRef("col3".into())],
                    },
                    alias: None,
                },
                Return {
                    index: table_reg_index_2,
                },
            ],
        };

        // `SELECT col2, col3 FROM table1 WHERE col1 = 1 AND col2 = 2`
        let table_reg_index = RegisterIndex::default();
        let table_reg_index_2 = table_reg_index.next_index();
        let _ = IntermediateCode {
            instrs: vec![
                Source {
                    index: table_reg_index,
                    name: "table1".into(),
                },
                Filter {
                    index: table_reg_index,
                    expr: Expr::Binary {
                        left: Box::new(Expr::Binary {
                            left: Box::new(Expr::ColumnRef("col1".into())),
                            op: BinOp::Equal,
                            right: Box::new(Expr::Value(value::Value::Int64(1))),
                        }),
                        op: BinOp::And,
                        right: Box::new(Expr::Binary {
                            left: Box::new(Expr::ColumnRef("col2".into())),
                            op: BinOp::Equal,
                            right: Box::new(Expr::Value(value::Value::Int64(2))),
                        }),
                    },
                },
                Empty {
                    index: table_reg_index_2,
                },
                Project {
                    input: table_reg_index,
                    output: table_reg_index_2,
                    expr: Expr::ColumnRef("col2".into()),
                    alias: None,
                },
                Project {
                    input: table_reg_index,
                    output: table_reg_index_2,
                    expr: Expr::ColumnRef("col3".into()),
                    alias: None,
                },
                Return {
                    index: table_reg_index_2,
                },
            ],
        };

        // `SELECT col2, col3 FROM table1 WHERE col1 = 1 OR col2 = 2`
        let table_reg_index = RegisterIndex::default();
        let table_reg_index_2 = table_reg_index.next_index();
        let _ = IntermediateCode {
            instrs: vec![
                Source {
                    index: table_reg_index,
                    name: "table1".into(),
                },
                Filter {
                    index: table_reg_index,
                    expr: Expr::Binary {
                        left: Box::new(Expr::Binary {
                            left: Box::new(Expr::ColumnRef("col1".into())),
                            op: BinOp::Equal,
                            right: Box::new(Expr::Value(value::Value::Int64(1))),
                        }),
                        op: BinOp::Or,
                        right: Box::new(Expr::Binary {
                            left: Box::new(Expr::ColumnRef("col2".into())),
                            op: BinOp::Equal,
                            right: Box::new(Expr::Value(value::Value::Int64(2))),
                        }),
                    },
                },
                Empty {
                    index: table_reg_index_2,
                },
                Project {
                    input: table_reg_index,
                    output: table_reg_index_2,
                    expr: Expr::ColumnRef("col2".into()),
                    alias: None,
                },
                Project {
                    input: table_reg_index,
                    output: table_reg_index_2,
                    expr: Expr::ColumnRef("col3".into()),
                    alias: None,
                },
                Return {
                    index: table_reg_index_2,
                },
            ],
        };
    }

    #[test]
    fn create_statements() {
        // `CREATE DATABASE db1`
        let _ = IntermediateCode {
            instrs: vec![NewDatabase {
                name: "db1".into(),
                exists_ok: false,
            }],
        };

        // `CREATE SCHEMA schema1`
        let _ = IntermediateCode {
            instrs: vec![NewSchema {
                name: "schema1".into(),
                exists_ok: false,
            }],
        };

        // `CREATE TABLE IF NOT EXISTS table1 (col1 INTEGER PRIMARY KEY NOT NULL, col2 STRING NOT NULL, col3 INTEGER UNIQUE)`
        let table_reg_index = RegisterIndex::default();
        let col_index = table_reg_index.next_index();
        let _ = IntermediateCode {
            instrs: vec![
                Empty {
                    index: table_reg_index,
                },
                ColumnDef {
                    index: col_index,
                    name: "col1".into(),
                    data_type: DataType::Int(None),
                },
                AddColumnOption {
                    index: col_index,
                    option: ColumnOptionDef {
                        name: None,
                        option: ColumnOption::Unique { is_primary: true },
                    },
                },
                AddColumnOption {
                    index: col_index,
                    option: ColumnOptionDef {
                        name: None,
                        option: ColumnOption::NotNull,
                    },
                },
                AddColumn {
                    table_reg_index,
                    col_index,
                },
                ColumnDef {
                    index: col_index,
                    name: "col2".into(),
                    data_type: DataType::String,
                },
                AddColumnOption {
                    index: col_index,
                    option: ColumnOptionDef {
                        name: None,
                        option: ColumnOption::NotNull,
                    },
                },
                AddColumn {
                    table_reg_index,
                    col_index,
                },
                ColumnDef {
                    index: col_index,
                    name: "col3".into(),
                    data_type: DataType::Int(None),
                },
                AddColumnOption {
                    index: col_index,
                    option: ColumnOptionDef {
                        name: None,
                        option: ColumnOption::Unique { is_primary: false },
                    },
                },
                AddColumn {
                    table_reg_index,
                    col_index,
                },
                NewTable {
                    index: table_reg_index,
                    name: "table1".into(),
                    exists_ok: true,
                },
            ],
        };
    }

    #[test]
    fn alter_statements() {
        // `ALTER TABLE table1 ADD COLUMN col4 STRING NULL`
        let table_reg_index = RegisterIndex::default();
        let col_index = table_reg_index.next_index();
        let _ = IntermediateCode {
            instrs: vec![
                Source {
                    index: table_reg_index,
                    name: "table1".into(),
                },
                ColumnDef {
                    index: col_index,
                    name: "col4".into(),
                    data_type: DataType::String,
                },
                AddColumnOption {
                    index: col_index,
                    option: ColumnOptionDef {
                        name: None,
                        option: ColumnOption::Null,
                    },
                },
                AddColumn {
                    table_reg_index,
                    col_index,
                },
            ],
        };

        // `ALTER TABLE table1 RENAME COLUMN col4 col5`
        let table_reg_index = RegisterIndex::default();
        let _ = IntermediateCode {
            instrs: vec![
                Source {
                    index: table_reg_index,
                    name: "table1".into(),
                },
                RenameColumn {
                    index: table_reg_index,
                    old_name: "col4".into(),
                    new_name: "col5".into(),
                },
            ],
        };

        // `ALTER TABLE table1 DROP COLUMN col5`
        let table_reg_index = RegisterIndex::default();
        let _ = IntermediateCode {
            instrs: vec![
                Source {
                    index: table_reg_index,
                    name: "table1".into(),
                },
                RemoveColumn {
                    index: table_reg_index,
                    col_name: "col5".into(),
                },
            ],
        };
    }

    #[test]
    fn select_with_joins() {
        // `SELECT col1, col2, col5 FROM table1 INNER JOIN table2 ON table1.col2 = table2.col3`
        let table_reg_index = RegisterIndex::default();
        let table_reg_index_2 = table_reg_index.next_index();
        let table_reg_index_3 = table_reg_index_2.next_index();
        let table_reg_index_4 = table_reg_index_3.next_index();
        let _ = IntermediateCode {
            instrs: vec![
                Source {
                    index: table_reg_index,
                    name: "table1".into(),
                },
                Source {
                    index: table_reg_index_2,
                    name: "table2".into(),
                },
                CrossJoin {
                    input1: table_reg_index,
                    input2: table_reg_index_2,
                    output: table_reg_index_3,
                },
                Filter {
                    index: table_reg_index_3,
                    expr: Expr::Binary {
                        left: Box::new(Expr::ColumnRef("table1.col2".into())),
                        op: BinOp::Equal,
                        right: Box::new(Expr::ColumnRef("table2.col3".into())),
                    },
                },
                // Inner join, so remove NULLs
                Filter {
                    index: table_reg_index_3,
                    expr: Expr::Unary {
                        op: UnOp::IsNotNull,
                        operand: Box::new(Expr::ColumnRef(
                            format!("table1.{}", TABLE_UNIQUE_KEY_NAME).as_str().into(),
                        )),
                    },
                },
                // Inner join, so remove NULLs
                Filter {
                    index: table_reg_index_3,
                    expr: Expr::Unary {
                        op: UnOp::IsNotNull,
                        operand: Box::new(Expr::ColumnRef(
                            format!("table2.{}", TABLE_UNIQUE_KEY_NAME).as_str().into(),
                        )),
                    },
                },
                Empty {
                    index: table_reg_index_4,
                },
                Project {
                    input: table_reg_index_3,
                    output: table_reg_index_4,
                    expr: Expr::ColumnRef("col1".into()),
                    alias: None,
                },
                Project {
                    input: table_reg_index_3,
                    output: table_reg_index_4,
                    expr: Expr::ColumnRef("col2".into()),
                    alias: None,
                },
                Project {
                    input: table_reg_index_3,
                    output: table_reg_index_4,
                    expr: Expr::ColumnRef("col5".into()),
                    alias: None,
                },
                Return {
                    index: table_reg_index_4,
                },
            ],
        };

        // `SELECT col1, col2, col5 FROM table1, table2`
        let table_reg_index = RegisterIndex::default();
        let table_reg_index_2 = table_reg_index.next_index();
        let table_reg_index_3 = table_reg_index_2.next_index();
        let table_reg_index_4 = table_reg_index_3.next_index();
        let _ = IntermediateCode {
            instrs: vec![
                Source {
                    index: table_reg_index,
                    name: "table1".into(),
                },
                Source {
                    index: table_reg_index_2,
                    name: "table2".into(),
                },
                CrossJoin {
                    input1: table_reg_index,
                    input2: table_reg_index_2,
                    output: table_reg_index_3,
                },
                Empty {
                    index: table_reg_index_4,
                },
                Project {
                    input: table_reg_index_3,
                    output: table_reg_index_4,
                    expr: Expr::ColumnRef("col1".into()),
                    alias: None,
                },
                Project {
                    input: table_reg_index_3,
                    output: table_reg_index_4,
                    expr: Expr::ColumnRef("col2".into()),
                    alias: None,
                },
                Project {
                    input: table_reg_index_3,
                    output: table_reg_index_4,
                    expr: Expr::ColumnRef("col5".into()),
                    alias: None,
                },
                Return {
                    index: table_reg_index_4,
                },
            ],
        };

        // `SELECT col1, col2, col5 FROM table1 NATURAL JOIN table2`
        let table_reg_index = RegisterIndex::default();
        let table_reg_index_2 = table_reg_index.next_index();
        let table_reg_index_3 = table_reg_index_2.next_index();
        let table_reg_index_4 = table_reg_index_3.next_index();
        let _ = IntermediateCode {
            instrs: vec![
                Source {
                    index: table_reg_index,
                    name: "table1".into(),
                },
                Source {
                    index: table_reg_index_2,
                    name: "table2".into(),
                },
                NaturalJoin {
                    input1: table_reg_index,
                    input2: table_reg_index_2,
                    output: table_reg_index_3,
                },
                // Not an outer join, so remove NULLs
                Filter {
                    index: table_reg_index_3,
                    expr: Expr::Unary {
                        op: UnOp::IsNotNull,
                        operand: Box::new(Expr::ColumnRef(
                            format!("table1.{}", TABLE_UNIQUE_KEY_NAME).as_str().into(),
                        )),
                    },
                },
                // Not an outer join, so remove NULLs
                Filter {
                    index: table_reg_index_3,
                    expr: Expr::Unary {
                        op: UnOp::IsNotNull,
                        operand: Box::new(Expr::ColumnRef(
                            format!("table2.{}", TABLE_UNIQUE_KEY_NAME).as_str().into(),
                        )),
                    },
                },
                Empty {
                    index: table_reg_index_4,
                },
                Project {
                    input: table_reg_index_3,
                    output: table_reg_index_4,
                    expr: Expr::ColumnRef("col1".into()),
                    alias: None,
                },
                Project {
                    input: table_reg_index_3,
                    output: table_reg_index_4,
                    expr: Expr::ColumnRef("col2".into()),
                    alias: None,
                },
                Project {
                    input: table_reg_index_3,
                    output: table_reg_index_4,
                    expr: Expr::ColumnRef("col5".into()),
                    alias: None,
                },
                Return {
                    index: table_reg_index_4,
                },
            ],
        };

        // `SELECT col1, col2, col5 FROM table1 LEFT OUTER JOIN table2 ON table1.col2 = table2.col3`
        let table_reg_index = RegisterIndex::default();
        let table_reg_index_2 = table_reg_index.next_index();
        let table_reg_index_3 = table_reg_index_2.next_index();
        let table_reg_index_4 = table_reg_index_3.next_index();
        let _ = IntermediateCode {
            instrs: vec![
                Source {
                    index: table_reg_index,
                    name: "table1".into(),
                },
                Source {
                    index: table_reg_index_2,
                    name: "table2".into(),
                },
                CrossJoin {
                    input1: table_reg_index,
                    input2: table_reg_index_2,
                    output: table_reg_index_3,
                },
                Filter {
                    index: table_reg_index_3,
                    expr: Expr::Binary {
                        left: Box::new(Expr::ColumnRef("table1.col2".into())),
                        op: BinOp::Equal,
                        right: Box::new(Expr::ColumnRef("table2.col3".into())),
                    },
                },
                // Left outer join, so don't remove NULLs from first table
                // Left outer join, so remove NULLs from second table
                Filter {
                    index: table_reg_index_3,
                    expr: Expr::Unary {
                        op: UnOp::IsNotNull,
                        operand: Box::new(Expr::ColumnRef(
                            format!("table2.{}", TABLE_UNIQUE_KEY_NAME).as_str().into(),
                        )),
                    },
                },
                Empty {
                    index: table_reg_index_4,
                },
                Project {
                    input: table_reg_index_3,
                    output: table_reg_index_4,
                    expr: Expr::ColumnRef("col1".into()),
                    alias: None,
                },
                Project {
                    input: table_reg_index_3,
                    output: table_reg_index_4,
                    expr: Expr::ColumnRef("col2".into()),
                    alias: None,
                },
                Project {
                    input: table_reg_index_3,
                    output: table_reg_index_4,
                    expr: Expr::ColumnRef("col5".into()),
                    alias: None,
                },
                Return {
                    index: table_reg_index_4,
                },
            ],
        };
    }

    #[test]
    fn insert_statements() {
        // TODO
        // `INSERT INTO table1 VALUES (1, 'foo', 2)`

        // `INSERT INTO table1 (col1, col2) VALUES (1, 'foo')`

        // `INSERT INTO table1 VALUES (2, 'bar', 3), (3, 'baz', 4)`
    }

    #[test]
    fn update_statements() {
        // TODO
        // `UPDATE table1 SET col2 = 'bar' WHERE col1 = 1`

        // `UPDATE table1 SET col2 = 'bar' WHERE col1 = 1 AND col3 = 2`

        // `UPDATE table1 SET col2 = 'bar', col3 = 4 WHERE col1 = 1 AND col3 = 2`

        // `UPDATE table1 SET col2 = 'bar' WHERE col1 = 1 OR col3 = 2`

        // `UPDATE table1 SET col3 = col3 + 1 WHERE col2 = 'foo'`

        // `UPDATE table1, table2 SET table1.col3 = table1.col3 + 1 WHERE table2.col2 = 'foo'`
    }
}
