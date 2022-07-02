use sqlparser::ast::{ColumnOptionDef, DataType, Expr};

use crate::{vm::{RegisterIndex, BinOp, UnOp}, value::Value, BoundedString};

/// The intermediate representation of a query.
pub struct IntermediateCode {
    pub instrs: Vec<Instruction>,
}

/// The instruction set.
#[derive(Debug, Clone)]
pub enum Instruction {
    /// Make a new [`Register::View`](`crate::vm::Register::View`) of an *existing* table into register `index`.
    Source {
        index: RegisterIndex,
        name: BoundedString,
    },

    /// Make a new [`Register::View`](`crate::vm::Register::View`) of an *existing* table in the specified schema into register `index`.
    SourceFromSchema {
        index: RegisterIndex,
        schema_name: BoundedString,
        name: BoundedString,
    },

    /// Create a new empty [`Register::View`](`crate::vm::Register::View`) into register `index`.
    Empty {
        index: RegisterIndex,
    },

    /// Reference an existing column from the given [`Register::View`](`crate::vm::Register::View`) at register `view_index` into register `index`.
    ///
    /// The value at register `index` will be of type [`Register::Expr`](`crate::vm::Register::Expr`).
    Column {
        index: RegisterIndex,
        view_index: RegisterIndex,
        col_name: BoundedString,
    },

    /// Construct a binary operation.
    ///
    /// Both the inputs must be of type [`Register::Expr`](`crate::vm::Register::Expr`).
    /// The output will also be a [`Register::Expr`](`crate::vm::Register::Expr`).
    Binary {
        output: RegisterIndex,
        input1: RegisterIndex,
        operator: BinOp,
        input2: RegisterIndex,
    },

    /// Construct a unary operation.
    ///
    /// The input must be of type [`Register::Expr`](`crate::vm::Register::Expr`).
    /// The output will also be a [`Register::Expr`](`crate::vm::Register::Expr`).
    Unary {
        output: RegisterIndex,
        operator: UnOp,
        input: RegisterIndex,
    },

    /// Add a filter over single column on the [`Register::View`](`crate::vm::Register::View`) in register `index`.
    ///
    /// This represents a `WHERE` clause in SQL.
    Filter {
        index: RegisterIndex,
        expr_index: RegisterIndex,
    },

    /// Add a projection of a single column on the [`Register::View`](`crate::vm::Register::View`) in register `index`.
    ///
    /// This represents the column list of the `SELECT` statement in SQL. If there are no
    /// projections given, all columns are considered/returned.
    Project {
        index: RegisterIndex,
        expr_index: RegisterIndex,
        alias: Option<BoundedString>,
    },

    /// Add a grouping on a single column to the [`Register::View`](`crate::vm::Register::View`) in register `index`.
    ///
    /// Must be added before any projections so as to catch errors in column selections.
    GroupBy {
        index: RegisterIndex,
        col_name: BoundedString,
    },

    /// Add an ordering for a single column on the [`Register::View`](`crate::vm::Register::View`) in register `index`.
    ///
    /// This represents the `ORDER BY` clause in SQL.
    Order {
        index: RegisterIndex,
        col_name: BoundedString,
        ascending: bool,
    },

    /// Add a row limit for the [`Register::View`](`crate::vm::Register::View`) in register `index`.
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

    /// Start defining a new table and store the temporary metadata in register `index`.
    ///
    /// The value stored in the register will be of type [`Register::TableDef`](`crate::vm::Register::TableDef`).
    TableDef {
        index: RegisterIndex,
        /// The table name.
        name: BoundedString,
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

    /// Add column in register `col_index` to the table in register `table_index`.
    ///
    /// The table can be a [`Register::TableDef`](`crate::vm::Register::TableDef`) or a [`Register::View`](`crate::vm::Register::View`).
    AddColumn {
        table_index: RegisterIndex,
        col_index: RegisterIndex,
    },

    /// Create table from the [`Register::TableDef`](`crate::vm::Register::TableDef`) in register `index`.
    ///
    /// This represents a `CREATE TABLE [IF NOT EXISTS]` statement.
    NewTable {
        index: RegisterIndex,
        /// If `true`, the table is not created if it exists and no error is returned.
        exists_ok: bool,
    },

    /// Remove the given column from the [`Register::View`](`crate::vm::Register::View`) in register `index`.
    RemoveColumn {
        index: RegisterIndex,
        col_name: BoundedString,
    },

    /// Rename an existing column from the [`Register::View`](`crate::vm::Register::View`) in register `index`.
    RenameColumn {
        index: RegisterIndex,
        old_name: BoundedString,
        new_name: BoundedString,
    },

    /// Start a new insertion into the [`Register::View`](`crate::vm::Register::View`) in register `view_index`.
    ///
    /// A [`Register::InsertDef`](`crate::vm::Register::InsertDef`) is stored in register `index`.
    InsertDef {
        view_index: RegisterIndex,
        index: RegisterIndex,
    },

    /// Add a column to the [`Register::InsertDef`](`crate::vm::Register::InsertDef`) in register `index`.
    ColumnInsertDef {
        index: RegisterIndex,
        col_name: BoundedString,
    },

    /// Start defining a new row of data to be inserted into the [`Register::InsertDef`](`crate::vm::Register::InsertDef`) in register `insert_index`.
    ///
    /// The value stored in the register `index` will be of type [`Register::InsertRow`](`crate::vm::Register::InsertRow`).
    RowDef {
        insert_index: RegisterIndex,
        index: RegisterIndex,
    },

    /// Add a value to the [`Register::InsertRow`](`crate::vm::Register::InsertRow`) in register `index`.
    AddValue { index: RegisterIndex, value: Value },

    /// Perform insertion defined in the [`Register::InsertRow`](`crate::vm::Register::InsertRow`) in register `index`.
    ///
    /// This represents an `INSERT INTO` statement.
    Insert { index: RegisterIndex },

    /// Update values of the [`Register::View`](`crate::vm::Register::View`) in register `index`.
    ///
    /// This represents an `UPDATE` statement.
    Update {
        index: RegisterIndex,
        /// Register where the column name is stored.
        col_index: RegisterIndex,
        expr: Expr,
    },

    /// Perform a union of the [`Register::View`](`crate::vm::Register::View`) in register `input1` and the [`Register::View`](`crate::vm::Register::View`) in register `input2`.
    ///
    /// The output is stored as a [`Register::View`](`crate::vm::Register::View`) in register
    /// `output`.
    Union {
        input1: RegisterIndex,
        input2: RegisterIndex,
        output: RegisterIndex,
    },

    /// Perform a cartesian join of the [`Register::View`](`crate::vm::Register::View`) in register `input1` and the [`Register::View`](`crate::vm::Register::View`) in register `input2`.
    ///
    /// The output is stored as a [`Register::View`](`crate::vm::Register::View`) in register `output`.
    CrossJoin {
        input1: RegisterIndex,
        input2: RegisterIndex,
        output: RegisterIndex,
    },

    /// Perform a natural join of the [`Register::View`](`crate::vm::Register::View`) in register `input1` and the [`Register::View`](`crate::vm::Register::View`) in register `input2`.
    ///
    /// The output is stored as a [`Register::View`](`crate::vm::Register::View`) in register `output`.
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

    use crate::{value::Value, vm::RegisterIndex};

    use super::{Instruction::*, IntermediateCode};

    // TODO: placeholder tests. Test actual AST -> IC conversion once that is implemented.
    #[test]
    fn select_statements() {
        // `SELECT 1`

        // `SELECT * FROM table`
        let view_index = RegisterIndex::default();
        let _ = IntermediateCode {
            instrs: vec![
                View {
                    index: view_index,
                    name: "table".into(),
                },
                Return { index: view_index },
            ],
        };

        // `SELECT * FROM table WHERE col1 = 1`
        let view_index = RegisterIndex::default();
        let _ = IntermediateCode {
            instrs: vec![
                View {
                    index: view_index,
                    name: "table".into(),
                },
                Filter {
                    index: view_index,
                    col_name: "col1".into(),
                    operator: 0,
                    // NOTE: All numbers from the AST will be assumed to be Int64.
                    // They can be downcasted later if needed.
                    value: Value::Int64(1),
                },
                Return { index: view_index },
            ],
        };

        // `SELECT col2, col3 FROM table WHERE col1 = 1`
        let view_index = RegisterIndex::default();
        let _ = IntermediateCode {
            instrs: vec![
                View {
                    index: view_index,
                    name: "table".into(),
                },
                Filter {
                    index: view_index,
                    col_name: "col1".into(),
                    operator: 0,
                    value: Value::Int64(1),
                },
                Project {
                    index: view_index,
                    col_name: "col2".into(),
                },
                Project {
                    index: view_index,
                    col_name: "col3".into(),
                },
                Return { index: view_index },
            ],
        };

        // `SELECT col2, col3 FROM table WHERE col1 = 1 ORDER BY col2 LIMIT 100`
        let view_index = RegisterIndex::default();
        let _ = IntermediateCode {
            instrs: vec![
                View {
                    index: view_index,
                    name: "table".into(),
                },
                Filter {
                    index: view_index,
                    col_name: "col1".into(),
                    // TODO: placeholder type
                    operator: 0,
                    value: Value::Int64(1),
                },
                Project {
                    index: view_index,
                    col_name: "col2".into(),
                },
                Project {
                    index: view_index,
                    col_name: "col3".into(),
                },
                Order {
                    index: view_index,
                    col_name: "col2".into(),
                    ascending: true,
                },
                Limit {
                    index: view_index,
                    limit: 100,
                },
                Return { index: view_index },
            ],
        };

        // `SELECT col2, MAX(col3) FROM table WHERE col1 = 1 GROUP BY col2 HAVING MAX(col3) > 10`
        let view_index = RegisterIndex::default();
        let _ = IntermediateCode {
            instrs: vec![
                View {
                    index: view_index,
                    name: "table".into(),
                },
                Filter {
                    index: view_index,
                    col_name: "col1".into(),
                    operator: 0,
                    value: Value::Int64(1),
                },
                GroupBy {
                    index: view_index,
                    col_name: "col2".into(),
                },
                Project {
                    index: view_index,
                    col_name: "col2".into(),
                },
                ProjectAggregate {
                    index: view_index,
                    // TODO: placeholder type
                    aggregation: 0,
                    col_name: "col3".into(),
                },
                Having {
                    index: view_index,
                    // TODO: placeholder type
                    aggregation: Some(0),
                    col_name: "col3".into(),
                    // TODO: placeholder type
                    operator: 1,
                    value: Value::Int64(10),
                },
                Return { index: view_index },
            ],
        };

        // `SELECT col2, col3 FROM table WHERE col1 = 1 AND col2 = 2`
        let view_index = RegisterIndex::default();
        let _ = IntermediateCode {
            instrs: vec![
                View {
                    index: view_index,
                    name: "table".into(),
                },
                Filter {
                    index: view_index,
                    col_name: "col1".into(),
                    operator: 0,
                    value: Value::Int64(1),
                },
                Filter {
                    index: view_index,
                    col_name: "col2".into(),
                    operator: 0,
                    value: Value::Int64(2),
                },
                Return { index: view_index },
            ],
        };

        // `SELECT col2, col3 FROM table WHERE col1 = 1 OR col2 = 2`
        let view_index_1 = RegisterIndex::default();
        let view_index_2 = view_index_1.next_index();
        let view_index_3 = view_index_2.next_index();
        let _ = IntermediateCode {
            instrs: vec![
                View {
                    index: view_index_1,
                    name: "table".into(),
                },
                Filter {
                    index: view_index_1,
                    col_name: "col1".into(),
                    operator: 1,
                    value: Value::Int64(1),
                },
                View {
                    index: view_index_2,
                    name: "table".into(),
                },
                Filter {
                    index: view_index_2,
                    col_name: "col2".into(),
                    operator: 0,
                    value: Value::Int64(2),
                },
                Union {
                    input1: view_index_1,
                    input2: view_index_2,
                    output: view_index_3,
                },
                Return {
                    index: view_index_3,
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
        let table_index = RegisterIndex::default();
        let col_index = table_index.next_index();
        let _ = IntermediateCode {
            instrs: vec![
                TableDef {
                    index: table_index,
                    name: "table1".into(),
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
                    table_index,
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
                    table_index,
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
                    table_index,
                    col_index,
                },
                NewTable {
                    index: table_index,
                    exists_ok: true,
                },
            ],
        };
    }

    #[test]
    fn alter_statements() {
        // `ALTER TABLE table1 ADD COLUMN col4 STRING NULL`
        let table_index = RegisterIndex::default();
        let col_index = table_index.next_index();
        let _ = IntermediateCode {
            instrs: vec![
                View {
                    index: table_index,
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
                    table_index,
                    col_index,
                },
            ],
        };

        // `ALTER TABLE table1 RENAME COLUMN col4 col5`
        let table_index = RegisterIndex::default();
        let _ = IntermediateCode {
            instrs: vec![
                View {
                    index: table_index,
                    name: "table1".into(),
                },
                RenameColumn {
                    index: table_index,
                    old_name: "col4".into(),
                    new_name: "col5".into(),
                },
            ],
        };

        // `ALTER TABLE table1 DROP COLUMN col5`
        let table_index = RegisterIndex::default();
        let _ = IntermediateCode {
            instrs: vec![
                View {
                    index: table_index,
                    name: "table1".into(),
                },
                RemoveColumn {
                    index: table_index,
                    col_name: "col5".into(),
                },
            ],
        };
    }

    #[test]
    fn insert_statements() {
        // `INSERT INTO table1 VALUES (1, 'foo', 2)`

        // `INSERT INTO table1 (col1, col2) VALUES (1, 'foo')`

        // `INSERT INTO table1 VALUES (2, 'bar', 3), (3, 'baz', 4)`
    }

    #[test]
    fn update_statements() {
        // `UPDATE table1 SET col2 = 'bar' WHERE col1 = 1`

        // `UPDATE table1 SET col2 = 'bar' WHERE col1 = 1 AND col3 = 2`

        // `UPDATE table1 SET col2 = 'bar', col3 = 4 WHERE col1 = 1 AND col3 = 2`

        // `UPDATE table1 SET col2 = 'bar' WHERE col1 = 1 OR col3 = 2`

        // `UPDATE table1 SET col3 = col3 + 1 WHERE col2 = 'foo'`
    }

    #[test]
    fn select_with_joins() {
        // `SELECT col1, col2, col5 FROM table1 INNER JOIN table2 ON table1.col2 = table2.col3`

        // `SELECT col1, col2, col5 FROM table1, table2`

        // `SELECT col1, col2, col5 FROM table1 NATURAL JOIN table2`

        // `SELECT col1, col2, col5 FROM table1 LEFT OUTER JOIN table2 ON table1.col2 = table2.col3`
    }
}
