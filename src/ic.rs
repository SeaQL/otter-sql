//! Intermediate representation (IR) and instruction set for an SQL database.

use sqlparser::ast::{ColumnOptionDef, DataType};

use crate::{
    expr::Expr,
    identifier::{SchemaRef, TableRef},
    value::Value,
    vm::RegisterIndex,
    BoundedString,
};

#[derive(Debug, Clone)]
/// The intermediate representation of a query. Made of up [`Instruction`]s.
pub struct IntermediateCode {
    pub instrs: Vec<Instruction>
}

/// The instruction set of OtterSql.
#[derive(Debug, Clone, PartialEq)]
pub enum Instruction {
    /// Load a [`Value`] into a register.
    Value { index: RegisterIndex, value: Value },

    /// Load a [`Expr`] into a register.
    Expr { index: RegisterIndex, expr: Expr },

    /// Load an *existing* table given by `name`.
    ///
    /// This will result in a [`Register::TableRef`](`crate::vm::Register::TableRef`) being stored at the
    /// given register.
    Source {
        index: RegisterIndex,
        name: TableRef,
    },

    /// Create a new empty [`Register::TableRef`](`crate::vm::Register::TableRef`).
    Empty { index: RegisterIndex },

    /// Create a new [`Register::TableRef](`crate::vm::Register::TableRef) pointing to a
    /// non-existent table.
    NonExistent { index: RegisterIndex },

    /// Filter the [`Register::TableRef`](`crate::vm::Register::TableRef`) at `index` using the given expression.
    ///
    /// This represents a `WHERE` clause of a `SELECT` statement in SQL.
    Filter { index: RegisterIndex, expr: Expr },

    /// Create a projection of the columns of the [`Register::TableRef`](`crate::vm::Register::TableRef`) at `input`.
    ///
    /// The resultant column is added to the [`Register::TableRef`](`crate::vm::Register::TableRef`)
    /// at `output`. It must be either an empty table or a table with the same number of rows.
    ///
    /// This represents the column list of the `SELECT` statement in SQL.
    Project {
        input: RegisterIndex,
        output: RegisterIndex,
        expr: Expr,
        alias: Option<BoundedString>,
    },

    /// Group the [`Register::TableRef`](`crate::vm::Register::TableRef`) at `index` by the given expression.
    ///
    /// This will result in a [`Register::GroupedTable`](`crate::vm::Register::GroupedTable`) being stored at the `index` register.
    ///
    /// Must be added before any projections so as to catch errors in column selections.
    GroupBy { index: RegisterIndex, expr: Expr },

    /// Order the [`Register::TableRef`](`crate::vm::Register::TableRef`) at `index` by the given expression.
    ///
    /// This represents the `ORDER BY` clause in SQL.
    Order {
        index: RegisterIndex,
        expr: Expr,
        ascending: bool,
    },

    /// Truncate the [`Register::TableRef`](`crate::vm::Register::TableRef`) at `index` to the given number of rows.
    ///
    /// This represents the `LIMIT` clause in SQL.
    Limit { index: RegisterIndex, limit: u64 },

    /// Return from register at `index`.
    ///
    /// Some values stored in a register may be intermediate values and cannot be returned.
    /// See [`Register`](`crate::vm::Register`) for more information.
    Return { index: RegisterIndex },

    /// Create a new schema.
    ///
    /// This represents a `CREATE SCHEMA [IF NOT EXISTS]` statement.
    NewSchema {
        schema_name: SchemaRef,
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

    /// Add column in register `col_index` to the [`Register::TableRef`](`crate::vm::Register::TableRef`) in `table_reg_index`.
    AddColumn {
        table_reg_index: RegisterIndex,
        col_index: RegisterIndex,
    },

    /// Create table from the [`Register::TableRef`](`crate::vm::Register::TableRef`) in register `index`.
    ///
    /// Creation implies that the table is added to the schema.
    ///
    /// This represents a `CREATE TABLE [IF NOT EXISTS]` statement.
    NewTable {
        index: RegisterIndex,
        name: TableRef,
        /// If `true`, the table is not created if it exists and no error is returned.
        exists_ok: bool,
    },

    /// Drop the table referenced by the [`Register::TableRef`](`crate::vm::Register::TableRef`) in register `index`.
    DropTable { index: RegisterIndex },

    /// Remove the given column from the [`Register::TableRef`](`crate::vm::Register::TableRef`) in register `index`.
    RemoveColumn {
        index: RegisterIndex,
        col_name: BoundedString,
    },

    /// Rename an existing column from the [`Register::TableRef`](`crate::vm::Register::TableRef`) in register `index`.
    RenameColumn {
        index: RegisterIndex,
        old_name: BoundedString,
        new_name: BoundedString,
    },

    /// Start a new insertion into the [`Register::TableRef`](`crate::vm::Register::TableRef`) in register `view_index`.
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

    /// Update values of the [`Register::TableRef`](`crate::vm::Register::TableRef`) in register `index`.
    ///
    /// This represents an `UPDATE` statement.
    Update {
        index: RegisterIndex,
        col: Expr,
        expr: Expr,
    },

    /// Perform a union of the [`Register::TableRef`](`crate::vm::Register::TableRef`) in register `input1` and the [`Register::TableRef`](`crate::vm::Register::TableRef`) in register `input2`.
    ///
    /// The output is stored as a [`Register::TableRef`](`crate::vm::Register::TableRef`) in register
    /// `output`.
    Union {
        input1: RegisterIndex,
        input2: RegisterIndex,
        output: RegisterIndex,
    },

    /// Perform a cartesian join of the [`Register::TableRef`](`crate::vm::Register::TableRef`) in register `input1` and the [`Register::TableRef`](`crate::vm::Register::TableRef`) in register `input2`.
    ///
    /// The output is stored as a [`Register::TableRef`](`crate::vm::Register::TableRef`) in register `output`.
    CrossJoin {
        input1: RegisterIndex,
        input2: RegisterIndex,
        output: RegisterIndex,
    },

    /// Perform a natural join of the [`Register::TableRef`](`crate::vm::Register::TableRef`) in register `input1` and the [`Register::TableRef](`crate::vm::Register::TableRef) in register `input2`.
    ///
    /// The output is stored as a [`Register::TableRef`](`crate::vm::Register::TableRef`) in register `output`.
    ///
    /// Note: this is both a left and a right join i.e., there will be `NULL`s where the common
    /// columns do not match. The result must be filtered at a later stage.
    NaturalJoin {
        input1: RegisterIndex,
        input2: RegisterIndex,
        output: RegisterIndex,
    },
}

// TODO: implement these features in the vm and use the SQL statements here to test them.
// #[cfg(test)]
// mod test {
//     use sqlparser::ast::{ColumnOption, ColumnOptionDef, DataType};

//     use crate::{
//         expr::{BinOp, Expr, UnOp},
//         table::TABLE_UNIQUE_KEY_NAME,
//         value,
//         vm::RegisterIndex,
//     };

//     use super::{Instruction::*, IntermediateCode};

//     #[test]
//     fn alter_statements() {
//         // `ALTER TABLE table1 ADD COLUMN col4 STRING NULL`
//         let table_reg_index = RegisterIndex::default();
//         let col_index = table_reg_index.next_index();
//         let _ = IntermediateCode {
//             instrs: vec![
//                 Source {
//                     index: table_reg_index,
//                     name: "table1".into(),
//                 },
//                 ColumnDef {
//                     index: col_index,
//                     name: "col4".into(),
//                     data_type: DataType::String,
//                 },
//                 AddColumnOption {
//                     index: col_index,
//                     option: ColumnOptionDef {
//                         name: None,
//                         option: ColumnOption::Null,
//                     },
//                 },
//                 AddColumn {
//                     table_reg_index,
//                     col_index,
//                 },
//             ],
//         };

//         // `ALTER TABLE table1 RENAME COLUMN col4 col5`
//         let table_reg_index = RegisterIndex::default();
//         let _ = IntermediateCode {
//             instrs: vec![
//                 Source {
//                     index: table_reg_index,
//                     name: "table1".into(),
//                 },
//                 RenameColumn {
//                     index: table_reg_index,
//                     old_name: "col4".into(),
//                     new_name: "col5".into(),
//                 },
//             ],
//         };

//         // `ALTER TABLE table1 DROP COLUMN col5`
//         let table_reg_index = RegisterIndex::default();
//         let _ = IntermediateCode {
//             instrs: vec![
//                 Source {
//                     index: table_reg_index,
//                     name: "table1".into(),
//                 },
//                 RemoveColumn {
//                     index: table_reg_index,
//                     col_name: "col5".into(),
//                 },
//             ],
//         };
//     }

//     #[test]
//     fn select_with_joins() {
//         // `SELECT col1, col2, col5 FROM table1 INNER JOIN table2 ON table1.col2 = table2.col3`
//         let table_reg_index = RegisterIndex::default();
//         let table_reg_index_2 = table_reg_index.next_index();
//         let table_reg_index_3 = table_reg_index_2.next_index();
//         let table_reg_index_4 = table_reg_index_3.next_index();
//         let _ = IntermediateCode {
//             instrs: vec![
//                 Source {
//                     index: table_reg_index,
//                     name: "table1".into(),
//                 },
//                 Source {
//                     index: table_reg_index_2,
//                     name: "table2".into(),
//                 },
//                 CrossJoin {
//                     input1: table_reg_index,
//                     input2: table_reg_index_2,
//                     output: table_reg_index_3,
//                 },
//                 Filter {
//                     index: table_reg_index_3,
//                     expr: Expr::Binary {
//                         left: Box::new(Expr::ColumnRef("table1.col2".into())),
//                         op: BinOp::Equal,
//                         right: Box::new(Expr::ColumnRef("table2.col3".into())),
//                     },
//                 },
//                 // Inner join, so remove NULLs
//                 Filter {
//                     index: table_reg_index_3,
//                     expr: Expr::Unary {
//                         op: UnOp::IsNotNull,
//                         operand: Box::new(Expr::ColumnRef(
//                             format!("table1.{}", TABLE_UNIQUE_KEY_NAME).as_str().into(),
//                         )),
//                     },
//                 },
//                 // Inner join, so remove NULLs
//                 Filter {
//                     index: table_reg_index_3,
//                     expr: Expr::Unary {
//                         op: UnOp::IsNotNull,
//                         operand: Box::new(Expr::ColumnRef(
//                             format!("table2.{}", TABLE_UNIQUE_KEY_NAME).as_str().into(),
//                         )),
//                     },
//                 },
//                 Empty {
//                     index: table_reg_index_4,
//                 },
//                 Project {
//                     input: table_reg_index_3,
//                     output: table_reg_index_4,
//                     expr: Expr::ColumnRef("col1".into()),
//                     alias: None,
//                 },
//                 Project {
//                     input: table_reg_index_3,
//                     output: table_reg_index_4,
//                     expr: Expr::ColumnRef("col2".into()),
//                     alias: None,
//                 },
//                 Project {
//                     input: table_reg_index_3,
//                     output: table_reg_index_4,
//                     expr: Expr::ColumnRef("col5".into()),
//                     alias: None,
//                 },
//                 Return {
//                     index: table_reg_index_4,
//                 },
//             ],
//         };

//         // `SELECT col1, col2, col5 FROM table1, table2`
//         let table_reg_index = RegisterIndex::default();
//         let table_reg_index_2 = table_reg_index.next_index();
//         let table_reg_index_3 = table_reg_index_2.next_index();
//         let table_reg_index_4 = table_reg_index_3.next_index();
//         let _ = IntermediateCode {
//             instrs: vec![
//                 Source {
//                     index: table_reg_index,
//                     name: "table1".into(),
//                 },
//                 Source {
//                     index: table_reg_index_2,
//                     name: "table2".into(),
//                 },
//                 CrossJoin {
//                     input1: table_reg_index,
//                     input2: table_reg_index_2,
//                     output: table_reg_index_3,
//                 },
//                 Empty {
//                     index: table_reg_index_4,
//                 },
//                 Project {
//                     input: table_reg_index_3,
//                     output: table_reg_index_4,
//                     expr: Expr::ColumnRef("col1".into()),
//                     alias: None,
//                 },
//                 Project {
//                     input: table_reg_index_3,
//                     output: table_reg_index_4,
//                     expr: Expr::ColumnRef("col2".into()),
//                     alias: None,
//                 },
//                 Project {
//                     input: table_reg_index_3,
//                     output: table_reg_index_4,
//                     expr: Expr::ColumnRef("col5".into()),
//                     alias: None,
//                 },
//                 Return {
//                     index: table_reg_index_4,
//                 },
//             ],
//         };

//         // `SELECT col1, col2, col5 FROM table1 NATURAL JOIN table2`
//         let table_reg_index = RegisterIndex::default();
//         let table_reg_index_2 = table_reg_index.next_index();
//         let table_reg_index_3 = table_reg_index_2.next_index();
//         let table_reg_index_4 = table_reg_index_3.next_index();
//         let _ = IntermediateCode {
//             instrs: vec![
//                 Source {
//                     index: table_reg_index,
//                     name: "table1".into(),
//                 },
//                 Source {
//                     index: table_reg_index_2,
//                     name: "table2".into(),
//                 },
//                 NaturalJoin {
//                     input1: table_reg_index,
//                     input2: table_reg_index_2,
//                     output: table_reg_index_3,
//                 },
//                 // Not an outer join, so remove NULLs
//                 Filter {
//                     index: table_reg_index_3,
//                     expr: Expr::Unary {
//                         op: UnOp::IsNotNull,
//                         operand: Box::new(Expr::ColumnRef(
//                             format!("table1.{}", TABLE_UNIQUE_KEY_NAME).as_str().into(),
//                         )),
//                     },
//                 },
//                 // Not an outer join, so remove NULLs
//                 Filter {
//                     index: table_reg_index_3,
//                     expr: Expr::Unary {
//                         op: UnOp::IsNotNull,
//                         operand: Box::new(Expr::ColumnRef(
//                             format!("table2.{}", TABLE_UNIQUE_KEY_NAME).as_str().into(),
//                         )),
//                     },
//                 },
//                 Empty {
//                     index: table_reg_index_4,
//                 },
//                 Project {
//                     input: table_reg_index_3,
//                     output: table_reg_index_4,
//                     expr: Expr::ColumnRef("col1".into()),
//                     alias: None,
//                 },
//                 Project {
//                     input: table_reg_index_3,
//                     output: table_reg_index_4,
//                     expr: Expr::ColumnRef("col2".into()),
//                     alias: None,
//                 },
//                 Project {
//                     input: table_reg_index_3,
//                     output: table_reg_index_4,
//                     expr: Expr::ColumnRef("col5".into()),
//                     alias: None,
//                 },
//                 Return {
//                     index: table_reg_index_4,
//                 },
//             ],
//         };

//         // `SELECT col1, col2, col5 FROM table1 LEFT OUTER JOIN table2 ON table1.col2 = table2.col3`
//         let table_reg_index = RegisterIndex::default();
//         let table_reg_index_2 = table_reg_index.next_index();
//         let table_reg_index_3 = table_reg_index_2.next_index();
//         let table_reg_index_4 = table_reg_index_3.next_index();
//         let _ = IntermediateCode {
//             instrs: vec![
//                 Source {
//                     index: table_reg_index,
//                     name: "table1".into(),
//                 },
//                 Source {
//                     index: table_reg_index_2,
//                     name: "table2".into(),
//                 },
//                 CrossJoin {
//                     input1: table_reg_index,
//                     input2: table_reg_index_2,
//                     output: table_reg_index_3,
//                 },
//                 Filter {
//                     index: table_reg_index_3,
//                     expr: Expr::Binary {
//                         left: Box::new(Expr::ColumnRef("table1.col2".into())),
//                         op: BinOp::Equal,
//                         right: Box::new(Expr::ColumnRef("table2.col3".into())),
//                     },
//                 },
//                 // Left outer join, so don't remove NULLs from first table
//                 // Left outer join, so remove NULLs from second table
//                 Filter {
//                     index: table_reg_index_3,
//                     expr: Expr::Unary {
//                         op: UnOp::IsNotNull,
//                         operand: Box::new(Expr::ColumnRef(
//                             format!("table2.{}", TABLE_UNIQUE_KEY_NAME).as_str().into(),
//                         )),
//                     },
//                 },
//                 Empty {
//                     index: table_reg_index_4,
//                 },
//                 Project {
//                     input: table_reg_index_3,
//                     output: table_reg_index_4,
//                     expr: Expr::ColumnRef("col1".into()),
//                     alias: None,
//                 },
//                 Project {
//                     input: table_reg_index_3,
//                     output: table_reg_index_4,
//                     expr: Expr::ColumnRef("col2".into()),
//                     alias: None,
//                 },
//                 Project {
//                     input: table_reg_index_3,
//                     output: table_reg_index_4,
//                     expr: Expr::ColumnRef("col5".into()),
//                     alias: None,
//                 },
//                 Return {
//                     index: table_reg_index_4,
//                 },
//             ],
//         };
//     }

//     #[test]
//     fn update_statements() {
//         // TODO
//         // `UPDATE table1 SET col2 = 'bar' WHERE col1 = 1`

//         // `UPDATE table1 SET col2 = 'bar' WHERE col1 = 1 AND col3 = 2`

//         // `UPDATE table1 SET col2 = 'bar', col3 = 4 WHERE col1 = 1 AND col3 = 2`

//         // `UPDATE table1 SET col2 = 'bar' WHERE col1 = 1 OR col3 = 2`

//         // `UPDATE table1 SET col3 = col3 + 1 WHERE col2 = 'foo'`

//         // `UPDATE table1, table2 SET table1.col3 = table1.col3 + 1 WHERE table2.col2 = 'foo'`
//     }
// }
