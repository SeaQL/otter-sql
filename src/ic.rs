use sqlparser::ast::{DataType, ColumnOptionDef};

use crate::{vm::RegisterIndex, value::Value};

/// The intermediate representation of a query.
pub struct IntermediateCode {
    pub instrs: Vec<Instruction>,
}

/// The instruction set.
pub enum Instruction {
    /// Make a new [`Register::View`](`crate::vm::Register::View`) of a table into register `index`.
    ///
    /// The table given by `name` is loaded into the view.
    // TODO: should the table exist already or do we allow making new temporary views?
    View {
        index: RegisterIndex,
        name: String,
    },

    /// Add a filter over single column on the [`Register::View`](`crate::vm::Register::View`) in register `index`.
    ///
    /// The filter applied is `col_name <operator> value`.
    ///
    /// This represents a `WHERE` clause in SQL.
    Filter {
        index: RegisterIndex,
        col_name: String,
        // TODO: placeholder type!
        operator: u32,
        value: Value,
    },

    /// Add a projection of single column on the [`Register::View`](`crate::vm::Register::View`) in register `index`.
    ///
    /// This represents the column list of the `SELECT` statement in SQL. If there are no
    /// projections given, all columns are considered/returned.
    Project {
        index: RegisterIndex,
        col_name: String,
    },

    /// Add an ordering for a single column on the [`Register::View`](`crate::vm::Register::View`) in register `index`.
    ///
    /// This represents the `ORDER BY` clause in SQL.
    Order {
        index: RegisterIndex,
        col_name: String,
        ascending: bool,
    },

    /// Add a row limit for the [`Register::View`](`crate::vm::Register::View`) in register `index`.
    ///
    /// This represents the `LIMIT` clause in SQL.
    Limit {
        index: RegisterIndex,
        limit: u64,
    },

    /// Return from register at `index`.
    ///
    /// Some values stored in a register may be intermediate values and cannot be returned.
    /// See [`Register`](`crate::vm::Register`) for more information.
    Return {
        index: RegisterIndex,
    },

    /// Create a new database.
    ///
    /// This represents a `CREATE DATABASE [IF NOT EXISTS]` statement.
    NewDatabase {
        name: String,
        /// If `true`, the database is not created if it exists and no error is returned.
        exists_ok: bool,
    },

    /// Create a new schema.
    ///
    /// This represents a `CREATE SCHEMA [IF NOT EXISTS]` statement.
    NewSchema {
        name: String,
    },

    /// Start defining a new table and store the temporary metadata in register `index`.
    ///
    /// The value stored in the register will be of type [`Register::TableDef`](`crate::vm::Register::TableDef`).
    TableDef {
        index: RegisterIndex,
        /// The table name.
        name: String,
    },

    /// Start defining a  new column and store the temporary metadata in register `index`.
    ///
    /// The value stored in the register will be of type [`Register::Column`](`crate::vm::Register::Column`).
    ColumnDef {
        index: RegisterIndex,
        /// The column name.
        name: String,
        data_type: DataType,
    },

    /// Add an option or constraint to the [`Column`](`crate::vm::Register::Column`) definition in register `index`.
    ColumnOption {
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
        col_name: String,
    },

    /// Rename an existing column from the [`Register::View`](`crate::vm::Register::View`) in register `index`.
    RenameColumn {
        index: RegisterIndex,
        old_name: String,
        new_name: String,
    },

    /// Start a new insertion into the [`Register::View`](`crate::vm::Register::View`) in register `view_index`.
    ///
    /// A [`Register::InsertDef`](`crate::vm::Register::InsertDef`) is stored in register `index`.
    InsertDef {
        view_index: RegisterIndex,
        index: RegisterIndex,
    },

    /// Add a column to the [`Register::InsertDef`](`crate::vm::Register::InsertDef`) in register `index`.
    Column {
        index: RegisterIndex,
        col_name: String,
    },

    /// Start defining a new row of data to be inserted into the [`Register::InsertDef`](`crate::vm::Register::InsertDef`) in register `insert_index`.
    ///
    /// The value stored in the register `index` will be of type [`Register::InsertRow`](`crate::vm::Register::InsertRow`).
    RowDef {
        insert_index: RegisterIndex,
        index: RegisterIndex,
    },

    /// Add a value to the [`Register::InsertRow`](`crate::vm::Register::InsertRow`) in register `index`.
    AddValue {
        index: RegisterIndex,
        value: Value,
    },

    /// Perform insertion defined in the [`Register::InsertRow`](`crate::vm::Register::InsertRow`) in register `index`.
    ///
    /// This represents an `INSERT INTO` statement.
    Insert {
        index: RegisterIndex,
    },


    /// Update values of the [`Register::View`](`crate::vm::Register::View`) in register `index`.
    ///
    /// The `col_name` column's value is set to `value` for all rows.
    ///
    /// This represents an `UPDATE` statement.
    Update {
        index: RegisterIndex,
        col_name: String,
        value: Value,
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
