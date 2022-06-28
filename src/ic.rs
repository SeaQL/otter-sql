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
}
