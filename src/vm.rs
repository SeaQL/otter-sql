use std::collections::HashMap;
use std::fmt::Display;

use crate::column::Column;
use crate::ic::IntermediateCode;
use crate::table::Row;
use crate::value::Value;
use crate::{BoundedString, Mrc};

/// An index that can be used to access a specific register.
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct RegisterIndex(usize);

impl RegisterIndex {
    /// Get the next index in the sequence.
    pub fn next_index(&self) -> RegisterIndex {
        RegisterIndex(self.0 + 1)
    }
}

impl Display for RegisterIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "%{}", self.0)
    }
}

/// Executor of an SQL query.
#[derive(Default)]
pub struct VirtualMachine {
    registers: HashMap<RegisterIndex, Register>,
}

impl VirtualMachine {
    /// Inserts a value for the register at the given index.
    pub fn insert(&mut self, index: RegisterIndex, reg: Register) {
        self.registers.insert(index.clone(), reg);
    }

    /// Gets the value for the register at the given index.
    pub fn get(&mut self, index: &RegisterIndex) -> Option<&Register> {
        self.registers.get(index)
    }

    /// Executes the given intermediate code.
    pub fn execute(&mut self, _code: &IntermediateCode) {
        // TODO
        todo!();
    }
}

/// A register in the executor VM.
pub enum Register {
    /// An entire table.
    Table {
        columns: Vec<Column>,
        data: Vec<Row>,
    },
    /// A grouped table.
    GroupedTable {
        grouped_col: Column,
        other_cols: Vec<Column>,
        /// The group, a mapping of grouped col value -> rows in that group.
        data: Vec<(Value, Vec<Row>)>,
    },
    /// A table definition.
    TableDef(TableDef),
    /// A column definition
    Column(Column),
    /// An insert statement
    InsertDef(InsertDef),
    /// A row to insert
    InsertRow(InsertRow),
    // TODO: an error value?
}

/// An abstract definition of a create table statement.
pub struct TableDef {
    pub name: BoundedString,
    pub columns: Vec<Column>,
}

/// An abstract definition of an insert statement.
pub struct InsertDef {
    /// The view to insert into
    pub table_name: BoundedString,
    /// The columns to insert into.
    ///
    /// Empty means all columns.
    pub columns: Vec<Column>,
    /// The values to insert.
    pub rows: Vec<InsertRow>,
}

impl InsertDef {
    pub fn new(table_name: BoundedString) -> Self {
        Self {
            table_name,
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }
}

/// A row of values to insert.
pub struct InsertRow {
    /// The values
    pub values: Vec<Value>,
    /// The insert definition which this belongs to
    pub def: Mrc<InsertDef>,
}

#[cfg(test)]
mod tests {
    use super::VirtualMachine;

    #[test]
    fn create_vm() {
        let _ = VirtualMachine::default();
    }
}
