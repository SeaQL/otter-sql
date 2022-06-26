use std::collections::HashMap;
use std::fmt::Display;

use crate::Mrc;
use crate::{ic::IntermediateCode, table::Table};

/// An index that can be used to access a specific register.
#[derive(Default, Clone, PartialEq, Eq, Hash)]
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
    /// A view into a table.
    View(Mrc<Table>),
    /// Filters applied over a table.
    Filter(Filter),
}

/// A view into a table with filters applied to it.
pub struct Filter {
    pub table: Mrc<Table>,
    // TODO: representation of filter - take from AST
}

#[cfg(test)]
mod tests {
    use super::VirtualMachine;

    #[test]
    fn create_vm() {
        let _ = VirtualMachine::default();
    }
}
