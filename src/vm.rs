use std::collections::HashMap;
use std::fmt::Display;

use crate::Mrc;
use crate::{ic::IntermediateCode, table::Table};

/// An index that can be used to access a specific register.
#[derive(Default, Clone, PartialEq, Eq, Hash)]
pub struct RegisterIndex(usize);

impl RegisterIndex {
    /// Get the next index in the sequence.
    fn next_index(&self) -> RegisterIndex {
        RegisterIndex(self.0 + 1)
    }
}

impl Display for RegisterIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "%{}", self.0)
    }
}

#[derive(Default)]
pub struct VirtualMachine {
    registers: HashMap<RegisterIndex, Register>,
    last_index: RegisterIndex,
}

impl VirtualMachine {
    pub fn insert(&mut self, reg: Register) -> RegisterIndex {
        let index = self.last_index.next_index();
        self.registers.insert(index.clone(), reg);
        self.last_index = index.clone();
        index
    }
}

impl VirtualMachine {
    pub fn execute(&mut self, _code: &IntermediateCode) {
        // TODO
        todo!();
    }
}

pub enum Register {
    Table(Mrc<Table>),
    Filter(Filter),
}

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
