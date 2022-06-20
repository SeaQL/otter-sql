use std::collections::HashMap;

use crate::Mrc;
use crate::{ic::IntermediateCode, table::Table};

#[derive(Default)]
pub struct VirtualMachine {
    pub registers: HashMap<usize, Register>,
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
