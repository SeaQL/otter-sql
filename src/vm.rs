use std::{rc::Rc, collections::HashMap};

use crate::{table::Table, ic::IntermediateCode};

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
    Table(Rc<Table>),
    Filter(Filter),
}

pub struct Filter {
    pub table: Rc<Table>,
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
