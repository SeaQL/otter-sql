use std::collections::HashMap;
use std::fmt::Display;

use sqlparser::ast::Value;

use crate::column::Column;
use crate::{Mrc, BoundedString};
use crate::{ic::IntermediateCode, table::Table};

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
    /// A view into a table.
    View(View),
    /// A table definition.
    TableDef(TableDef),
    /// A column definition
    Column(Column),
    /// An insert statement
    InsertDef(InsertDef),
    /// A row to insert
    InsertRow(InsertRow),
    /// An expression
    Expr(Expr),
    // TODO: an error value?
}

/// An expression
#[derive(Debug, Clone)]
pub enum Expr {
    Value(Value),
    ColumnRef(BoundedString),
    Binary {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    Unary {
        op: UnOp,
        operand: Box<Expr>,
    },
}

/// A binary operator.
#[derive(Debug, Copy, Clone)]
pub enum BinOp {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Like,
    ILike,
    Between,
    NotBetween,
}

/// A unary operator
#[derive(Debug, Copy, Clone)]
pub enum UnOp {
    Plus,
    Minus,
    IsFalse,
    IsTrue,
    IsNull,
    IsNotNull,
}

impl From<sqlparser::ast::Expr> for Expr {
    fn from(_: sqlparser::ast::Expr) -> Self {
        // TODO: implement
        todo!()
    }
}

/// An abstract definition of a create table statement.
pub struct TableDef {
    pub name: BoundedString,
    pub columns: Vec<Column>,
}

/// An abstract definition of an insert statement.
pub struct InsertDef {
    /// The view to insert into
    pub view: Mrc<View>,
    /// The columns to insert into.
    ///
    /// Empty means all columns.
    pub columns: Vec<Column>,
    /// The values to insert.
    pub rows: Vec<InsertRow>,
}

impl InsertDef {
    pub fn new(view: Mrc<View>) -> Self {
        Self {
            view,
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

/// A mutable "view" into a table.
///
/// Filters, projections and ordering may be applied to the view.
///
/// Note: not related to views of queries that some databases support.
pub struct View {
    table: Mrc<Table>,
    // TODO: type for filters. usize is a placeholder.
    filters: Vec<usize>,
    // TODO: type for projections. usize is a placeholder.
    // TODO: must support aggregation projections too.
    projections: Vec<usize>,
    // TODO: type for ordering. usize is a placeholder.
    orderings: Vec<usize>,
    // TODO: type for group by. usize is a placeholder.
    group_bys: Vec<usize>,
    // TODO: type for having. usize is a placeholder.
    havings: Vec<usize>,
}

impl View {
    /// Creates a new view into the given table.
    pub fn new(table: Mrc<Table>) -> Self {
        View {
            table,
            filters: Vec::new(),
            projections: Vec::new(),
            orderings: Vec::new(),
            group_bys: Vec::new(),
            havings: Vec::new(),
        }
    }

    /// Returns the final data after applying all operations.
    pub fn get_data(&self) -> Table {
        // TODO: implement fn and remove placeholders
        let _ = &self.table;
        let _ = &self.filters;
        let _ = &self.projections;
        let _ = &self.orderings;
        let _ = &self.group_bys;
        let _ = &self.havings;
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::VirtualMachine;

    #[test]
    fn create_vm() {
        let _ = VirtualMachine::default();
    }
}
