use hashbrown::HashMap;
use permutation::permutation;
use sqlparser::ast::DataType;
use std::error::Error;
use std::fmt::Display;

use sqlparser::parser::ParserError;

use crate::codegen::{codegen, CodegenError};
use crate::column::Column;
use crate::expr::eval::ExprExecError;
use crate::expr::Expr;
use crate::ic::{Instruction, IntermediateCode};
use crate::identifier::{ColumnRef, TableRef};
use crate::parser::parse;
use crate::schema::Schema;
use crate::table::{Row, RowShared, Table};
use crate::value::Value;
use crate::{BoundedString, Database};

const DEFAULT_DATABASE_NAME: &str = "default";

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

/// An index that can be used as a reference to a table.
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct TableIndex(usize);

impl TableIndex {
    /// Get the next index in the sequence.
    pub fn next_index(&self) -> Self {
        TableIndex(self.0 + 1)
    }
}

/// Executor of an SQL query.
pub struct VirtualMachine {
    database: Database,
    registers: HashMap<RegisterIndex, Register>,
    tables: HashMap<TableIndex, Table>,
    last_table_index: TableIndex,
}

impl VirtualMachine {
    pub fn new(name: BoundedString) -> Self {
        Self {
            database: Database::new(name),
            registers: Default::default(),
            tables: Default::default(),
            last_table_index: Default::default(),
        }
    }

    /// Inserts a value for the register at the given index.
    pub fn insert_register(&mut self, index: RegisterIndex, reg: Register) {
        self.registers.insert(index.clone(), reg);
    }

    /// Gets the value for the register at the given index.
    pub fn get_register(&mut self, index: &RegisterIndex) -> Option<&Register> {
        self.registers.get(index)
    }

    /// Creates a new table with a temp name and returns its index.
    pub fn new_temp_table(&mut self) -> TableIndex {
        let index = self.last_table_index.next_index();
        self.tables.insert(index, Table::new_temp(index.0));
        self.last_table_index = index;
        index
    }

    /// Creates a new empty table from another table (with the same schema)
    pub fn new_table_from(&mut self, table: &TableIndex) -> TableIndex {
        let table = self.tables.get(table).unwrap();
        let index = self.last_table_index.next_index();
        self.tables.insert(index, Table::new_from(table));
        self.last_table_index = index;
        index
    }

    /// Get a reference to an existing table at the given index.
    pub fn table(&self, index: &TableIndex) -> Option<&Table> {
        self.tables.get(index)
    }

    /// Drop an existing table from the VM.
    ///
    /// Note: does NOT remove the table from the schema (if it was added to a schema).
    // TODO: ensure that IC gen calls this when a temp table is created.
    pub fn drop_table(&mut self, index: &TableIndex) {
        self.tables.remove(index);
    }

    /// Executes the given SQL.
    pub fn execute(&mut self, code: &str) -> Result<Option<Table>, ExecutionError> {
        let ast = parse(code)?;
        let mut ret = None;
        for stmt in ast {
            let ic = codegen(&stmt)?;
            ret = self.execute_ic(&ic)?;
        }
        Ok(ret)
    }

    /// Executes the given intermediate code.
    fn execute_ic(&mut self, ic: &IntermediateCode) -> Result<Option<Table>, RuntimeError> {
        let mut ret = None;
        for instr in &ic.instrs {
            ret = self.execute_instr(instr)?;
        }
        Ok(ret)
    }

    /// Executes the given instruction.
    fn execute_instr(&mut self, instr: &Instruction) -> Result<Option<Table>, RuntimeError> {
        let _ = &self.database;
        match instr {
            Instruction::Value { index, value } => {
                self.registers
                    .insert(*index, Register::Value(value.clone()));
            }
            Instruction::Expr { index, expr } => {
                self.registers.insert(*index, Register::Expr(expr.clone()));
            }
            Instruction::Source { index, name } => match name {
                TableRef {
                    schema_name: None,
                    table_name: _,
                } => {
                    let table_index = self.find_table(self.database.default_schema(), name)?;
                    self.registers
                        .insert(*index, Register::TableRef(table_index));
                }
                TableRef {
                    schema_name: Some(schema_name),
                    table_name: _,
                } => {
                    let schema = if let Some(schema) = self.database.schema_by_name(schema_name) {
                        schema
                    } else {
                        return Err(RuntimeError::SchemaNotFound(*schema_name));
                    };

                    let table_index = self.find_table(schema, name)?;
                    self.registers
                        .insert(*index, Register::TableRef(table_index));
                }
            },
            Instruction::Empty { index } => {
                let table_index = self.new_temp_table();
                self.registers
                    .insert(*index, Register::TableRef(table_index));
            }
            Instruction::NonExistent { index } => {
                self.registers.insert(*index, Register::NonExistentTable);
            }
            Instruction::Return { index } => match self.registers.remove(index) {
                None => return Err(RuntimeError::EmptyRegister(*index)),
                Some(Register::TableRef(t)) => return Ok(Some(self.tables[&t].clone())),
                Some(Register::Value(v)) => {
                    let mut table = Table::new_temp(self.last_table_index.next_index().0);
                    self.last_table_index = self.last_table_index.next_index();
                    table.add_column(Column::new("?column?".into(), v.data_type(), vec![], false));
                    table.new_row(vec![v]);
                    return Ok(Some(table));
                }
                Some(register) => return Err(RuntimeError::CannotReturn(register.clone())),
            },
            Instruction::Filter { index, expr } => match self.registers.get(index) {
                None => return Err(RuntimeError::EmptyRegister(*index)),
                Some(Register::TableRef(table_index)) => {
                    let table_index = *table_index;
                    // TODO: should be safe to unwrap, but make it an error anyway?
                    let table = self.tables.get(&table_index).unwrap();
                    let filtered_data = table
                        .raw_data
                        .iter()
                        .filter_map(|row| {
                            match Expr::execute(expr, table, RowShared::from_raw(row, &table)) {
                                Ok(val) => {
                                    match val {
                                        Value::Bool(b) => {
                                            if b {
                                                Some(Ok(row.clone()))
                                            } else {
                                                None
                                            }
                                        }
                                        _ => Some(Err(RuntimeError::FilterWithNonBoolean(
                                            expr.clone(),
                                            val.clone(),
                                        ))),
                                    }
                                }
                                Err(e) => Some(Err(e.into())),
                            }
                        })
                        .collect::<Result<_, _>>()?;
                    let new_table_index = self.new_table_from(&table_index);
                    self.tables.get_mut(&new_table_index).unwrap().raw_data = filtered_data;
                    self.insert_register(*index, Register::TableRef(new_table_index));
                }
                Some(reg) => return Err(RuntimeError::RegisterNotATable("filter", reg.clone())),
            },
            Instruction::Project {
                input,
                output,
                expr,
                alias,
            } => match (self.registers.get(input), self.registers.get(output)) {
                (None, _) => return Err(RuntimeError::EmptyRegister(*input)),
                (_, None) => return Err(RuntimeError::EmptyRegister(*output)),
                (Some(Register::NonExistentTable), Some(Register::TableRef(out_table_index))) => {
                    let out_table = self.tables.get_mut(out_table_index).unwrap();
                    // we assume out table is empty at this point, so use it like an input table
                    // because why not.
                    let val =
                        Expr::execute(expr, &out_table, out_table.sentinel_row()?.to_shared())?;
                    let data_type = val.data_type();
                    out_table.new_row(vec![val]);

                    // TODO: provide a unique name here
                    let new_col = Column::new(
                        alias.unwrap_or("PLACEHOLDER".into()),
                        data_type,
                        vec![],
                        false,
                    );

                    out_table.add_column(new_col);
                }
                (
                    Some(Register::TableRef(inp_table_index)),
                    Some(Register::TableRef(out_table_index)),
                ) => {
                    let [inp_table, out_table] = self
                        .tables
                        .get_many_mut([inp_table_index, out_table_index])
                        .unwrap();

                    if !out_table.is_empty()
                        && (inp_table.raw_data.len() != out_table.raw_data.len())
                    {
                        return Err(RuntimeError::ProjectTableSizeMismatch {
                            inp_table_name: inp_table.name().to_owned(),
                            inp_table_len: inp_table.raw_data.len(),
                            out_table_name: out_table.name().to_owned(),
                            out_table_len: out_table.raw_data.len(),
                        });
                    }

                    if let Expr::Wildcard = expr {
                        // TODO: this could be optimized.
                        for col in inp_table.columns() {
                            out_table.add_column(col.clone());
                            out_table.add_column_data(
                                col.name(),
                                inp_table.get_column_data(col.name())?,
                            )?;
                        }
                    } else {
                        if inp_table.raw_data.len() == out_table.raw_data.len() {
                            for (inp_row, out_row) in
                                inp_table.raw_data.iter().zip(out_table.raw_data.iter_mut())
                            {
                                let val = Expr::execute(
                                    expr,
                                    inp_table,
                                    RowShared::from_raw(&inp_row, &inp_table),
                                )?;
                                out_row.raw_data.push(val);
                            }
                        } else {
                            for inp_row in inp_table.raw_data.iter() {
                                let val = Expr::execute(
                                    expr,
                                    inp_table,
                                    RowShared::from_raw(&inp_row, &inp_table),
                                )?;
                                out_table.new_row(vec![val]);
                            }
                        }

                        let data_type = if !out_table.raw_data.is_empty() {
                            let newly_added =
                                out_table.raw_data.first().unwrap().raw_data.last().unwrap();
                            newly_added.data_type()
                        } else {
                            let sentinel = inp_table.sentinel_row()?;
                            let output_val = Expr::execute(expr, inp_table, sentinel.to_shared())?;
                            output_val.data_type()
                        };

                        // TODO: provide a unique name here
                        let new_col = Column::new(
                            alias.unwrap_or("PLACEHOLDER".into()),
                            data_type,
                            vec![],
                            false,
                        );

                        out_table.add_column(new_col);
                    }
                }
                (Some(reg), Some(Register::TableRef(_))) => {
                    return Err(RuntimeError::RegisterNotATable("project", reg.clone()))
                }
                (Some(Register::TableRef(_)), Some(reg)) => {
                    return Err(RuntimeError::RegisterNotATable("project", reg.clone()))
                }
                (Some(reg), Some(_)) => {
                    return Err(RuntimeError::RegisterNotATable("project", reg.clone()))
                }
            },
            Instruction::GroupBy { index, expr } => todo!(),
            Instruction::Order {
                index,
                expr,
                ascending,
            } => {
                let table_index = match self.registers.get(index) {
                    None => return Err(RuntimeError::EmptyRegister(*index)),
                    Some(Register::TableRef(table_index)) => table_index,
                    Some(register) => {
                        return Err(RuntimeError::RegisterNotATable(
                            "order by",
                            register.clone(),
                        ))
                    }
                };
                let table = self.tables.get_mut(table_index).unwrap();

                let expr_values = table
                    .raw_data
                    .iter()
                    .map(|row| Expr::execute(expr, table, RowShared::from_raw(&row, &table)))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut perm = permutation::sort(expr_values);
                perm.apply_slice_in_place(&mut table.raw_data);

                if !ascending {
                    table.raw_data.reverse();
                }
            }
            Instruction::Limit { index, limit } => {
                let table_index = match self.registers.get(index) {
                    None => return Err(RuntimeError::EmptyRegister(*index)),
                    Some(Register::TableRef(table_index)) => table_index,
                    Some(register) => {
                        return Err(RuntimeError::RegisterNotATable("limit", register.clone()))
                    }
                };
                let table = self.tables.get_mut(table_index).unwrap();

                table.raw_data.truncate(*limit as usize);
            }
            Instruction::NewSchema {
                schema_name,
                exists_ok,
            } => {
                let name = schema_name.0;
                if let None = self.database.schema_by_name(&name) {
                    self.database.add_schema(Schema::new(name));
                } else if !*exists_ok {
                    return Err(RuntimeError::SchemaExists(name));
                }
            }
            Instruction::ColumnDef {
                index,
                name,
                data_type,
            } => {
                self.registers.insert(
                    *index,
                    Register::Column(Column::new(*name, data_type.clone(), vec![], false)),
                );
            }
            Instruction::AddColumnOption { index, option } => {
                let column = match self.registers.get_mut(index) {
                    Some(Register::Column(column)) => column,
                    Some(register) => {
                        return Err(RuntimeError::RegisterNotAColumn(
                            "add column option",
                            register.clone(),
                        ))
                    }
                    None => return Err(RuntimeError::EmptyRegister(*index)),
                };
                column.add_column_option(option.clone());
            }
            Instruction::AddColumn {
                table_reg_index,
                col_index,
            } => {
                let table_index = match self.registers.get(table_reg_index) {
                    None => return Err(RuntimeError::EmptyRegister(*table_reg_index)),
                    Some(Register::TableRef(table_index)) => table_index,
                    Some(register) => {
                        return Err(RuntimeError::RegisterNotATable(
                            "add column",
                            register.clone(),
                        ))
                    }
                };
                let table = self.tables.get_mut(table_index).unwrap();

                let column = match self.registers.get(col_index) {
                    Some(Register::Column(column)) => column,
                    Some(register) => {
                        return Err(RuntimeError::RegisterNotAColumn(
                            "add column",
                            register.clone(),
                        ))
                    }
                    None => return Err(RuntimeError::EmptyRegister(*col_index)),
                };

                table.add_column(column.clone());
            }
            Instruction::NewTable {
                index,
                name,
                exists_ok,
            } => {
                let table_index = *match self.registers.get(index) {
                    None => return Err(RuntimeError::EmptyRegister(*index)),
                    Some(Register::TableRef(table_index)) => table_index,
                    Some(register) => {
                        return Err(RuntimeError::RegisterNotATable(
                            "new table",
                            register.clone(),
                        ))
                    }
                };

                let table = self.tables.get_mut(&table_index).unwrap();
                table.rename(name.table_name);

                let schema = self.find_schema(name.schema_name)?;

                match self.find_table(schema, name) {
                    Ok(_) => {
                        if !exists_ok {
                            return Err(RuntimeError::TableExists(*name));
                        }
                    }
                    Err(RuntimeError::TableNotFound(_)) => {
                        self.find_schema_mut(name.schema_name)?
                            .add_table(table_index);
                    }
                    Err(e) => return Err(e),
                }
            }
            Instruction::DropTable { index } => todo!(),
            Instruction::RemoveColumn { index, col_name } => todo!(),
            Instruction::RenameColumn {
                index,
                old_name,
                new_name,
            } => todo!(),
            Instruction::InsertDef {
                table_reg_index,
                index,
            } => {
                let table_index = *match self.registers.get(table_reg_index) {
                    None => return Err(RuntimeError::EmptyRegister(*table_reg_index)),
                    Some(Register::TableRef(table_index)) => table_index,
                    Some(register) => {
                        return Err(RuntimeError::RegisterNotATable(
                            "insert def",
                            register.clone(),
                        ))
                    }
                };

                self.registers
                    .insert(*index, Register::InsertDef(InsertDef::new(table_index)));
            }
            Instruction::ColumnInsertDef {
                insert_index,
                col_name,
            } => {
                let insert = match self.registers.get_mut(insert_index) {
                    Some(Register::InsertDef(insert)) => insert,
                    Some(register) => {
                        return Err(RuntimeError::RegisterNotAInsert(
                            "column insert def",
                            register.clone(),
                        ))
                    }
                    None => return Err(RuntimeError::EmptyRegister(*insert_index)),
                };

                let table = self.tables.get(&insert.table).unwrap();

                let col_info = table.get_column(col_name)?;

                insert.columns.push((col_info.0, col_info.1.to_owned()));
            }
            Instruction::RowDef {
                insert_index,
                row_index: row_reg_index,
            } => {
                let insert = match self.registers.get_mut(insert_index) {
                    Some(Register::InsertDef(insert)) => insert,
                    Some(register) => {
                        return Err(RuntimeError::RegisterNotAInsert(
                            "row def",
                            register.clone(),
                        ))
                    }
                    None => return Err(RuntimeError::EmptyRegister(*insert_index)),
                };

                insert.rows.push(vec![]);
                let row_index = insert.rows.len() - 1;

                self.registers.insert(
                    *row_reg_index,
                    Register::InsertRow(InsertRow {
                        def: *insert_index,
                        row_index,
                    }),
                );
            }
            Instruction::AddValue {
                row_index: row_reg_index,
                expr,
            } => {
                let &InsertRow {
                    def: insert_reg_index,
                    row_index,
                } = match self.registers.get(row_reg_index) {
                    Some(Register::InsertRow(insert_row)) => insert_row,
                    Some(register) => {
                        return Err(RuntimeError::RegisterNotAInsertRow(
                            "add value",
                            register.clone(),
                        ))
                    }
                    None => return Err(RuntimeError::EmptyRegister(*row_reg_index)),
                };

                let insert = match self.registers.get_mut(&insert_reg_index) {
                    Some(Register::InsertDef(insert)) => insert,
                    Some(register) => {
                        return Err(RuntimeError::RegisterNotAInsert(
                            "row def",
                            register.clone(),
                        ))
                    }
                    None => return Err(RuntimeError::EmptyRegister(insert_reg_index)),
                };

                let table = self.tables.get(&insert.table).unwrap();

                let value = Expr::execute(expr, table, table.sentinel_row()?.to_shared())?;

                if insert.rows[row_index].len() + 1 > table.num_columns() {
                    return Err(RuntimeError::TooManyValuesToInsert(
                        *table.name(),
                        insert.rows[row_index].len() + 1,
                        table.num_columns(),
                    ));
                }

                insert.rows[row_index].push(value);
            }
            Instruction::Insert {
                index: insert_index,
            } => {
                let insert = match self.registers.remove(insert_index) {
                    Some(Register::InsertDef(insert)) => insert,
                    Some(register) => {
                        return Err(RuntimeError::RegisterNotAInsert("insert", register.clone()))
                    }
                    None => return Err(RuntimeError::EmptyRegister(*insert_index)),
                };

                let table = self.tables.get_mut(&insert.table).unwrap();

                if !insert.columns.is_empty() && (insert.columns.len() != table.num_columns()) {
                    return Err(RuntimeError::Unsupported(concat!(
                        "Default values are not supported yet. ",
                        "Some columns were missing in INSERT."
                    )));
                }

                for row in insert.rows {
                    if table.num_columns() != row.len() {
                        return Err(RuntimeError::NotEnoughValuesToInsert(
                            *table.name(),
                            row.len(),
                            table.num_columns(),
                        ));
                    }
                    table.new_row(row);
                }
            }
            Instruction::Update { index, col, expr } => todo!(),
            Instruction::Union {
                input1,
                input2,
                output,
            } => todo!(),
            Instruction::CrossJoin {
                input1,
                input2,
                output,
            } => todo!(),
            Instruction::NaturalJoin {
                input1,
                input2,
                output,
            } => todo!(),
        }
        Ok(None)
    }

    /// Find [`TableIndex`] given the schema and its name.
    fn find_table(&self, schema: &Schema, table: &TableRef) -> Result<TableIndex, RuntimeError> {
        if let Some(table_index) = schema
            .tables()
            .iter()
            .find(|table_index| self.tables[table_index].name() == &table.table_name)
        {
            Ok(*table_index)
        } else {
            Err(RuntimeError::TableNotFound(table.clone()))
        }
    }

    /// A reference to the given schema, or default schema if it's `None`.
    fn find_schema(&self, name: Option<BoundedString>) -> Result<&Schema, RuntimeError> {
        if let Some(schema_name) = name {
            match self.database.schema_by_name(&schema_name) {
                Some(schema) => Ok(schema),
                None => return Err(RuntimeError::SchemaNotFound(schema_name)),
            }
        } else {
            Ok(self.database.default_schema())
        }
    }

    /// A mutable reference to the given schema, or default schema if it's `None`.
    fn find_schema_mut(
        &mut self,
        name: Option<BoundedString>,
    ) -> Result<&mut Schema, RuntimeError> {
        if let Some(schema_name) = name {
            match self.database.schema_by_name_mut(&schema_name) {
                Some(schema) => Ok(schema),
                None => return Err(RuntimeError::SchemaNotFound(schema_name)),
            }
        } else {
            Ok(self.database.default_schema_mut())
        }
    }
}

impl Default for VirtualMachine {
    fn default() -> Self {
        Self::new(DEFAULT_DATABASE_NAME.into())
    }
}

#[derive(Debug, Clone, PartialEq)]
/// A register in the executor VM.
pub enum Register {
    /// A reference to a table.
    TableRef(TableIndex),
    /// A reference to a non-existent table.
    NonExistentTable,
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
    /// A value
    Value(Value),
    /// An expression
    Expr(Expr),
    // TODO: an error value?
}

#[derive(Debug, Clone, PartialEq)]
/// An abstract definition of a create table statement.
pub struct TableDef {
    pub name: BoundedString,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, PartialEq)]
/// An abstract definition of an insert statement.
pub struct InsertDef {
    /// The view to insert into
    pub table: TableIndex,
    /// The columns to insert into.
    ///
    /// Empty means all columns.
    pub columns: Vec<(usize, Column)>,
    /// The values to insert.
    pub rows: Vec<Vec<Value>>,
}

impl InsertDef {
    pub fn new(table: TableIndex) -> Self {
        Self {
            table,
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// A row of values to insert.
pub struct InsertRow {
    /// The insert definition which this belongs to
    pub def: RegisterIndex,
    /// Which row of the insert definition this refers to
    pub row_index: usize,
}

#[derive(Debug)]
pub enum ExecutionError {
    ParseError(ParserError),
    CodegenError(CodegenError),
    RuntimeError(RuntimeError),
}

impl From<ParserError> for ExecutionError {
    fn from(err: ParserError) -> Self {
        ExecutionError::ParseError(err)
    }
}

impl From<CodegenError> for ExecutionError {
    fn from(err: CodegenError) -> Self {
        ExecutionError::CodegenError(err)
    }
}

impl From<RuntimeError> for ExecutionError {
    fn from(err: RuntimeError) -> Self {
        ExecutionError::RuntimeError(err)
    }
}

impl Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ParseError(e) => write!(f, "{}", e),
            Self::CodegenError(e) => write!(f, "{}", e),
            Self::RuntimeError(e) => write!(f, "{}", e),
        }
    }
}

impl Error for ExecutionError {}

#[derive(Debug, PartialEq)]
pub enum RuntimeError {
    ColumnNotFound(ColumnRef),
    TableNotFound(TableRef),
    TableExists(TableRef),
    SchemaNotFound(BoundedString),
    SchemaExists(BoundedString),
    EmptyRegister(RegisterIndex),
    RegisterNotATable(&'static str, Register),
    RegisterNotAColumn(&'static str, Register),
    RegisterNotAInsert(&'static str, Register),
    RegisterNotAInsertRow(&'static str, Register),
    CannotReturn(Register),
    FilterWithNonBoolean(Expr, Value),
    ProjectOnNonEmptyTable(BoundedString),
    ProjectTableSizeMismatch {
        inp_table_name: BoundedString,
        inp_table_len: usize,
        out_table_name: BoundedString,
        out_table_len: usize,
    },
    TableNewColumnSizeMismatch {
        table_name: BoundedString,
        table_len: usize,
        col_name: BoundedString,
        col_len: usize,
    },
    UnsupportedType(DataType),
    ExprExecError(ExprExecError),
    TooManyValuesToInsert(BoundedString, usize, usize),
    NotEnoughValuesToInsert(BoundedString, usize, usize),
    Unsupported(&'static str),
}

impl From<ExprExecError> for RuntimeError {
    fn from(e: ExprExecError) -> Self {
        Self::ExprExecError(e)
    }
}

impl Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ColumnNotFound(c) => write!(f, "Column not found: '{}'", c),
            Self::TableNotFound(t) => write!(f, "Table not found: '{}'", t),
            Self::TableExists(s) => write!(f, "Table already exists: '{}'", s),
            Self::SchemaNotFound(s) => write!(f, "Schema not found: '{}'", s),
            Self::SchemaExists(s) => write!(f, "Schema already exists: '{}'", s),
            Self::EmptyRegister(r) => write!(
                f,
                "Register is not initialized: '{}' (critical error. Please file an issue.)",
                r
            ),
            Self::RegisterNotATable(operation, reg) => write!(
                f,
                "Register is not a table. Cannot perform '{}' on '{:?}'",
                operation, reg
            ),
            Self::RegisterNotAColumn(operation, reg) => write!(
                f,
                "Register is not a column. Cannot perform '{}' on '{:?}'",
                operation, reg
            ),
            Self::RegisterNotAInsert(operation, reg) => write!(
                f,
                "Register is not an insert def. Cannot perform '{}' on '{:?}'",
                operation, reg
            ),
            Self::RegisterNotAInsertRow(operation, reg) => write!(
                f,
                "Register is not an insert row. Cannot perform '{}' on '{:?}'",
                operation, reg
            ),
            Self::CannotReturn(r) => write!(
                f,
                "Register value cannot be returned: '{:?}' \
                 (critical error. Please file an issue)",
                r
            ),
            Self::FilterWithNonBoolean(e, v) => write!(
                f,
                "WHERE clause used with a non-boolean value. \
                 Expression: '{}' evaluated to value: '{}'",
                e, v
            ),
            Self::ProjectOnNonEmptyTable(table_name) => write!(
                f,
                "Projecting on a non-empty table is not supported. \
                 Tried projecting onto table: '{}'",
                table_name
            ),
            Self::ProjectTableSizeMismatch {
                inp_table_name,
                inp_table_len,
                out_table_name,
                out_table_len,
            } => write!(
                f,
                "Projection input and output table had different number of rows. \
                 Input: '{}' with length {}, Output: '{}' with length {}",
                inp_table_name, inp_table_len, out_table_name, out_table_len
            ),
            Self::TableNewColumnSizeMismatch {
                table_name,
                table_len,
                col_name,
                col_len,
            } => write!(
                f,
                "New column data size does not match table size. \
                 Table: '{}' with length {}, New column: '{}' with length {}",
                table_name, table_len, col_name, col_len,
            ),
            Self::UnsupportedType(d) => write!(f, "Unsupported type: {}", d),
            Self::ExprExecError(e) => write!(f, "{}", e),
            Self::TooManyValuesToInsert(table_name, got_num, expected_num) => write!(
                f,
                concat!(
                    "Too many values to insert into table '{}'. ",
                    "Got at least {} values while the table has {} columns."
                ),
                table_name, got_num, expected_num
            ),
            Self::NotEnoughValuesToInsert(table_name, got_num, expected_num) => write!(
                f,
                concat!(
                    "Not enough values to insert into table '{}'. ",
                    "Got at {} values while {} columns were expected."
                ),
                table_name, got_num, expected_num
            ),
            Self::Unsupported(err) => write!(f, "{}", err,),
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{ColumnOption, ColumnOptionDef, DataType};

    use crate::{
        codegen::codegen,
        column::Column,
        expr::{eval::ExprExecError, BinOp, Expr},
        identifier::{ColumnRef, TableRef},
        parser::parse,
        table::{Row, Table},
        value::Value,
    };

    use super::{RuntimeError, VirtualMachine};

    #[test]
    fn create_vm() {
        let _ = VirtualMachine::default();
    }

    fn check_single_statement(
        query: &str,
        vm: &mut VirtualMachine,
    ) -> Result<Option<Table>, RuntimeError> {
        let parsed = parse(query).unwrap();
        assert_eq!(parsed.len(), 1);

        let statement = &parsed[0];
        let ic = codegen(&statement).unwrap();

        println!("ic: {ic:#?}");

        vm.execute_ic(&ic)
    }

    #[test]
    fn create_schema() {
        let mut vm = VirtualMachine::default();

        let _res = check_single_statement("CREATE SCHEMA abc", &mut vm).unwrap();
        let schema = vm.database.schema_by_name(&"abc".into()).unwrap();
        assert_eq!(schema.name(), "abc");
        assert_eq!(schema.tables(), &vec![]);

        let res = check_single_statement("CREATE SCHEMA abc", &mut vm).unwrap_err();
        assert_eq!(res, RuntimeError::SchemaExists("abc".into()));

        let _res = check_single_statement("CREATE SCHEMA IF NOT EXISTS abc", &mut vm).unwrap();
        let schema = vm.database.schema_by_name(&"abc".into()).unwrap();
        assert_eq!(schema.name(), "abc");
        assert_eq!(schema.tables(), &vec![]);
    }

    #[test]
    fn create_table() {
        let mut vm = VirtualMachine::default();
        let _res = check_single_statement(
            "CREATE TABLE
             IF NOT EXISTS table1
             (
                 col1 INTEGER PRIMARY KEY NOT NULL,
                 col2 STRING NOT NULL,
                 col3 INTEGER UNIQUE
             )",
            &mut vm,
        )
        .unwrap();

        let table_index = vm
            .find_table(
                vm.database.default_schema(),
                &TableRef {
                    schema_name: None,
                    table_name: "table1".into(),
                },
            )
            .unwrap();

        let table = vm.table(&table_index).unwrap();
        assert_eq!(
            table.columns().cloned().collect::<Vec<_>>(),
            vec![
                Column::new(
                    "col1".into(),
                    DataType::Int(None),
                    vec![
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Unique { is_primary: true },
                        },
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull
                        }
                    ],
                    false
                ),
                Column::new(
                    "col2".into(),
                    DataType::String,
                    vec![ColumnOptionDef {
                        name: None,
                        option: ColumnOption::NotNull
                    }],
                    false
                ),
                Column::new(
                    "col3".into(),
                    DataType::Int(None),
                    vec![ColumnOptionDef {
                        name: None,
                        option: ColumnOption::Unique { is_primary: false },
                    }],
                    false
                ),
            ]
        )
    }

    #[test]
    fn insert_values() {
        let mut vm = VirtualMachine::default();

        check_single_statement(
            "
            CREATE TABLE table1
            (
                col1 INTEGER PRIMARY KEY NOT NULL,
                col2 STRING NOT NULL
            )
            ",
            &mut vm,
        )
        .unwrap();

        let _res = check_single_statement(
            "
            INSERT INTO table1 VALUES
                (2, 'bar'),
                (3, 'aaa')
            ",
            &mut vm,
        )
        .unwrap();

        let table_index = vm
            .find_table(
                vm.database.default_schema(),
                &TableRef {
                    schema_name: None,
                    table_name: "table1".into(),
                },
            )
            .unwrap();

        let table = vm.table(&table_index).unwrap();

        assert_eq!(
            table.all_data(),
            vec![
                Row::new(vec![Value::Int64(2), Value::String("bar".to_owned())]),
                Row::new(vec![Value::Int64(3), Value::String("aaa".to_owned())])
            ]
        );

        let res = check_single_statement(
            "
            INSERT INTO table1 VALUES
                (2, 'bar', 1.9)
            ",
            &mut vm,
        )
        .unwrap_err();

        assert_eq!(
            res,
            RuntimeError::TooManyValuesToInsert("table1".into(), 3, 2)
        );

        let res = check_single_statement(
            "
            INSERT INTO table1 VALUES
                ('bar')
            ",
            &mut vm,
        );

        assert_eq!(
            res.unwrap_err(),
            RuntimeError::NotEnoughValuesToInsert("table1".into(), 1, 2)
        );

        let _res = check_single_statement(
            "
            INSERT INTO table1 (col1, col2) VALUES
                (4, 'car'),
                (5, 'yak')
            ",
            &mut vm,
        )
        .unwrap();

        let table_index = vm
            .find_table(
                vm.database.default_schema(),
                &TableRef {
                    schema_name: None,
                    table_name: "table1".into(),
                },
            )
            .unwrap();

        let table = vm.table(&table_index).unwrap();

        assert_eq!(
            table.all_data(),
            vec![
                Row::new(vec![Value::Int64(2), Value::String("bar".to_owned())]),
                Row::new(vec![Value::Int64(3), Value::String("aaa".to_owned())]),
                Row::new(vec![Value::Int64(4), Value::String("car".to_owned())]),
                Row::new(vec![Value::Int64(5), Value::String("yak".to_owned())]),
            ]
        );

        let res = check_single_statement(
            "
            INSERT INTO table1 (col2) VALUES
                ('bar')
            ",
            &mut vm,
        );
        matches!(res.unwrap_err(), RuntimeError::Unsupported(_));
    }

    #[test]
    fn select() {
        let mut vm = VirtualMachine::default();

        let res = check_single_statement("SELECT 1", &mut vm)
            .unwrap()
            .unwrap();

        assert_eq!(
            res.columns().collect::<Vec<_>>(),
            vec![&Column::new(
                "PLACEHOLDER".into(),
                DataType::Int(None),
                vec![],
                false
            ),]
        );

        assert_eq!(res.all_data(), vec![Row::new(vec![Value::Int64(1)])]);
        assert_eq!(
            check_single_statement("SELECT 10 * 20 + 5", &mut vm)
                .unwrap()
                .unwrap()
                .all_data(),
            vec![Row::new(vec![Value::Int64(205)])]
        );
        assert_eq!(
            check_single_statement("SELECT 'a'", &mut vm)
                .unwrap()
                .unwrap()
                .all_data(),
            vec![Row::new(vec![Value::String("a".to_owned())])]
        );

        check_single_statement(
            "
            CREATE TABLE table1
            (
                col1 INTEGER PRIMARY KEY NOT NULL,
                col2 STRING NOT NULL
            )
            ",
            &mut vm,
        )
        .unwrap();

        check_single_statement(
            "
            INSERT INTO table1 VALUES
                (2, 'bar'),
                (3, 'aaa')
            ",
            &mut vm,
        )
        .unwrap();

        let res = check_single_statement("SELECT * FROM table1", &mut vm)
            .unwrap()
            .unwrap();

        assert_eq!(
            res.columns().cloned().collect::<Vec<_>>(),
            vec![
                Column::new(
                    "col1".into(),
                    DataType::Int(None),
                    vec![
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Unique { is_primary: true },
                        },
                        ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull
                        }
                    ],
                    false
                ),
                Column::new(
                    "col2".into(),
                    DataType::String,
                    vec![ColumnOptionDef {
                        name: None,
                        option: ColumnOption::NotNull
                    }],
                    false
                ),
            ]
        );

        assert_eq!(
            res.all_data(),
            vec![
                Row::new(vec![Value::Int64(2), Value::String("bar".to_owned())]),
                Row::new(vec![Value::Int64(3), Value::String("aaa".to_owned())])
            ]
        );

        let res = check_single_statement("SELECT * FROM table1 WHERE col1 = 2", &mut vm)
            .unwrap()
            .unwrap();
        assert_eq!(
            res.all_data(),
            vec![Row::new(vec![
                Value::Int64(2),
                Value::String("bar".to_owned())
            ])]
        );

        let res = check_single_statement("SELECT * FROM table1 WHERE col1 = 1", &mut vm)
            .unwrap()
            .unwrap();
        assert_eq!(res.all_data(), vec![]);

        let res = check_single_statement("SELECT * FROM table1 WHERE col1 = 2", &mut vm)
            .unwrap()
            .unwrap();
        assert_eq!(
            res.all_data(),
            vec![Row::new(vec![
                Value::Int64(2),
                Value::String("bar".to_owned())
            ])]
        );

        let res =
            check_single_statement("SELECT * FROM table1 WHERE col1 = 2 or col1 = 3", &mut vm)
                .unwrap()
                .unwrap();
        assert_eq!(
            res.all_data(),
            vec![
                Row::new(vec![Value::Int64(2), Value::String("bar".to_owned())]),
                Row::new(vec![Value::Int64(3), Value::String("aaa".to_owned())])
            ]
        );

        let res = check_single_statement("SELECT col1 FROM table1", &mut vm)
            .unwrap()
            .unwrap();
        assert_eq!(
            res.all_data(),
            vec![
                Row::new(vec![Value::Int64(2)]),
                Row::new(vec![Value::Int64(3)])
            ]
        );

        let res = check_single_statement("SELECT col1, col2 FROM table1", &mut vm)
            .unwrap()
            .unwrap();
        assert_eq!(
            res.all_data(),
            vec![
                Row::new(vec![Value::Int64(2), Value::String("bar".to_owned())]),
                Row::new(vec![Value::Int64(3), Value::String("aaa".to_owned())])
            ]
        );

        let res = check_single_statement("SELECT * FROM table1 ORDER BY col1", &mut vm)
            .unwrap()
            .unwrap();
        assert_eq!(
            res.all_data(),
            vec![
                Row::new(vec![Value::Int64(2), Value::String("bar".to_owned())]),
                Row::new(vec![Value::Int64(3), Value::String("aaa".to_owned())])
            ]
        );

        let res = check_single_statement("SELECT * FROM table1 ORDER BY col2", &mut vm)
            .unwrap()
            .unwrap();
        assert_eq!(
            res.all_data(),
            vec![
                Row::new(vec![Value::Int64(3), Value::String("aaa".to_owned())]),
                Row::new(vec![Value::Int64(2), Value::String("bar".to_owned())]),
            ]
        );

        let res = check_single_statement("SELECT * FROM table1 ORDER BY col1 DESC", &mut vm)
            .unwrap()
            .unwrap();
        assert_eq!(
            res.all_data(),
            vec![
                Row::new(vec![Value::Int64(3), Value::String("aaa".to_owned())]),
                Row::new(vec![Value::Int64(2), Value::String("bar".to_owned())]),
            ]
        );

        let res = check_single_statement("SELECT * FROM table1 ORDER BY col2 DESC", &mut vm)
            .unwrap()
            .unwrap();
        assert_eq!(
            res.all_data(),
            vec![
                Row::new(vec![Value::Int64(2), Value::String("bar".to_owned())]),
                Row::new(vec![Value::Int64(3), Value::String("aaa".to_owned())]),
            ]
        );

        let res = check_single_statement("SELECT * FROM table1 ORDER BY col1 LIMIT 1", &mut vm)
            .unwrap()
            .unwrap();
        assert_eq!(
            res.all_data(),
            vec![Row::new(vec![
                Value::Int64(2),
                Value::String("bar".to_owned())
            ]),]
        );

        let res = check_single_statement("SELECT col3 FROM table1", &mut vm);
        assert_eq!(
            res.unwrap_err(),
            RuntimeError::ExprExecError(ExprExecError::NoSuchColumn("col3".into()))
        );

        let res = check_single_statement("SELECT col1 FROM table2", &mut vm);
        assert_eq!(
            res.unwrap_err(),
            RuntimeError::TableNotFound(TableRef {
                schema_name: None,
                table_name: "table2".into()
            })
        );

        let res = check_single_statement("SELECT col1 FROM table1 ORDER BY col3", &mut vm);
        assert_eq!(
            res.unwrap_err(),
            RuntimeError::ExprExecError(ExprExecError::NoSuchColumn("col3".into()))
        );

        let res = check_single_statement("SELECT col1 FROM table1 WHERE col3 = 1", &mut vm);
        assert_eq!(
            res.unwrap_err(),
            RuntimeError::ExprExecError(ExprExecError::NoSuchColumn("col3".into()))
        );

        let res = check_single_statement("SELECT col1 FROM table1 WHERE col1 + 1", &mut vm);
        assert_eq!(
            res.unwrap_err(),
            RuntimeError::FilterWithNonBoolean(
                Expr::Binary {
                    left: Box::new(Expr::ColumnRef(ColumnRef {
                        schema_name: None,
                        table_name: None,
                        col_name: "col1".into()
                    })),
                    op: BinOp::Plus,
                    right: Box::new(Expr::Value(Value::Int64(1)))
                },
                Value::Int64(3)
            )
        );
    }
}
