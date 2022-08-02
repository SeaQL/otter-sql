/// Intermediate code generation from the AST.
use sqlparser::ast::Statement;

use std::{error::Error, fmt::Display};

use crate::{
    ic::{Instruction, IntermediateCode},
    identifier::IdentifierError,
    vm::RegisterIndex,
};

/// Generates intermediate code from the AST.
pub fn codegen(ast: &Statement) -> Result<IntermediateCode, CodegenError> {
    let mut instrs = Vec::<Instruction>::new();

    let mut current_reg = RegisterIndex::default();

    match ast {
        Statement::Query(query) => todo!(),
        Statement::CreateTable {
            // TODO: support `OR REPLACE`
            or_replace: _,
            // TODO: support temp tables
            temporary: _,
            external: _,
            global: _,
            if_not_exists,
            name,
            columns,
            // TODO: support table level constraints
            constraints: _,
            hive_distribution: _,
            hive_formats: _,
            table_properties: _,
            with_options: _,
            file_format: _,
            location: _,
            query: _,
            without_rowid: _,
            like: _,
            engine: _,
            default_charset: _,
            collation: _,
            on_commit: _,
        } => {
            let table_reg_index = current_reg;
            instrs.push(Instruction::Empty {
                index: table_reg_index,
            });
            current_reg = current_reg.next_index();

            let col_reg_index = current_reg;
            current_reg = current_reg.next_index();
            for col in columns {
                instrs.push(Instruction::ColumnDef {
                    index: col_reg_index,
                    name: col.name.value.as_str().into(),
                    data_type: col.data_type.clone(),
                });

                for option in col.options.iter() {
                    instrs.push(Instruction::AddColumnOption {
                        index: col_reg_index,
                        option: option.clone(),
                    });
                }

                instrs.push(Instruction::AddColumn {
                    table_reg_index,
                    col_index: col_reg_index,
                });
            }

            instrs.push(Instruction::NewTable {
                index: table_reg_index,
                name: name.0.clone().try_into()?,
                exists_ok: *if_not_exists,
            });
            Ok(())
        }
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
        } => {
            instrs.push(Instruction::NewSchema {
                schema_name: schema_name.0.clone().try_into()?,
                exists_ok: *if_not_exists,
            });
            Ok(())
        }
        _ => Err(CodegenError::UnsupportedStatement(ast.to_string())),
    }?;

    Ok(IntermediateCode { instrs })
}

#[derive(Debug)]
pub enum CodegenError {
    UnsupportedStatement(String),
    InvalidIdentifier(IdentifierError),
}

impl Display for CodegenError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            CodegenError::UnsupportedStatement(s) => write!(f, "Unsupported statement: {}", s),
            CodegenError::InvalidIdentifier(i) => {
                write!(f, "{}", i)
            }
        }
    }
}

impl From<IdentifierError> for CodegenError {
    fn from(i: IdentifierError) -> Self {
        CodegenError::InvalidIdentifier(i)
    }
}

impl Error for CodegenError {}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{ColumnOption, ColumnOptionDef, DataType};

    use crate::{
        codegen::codegen, ic::Instruction, identifier::TableRef, parser::parse, vm::RegisterIndex,
    };

    fn check_single_statement(
        query: &str,
        callback: impl Fn(&[Instruction]) -> bool,
    ) -> Result<(), String> {
        let parsed = parse(query).unwrap();
        assert_eq!(parsed.len(), 1);

        let statement = &parsed[0];
        let ic = codegen(&statement).unwrap();
        if !callback(ic.instrs.as_slice()) {
            Err(format!("Instructions did not match: {:?}", ic.instrs))
        } else {
            Ok(())
        }
    }

    #[test]
    fn create_schema() {
        check_single_statement("CREATE SCHEMA abc", |instrs| match instrs {
            &[Instruction::NewSchema {
                schema_name,
                exists_ok: false,
            }] => {
                assert_eq!(schema_name.0.as_str(), "abc");
                true
            }
            _ => false,
        })
        .unwrap();

        check_single_statement("CREATE SCHEMA IF NOT EXISTS abc", |instrs| match instrs {
            &[Instruction::NewSchema {
                schema_name,
                exists_ok: true,
            }] => {
                assert_eq!(schema_name.0.as_str(), "abc");
                true
            }
            _ => false,
        })
        .unwrap();
    }

    #[test]
    fn create_table() {
        check_single_statement(
            "CREATE TABLE
             IF NOT EXISTS table1
             (
                 col1 INTEGER PRIMARY KEY NOT NULL,
                 col2 STRING NOT NULL,
                 col3 INTEGER UNIQUE
             )",
            |instrs| {
                let mut index = 0;
                if let Instruction::Empty { index } = instrs[index] {
                    assert_eq!(index, RegisterIndex::default());
                } else {
                    panic!("Did not match: {:?}", instrs[index]);
                }
                index += 1;

                if let Instruction::ColumnDef {
                    index,
                    name,
                    ref data_type,
                } = instrs[index]
                {
                    assert_eq!(index, RegisterIndex::default().next_index());
                    assert_eq!(name.as_str(), "col1");
                    assert_eq!(data_type, &DataType::Int(None));
                } else {
                    panic!("Did not match: {:?}", instrs[index]);
                }
                index += 1;

                if let Instruction::AddColumnOption { index, ref option } = instrs[index] {
                    assert_eq!(index, RegisterIndex::default().next_index());
                    assert_eq!(
                        option,
                        &ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Unique { is_primary: true }
                        }
                    );
                } else {
                    panic!("Did not match: {:?}", instrs[index]);
                }
                index += 1;

                if let Instruction::AddColumnOption { index, ref option } = instrs[index] {
                    assert_eq!(index, RegisterIndex::default().next_index());
                    assert_eq!(
                        option,
                        &ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull
                        }
                    );
                } else {
                    panic!("Did not match: {:?}", instrs[index]);
                }
                index += 1;

                if let Instruction::AddColumn {
                    table_reg_index,
                    col_index,
                } = instrs[index]
                {
                    assert_eq!(table_reg_index, RegisterIndex::default());
                    assert_eq!(col_index, RegisterIndex::default().next_index());
                } else {
                    panic!("Did not match: {:?}", instrs[index]);
                }
                index += 1;

                if let Instruction::ColumnDef {
                    index,
                    name,
                    ref data_type,
                } = instrs[index]
                {
                    assert_eq!(index, RegisterIndex::default().next_index());
                    assert_eq!(name.as_str(), "col2");
                    assert_eq!(data_type, &DataType::String);
                } else {
                    panic!("Did not match: {:?}", instrs[index]);
                }
                index += 1;

                if let Instruction::AddColumnOption { index, ref option } = instrs[index] {
                    assert_eq!(index, RegisterIndex::default().next_index());
                    assert_eq!(
                        option,
                        &ColumnOptionDef {
                            name: None,
                            option: ColumnOption::NotNull
                        }
                    );
                } else {
                    panic!("Did not match: {:?}", instrs[index]);
                }
                index += 1;

                if let Instruction::AddColumn {
                    table_reg_index,
                    col_index,
                } = instrs[index]
                {
                    assert_eq!(table_reg_index, RegisterIndex::default());
                    assert_eq!(col_index, RegisterIndex::default().next_index());
                } else {
                    panic!("Did not match: {:?}", instrs[index]);
                }
                index += 1;

                if let Instruction::ColumnDef {
                    index,
                    name,
                    ref data_type,
                } = instrs[index]
                {
                    assert_eq!(index, RegisterIndex::default().next_index());
                    assert_eq!(name.as_str(), "col3");
                    assert_eq!(data_type, &DataType::Int(None));
                } else {
                    panic!("Did not match: {:?}", instrs[index]);
                }
                index += 1;

                if let Instruction::AddColumnOption { index, ref option } = instrs[index] {
                    assert_eq!(index, RegisterIndex::default().next_index());
                    assert_eq!(
                        option,
                        &ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Unique { is_primary: false }
                        }
                    );
                } else {
                    panic!("Did not match: {:?}", instrs[index]);
                }
                index += 1;

                if let Instruction::AddColumn {
                    table_reg_index,
                    col_index,
                } = instrs[index]
                {
                    assert_eq!(table_reg_index, RegisterIndex::default());
                    assert_eq!(col_index, RegisterIndex::default().next_index());
                } else {
                    panic!("Did not match: {:?}", instrs[index]);
                }
                index += 1;

                if let Instruction::NewTable {
                    index,
                    name,
                    exists_ok,
                } = instrs[index]
                {
                    assert_eq!(index, RegisterIndex::default());
                    assert_eq!(
                        name,
                        TableRef {
                            schema_name: None,
                            table_name: "table1".into()
                        }
                    );
                    assert_eq!(exists_ok, true);
                } else {
                    panic!("Did not match: {:?}", instrs[index]);
                }
                index += 1;

                assert_eq!(instrs.len(), index);

                true
            },
        )
        .unwrap();
    }
}
