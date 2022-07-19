/// Intermediate code generation from the AST.
use sqlparser::ast::Statement;

use std::{error::Error, fmt::Display};

use crate::ic::{Instruction, IntermediateCode};

/// Generates intermediate code from the AST.
pub fn codegen(ast: &Statement) -> Result<IntermediateCode, CodegenError> {
    let mut instrs = Vec::<Instruction>::new();

    match ast {
        Statement::Query(query) => todo!(),
        Statement::CreateDatabase {
            db_name,
            if_not_exists,
            location: _,
            managed_location: _,
        } => {
            if db_name.0.len() > 1 {
                Err(CodegenError::InvalidIdentifier {
                    ident: db_name.to_string(),
                    reason: "Database name must consist of only one part.",
                })
            } else {
                instrs.push(Instruction::NewDatabase {
                    name: db_name.0[0].value.as_str().into(),
                    exists_ok: *if_not_exists,
                });
                Ok(())
            }
        }
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
        } => match &schema_name.0[..] {
            [schema_name] => {
                instrs.push(Instruction::NewSchema {
                    db_name: None,
                    schema_name: schema_name.value.as_str().into(),
                    exists_ok: *if_not_exists,
                });
                Ok(())
            }
            [db_name, schema_name] => {
                instrs.push(Instruction::NewSchema {
                    db_name: Some(db_name.value.as_str().into()),
                    schema_name: schema_name.value.as_str().into(),
                    exists_ok: *if_not_exists,
                });
                Ok(())
            }
            _ => Err(CodegenError::InvalidIdentifier {
                ident: schema_name.to_string(),
                reason: "Schema name must consist of one or two parts.",
            }),
        },
        _ => Err(CodegenError::UnsupportedStatement(ast.to_string())),
    }?;

    Ok(IntermediateCode { instrs })
}

#[derive(Debug)]
pub enum CodegenError {
    UnsupportedStatement(String),
    InvalidIdentifier { ident: String, reason: &'static str },
}

impl Display for CodegenError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            CodegenError::UnsupportedStatement(s) => write!(f, "Unsupported statement: {}", s),
            CodegenError::InvalidIdentifier { ident, reason } => {
                write!(f, "Invalid identifier: {} (Got: '{}')", ident, reason)
            }
        }
    }
}

impl Error for CodegenError {}
