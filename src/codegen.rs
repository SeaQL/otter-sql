/// Intermediate code generation from the AST.
use sqlparser::ast::Statement;

use std::{error::Error, fmt::Display};

use crate::ic::{Instruction, IntermediateCode};

/// Generates intermediate code from the AST.
fn codegen(ast: &Statement) -> Result<IntermediateCode, CodegenError> {
    let instrs = Vec::<Instruction>::new();

    match ast {
        Statement::Query(query) => todo!(),
        Statement::CreateDatabase {
            db_name,
            if_not_exists,
            location: _,
            managed_location: _,
        } => {
            todo!()
        }
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
        } => {
            todo!()
        }
        _ => Err(CodegenError::UnsupportedStatement(ast.to_string())),
    }?;

    Ok(IntermediateCode { instrs })
}

#[derive(Debug)]
enum CodegenError {
    UnsupportedStatement(String),
}

impl Display for CodegenError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            CodegenError::UnsupportedStatement(s) => write!(f, "Unsupported statement: {}", s),
        }
    }
}

impl Error for CodegenError {}
