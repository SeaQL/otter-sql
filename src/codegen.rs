/// Intermediate code generation from the AST.
use sqlparser::ast::Statement;

use std::{error::Error, fmt::Display};

use crate::ic::{Instruction, IntermediateCode};

/// Generates intermediate code from the AST.
pub fn codegen(ast: &Statement) -> Result<IntermediateCode, CodegenError> {
    let mut instrs = Vec::<Instruction>::new();

    match ast {
        Statement::Query(query) => todo!(),
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

#[cfg(test)]
mod tests {
    use crate::{codegen::codegen, ic::Instruction, parser::parse};

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
                db_name: None,
                schema_name,
                exists_ok: false,
            }] => {
                assert_eq!(schema_name.as_str(), "abc");
                true
            }
            _ => false,
        })
        .unwrap();

        check_single_statement("CREATE SCHEMA IF NOT EXISTS abc", |instrs| match instrs {
            &[Instruction::NewSchema {
                db_name: None,
                schema_name,
                exists_ok: true,
            }] => {
                assert_eq!(schema_name.as_str(), "abc");
                true
            }
            _ => false,
        })
        .unwrap();

        check_single_statement(
            "CREATE SCHEMA IF NOT EXISTS db1.abc",
            |instrs| match instrs {
                &[Instruction::NewSchema {
                    db_name: Some(db_name),
                    schema_name,
                    exists_ok: true,
                }] => {
                    assert_eq!(db_name.as_str(), "db1");
                    assert_eq!(schema_name.as_str(), "abc");
                    true
                }
                _ => false,
            },
        )
        .unwrap();
    }
}
