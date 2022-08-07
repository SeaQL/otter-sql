/// Intermediate code generation from the AST.
use sqlparser::ast::{self, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins};

use std::{error::Error, fmt::Display};

use crate::{
    expr::{Expr, ExprError},
    ic::{Instruction, IntermediateCode},
    identifier::IdentifierError,
    value::{Value, ValueError},
    vm::RegisterIndex,
};

/// Generates intermediate code from the AST.
pub fn codegen(ast: &Statement) -> Result<IntermediateCode, CodegenError> {
    let mut instrs = Vec::<Instruction>::new();

    let mut current_reg = RegisterIndex::default();

    match ast {
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
        Statement::Insert {
            or: _,
            into: _,
            table_name,
            columns,
            overwrite: _,
            source,
            partitioned: _,
            after_columns: _,
            table: _,
            on: _,
        } => {
            let table_reg_index = current_reg;
            instrs.push(Instruction::Source {
                index: table_reg_index,
                name: table_name.0.clone().try_into()?,
            });
            current_reg = current_reg.next_index();

            let insert_reg_index = current_reg;
            instrs.push(Instruction::InsertDef {
                table_reg_index,
                index: insert_reg_index,
            });
            current_reg = current_reg.next_index();

            for col in columns {
                instrs.push(Instruction::ColumnInsertDef {
                    insert_index: insert_reg_index,
                    col_name: col.value.as_str().into(),
                })
            }

            // only values source for now
            match source.as_ref() {
                ast::Query {
                    body: ast::SetExpr::Values(values),
                    ..
                } => {
                    for row in values.0.clone() {
                        let row_reg = current_reg;
                        current_reg = current_reg.next_index();

                        instrs.push(Instruction::RowDef {
                            insert_index: insert_reg_index,
                            row_index: row_reg,
                        });

                        for value in row {
                            instrs.push(Instruction::AddValue {
                                row_index: row_reg,
                                expr: value.try_into()?,
                            });
                        }
                    }
                    Ok(())
                }
                _ => Err(CodegenError::UnsupportedStatementForm(
                    "Only insert with values is supported.",
                    ast.to_string(),
                )),
            }?;

            instrs.push(Instruction::Insert {
                index: insert_reg_index,
            });

            Ok(())
        }
        Statement::Query(query) => {
            // TODO: support CTEs
            let mut table_reg_index = current_reg;
            current_reg = current_reg.next_index();

            match &query.body {
                SetExpr::Select(select) => {
                    match select.from.as_slice() {
                        [TableWithJoins { relation, joins }] => {
                            if !joins.is_empty() {
                                // TODO: joins
                                return Err(CodegenError::UnsupportedStatementForm(
                                    "Joins are not supported yet",
                                    select.to_string(),
                                ));
                            }

                            match relation {
                                TableFactor::Table {
                                    name,
                                    // TODO: support table alias
                                    alias: _,
                                    args: _,
                                    with_hints: _,
                                } => instrs.push(Instruction::Source {
                                    index: table_reg_index,
                                    name: name.0.clone().try_into()?,
                                }),
                                TableFactor::Derived { .. } => {
                                    // TODO: support tables derived from a query
                                    return Err(CodegenError::UnsupportedStatementForm(
                                        "Derived tables are not supportd yet",
                                        relation.to_string(),
                                    ));
                                }
                                TableFactor::NestedJoin(_) => {
                                    // TODO: support nested joins
                                    return Err(CodegenError::UnsupportedStatementForm(
                                        "Nested JOINs are not supportd yet",
                                        relation.to_string(),
                                    ));
                                }
                                TableFactor::TableFunction { .. } => {
                                    // no plans to support these yet
                                    return Err(CodegenError::UnsupportedStatementForm(
                                        "Table functions are not supportd yet",
                                        relation.to_string(),
                                    ));
                                }
                                TableFactor::UNNEST { .. } => {
                                    // no plans to support these yet
                                    return Err(CodegenError::UnsupportedStatementForm(
                                        "UNNEST are not supportd yet",
                                        relation.to_string(),
                                    ));
                                }
                            }
                        }
                        &[] => instrs.push(Instruction::Empty {
                            index: table_reg_index,
                        }),
                        _ => {
                            // TODO: multiple tables
                            return Err(CodegenError::UnsupportedStatementForm(
                                "Only select from a single table is supported for now",
                                format!("{:?}", select.from),
                            ));
                        }
                    }

                    if let Some(expr) = select.selection.clone() {
                        instrs.push(Instruction::Filter {
                            index: table_reg_index,
                            expr: expr.try_into()?,
                        })
                    }

                    for group_by in select.group_by.clone() {
                        instrs.push(Instruction::GroupBy {
                            index: table_reg_index,
                            expr: group_by.try_into()?,
                        });
                    }

                    if let Some(expr) = select.having.clone() {
                        instrs.push(Instruction::Filter {
                            index: table_reg_index,
                            expr: expr.try_into()?,
                        })
                    }

                    if !select.projection.is_empty() {
                        let original_table_reg_index = table_reg_index;
                        let table_reg_index = current_reg;
                        current_reg = current_reg.next_index();

                        instrs.push(Instruction::Empty {
                            index: table_reg_index,
                        });

                        for projection in select.projection.clone() {
                            instrs.push(Instruction::Project {
                                input: original_table_reg_index,
                                output: table_reg_index,
                                expr: match projection {
                                    SelectItem::UnnamedExpr(ref expr) => expr.clone().try_into()?,
                                    SelectItem::ExprWithAlias { ref expr, .. } => {
                                        expr.clone().try_into()?
                                    }
                                    SelectItem::QualifiedWildcard(_) => Expr::Wildcard,
                                    SelectItem::Wildcard => Expr::Wildcard,
                                },
                                alias: match projection {
                                    SelectItem::UnnamedExpr(_) => None,
                                    SelectItem::ExprWithAlias { alias, .. } => {
                                        Some(alias.value.as_str().into())
                                    }
                                    SelectItem::QualifiedWildcard(name) => {
                                        return Err(CodegenError::UnsupportedStatementForm(
                                            "Qualified wildcards are not supported yet",
                                            name.to_string(),
                                        ))
                                    }
                                    SelectItem::Wildcard => None,
                                },
                            })
                        }

                        if select.distinct {
                            return Err(CodegenError::UnsupportedStatementForm(
                                "DISTINCT is not supported yet",
                                select.to_string(),
                            ));
                        }
                    }
                }
                SetExpr::Values(exprs) => {
                    if exprs.0.len() == 1 && exprs.0[0].len() == 1 {
                        let expr: Expr = exprs.0[0][0].clone().try_into()?;
                        instrs.push(Instruction::Expr {
                            index: table_reg_index,
                            expr,
                        });
                    } else {
                        // TODO: selecting multiple values.
                        //       the problem here is creating a temp table
                        //       without information about column names
                        //       and (more importantly) types.
                        return Err(CodegenError::UnsupportedStatementForm(
                            "Selecting more than one value is not supported yet",
                            exprs.to_string(),
                        ));
                    }
                }
                SetExpr::Query(query) => {
                    // TODO: figure out what syntax this corresponds to
                    //       and implement it if necessary
                    return Err(CodegenError::UnsupportedStatementForm(
                        "Query within a query not supported yet",
                        query.to_string(),
                    ));
                }
                SetExpr::SetOperation {
                    op: _,
                    all: _,
                    left: _,
                    right: _,
                } => {
                    // TODO: set operations
                    return Err(CodegenError::UnsupportedStatementForm(
                        "Set operations are not supported yet",
                        query.to_string(),
                    ));
                }
                SetExpr::Insert(insert) => {
                    // TODO: figure out what syntax this corresponds to
                    //       and implement it if necessary
                    return Err(CodegenError::UnsupportedStatementForm(
                        "Insert within query not supported yet",
                        insert.to_string(),
                    ));
                }
            };

            if let Some(limit) = query.limit.clone() {
                if let ast::Expr::Value(val) = limit.clone() {
                    if let Value::Int64(limit) = val.clone().try_into()? {
                        instrs.push(Instruction::Limit {
                            index: table_reg_index,
                            limit: limit as u64,
                        });
                    } else {
                        // TODO: what are non constant limits anyway?
                        return Err(CodegenError::Expr(ExprError::Value(ValueError {
                            reason: "Only constant integer LIMITs are supported",
                            value: val,
                        })));
                    }
                } else {
                    // TODO: what are non constant limits anyway?
                    return Err(CodegenError::Expr(ExprError::Expr {
                        reason: "Only constant integer LIMITs are supported",
                        expr: limit,
                    }));
                }
            }

            for order_by in query.order_by.clone() {
                instrs.push(Instruction::Order {
                    index: table_reg_index,
                    expr: order_by.expr.try_into()?,
                    ascending: order_by.asc.unwrap_or(true),
                });
                // TODO: support NULLS FIRST/NULLS LAST
            }

            instrs.push(Instruction::Return {
                index: table_reg_index,
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
    UnsupportedStatementForm(&'static str, String),
    InvalidIdentifier(IdentifierError),
    Expr(ExprError),
}

impl Display for CodegenError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            CodegenError::UnsupportedStatement(s) => write!(f, "Unsupported statement: {}", s),
            CodegenError::InvalidIdentifier(i) => {
                write!(f, "{}", i)
            }
            CodegenError::UnsupportedStatementForm(reason, statement) => {
                write!(f, "Unsupported: {} (Got: '{}')", reason, statement)
            }
            CodegenError::Expr(e) => write!(f, "{}", e),
        }
    }
}

impl From<IdentifierError> for CodegenError {
    fn from(i: IdentifierError) -> Self {
        CodegenError::InvalidIdentifier(i)
    }
}

impl From<ExprError> for CodegenError {
    fn from(e: ExprError) -> Self {
        CodegenError::Expr(e)
    }
}

impl From<ValueError> for CodegenError {
    fn from(v: ValueError) -> Self {
        CodegenError::Expr(v.into())
    }
}

impl Error for CodegenError {}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{ColumnOption, ColumnOptionDef, DataType};

    use pretty_assertions::assert_eq;

    use crate::{
        codegen::codegen,
        expr::Expr,
        ic::Instruction,
        identifier::{SchemaRef, TableRef},
        parser::parse,
        value::Value,
        vm::RegisterIndex,
    };

    fn check_single_statement(query: &str, callback: impl Fn(&[Instruction])) {
        let parsed = parse(query).unwrap();
        assert_eq!(parsed.len(), 1);

        let statement = &parsed[0];
        let ic = codegen(&statement).unwrap();
        callback(ic.instrs.as_slice());
    }

    #[test]
    fn create_schema() {
        check_single_statement("CREATE SCHEMA abc", |instrs| {
            assert_eq!(
                instrs,
                &[Instruction::NewSchema {
                    schema_name: SchemaRef("abc".into()),
                    exists_ok: false,
                }]
            )
        });

        check_single_statement("CREATE SCHEMA IF NOT EXISTS abcd", |instrs| {
            assert_eq!(
                instrs,
                &[Instruction::NewSchema {
                    schema_name: SchemaRef("abcd".into()),
                    exists_ok: true,
                }]
            )
        });
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
                assert_eq!(
                    instrs,
                    &[
                        Instruction::Empty {
                            index: RegisterIndex::default()
                        },
                        Instruction::ColumnDef {
                            index: RegisterIndex::default().next_index(),
                            name: "col1".into(),
                            data_type: DataType::Int(None),
                        },
                        Instruction::AddColumnOption {
                            index: RegisterIndex::default().next_index(),
                            option: ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Unique { is_primary: true }
                            }
                        },
                        Instruction::AddColumnOption {
                            index: RegisterIndex::default().next_index(),
                            option: ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull
                            }
                        },
                        Instruction::AddColumn {
                            table_reg_index: RegisterIndex::default(),
                            col_index: RegisterIndex::default().next_index(),
                        },
                        Instruction::ColumnDef {
                            index: RegisterIndex::default().next_index(),
                            name: "col2".into(),
                            data_type: DataType::String,
                        },
                        Instruction::AddColumnOption {
                            index: RegisterIndex::default().next_index(),
                            option: ColumnOptionDef {
                                name: None,
                                option: ColumnOption::NotNull
                            }
                        },
                        Instruction::AddColumn {
                            table_reg_index: RegisterIndex::default(),
                            col_index: RegisterIndex::default().next_index(),
                        },
                        Instruction::ColumnDef {
                            index: RegisterIndex::default().next_index(),
                            name: "col3".into(),
                            data_type: DataType::Int(None),
                        },
                        Instruction::AddColumnOption {
                            index: RegisterIndex::default().next_index(),
                            option: ColumnOptionDef {
                                name: None,
                                option: ColumnOption::Unique { is_primary: false }
                            }
                        },
                        Instruction::AddColumn {
                            table_reg_index: RegisterIndex::default(),
                            col_index: RegisterIndex::default().next_index(),
                        },
                        Instruction::NewTable {
                            index: RegisterIndex::default(),
                            name: TableRef {
                                schema_name: None,
                                table_name: "table1".into()
                            },
                            exists_ok: true,
                        }
                    ]
                )
            },
        );
    }

    #[test]
    fn insert_values() {
        check_single_statement(
            "
            INSERT INTO table1 VALUES
                (2, 'bar'),
                (3, 'baz')
            ",
            |instrs| {
                assert_eq!(
                    instrs,
                    &[
                        Instruction::Source {
                            index: RegisterIndex::default(),
                            name: TableRef {
                                schema_name: None,
                                table_name: "table1".into()
                            }
                        },
                        Instruction::InsertDef {
                            table_reg_index: RegisterIndex::default(),
                            index: RegisterIndex::default().next_index(),
                        },
                        Instruction::RowDef {
                            insert_index: RegisterIndex::default().next_index(),
                            row_index: RegisterIndex::default().next_index().next_index(),
                        },
                        Instruction::AddValue {
                            row_index: RegisterIndex::default().next_index().next_index(),
                            expr: Expr::Value(Value::Int64(2)),
                        },
                        Instruction::AddValue {
                            row_index: RegisterIndex::default().next_index().next_index(),
                            expr: Expr::Value(Value::String("bar".to_owned())),
                        },
                        Instruction::RowDef {
                            insert_index: RegisterIndex::default().next_index(),
                            row_index: RegisterIndex::default()
                                .next_index()
                                .next_index()
                                .next_index(),
                        },
                        Instruction::AddValue {
                            row_index: RegisterIndex::default()
                                .next_index()
                                .next_index()
                                .next_index(),
                            expr: Expr::Value(Value::Int64(3)),
                        },
                        Instruction::AddValue {
                            row_index: RegisterIndex::default()
                                .next_index()
                                .next_index()
                                .next_index(),
                            expr: Expr::Value(Value::String("baz".to_owned())),
                        },
                        Instruction::Insert {
                            index: RegisterIndex::default().next_index()
                        },
                    ]
                )
            },
        );

        check_single_statement(
            "
            INSERT INTO table1 (col1, col2) VALUES
                (2, 'bar'),
                (3, 'baz')
            ",
            |instrs| {
                assert_eq!(
                    instrs,
                    &[
                        Instruction::Source {
                            index: RegisterIndex::default(),
                            name: TableRef {
                                schema_name: None,
                                table_name: "table1".into()
                            }
                        },
                        Instruction::InsertDef {
                            table_reg_index: RegisterIndex::default(),
                            index: RegisterIndex::default().next_index(),
                        },
                        Instruction::ColumnInsertDef {
                            insert_index: RegisterIndex::default().next_index(),
                            col_name: "col1".into(),
                        },
                        Instruction::ColumnInsertDef {
                            insert_index: RegisterIndex::default().next_index(),
                            col_name: "col2".into(),
                        },
                        Instruction::RowDef {
                            insert_index: RegisterIndex::default().next_index(),
                            row_index: RegisterIndex::default().next_index().next_index(),
                        },
                        Instruction::AddValue {
                            row_index: RegisterIndex::default().next_index().next_index(),
                            expr: Expr::Value(Value::Int64(2)),
                        },
                        Instruction::AddValue {
                            row_index: RegisterIndex::default().next_index().next_index(),
                            expr: Expr::Value(Value::String("bar".to_owned())),
                        },
                        Instruction::RowDef {
                            insert_index: RegisterIndex::default().next_index(),
                            row_index: RegisterIndex::default()
                                .next_index()
                                .next_index()
                                .next_index(),
                        },
                        Instruction::AddValue {
                            row_index: RegisterIndex::default()
                                .next_index()
                                .next_index()
                                .next_index(),
                            expr: Expr::Value(Value::Int64(3)),
                        },
                        Instruction::AddValue {
                            row_index: RegisterIndex::default()
                                .next_index()
                                .next_index()
                                .next_index(),
                            expr: Expr::Value(Value::String("baz".to_owned())),
                        },
                        Instruction::Insert {
                            index: RegisterIndex::default().next_index()
                        },
                    ]
                )
            },
        );
    }

    #[test]
    fn select() {
        check_single_statement("SELECT 1", |instrs| {
            assert_eq!(
                instrs,
                &[
                    Instruction::Empty {
                        index: RegisterIndex::default()
                    },
                    Instruction::Empty {
                        index: RegisterIndex::default().next_index()
                    },
                    Instruction::Project {
                        input: RegisterIndex::default(),
                        output: RegisterIndex::default().next_index(),
                        expr: Expr::Value(Value::Int64(1)),
                        alias: None
                    },
                    Instruction::Return {
                        index: RegisterIndex::default().next_index()
                    },
                ]
            )
        });
    }
}
