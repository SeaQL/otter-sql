//! Intermediate code generation from the AST.
use sqlparser::{
    ast::{self, FunctionArg, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins},
    parser::ParserError,
};

use phf::{phf_set, Set};

use std::{error::Error, fmt::Display};

use crate::{
    expr::{agg::AggregateFunction, BinOp, Expr, ExprError, UnOp},
    identifier::IdentifierError,
    ir::{Instruction, IntermediateCode},
    parser::parse,
    value::{Value, ValueError},
    vm::RegisterIndex,
    BoundedString,
};

/// Represents either a parser error or a codegen error.
#[derive(Debug)]
pub enum ParserOrCodegenError {
    ParserError(ParserError),
    CodegenError(CodegenError),
}

impl Display for ParserOrCodegenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ParserError(e) => write!(f, "{}", e),
            Self::CodegenError(e) => write!(f, "{}", e),
        }
    }
}

impl Error for ParserOrCodegenError {}

impl From<ParserError> for ParserOrCodegenError {
    fn from(e: ParserError) -> Self {
        Self::ParserError(e)
    }
}

impl From<CodegenError> for ParserOrCodegenError {
    fn from(e: CodegenError) -> Self {
        Self::CodegenError(e)
    }
}

/// Generates intermediate code for the given SQL.
pub fn codegen_str(code: &str) -> Result<Vec<IntermediateCode>, ParserOrCodegenError> {
    let ast = parse(code)?;
    Ok(ast
        .into_iter()
        .map(|stmt| codegen_ast(&stmt))
        .collect::<Result<Vec<_>, CodegenError>>()?)
}

const TEMP_COL_NAME_PREFIX: &'static str = "__otter_temp_col";

/// Context passed around to any func that needs codegen.
struct CodegenContext {
    pub instrs: Vec<Instruction>,
    current_reg: RegisterIndex,
    last_temp_col_num: usize,
}

impl CodegenContext {
    pub fn new() -> Self {
        Self {
            instrs: Vec::new(),
            current_reg: RegisterIndex::default(),
            last_temp_col_num: 0,
        }
    }

    pub fn get_and_increment_reg(&mut self) -> RegisterIndex {
        let reg = self.current_reg;
        self.current_reg = self.current_reg.next_index();
        reg
    }

    pub fn last_used_reg(&self) -> RegisterIndex {
        self.current_reg
    }

    pub fn get_new_temp_col(&mut self) -> BoundedString {
        self.last_temp_col_num += 1;
        format!("{TEMP_COL_NAME_PREFIX}_{}", self.last_temp_col_num)
            .as_str()
            .into()
    }
}

impl Default for CodegenContext {
    fn default() -> Self {
        Self::new()
    }
}

static AGGREGATE_FUNCTIONS: Set<&'static str> = phf_set! {
    "count",
    "max",
    "min",
    "sum",
};

fn extract_expr_ast_from_project(projection: SelectItem) -> Option<ast::Expr> {
    match projection {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => Some(expr),
        SelectItem::QualifiedWildcard(_) => None,
        SelectItem::Wildcard => None,
    }
}

fn extract_alias_from_project(
    projection: &SelectItem,
) -> Result<Option<BoundedString>, CodegenError> {
    match projection {
        SelectItem::UnnamedExpr(_) => Ok(None),
        SelectItem::ExprWithAlias { alias, .. } => Ok(Some(alias.value.as_str().into())),
        SelectItem::QualifiedWildcard(name) => Err(CodegenError::UnsupportedStatementForm(
            "Qualified wildcards are not supported yet",
            name.to_string(),
        )),
        SelectItem::Wildcard => Ok(None),
    }
}

/// Generates intermediate code from the AST.
pub fn codegen_ast(ast: &Statement) -> Result<IntermediateCode, CodegenError> {
    let mut ctx = CodegenContext::default();

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
            let table_reg_index = ctx.get_and_increment_reg();
            ctx.instrs.push(Instruction::Empty {
                index: table_reg_index,
            });

            let col_reg_index = ctx.get_and_increment_reg();
            for col in columns {
                ctx.instrs.push(Instruction::ColumnDef {
                    index: col_reg_index,
                    name: col.name.value.as_str().into(),
                    data_type: col.data_type.clone(),
                });

                for option in col.options.iter() {
                    ctx.instrs.push(Instruction::AddColumnOption {
                        index: col_reg_index,
                        option: option.clone(),
                    });
                }

                ctx.instrs.push(Instruction::AddColumn {
                    table_reg_index,
                    col_index: col_reg_index,
                });
            }

            ctx.instrs.push(Instruction::NewTable {
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
            let table_reg_index = ctx.get_and_increment_reg();
            ctx.instrs.push(Instruction::Source {
                index: table_reg_index,
                name: table_name.0.clone().try_into()?,
            });

            let insert_reg_index = ctx.get_and_increment_reg();
            ctx.instrs.push(Instruction::InsertDef {
                table_reg_index,
                index: insert_reg_index,
            });

            for col in columns {
                ctx.instrs.push(Instruction::ColumnInsertDef {
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
                        let row_reg = ctx.get_and_increment_reg();

                        ctx.instrs.push(Instruction::RowDef {
                            insert_index: insert_reg_index,
                            row_index: row_reg,
                        });

                        for value in row {
                            let value = codegen_expr(value, &mut ctx)?;
                            ctx.instrs.push(Instruction::AddValue {
                                row_index: row_reg,
                                expr: value,
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

            ctx.instrs.push(Instruction::Insert {
                index: insert_reg_index,
            });

            Ok(())
        }
        Statement::Query(query) => {
            // TODO: support CTEs
            let mut table_reg_index = ctx.get_and_increment_reg();

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
                                } => ctx.instrs.push(Instruction::Source {
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
                        &[] => ctx.instrs.push(Instruction::NonExistent {
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
                        let expr = codegen_expr(expr, &mut ctx)?;
                        ctx.instrs.push(Instruction::Filter {
                            index: table_reg_index,
                            expr,
                        })
                    }

                    // if there are groupby + aggregations, we project all operations within an
                    // aggregation to another table first. for example, `SUM(col * col)` would be
                    // evaluated as `Project (col * col)` into `%2` and then apply the group by on
                    // `%2`.
                    // TODO: possible idea for refactor: make an intermediate representation of
                    // Projection that separates non-agg and agg projections.
                    let pre_grouped_reg_index = table_reg_index;
                    let mut agg_intermediate_cols = Vec::new();
                    if !select.projection.is_empty() {
                        let grouped_reg_index = ctx.get_and_increment_reg();
                        ctx.instrs.push(Instruction::Empty {
                            index: grouped_reg_index,
                        });

                        table_reg_index = grouped_reg_index;

                        for projection in &select.projection {
                            let alias = extract_alias_from_project(&projection)?;

                            let expr_ast = extract_expr_ast_from_project(projection.clone());

                            match expr_ast {
                                // an aggregate operation
                                Some(expr_ast) if is_expr_agg(&expr_ast) => match expr_ast {
                                    ast::Expr::Function(ref f) => {
                                        let expr = codegen_fn_arg(&expr_ast, &f.args[0], &mut ctx)?;
                                        let projected_col_name = ctx.get_new_temp_col();
                                        agg_intermediate_cols.push(projected_col_name);
                                        ctx.instrs.push(Instruction::Project {
                                            input: pre_grouped_reg_index,
                                            output: grouped_reg_index,
                                            expr,
                                            alias: Some(projected_col_name),
                                        });
                                    }
                                    _ => {}
                                },
                                // a non-aggregate operation
                                _ => {
                                    let expr = match projection {
                                        SelectItem::UnnamedExpr(ref expr) => {
                                            codegen_expr(expr.clone(), &mut ctx)?
                                        }
                                        SelectItem::ExprWithAlias { ref expr, .. } => {
                                            codegen_expr(expr.clone(), &mut ctx)?
                                        }
                                        SelectItem::QualifiedWildcard(_) => Expr::Wildcard,
                                        SelectItem::Wildcard => Expr::Wildcard,
                                    };
                                    let projection = Instruction::Project {
                                        input: pre_grouped_reg_index,
                                        output: grouped_reg_index,
                                        expr,
                                        alias,
                                    };
                                    ctx.instrs.push(projection)
                                }
                            }
                        }
                    }

                    for group_by in select.group_by.clone() {
                        let group_by = codegen_expr(group_by, &mut ctx)?;
                        let grouped_reg_index = ctx.get_and_increment_reg();
                        ctx.instrs.push(Instruction::Empty {
                            index: grouped_reg_index,
                        });
                        ctx.instrs.push(Instruction::GroupBy {
                            input: table_reg_index,
                            output: grouped_reg_index,
                            expr: group_by,
                        });
                        table_reg_index = grouped_reg_index
                    }

                    // this is only for aggregations.
                    // aggs are applied on the grouped table created by the `GroupBy` instructions
                    // generated above.
                    if !select.projection.is_empty() {
                        let has_aggs = select.projection.iter().any(|p| is_projection_agg(p));

                        if has_aggs {
                            let original_table_reg_index = table_reg_index;
                            table_reg_index = ctx.get_and_increment_reg();

                            ctx.instrs.push(Instruction::Empty {
                                index: table_reg_index,
                            });

                            let mut agg_index = 0;
                            for projection in &select.projection {
                                let alias = extract_alias_from_project(&projection)?;

                                let expr_ast = extract_expr_ast_from_project(projection.clone());

                                match expr_ast {
                                    // an aggregate operation
                                    Some(expr_ast) if is_expr_agg(&expr_ast) => match expr_ast {
                                        ast::Expr::Function(ref f) => {
                                            ctx.instrs.push(Instruction::Aggregate {
                                                input: original_table_reg_index,
                                                output: table_reg_index,
                                                func: AggregateFunction::from_name(
                                                    f.name.to_string().to_lowercase().as_str(),
                                                )?,
                                                col_name: agg_intermediate_cols[agg_index],
                                                alias,
                                            });
                                            agg_index += 1;
                                        }
                                        _ => unreachable!(
                                            "check for fn is already done. this should not happen."
                                        ),
                                    },
                                    _ => {}
                                }
                            }
                        }

                        if select.distinct {
                            return Err(CodegenError::UnsupportedStatementForm(
                                "DISTINCT is not supported yet",
                                select.to_string(),
                            ));
                        }
                    }

                    if let Some(expr) = select.having.clone() {
                        let expr = codegen_expr(expr, &mut ctx)?;
                        ctx.instrs.push(Instruction::Filter {
                            index: table_reg_index,
                            expr,
                        })
                    }
                }
                SetExpr::Values(exprs) => {
                    if exprs.0.len() == 1 && exprs.0[0].len() == 1 {
                        let expr: Expr = codegen_expr(exprs.0[0][0].clone(), &mut ctx)?;
                        ctx.instrs.push(Instruction::Expr {
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

            for order_by in query.order_by.clone() {
                let order_by_expr = codegen_expr(order_by.expr, &mut ctx)?;
                ctx.instrs.push(Instruction::Order {
                    index: table_reg_index,
                    expr: order_by_expr,
                    ascending: order_by.asc.unwrap_or(true),
                });
                // TODO: support NULLS FIRST/NULLS LAST
            }

            if let Some(limit) = query.limit.clone() {
                if let ast::Expr::Value(val) = limit.clone() {
                    if let Value::Int64(limit) = val.clone().try_into()? {
                        ctx.instrs.push(Instruction::Limit {
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

            ctx.instrs.push(Instruction::Return {
                index: table_reg_index,
            });

            Ok(())
        }
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
        } => {
            ctx.instrs.push(Instruction::NewSchema {
                schema_name: schema_name.0.clone().try_into()?,
                exists_ok: *if_not_exists,
            });
            Ok(())
        }
        _ => Err(CodegenError::UnsupportedStatement(ast.to_string())),
    }?;

    Ok(IntermediateCode { instrs: ctx.instrs })
}

fn codegen_expr(expr_ast: ast::Expr, ctx: &mut CodegenContext) -> Result<Expr, ExprError> {
    match expr_ast {
        ast::Expr::Identifier(i) => Ok(Expr::ColumnRef(vec![i].try_into()?)),
        ast::Expr::CompoundIdentifier(i) => Ok(Expr::ColumnRef(i.try_into()?)),
        ast::Expr::IsFalse(e) => Ok(Expr::Unary {
            op: UnOp::IsFalse,
            operand: Box::new(codegen_expr(*e, ctx)?),
        }),
        ast::Expr::IsTrue(e) => Ok(Expr::Unary {
            op: UnOp::IsTrue,
            operand: Box::new(codegen_expr(*e, ctx)?),
        }),
        ast::Expr::IsNull(e) => Ok(Expr::Unary {
            op: UnOp::IsNull,
            operand: Box::new(codegen_expr(*e, ctx)?),
        }),
        ast::Expr::IsNotNull(e) => Ok(Expr::Unary {
            op: UnOp::IsNotNull,
            operand: Box::new(codegen_expr(*e, ctx)?),
        }),
        ast::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let expr: Box<Expr> = Box::new(codegen_expr(*expr, ctx)?);
            let left = Box::new(codegen_expr(*low, ctx)?);
            let right = Box::new(codegen_expr(*high, ctx)?);
            let between = Expr::Binary {
                left: Box::new(Expr::Binary {
                    left,
                    op: BinOp::LessThanOrEqual,
                    right: expr.clone(),
                }),
                op: BinOp::And,
                right: Box::new(Expr::Binary {
                    left: expr,
                    op: BinOp::LessThanOrEqual,
                    right,
                }),
            };
            if negated {
                Ok(Expr::Unary {
                    op: UnOp::Not,
                    operand: Box::new(between),
                })
            } else {
                Ok(between)
            }
        }
        ast::Expr::BinaryOp { left, op, right } => Ok(Expr::Binary {
            left: Box::new(codegen_expr(*left, ctx)?),
            op: op.try_into()?,
            right: Box::new(codegen_expr(*right, ctx)?),
        }),
        ast::Expr::UnaryOp { op, expr } => Ok(Expr::Unary {
            op: op.try_into()?,
            operand: Box::new(codegen_expr(*expr, ctx)?),
        }),
        ast::Expr::Value(v) => Ok(Expr::Value(v.try_into()?)),
        ast::Expr::Function(ref f) => {
            let fn_name = f.name.to_string();
            Ok(Expr::Function {
                name: fn_name.as_str().into(),
                args: f
                    .args
                    .iter()
                    .map(|arg| codegen_fn_arg(&expr_ast, arg, ctx))
                    .collect::<Result<Vec<_>, _>>()?,
            })
        }
        _ => Err(ExprError::Expr {
            reason: "Unsupported expression",
            expr: expr_ast,
        }),
    }
}

fn is_fn_name_aggregate(fn_name: &str) -> bool {
    AGGREGATE_FUNCTIONS.contains(fn_name)
}

fn is_expr_agg(e: &ast::Expr) -> bool {
    match e {
        ast::Expr::Function(ref f) => is_fn_name_aggregate(&f.name.to_string().to_lowercase()),
        _ => false,
    }
}

fn is_projection_agg(p: &SelectItem) -> bool {
    match p {
        SelectItem::UnnamedExpr(ref expr) => is_expr_agg(expr),
        SelectItem::ExprWithAlias { ref expr, .. } => is_expr_agg(expr),
        SelectItem::QualifiedWildcard(_) => false,
        SelectItem::Wildcard => false,
    }
}

fn codegen_fn_arg(
    expr_ast: &ast::Expr,
    arg: &FunctionArg,
    ctx: &mut CodegenContext,
) -> Result<Expr, ExprError> {
    match arg {
        ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
            ast::FunctionArgExpr::Expr(e) => Ok(codegen_expr(e.clone(), ctx)?),
            ast::FunctionArgExpr::Wildcard => Ok(Expr::Wildcard),
            ast::FunctionArgExpr::QualifiedWildcard(_) => Err(ExprError::Expr {
                reason: "Qualified wildcards are not supported yet",
                expr: expr_ast.clone(),
            }),
        },
        ast::FunctionArg::Named { .. } => Err(ExprError::Expr {
            reason: "Named function arguments are not supported",
            expr: expr_ast.clone(),
        }),
    }
}

/// Error while generating an intermediate code from the AST.
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
mod codegen_tests {
    use sqlparser::ast::{ColumnOption, ColumnOptionDef, DataType};

    use pretty_assertions::assert_eq;

    use crate::{
        codegen::codegen_ast,
        expr::{agg::AggregateFunction, BinOp, Expr},
        identifier::{ColumnRef, SchemaRef, TableRef},
        ir::Instruction,
        parser::parse,
        value::Value,
        vm::RegisterIndex,
    };

    fn check_single_statement(query: &str, callback: impl Fn(&[Instruction])) {
        let parsed = parse(query).unwrap();
        assert_eq!(parsed.len(), 1);

        let statement = &parsed[0];
        let ic = codegen_ast(&statement).unwrap();
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
                    Instruction::NonExistent {
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

        check_single_statement("SELECT * FROM table1", |instrs| {
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
                    Instruction::Empty {
                        index: RegisterIndex::default().next_index()
                    },
                    Instruction::Project {
                        input: RegisterIndex::default(),
                        output: RegisterIndex::default().next_index(),
                        expr: Expr::Wildcard,
                        alias: None
                    },
                    Instruction::Return {
                        index: RegisterIndex::default().next_index(),
                    }
                ]
            )
        });

        check_single_statement("SELECT * FROM table1 WHERE col1 = 1", |instrs| {
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
                    Instruction::Filter {
                        index: RegisterIndex::default(),
                        expr: Expr::Binary {
                            left: Box::new(Expr::ColumnRef(ColumnRef {
                                schema_name: None,
                                table_name: None,
                                col_name: "col1".into(),
                            },)),
                            op: BinOp::Equal,
                            right: Box::new(Expr::Value(Value::Int64(1)))
                        },
                    },
                    Instruction::Empty {
                        index: RegisterIndex::default().next_index()
                    },
                    Instruction::Project {
                        input: RegisterIndex::default(),
                        output: RegisterIndex::default().next_index(),
                        expr: Expr::Wildcard,
                        alias: None
                    },
                    Instruction::Return {
                        index: RegisterIndex::default().next_index(),
                    }
                ]
            )
        });

        check_single_statement(
            "SELECT col2, col3
            FROM table1
            WHERE col1 = 1
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
                        Instruction::Filter {
                            index: RegisterIndex::default(),
                            expr: Expr::Binary {
                                left: Box::new(Expr::ColumnRef(ColumnRef {
                                    schema_name: None,
                                    table_name: None,
                                    col_name: "col1".into(),
                                },)),
                                op: BinOp::Equal,
                                right: Box::new(Expr::Value(Value::Int64(1)))
                            },
                        },
                        Instruction::Empty {
                            index: RegisterIndex::default().next_index()
                        },
                        Instruction::Project {
                            input: RegisterIndex::default(),
                            output: RegisterIndex::default().next_index(),
                            expr: Expr::ColumnRef(ColumnRef {
                                schema_name: None,
                                table_name: None,
                                col_name: "col2".into(),
                            }),
                            alias: None
                        },
                        Instruction::Project {
                            input: RegisterIndex::default(),
                            output: RegisterIndex::default().next_index(),
                            expr: Expr::ColumnRef(ColumnRef {
                                schema_name: None,
                                table_name: None,
                                col_name: "col3".into(),
                            }),
                            alias: None
                        },
                        Instruction::Return {
                            index: RegisterIndex::default().next_index(),
                        }
                    ]
                )
            },
        );

        check_single_statement(
            "SELECT col2, col3
            FROM table1
            WHERE col1 = 1 AND col2 = 2
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
                        Instruction::Filter {
                            index: RegisterIndex::default(),
                            expr: Expr::Binary {
                                left: Box::new(Expr::Binary {
                                    left: Box::new(Expr::ColumnRef(ColumnRef {
                                        schema_name: None,
                                        table_name: None,
                                        col_name: "col1".into(),
                                    },)),
                                    op: BinOp::Equal,
                                    right: Box::new(Expr::Value(Value::Int64(1)))
                                }),
                                op: BinOp::And,
                                right: Box::new(Expr::Binary {
                                    left: Box::new(Expr::ColumnRef(ColumnRef {
                                        schema_name: None,
                                        table_name: None,
                                        col_name: "col2".into(),
                                    },)),
                                    op: BinOp::Equal,
                                    right: Box::new(Expr::Value(Value::Int64(2)))
                                })
                            },
                        },
                        Instruction::Empty {
                            index: RegisterIndex::default().next_index()
                        },
                        Instruction::Project {
                            input: RegisterIndex::default(),
                            output: RegisterIndex::default().next_index(),
                            expr: Expr::ColumnRef(ColumnRef {
                                schema_name: None,
                                table_name: None,
                                col_name: "col2".into(),
                            }),
                            alias: None
                        },
                        Instruction::Project {
                            input: RegisterIndex::default(),
                            output: RegisterIndex::default().next_index(),
                            expr: Expr::ColumnRef(ColumnRef {
                                schema_name: None,
                                table_name: None,
                                col_name: "col3".into(),
                            }),
                            alias: None
                        },
                        Instruction::Return {
                            index: RegisterIndex::default().next_index(),
                        }
                    ]
                )
            },
        );

        check_single_statement(
            "SELECT col2, col3
            FROM table1
            WHERE col1 = 1 OR col2 = 2
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
                        Instruction::Filter {
                            index: RegisterIndex::default(),
                            expr: Expr::Binary {
                                left: Box::new(Expr::Binary {
                                    left: Box::new(Expr::ColumnRef(ColumnRef {
                                        schema_name: None,
                                        table_name: None,
                                        col_name: "col1".into(),
                                    },)),
                                    op: BinOp::Equal,
                                    right: Box::new(Expr::Value(Value::Int64(1)))
                                }),
                                op: BinOp::Or,
                                right: Box::new(Expr::Binary {
                                    left: Box::new(Expr::ColumnRef(ColumnRef {
                                        schema_name: None,
                                        table_name: None,
                                        col_name: "col2".into(),
                                    },)),
                                    op: BinOp::Equal,
                                    right: Box::new(Expr::Value(Value::Int64(2)))
                                })
                            },
                        },
                        Instruction::Empty {
                            index: RegisterIndex::default().next_index()
                        },
                        Instruction::Project {
                            input: RegisterIndex::default(),
                            output: RegisterIndex::default().next_index(),
                            expr: Expr::ColumnRef(ColumnRef {
                                schema_name: None,
                                table_name: None,
                                col_name: "col2".into(),
                            }),
                            alias: None
                        },
                        Instruction::Project {
                            input: RegisterIndex::default(),
                            output: RegisterIndex::default().next_index(),
                            expr: Expr::ColumnRef(ColumnRef {
                                schema_name: None,
                                table_name: None,
                                col_name: "col3".into(),
                            }),
                            alias: None
                        },
                        Instruction::Return {
                            index: RegisterIndex::default().next_index(),
                        }
                    ]
                )
            },
        );

        check_single_statement(
            "SELECT col2, col3
            FROM table1
            WHERE col1 = 1
            ORDER BY col2
            LIMIT 100
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
                        Instruction::Filter {
                            index: RegisterIndex::default(),
                            expr: Expr::Binary {
                                left: Box::new(Expr::ColumnRef(ColumnRef {
                                    schema_name: None,
                                    table_name: None,
                                    col_name: "col1".into(),
                                },)),
                                op: BinOp::Equal,
                                right: Box::new(Expr::Value(Value::Int64(1)))
                            },
                        },
                        Instruction::Empty {
                            index: RegisterIndex::default().next_index()
                        },
                        Instruction::Project {
                            input: RegisterIndex::default(),
                            output: RegisterIndex::default().next_index(),
                            expr: Expr::ColumnRef(ColumnRef {
                                schema_name: None,
                                table_name: None,
                                col_name: "col2".into(),
                            }),
                            alias: None
                        },
                        Instruction::Project {
                            input: RegisterIndex::default(),
                            output: RegisterIndex::default().next_index(),
                            expr: Expr::ColumnRef(ColumnRef {
                                schema_name: None,
                                table_name: None,
                                col_name: "col3".into(),
                            }),
                            alias: None
                        },
                        Instruction::Order {
                            index: RegisterIndex::default().next_index(),
                            expr: Expr::ColumnRef(ColumnRef {
                                schema_name: None,
                                table_name: None,
                                col_name: "col2".into(),
                            }),
                            ascending: true
                        },
                        Instruction::Limit {
                            index: RegisterIndex::default().next_index(),
                            limit: 100,
                        },
                        Instruction::Return {
                            index: RegisterIndex::default().next_index(),
                        }
                    ]
                )
            },
        );

        check_single_statement(
            "SELECT col2, MAX(col3) AS max_col3
            FROM table1
            WHERE col1 = 1
            GROUP BY col2
            HAVING max_col3 > 10
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
                        Instruction::Filter {
                            index: RegisterIndex::default(),
                            expr: Expr::Binary {
                                left: Box::new(Expr::ColumnRef(ColumnRef {
                                    schema_name: None,
                                    table_name: None,
                                    col_name: "col1".into(),
                                },)),
                                op: BinOp::Equal,
                                right: Box::new(Expr::Value(Value::Int64(1)))
                            },
                        },
                        Instruction::Empty {
                            index: RegisterIndex::default().next_index()
                        },
                        Instruction::Project {
                            input: RegisterIndex::default(),
                            output: RegisterIndex::default().next_index(),
                            expr: Expr::ColumnRef(ColumnRef {
                                schema_name: None,
                                table_name: None,
                                col_name: "col2".into(),
                            }),
                            alias: None
                        },
                        Instruction::Project {
                            input: RegisterIndex::default(),
                            output: RegisterIndex::default().next_index(),
                            expr: Expr::ColumnRef(ColumnRef {
                                schema_name: None,
                                table_name: None,
                                col_name: "col3".into(),
                            }),
                            alias: Some("__otter_temp_col_1".into())
                        },
                        Instruction::Empty {
                            index: RegisterIndex::default().next_index().next_index()
                        },
                        Instruction::GroupBy {
                            input: RegisterIndex::default().next_index(),
                            output: RegisterIndex::default().next_index().next_index(),
                            expr: Expr::ColumnRef(ColumnRef {
                                schema_name: None,
                                table_name: None,
                                col_name: "col2".into(),
                            })
                        },
                        Instruction::Empty {
                            index: RegisterIndex::default()
                                .next_index()
                                .next_index()
                                .next_index()
                        },
                        Instruction::Aggregate {
                            input: RegisterIndex::default().next_index().next_index(),
                            output: RegisterIndex::default()
                                .next_index()
                                .next_index()
                                .next_index(),
                            func: AggregateFunction::Max,
                            col_name: "__otter_temp_col_1".into(),
                            alias: Some("max_col3".into()),
                        },
                        Instruction::Filter {
                            index: RegisterIndex::default()
                                .next_index()
                                .next_index()
                                .next_index(),
                            expr: Expr::Binary {
                                left: Box::new(Expr::ColumnRef(ColumnRef {
                                    schema_name: None,
                                    table_name: None,
                                    col_name: "max_col3".into(),
                                })),
                                op: BinOp::GreaterThan,
                                right: Box::new(Expr::Value(Value::Int64(10)))
                            },
                        },
                        Instruction::Return {
                            index: RegisterIndex::default()
                                .next_index()
                                .next_index()
                                .next_index(),
                        }
                    ]
                )
            },
        );

        check_single_statement(
            "SELECT col2, col3, SUM(col4 * col4) AS sos
            FROM table1
            WHERE col1 = 1
            GROUP BY col2, col3
            ",
            |instrs| {
                assert_eq!(
                    &[
                        Instruction::Source {
                            index: RegisterIndex::default(),
                            name: TableRef {
                                schema_name: None,
                                table_name: "table1".into()
                            }
                        },
                        Instruction::Filter {
                            index: RegisterIndex::default(),
                            expr: Expr::Binary {
                                left: Box::new(Expr::ColumnRef(ColumnRef {
                                    schema_name: None,
                                    table_name: None,
                                    col_name: "col1".into(),
                                },)),
                                op: BinOp::Equal,
                                right: Box::new(Expr::Value(Value::Int64(1)))
                            },
                        },
                        Instruction::Empty {
                            index: RegisterIndex::default().next_index()
                        },
                        Instruction::Project {
                            input: RegisterIndex::default(),
                            output: RegisterIndex::default().next_index(),
                            expr: Expr::ColumnRef(ColumnRef {
                                schema_name: None,
                                table_name: None,
                                col_name: "col2".into(),
                            }),
                            alias: None
                        },
                        Instruction::Project {
                            input: RegisterIndex::default(),
                            output: RegisterIndex::default().next_index(),
                            expr: Expr::ColumnRef(ColumnRef {
                                schema_name: None,
                                table_name: None,
                                col_name: "col3".into(),
                            }),
                            alias: None
                        },
                        Instruction::Project {
                            input: RegisterIndex::default(),
                            output: RegisterIndex::default().next_index(),
                            expr: Expr::Binary {
                                left: Box::new(Expr::ColumnRef(ColumnRef {
                                    schema_name: None,
                                    table_name: None,
                                    col_name: "col4".into(),
                                })),
                                op: BinOp::Multiply,
                                right: Box::new(Expr::ColumnRef(ColumnRef {
                                    schema_name: None,
                                    table_name: None,
                                    col_name: "col4".into(),
                                }))
                            },
                            alias: Some("__otter_temp_col_1".into())
                        },
                        Instruction::Empty {
                            index: RegisterIndex::default().next_index().next_index()
                        },
                        Instruction::GroupBy {
                            input: RegisterIndex::default().next_index(),
                            output: RegisterIndex::default().next_index().next_index(),
                            expr: Expr::ColumnRef(ColumnRef {
                                schema_name: None,
                                table_name: None,
                                col_name: "col2".into(),
                            })
                        },
                        Instruction::Empty {
                            index: RegisterIndex::default()
                                .next_index()
                                .next_index()
                                .next_index()
                        },
                        Instruction::GroupBy {
                            input: RegisterIndex::default().next_index().next_index(),
                            output: RegisterIndex::default()
                                .next_index()
                                .next_index()
                                .next_index(),
                            expr: Expr::ColumnRef(ColumnRef {
                                schema_name: None,
                                table_name: None,
                                col_name: "col3".into(),
                            })
                        },
                        Instruction::Empty {
                            index: RegisterIndex::default()
                                .next_index()
                                .next_index()
                                .next_index()
                                .next_index()
                        },
                        Instruction::Aggregate {
                            input: RegisterIndex::default()
                                .next_index()
                                .next_index()
                                .next_index(),
                            output: RegisterIndex::default()
                                .next_index()
                                .next_index()
                                .next_index()
                                .next_index(),
                            func: AggregateFunction::Sum,
                            col_name: "__otter_temp_col_1".into(),
                            alias: Some("sos".into()),
                        },
                        Instruction::Return {
                            index: RegisterIndex::default()
                                .next_index()
                                .next_index()
                                .next_index()
                                .next_index(),
                        }
                    ],
                    instrs
                )
            },
        );
    }
}

#[cfg(test)]
mod expr_codegen_tests {
    use sqlparser::{ast, dialect::GenericDialect, parser::Parser, tokenizer::Tokenizer};

    use crate::{
        codegen::{codegen_expr, CodegenContext},
        expr::{BinOp, Expr, ExprError, UnOp},
        identifier::ColumnRef,
        value::Value,
    };

    #[test]
    fn conversion_from_ast() {
        fn parse_expr(s: &str) -> ast::Expr {
            let dialect = GenericDialect {};
            let mut tokenizer = Tokenizer::new(&dialect, s);
            let tokens = tokenizer.tokenize().unwrap();
            let mut parser = Parser::new(tokens, &dialect);
            parser.parse_expr().unwrap()
        }

        fn codegen_expr_wrapper(expr_ast: ast::Expr) -> Result<Expr, ExprError> {
            let mut ctx = CodegenContext::new();
            codegen_expr(expr_ast, &mut ctx)
        }

        assert_eq!(
            codegen_expr_wrapper(parse_expr("abc")),
            Ok(Expr::ColumnRef(ColumnRef {
                schema_name: None,
                table_name: None,
                col_name: "abc".into()
            }))
        );

        assert_ne!(
            codegen_expr_wrapper(parse_expr("abc")),
            Ok(Expr::ColumnRef(ColumnRef {
                schema_name: None,
                table_name: None,
                col_name: "cab".into()
            }))
        );

        assert_eq!(
            codegen_expr_wrapper(parse_expr("table1.col1")),
            Ok(Expr::ColumnRef(ColumnRef {
                schema_name: None,
                table_name: Some("table1".into()),
                col_name: "col1".into()
            }))
        );

        assert_eq!(
            codegen_expr_wrapper(parse_expr("schema1.table1.col1")),
            Ok(Expr::ColumnRef(ColumnRef {
                schema_name: Some("schema1".into()),
                table_name: Some("table1".into()),
                col_name: "col1".into()
            }))
        );

        assert_eq!(
            codegen_expr_wrapper(parse_expr("5 IS NULL")),
            Ok(Expr::Unary {
                op: UnOp::IsNull,
                operand: Box::new(Expr::Value(Value::Int64(5)))
            })
        );

        assert_eq!(
            codegen_expr_wrapper(parse_expr("1 IS TRUE")),
            Ok(Expr::Unary {
                op: UnOp::IsTrue,
                operand: Box::new(Expr::Value(Value::Int64(1)))
            })
        );

        assert_eq!(
            codegen_expr_wrapper(parse_expr("4 BETWEEN 3 AND 5")),
            Ok(Expr::Binary {
                left: Box::new(Expr::Binary {
                    left: Box::new(Expr::Value(Value::Int64(3))),
                    op: BinOp::LessThanOrEqual,
                    right: Box::new(Expr::Value(Value::Int64(4)))
                }),
                op: BinOp::And,
                right: Box::new(Expr::Binary {
                    left: Box::new(Expr::Value(Value::Int64(4))),
                    op: BinOp::LessThanOrEqual,
                    right: Box::new(Expr::Value(Value::Int64(5)))
                })
            })
        );

        assert_eq!(
            codegen_expr_wrapper(parse_expr("4 NOT BETWEEN 3 AND 5")),
            Ok(Expr::Unary {
                op: UnOp::Not,
                operand: Box::new(Expr::Binary {
                    left: Box::new(Expr::Binary {
                        left: Box::new(Expr::Value(Value::Int64(3))),
                        op: BinOp::LessThanOrEqual,
                        right: Box::new(Expr::Value(Value::Int64(4)))
                    }),
                    op: BinOp::And,
                    right: Box::new(Expr::Binary {
                        left: Box::new(Expr::Value(Value::Int64(4))),
                        op: BinOp::LessThanOrEqual,
                        right: Box::new(Expr::Value(Value::Int64(5)))
                    })
                })
            })
        );

        assert_eq!(
            codegen_expr_wrapper(parse_expr("MAX(col1)")),
            Ok(Expr::Function {
                name: "MAX".into(),
                args: vec![Expr::ColumnRef(ColumnRef {
                    schema_name: None,
                    table_name: None,
                    col_name: "col1".into()
                })]
            })
        );

        assert_eq!(
            codegen_expr_wrapper(parse_expr("some_func(col1, 1, 'abc')")),
            Ok(Expr::Function {
                name: "some_func".into(),
                args: vec![
                    Expr::ColumnRef(ColumnRef {
                        schema_name: None,
                        table_name: None,
                        col_name: "col1".into()
                    }),
                    Expr::Value(Value::Int64(1)),
                    Expr::Value(Value::String("abc".to_owned()))
                ]
            })
        );

        assert_eq!(
            codegen_expr_wrapper(parse_expr("COUNT(*)")),
            Ok(Expr::Function {
                name: "COUNT".into(),
                args: vec![Expr::Wildcard]
            })
        );
    }
}

#[cfg(test)]
mod expr_eval_tests {
    use sqlparser::{
        ast::{ColumnOption, ColumnOptionDef, DataType},
        dialect::GenericDialect,
        parser::Parser,
        tokenizer::Tokenizer,
    };

    use crate::{
        column::Column,
        expr::{eval::ExprExecError, BinOp, Expr, UnOp},
        table::{Row, Table},
        value::{Value, ValueBinaryOpError, ValueUnaryOpError},
    };

    use super::{codegen_expr, CodegenContext};

    fn str_to_expr(s: &str) -> Expr {
        let dialect = GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, s);
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(tokens, &dialect);
        let mut ctx = CodegenContext::new();
        codegen_expr(parser.parse_expr().unwrap(), &mut ctx).unwrap()
    }

    fn exec_expr_no_context(expr: Expr) -> Result<Value, ExprExecError> {
        let mut table = Table::new_temp(0);
        table.new_row(vec![]);
        Expr::execute(&expr, &table, table.all_data()[0].to_shared())
    }

    fn exec_str_no_context(s: &str) -> Result<Value, ExprExecError> {
        let expr = str_to_expr(s);
        exec_expr_no_context(expr)
    }

    fn exec_str_with_context(s: &str, table: &Table, row: &Row) -> Result<Value, ExprExecError> {
        let expr = str_to_expr(s);
        Expr::execute(&expr, table, row.to_shared())
    }

    #[test]
    fn exec_value() {
        assert_eq!(exec_str_no_context("NULL"), Ok(Value::Null));

        assert_eq!(exec_str_no_context("true"), Ok(Value::Bool(true)));

        assert_eq!(exec_str_no_context("1"), Ok(Value::Int64(1)));

        assert_eq!(exec_str_no_context("1.1"), Ok(Value::Float64(1.1.into())));

        assert_eq!(exec_str_no_context(".1"), Ok(Value::Float64(0.1.into())));

        assert_eq!(
            exec_str_no_context("'str'"),
            Ok(Value::String("str".to_owned()))
        );
    }

    #[test]
    fn exec_logical() {
        assert_eq!(exec_str_no_context("true and true"), Ok(Value::Bool(true)));
        assert_eq!(
            exec_str_no_context("true and false"),
            Ok(Value::Bool(false))
        );
        assert_eq!(
            exec_str_no_context("false and true"),
            Ok(Value::Bool(false))
        );
        assert_eq!(
            exec_str_no_context("false and false"),
            Ok(Value::Bool(false))
        );
        assert_eq!(
            exec_str_no_context("false and 10"),
            Err(ValueBinaryOpError {
                operator: BinOp::And,
                values: (Value::Bool(false), Value::Int64(10))
            }
            .into())
        );
        assert_eq!(
            exec_str_no_context("10 and false"),
            Err(ValueBinaryOpError {
                operator: BinOp::And,
                values: (Value::Int64(10), Value::Bool(false))
            }
            .into())
        );

        assert_eq!(exec_str_no_context("true or true"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("true or false"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("false or true"), Ok(Value::Bool(true)));
        assert_eq!(
            exec_str_no_context("false or false"),
            Ok(Value::Bool(false))
        );
        assert_eq!(
            exec_str_no_context("true or 10"),
            Err(ValueBinaryOpError {
                operator: BinOp::Or,
                values: (Value::Bool(true), Value::Int64(10))
            }
            .into())
        );
        assert_eq!(
            exec_str_no_context("10 or true"),
            Err(ValueBinaryOpError {
                operator: BinOp::Or,
                values: (Value::Int64(10), Value::Bool(true))
            }
            .into())
        );
    }

    #[test]
    fn exec_arithmetic() {
        assert_eq!(exec_str_no_context("1 + 1"), Ok(Value::Int64(2)));
        assert_eq!(
            exec_str_no_context("1.1 + 1.1"),
            Ok(Value::Float64(2.2.into()))
        );

        // this applies to all binary ops
        assert_eq!(
            exec_str_no_context("1 + 1.1"),
            Err(ValueBinaryOpError {
                operator: BinOp::Plus,
                values: (Value::Int64(1), Value::Float64(1.1.into()))
            }
            .into())
        );

        assert_eq!(exec_str_no_context("4 - 2"), Ok(Value::Int64(2)));
        assert_eq!(exec_str_no_context("4 - 6"), Ok(Value::Int64(-2)));
        assert_eq!(
            exec_str_no_context("4.5 - 2.2"),
            Ok(Value::Float64(2.3.into()))
        );

        assert_eq!(exec_str_no_context("4 * 2"), Ok(Value::Int64(8)));
        assert_eq!(
            exec_str_no_context("0.5 * 2.2"),
            Ok(Value::Float64(1.1.into()))
        );

        assert_eq!(exec_str_no_context("4 / 2"), Ok(Value::Int64(2)));
        assert_eq!(exec_str_no_context("4 / 3"), Ok(Value::Int64(1)));
        assert_eq!(
            exec_str_no_context("4.0 / 2.0"),
            Ok(Value::Float64(2.0.into()))
        );
        assert_eq!(
            exec_str_no_context("5.1 / 2.5"),
            Ok(Value::Float64(2.04.into()))
        );

        assert_eq!(exec_str_no_context("5 % 2"), Ok(Value::Int64(1)));
        assert_eq!(
            exec_str_no_context("5.5 % 2.5"),
            Ok(Value::Float64(0.5.into()))
        );
    }

    #[test]
    fn exec_comparison() {
        assert_eq!(exec_str_no_context("1 = 1"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("1 = 2"), Ok(Value::Bool(false)));
        assert_eq!(exec_str_no_context("1 != 2"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("1.1 = 1.1"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("1.2 = 1.22"), Ok(Value::Bool(false)));
        assert_eq!(exec_str_no_context("1.2 != 1.22"), Ok(Value::Bool(true)));

        assert_eq!(exec_str_no_context("1 < 2"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("1 < 1"), Ok(Value::Bool(false)));
        assert_eq!(exec_str_no_context("1 <= 2"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("1 <= 1"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("3 > 2"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("3 > 3"), Ok(Value::Bool(false)));
        assert_eq!(exec_str_no_context("3 >= 2"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("3 >= 3"), Ok(Value::Bool(true)));
    }

    #[test]
    fn exec_pattern_match() {
        assert_eq!(
            exec_str_no_context("'my name is yoshikage kira' LIKE 'kira'"),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            exec_str_no_context("'my name is yoshikage kira' LIKE 'KIRA'"),
            Ok(Value::Bool(false))
        );
        assert_eq!(
            exec_str_no_context("'my name is yoshikage kira' LIKE 'kira yoshikage'"),
            Ok(Value::Bool(false))
        );

        assert_eq!(
            exec_str_no_context("'my name is Yoshikage Kira' ILIKE 'kira'"),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            exec_str_no_context("'my name is Yoshikage Kira' ILIKE 'KIRA'"),
            Ok(Value::Bool(true))
        );
        assert_eq!(
            exec_str_no_context("'my name is Yoshikage Kira' ILIKE 'KIRAA'"),
            Ok(Value::Bool(false))
        );
    }

    #[test]
    fn exec_unary() {
        assert_eq!(exec_str_no_context("+1"), Ok(Value::Int64(1)));
        assert_eq!(exec_str_no_context("+ -1"), Ok(Value::Int64(-1)));
        assert_eq!(exec_str_no_context("-1"), Ok(Value::Int64(-1)));
        assert_eq!(exec_str_no_context("- -1"), Ok(Value::Int64(1)));
        assert_eq!(exec_str_no_context("not true"), Ok(Value::Bool(false)));
        assert_eq!(exec_str_no_context("not false"), Ok(Value::Bool(true)));

        assert_eq!(exec_str_no_context("true is true"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("false is false"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("false is true"), Ok(Value::Bool(false)));
        assert_eq!(exec_str_no_context("true is false"), Ok(Value::Bool(false)));
        assert_eq!(
            exec_str_no_context("1 is true"),
            Err(ValueUnaryOpError {
                operator: UnOp::IsTrue,
                value: Value::Int64(1)
            }
            .into())
        );

        assert_eq!(exec_str_no_context("NULL is NULL"), Ok(Value::Bool(true)));
        assert_eq!(
            exec_str_no_context("NULL is not NULL"),
            Ok(Value::Bool(false))
        );
        assert_eq!(exec_str_no_context("1 is NULL"), Ok(Value::Bool(false)));
        assert_eq!(exec_str_no_context("1 is not NULL"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("0 is not NULL"), Ok(Value::Bool(true)));
        assert_eq!(exec_str_no_context("'' is not NULL"), Ok(Value::Bool(true)));
    }

    #[test]
    fn exec_wildcard() {
        assert_eq!(
            exec_expr_no_context(Expr::Wildcard),
            Err(ExprExecError::CannotExecute(Expr::Wildcard))
        );
    }

    #[test]
    fn exec_column_ref() {
        let mut table = Table::new(
            "table1".into(),
            vec![
                Column::new(
                    "col1".into(),
                    DataType::Int(None),
                    vec![ColumnOptionDef {
                        name: None,
                        option: ColumnOption::Unique { is_primary: true },
                    }],
                    false,
                ),
                Column::new(
                    "col2".into(),
                    DataType::Int(None),
                    vec![ColumnOptionDef {
                        name: None,
                        option: ColumnOption::Unique { is_primary: false },
                    }],
                    false,
                ),
                Column::new("col3".into(), DataType::String, vec![], false),
            ],
        );
        table.new_row(vec![
            Value::Int64(4),
            Value::Int64(10),
            Value::String("brr".to_owned()),
        ]);

        assert_eq!(
            table.all_data(),
            vec![Row::new(vec![
                Value::Int64(4),
                Value::Int64(10),
                Value::String("brr".to_owned())
            ])]
        );

        assert_eq!(
            exec_str_with_context("col1", &table, &table.all_data()[0]),
            Ok(Value::Int64(4))
        );

        assert_eq!(
            exec_str_with_context("col3", &table, &table.all_data()[0]),
            Ok(Value::String("brr".to_owned()))
        );

        assert_eq!(
            exec_str_with_context("col1 = 4", &table, &table.all_data()[0]),
            Ok(Value::Bool(true))
        );

        assert_eq!(
            exec_str_with_context("col1 + 1", &table, &table.all_data()[0]),
            Ok(Value::Int64(5))
        );

        assert_eq!(
            exec_str_with_context("col1 + col2", &table, &table.all_data()[0]),
            Ok(Value::Int64(14))
        );

        assert_eq!(
            exec_str_with_context(
                "col1 + col2 = 10 or col1 * col2 = 40",
                &table,
                &table.all_data()[0]
            ),
            Ok(Value::Bool(true))
        );
    }
}
