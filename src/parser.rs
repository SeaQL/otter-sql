/// SQL parsing.
///
/// Makes use of [`sqlparser`].

use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};

/// Parses a SQL statement.
pub fn parse(sql: &str) -> Result<Vec<Statement>, ParserError> {
    let dialect = GenericDialect {};
    Parser::parse_sql(&dialect, sql)
}

#[cfg(test)]
mod test {
    use super::parse;

    #[test]
    fn sanity_check() {
        let sql = "SELECT a, b, c FROM foo WHERE c = 10 ORDER BY a LIMIT 10";
        let ast = parse(sql);
        assert!(ast.is_ok());
        let ast = ast.unwrap();
        assert_eq!(ast.len(), 1);
        let stmt = &ast[0];
        assert_eq!(stmt.to_string(), sql);
    }
}
