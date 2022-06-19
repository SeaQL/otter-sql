use std::borrow::Borrow;

use crate::schema::Schema;

const DEFAULT_SCHEMA_NAME: &str = "main";

pub struct Database {
    pub name: String,
    pub schemas: Vec<Schema>,
}

impl Database {
    pub fn new<S>(name: S) -> Self
    where
        S: Borrow<str>,
    {
        Self {
            name: name.borrow().to_owned(),
            schemas: vec![Schema::new(DEFAULT_SCHEMA_NAME)],
        }
    }

    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schemas.push(schema);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::Database;

    #[test]
    fn create_database() {
        let _db = Database::new("test");
    }
}
