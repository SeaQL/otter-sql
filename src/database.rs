use crate::schema::Schema;

const DEFAULT_SCHEMA_NAME: &str = "main";

/// An in-memory database.
pub struct Database {
    name: String,
    schemas: Vec<Schema>,
}

impl Database {
    pub fn new(name: String) -> Self {
        Self {
            name,
            schemas: vec![Schema::new(DEFAULT_SCHEMA_NAME.to_owned())],
        }
    }

    /// Add a new schema.
    pub fn add_schema(&mut self, schema: Schema) -> &mut Self {
        self.schemas.push(schema);
        self
    }

    /// The name of the database.
    pub fn name(&self) -> &String {
        &self.name
    }

    /// All the schemas in the database.
    pub fn schemas(&self) -> &Vec<Schema> {
        &self.schemas
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::Schema;

    use super::{Database, DEFAULT_SCHEMA_NAME};

    #[test]
    fn create_database() {
        let mut db = Database::new("test".to_owned());

        assert_eq!(db.name(), "test");
        assert_eq!(db.schemas().len(), 1);
        assert_eq!(db.schemas()[0].name(), DEFAULT_SCHEMA_NAME);
        assert_eq!(db.schemas()[0].tables().len(), 0);

        db.add_schema(Schema::new("test".to_owned()));
        assert_eq!(db.schemas().len(), 2);
        assert_eq!(db.schemas()[1].name(), "test");
        assert_eq!(db.schemas()[1].tables().len(), 0);
    }
}
