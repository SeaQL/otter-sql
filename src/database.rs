use crate::{schema::Schema, BoundedString};

const DEFAULT_SCHEMA_NAME: &str = "main";

/// An in-memory database.
pub struct Database {
    name: BoundedString,
    schemas: Vec<Schema>,
}

impl Database {
    pub fn new(name: BoundedString) -> Self {
        Self {
            name,
            schemas: vec![Schema::new(DEFAULT_SCHEMA_NAME.into())],
        }
    }

    /// Add a new schema.
    pub fn add_schema(&mut self, schema: Schema) -> &mut Self {
        self.schemas.push(schema);
        self
    }

    /// The name of the database.
    pub fn name(&self) -> &BoundedString {
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
        let mut db = Database::new("test".into());

        assert_eq!(db.name(), "test");
        assert_eq!(db.schemas().len(), 1);
        assert_eq!(db.schemas()[0].name(), DEFAULT_SCHEMA_NAME);
        assert_eq!(db.schemas()[0].tables().len(), 0);

        db.add_schema(Schema::new("test".into()));
        assert_eq!(db.schemas().len(), 2);
        assert_eq!(db.schemas()[1].name(), "test");
        assert_eq!(db.schemas()[1].tables().len(), 0);
    }
}
