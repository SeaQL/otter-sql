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
            // the default schema needs to be the first
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

    /// The default schema.
    pub fn default_schema(&self) -> &Schema {
        // ensure that the default schema is always the first
        &self.schemas[0]
    }

    /// Mutable reference to the default schema.
    pub fn default_schema_mut(&mut self) -> &mut Schema {
        // ensure that the default schema is always the first
        &mut self.schemas[0]
    }

    pub fn schema_by_name(&self, name: &BoundedString) -> Option<&Schema> {
        self.schemas().iter().find(|s| s.name() == name)
    }

    pub fn schema_by_name_mut(&mut self, name: &BoundedString) -> Option<&mut Schema> {
        self.schemas.iter_mut().find(|s| s.name() == name)
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
