use crate::schema::Schema;

const DEFAULT_SCHEMA_NAME: &str = "main";

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

    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schemas.push(schema);
        self
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn schemas(&self) -> &Vec<Schema> {
        &self.schemas
    }
}

#[cfg(test)]
mod tests {
    use super::Database;

    #[test]
    fn create_database() {
        let _db = Database::new("test".to_owned());
    }
}
