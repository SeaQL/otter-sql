use crate::schema::Schema;

const DEFAULT_SCHEMA_NAME: &str = "main";

pub struct Database {
    name_: String,
    schemas_: Vec<Schema>,
}

impl Database {
    pub fn new(name_: String) -> Self
    {
        Self {
            name_,
            schemas_: vec![Schema::new(DEFAULT_SCHEMA_NAME.to_owned())],
        }
    }

    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schemas_.push(schema);
        self
    }

    pub fn name(&self) -> &String {
        &self.name_
    }

    pub fn schemas(&self) -> &Vec<Schema> {
        &self.schemas_
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
