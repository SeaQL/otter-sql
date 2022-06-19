mod column;
mod database;
mod ic;
mod schema;
mod table;
mod vm;

pub use database::Database;
pub use vm::VirtualMachine;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
