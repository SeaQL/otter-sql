#![doc = include_str!("cargo_docs.md")]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/SeaQL/otter-sql/main/assets/SeaQL logo dual.png"
)]

pub mod codegen;
pub mod column;
pub mod database;
pub mod expr;
pub mod ic;
pub mod identifier;
pub mod parser;
pub mod schema;
pub mod table;
pub mod value;
pub mod vm;

pub use column::Column;
pub use database::Database;
pub use ic::{Instruction, IntermediateCode};
pub use identifier::BoundedString;
pub use table::Table;
pub use value::Value;
pub use vm::VirtualMachine;
