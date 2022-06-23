mod column;
mod database;
mod ic;
mod schema;
mod table;
mod value;
mod vm;

pub use database::Database;
pub use vm::VirtualMachine;

/// `Mrc` stands for "maybe-atomic Rc".
/// It's an `Arc` if thread safety is enabled, `Rc` otherwise.
#[cfg(not(feature = "thread-safe"))]
use std::rc::Rc as Mrc;
#[cfg(feature = "thread-safe")]
use std::sync::Arc as Mrc;
