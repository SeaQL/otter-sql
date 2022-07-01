mod column;
mod database;
mod ic;
mod schema;
mod table;
mod value;
mod vm;

pub use database::Database;
pub use vm::VirtualMachine;

use arraystring::{typenum::U63, ArrayString};

/// `Mrc` stands for "maybe-atomic Rc".
/// It's an `Arc` if thread safety is enabled, `Rc` otherwise.
#[cfg(not(feature = "thread-safe"))]
use std::rc::Rc as Mrc;
#[cfg(feature = "thread-safe")]
use std::sync::Arc as Mrc;

/// A fixed capacity copy-able string.
type BoundedString = ArrayString<U63>;
