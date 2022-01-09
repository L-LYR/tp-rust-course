pub use engines::{toy_bitcask, KvsEngine};
pub use errors::{KvsError, Result};

mod engines;
mod errors;

pub mod cli_common;
pub mod server;

#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;
