use std::env::current_dir;
use std::process::exit;

use clap::Parser;
use kvs::{EngineType, KvStore, KvsServer, Result, ServerOption, SledWrapper};
use slog::Logger;

#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

fn main() -> Result<()> {
    // kvs-server [--addr IP-PORT] [--engine ENGINE-NAME]
    // kvs-server -V
    let logger = init_logger();
    let option = ServerOption::parse();
    if let Err(e) = run(option, logger) {
        eprintln!("{}", e);
        exit(1);
    }
    Ok(())
}

fn run(option: ServerOption, logger: Logger) -> Result<()> {
    let path = &current_dir()?;
    slog::info!(logger, "Server Start!";
        "server version" => env!("CARGO_PKG_VERSION"),
        "listening address" => option.addr,
        "kv engine type" => option.engine_type,
        "path" => path.display(),
    );
    match option.engine_type {
        EngineType::kvs => {
            let engine = KvStore::open(path)?;
            KvsServer::new(engine, logger).run(&option.addr)?;
        }
        EngineType::sled => {
            let engine = SledWrapper::new(sled::open(path)?);
            KvsServer::new(engine, logger).run(&option.addr)?;
        }
    }
    Ok(())
}

fn init_logger() -> slog::Logger {
    use slog::Drain;
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, slog_o!())
}