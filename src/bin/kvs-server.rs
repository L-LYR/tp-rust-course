use std::env::current_dir;
use std::process::exit;

use clap::Parser;
use kvs::{
    engine_type_of, set_engine_type,
    thread_pool::{RayonThreadPool, ThreadPool},
    EngineType, KvStore, KvsError, KvsServer, Result, ServerOption, SledWrapper,
};

#[macro_use]
extern crate log;
#[macro_use(slog_o)]
extern crate slog;
extern crate slog_async;
extern crate slog_scope;
extern crate slog_stdlog;
extern crate slog_term;

fn main() -> Result<()> {
    // kvs-server [--addr IP-PORT] [--engine ENGINE-NAME]
    // kvs-server -V
    init_logger();
    let option = ServerOption::parse();
    if let Err(e) = run(option) {
        eprintln!("{}", e);
        exit(1);
    }
    Ok(())
}

fn run(option: ServerOption) -> Result<()> {
    let path = &current_dir()?;
    if let Some(previous_engine_type) = engine_type_of(path)? {
        if previous_engine_type != option.engine_type {
            error!(
                "Imcompatible engine type, want {}, got {}",
                previous_engine_type, option.engine_type
            );
            return Err(KvsError::ImcompatibleEngineType);
        }
    }
    set_engine_type(path, &option.engine_type)?;
    let pool = RayonThreadPool::new(num_cpus::get())?;
    info!(
        "Server start!\nVersion: {}, Address: {}, type: {}, path: {}",
        env!("CARGO_PKG_VERSION"),
        option.addr,
        option.engine_type,
        path.display()
    );
    match option.engine_type {
        EngineType::kvs => {
            let engine = KvStore::open(path)?;
            KvsServer::new(engine, pool).run(&option.addr)?;
        }
        EngineType::sled => {
            let engine = SledWrapper::new(sled::open(path)?);
            KvsServer::new(engine, pool).run(&option.addr)?;
        }
    }
    info!("Server done!");
    Ok(())
}

fn init_logger() {
    use slog::Drain;
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, slog_o!());
    let _scope_guard = slog_scope::set_global_logger(logger);
    _scope_guard.cancel_reset();
    slog_stdlog::init_with_level(log::Level::Info).unwrap();
}
