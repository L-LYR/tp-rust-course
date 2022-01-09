use clap::Parser;
use kvs::cli_common;
use kvs::Result;

fn main() -> Result<()> {
    // kvs-server [--addr IP-PORT] [--engine ENGINE-NAME]
    // kvs-server -V
    let logger = cli_common::init_logger();
    let opt = cli_common::ServerOption::parse();
    slog::info!(logger, "Server Start!"; "listening address" => opt.addr, "kv engine type" => opt.engine_type);

    Ok(())
}
