use clap::Parser;
use kvs::{cli_common, cli_common::Command, KvsClient, Result};
use std::process::exit;

fn main() -> Result<()> {
    // `kvs-client set <KEY> <VALUE> [--addr IP-PORT]`
    // `kvs-client get <KEY> [--addr IP-PORT]`
    // `kvs-client rm <KEY> [--addr IP-PORT]`
    // `kvs-client -V`
    if let Err(e) = run(cli_common::ClientOption::parse()) {
        eprintln!("{}", e);
        exit(1);
    }
    Ok(())
}

fn run(opt: cli_common::ClientOption) -> Result<()> {
    let mut client = KvsClient::connect(opt.addr)?;
    match opt.command {
        Command::get { key } => {
            if let Some(value) = client.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Command::set { key, value } => {
            client.set(key, value)?;
        }
        Command::rm { key } => {
            client.remove(key)?;
        }
    }
    Ok(())
}
