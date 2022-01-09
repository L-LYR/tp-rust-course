use std::process::exit;

use clap::Parser;
use kvs::{ClientCommand, ClientOption, KvsClient, Result};

fn main() -> Result<()> {
    // `kvs-client set <KEY> <VALUE> [--addr IP-PORT]`
    // `kvs-client get <KEY> [--addr IP-PORT]`
    // `kvs-client rm <KEY> [--addr IP-PORT]`
    // `kvs-client -V`
    if let Err(e) = run(ClientOption::parse()) {
        eprintln!("{}", e);
        exit(1);
    }
    Ok(())
}

fn run(opt: ClientOption) -> Result<()> {
    match opt.command {
        ClientCommand::get { key, addr } => {
            if let Some(value) = KvsClient::connect(addr)?.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        ClientCommand::set { key, value, addr } => {
            KvsClient::connect(addr)?.set(key, value)?;
        }
        ClientCommand::rm { key, addr } => {
            KvsClient::connect(addr)?.remove(key)?;
        }
    }
    Ok(())
}
