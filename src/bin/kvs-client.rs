use clap::Parser;
use kvs::{cli_common, Result};

fn main() -> Result<()> {
    // `kvs-client set <KEY> <VALUE> [--addr IP-PORT]`
    // `kvs-client get <KEY> [--addr IP-PORT]`
    // `kvs-client rm <KEY> [--addr IP-PORT]`
    // `kvs-client -V`
    let opt = cli_common::ClientOption::parse();
    println!("{:?}", opt);

    Ok(())
}
