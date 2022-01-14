use std::{env::current_dir, process::exit};

use clap::Parser;
use kvs::{Command, KvStore, KvsCliOption, KvsEngine, Result};

fn main() -> Result<()> {
    let cli = KvsCliOption::parse();

    // mathes for command line arguments
    // `kvs set <KEY> <VALUE>`
    // Set the value of a string key to a string
    // `kvs get <KEY>`
    // Get the string value of a given string key
    // `kvs rm <KEY>`
    // Remove a given key
    // `kvs -V`
    // Print the version

    match cli.command {
        Command::set { key, value } => {
            let store = KvStore::open(current_dir()?)?;
            store.set(key.to_string(), value.to_string())?;
        }
        Command::get { key } => {
            let store = KvStore::open(current_dir()?)?;
            if let Some(value) = store.get(key.to_string())? {
                println!("{}", value)
            } else {
                println!("Key not found")
            }
        }
        Command::rm { key } => {
            let store = KvStore::open(current_dir()?)?;
            match store.remove(key.to_string()) {
                Ok(()) => {}
                Err(kvs::KvsError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1)
                }
                Err(e) => return Err(e),
            }
        }
    }
    Ok(())
}
