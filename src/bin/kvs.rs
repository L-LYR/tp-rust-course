use clap::{AppSettings, Parser, Subcommand};
use kvs::{toy_bitcask::KvStore, KvsEngine, Result};
use std::{env::current_dir, process::exit};

#[derive(Parser)]
#[clap(name("kvs"))]
#[clap(
    about(clap::crate_description!()),
    version(clap::crate_version!()),
    author(clap::crate_authors!())
)]
struct KvsCli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
#[clap(setting(AppSettings::DisableHelpSubcommand))]
#[allow(non_camel_case_types)]
enum Commands {
    /// Get the string value of a string key
    #[clap(setting(AppSettings::ArgRequiredElseHelp))]
    get {
        /// Key of the value that you want to get
        key: String,
    },

    /// Set a string key mapped to a string value
    #[clap(setting(AppSettings::ArgRequiredElseHelp))]
    set {
        /// Key of the value that you want to set
        key: String,
        /// Value that you want to set
        value: String,
    },

    /// Remove a key-value pair with the given string key
    #[clap(setting(AppSettings::ArgRequiredElseHelp))]
    rm {
        /// Key of the value that you want to remove
        key: String,
    },
}

fn main() -> Result<()> {
    let cli = KvsCli::parse();

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
        Commands::set { key, value } => {
            let mut store = KvStore::open(current_dir()?)?;
            store.set(key.to_string(), value.to_string())?;
        }
        Commands::get { key } => {
            let mut store = KvStore::open(current_dir()?)?;
            if let Some(value) = store.get(key.to_string())? {
                println!("{}", value)
            } else {
                println!("Key not found")
            }
        }
        Commands::rm { key } => {
            let mut store = KvStore::open(current_dir()?)?;
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
