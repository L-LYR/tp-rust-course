use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{toy_bitcask::KvStore, KvsEngine, Result};
use std::{env::current_dir, process::exit};

fn main() -> Result<()> {
    // mathes for command line arguments
    // `kvs set <KEY> <VALUE>`
    // Set the value of a string key to a string
    // `kvs get <KEY>`
    // Get the string value of a given string key
    // `kvs rm <KEY>`
    // Remove a given key
    // `kvs -V`
    // Print the version
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .settings(&[
            AppSettings::DisableHelpSubcommand,
            AppSettings::SubcommandRequiredElseHelp,
        ])
        .subcommands(vec![
            SubCommand::with_name("set")
                .about("set a string key mapped to a string value")
                .args(&[
                    Arg::with_name("key")
                        .help("key of the value that you want to set")
                        .required(true),
                    Arg::with_name("value")
                        .help("value that you want to set")
                        .required(true),
                ]),
            SubCommand::with_name("get")
                .about("get the string value of a string key")
                .arg(
                    Arg::with_name("key")
                        .help("key of the value that you want to get")
                        .required(true),
                ),
            SubCommand::with_name("rm")
                .about("remove a given string key")
                .arg(
                    Arg::with_name("key")
                        .help("key of the value that you want to remove")
                        .required(true),
                ),
        ])
        .get_matches();

    match matches.subcommand() {
        ("set", Some(matches)) => {
            let key = matches.value_of("key").unwrap();
            let value = matches.value_of("value").unwrap();

            let mut store = KvStore::open(current_dir()?)?;
            store.set(key.to_string(), value.to_string())?;
        }
        ("get", Some(matches)) => {
            let key = matches.value_of("key").unwrap();

            let mut store = KvStore::open(current_dir()?)?;
            if let Some(value) = store.get(key.to_string())? {
                println!("{}", value)
            } else {
                println!("Key not found")
            }
        }
        ("rm", Some(matches)) => {
            let key = matches.value_of("key").unwrap();

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
        _ => unreachable!(),
    }
    Ok(())
}
