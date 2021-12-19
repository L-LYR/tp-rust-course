use std::process::exit;

use clap::{App, Arg, SubCommand};
fn main() {
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
        .subcommands(vec![
            SubCommand::with_name("set").args(&[
                Arg::with_name("key")
                    .help("key of the value that you want to set")
                    .required(true),
                Arg::with_name("value")
                    .help("value that you want to set")
                    .required(true),
            ]),
            SubCommand::with_name("get").arg(
                Arg::with_name("key")
                    .help("key of the value that you want to get")
                    .required(true),
            ),
            SubCommand::with_name("rm").arg(
                Arg::with_name("key")
                    .help("key of the value that you want to remove")
                    .required(true),
            ),
        ])
        .get_matches();

    match matches.subcommand() {
        ("set", Some(_match)) => {
            eprintln!("unimplemented!")
        }
        ("get", Some(_match)) => {
            eprintln!("unimplemented!")
        }
        ("rm", Some(_match)) => {
            eprintln!("unimplemented!")
        }
        _ => {
            eprintln!("unknown command!")
        }
    }
    exit(1)
}
