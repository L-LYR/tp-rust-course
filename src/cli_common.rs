use clap::{AppSettings, ArgEnum, Parser, Subcommand};
use std::{fmt::Display, net::SocketAddr};

const DEFAULT_LISTENING_ADDR: &str = "127.0.0.1:4000";
const DEFAULT_KV_STORAGE_ENGINE: EngineType = EngineType::kvs;

#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone, ArgEnum, PartialEq, Eq)]
pub enum EngineType {
    kvs,
    sled,
}

impl Display for EngineType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineType::kvs => f.write_str("kvs"),
            EngineType::sled => f.write_str("sled"),
        }
    }
}

impl slog::Value for EngineType {
    fn serialize(
        &self,
        _record: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        match self {
            EngineType::kvs => serializer.emit_arguments(key, &format_args!("kvs")),
            EngineType::sled => serializer.emit_arguments(key, &format_args!("sled")),
        }
    }
}

#[allow(non_camel_case_types)]
#[derive(Subcommand, Clone, Debug)]
pub enum Command {
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

#[derive(Debug, Parser)]
#[clap(
    name("kvs-server"),
    about(clap::crate_description!()),
    version(clap::crate_version!()),
    author(clap::crate_authors!()),
)]
pub struct ServerOption {
    #[clap(
        long("addr"), 
        value_name("IP-PORT"),
        default_value_t = DEFAULT_LISTENING_ADDR.parse().unwrap(),
        parse(try_from_str),
    )]
    /// Listening address of kvs server.
    /// If not set, '127.0.0.1:4000' is the default value.
    pub addr: SocketAddr,
    #[clap(
        long("engine"),
        value_name("ENGINE-NAME"),
        default_value_t = DEFAULT_KV_STORAGE_ENGINE,
        parse(try_from_str),
    )]
    #[clap(arg_enum)]
    /// Storage engine type of kvs server.
    pub engine_type: EngineType,
}

#[allow(non_camel_case_types)]
#[derive(Subcommand, Clone, Debug)]
pub enum ClientCommand {
    /// Get the string value of a string key
    #[clap(setting(AppSettings::ArgRequiredElseHelp))]
    get {
        /// Key of the value that you want to get
        key: String,
        #[clap(
            long("addr"),
            value_name("IP-PORT"),
            default_value_t = DEFAULT_LISTENING_ADDR.parse().unwrap(),
            parse(try_from_str),
        )]
        /// Target address of this command
        addr: SocketAddr,
    },

    /// Set a string key mapped to a string value
    #[clap(setting(AppSettings::ArgRequiredElseHelp))]
    set {
        /// Key of the value that you want to set
        key: String,
        /// Value that you want to set
        value: String,
        #[clap(
            long("addr"),
            value_name("IP-PORT"),
            default_value_t = DEFAULT_LISTENING_ADDR.parse().unwrap(),
            parse(try_from_str),
        )]
        /// Target address of this command
        addr: SocketAddr,
    },

    /// Remove a key-value pair with the given string key
    #[clap(setting(AppSettings::ArgRequiredElseHelp))]
    rm {
        /// Key of the value that you want to remove
        key: String,
        #[clap(
            long("addr"),
            value_name("IP-PORT"),
            default_value_t = DEFAULT_LISTENING_ADDR.parse().unwrap(),
            parse(try_from_str),
        )]
        /// Target address of this command
        addr: SocketAddr,
    },
}

#[derive(Debug, Parser)]
#[clap(
    name("kvs-client"),
    about(clap::crate_description!()),
    version(clap::crate_version!()),
    author(clap::crate_authors!()),
)]
pub struct ClientOption {
    #[clap(subcommand)]
    pub command: ClientCommand,
}

#[derive(Debug, Parser)]
#[clap(
    name("kvs"),
    about(clap::crate_description!()),
    version(clap::crate_version!()),
    author(clap::crate_authors!()),
)]
pub struct KvsCliOption {
    #[clap(subcommand)]
    pub command: Command,
}
