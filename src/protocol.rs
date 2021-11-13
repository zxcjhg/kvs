use clap::{Subcommand, ArgEnum};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Subcommand, Serialize, Deserialize)]
pub enum Command {
    #[clap(name = "set", about = "Sets a value for a given key")]
    Set { key: String, value: String },
    #[clap(name = "get", about = "Returns a value for a given key")]
    Get { key: String },
    #[clap(name = "rm", about = "Removes entry with a given key")]
    Rm { key: String },
}

#[derive(Serialize, Deserialize)]
pub enum Response {
    Ok(Option<String>),
    Err(String),
}


#[derive(ArgEnum, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Engine {
    #[clap(alias = "kvs")]
    Kvs,
    #[clap(alias = "sled")]
    Sled,
}

impl fmt::Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}