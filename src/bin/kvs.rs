use clap::{App, Arg};
use kvs::{KvStore, KvsError, Result};
use std::path::PathBuf;
use std::process::exit;

fn main() -> Result<()> {
    let mut kv_store = KvStore::open(PathBuf::from("")).unwrap();

    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author("nobody@gmail.com")
        .about("Key-Value Storage")
        .subcommand(
            App::new("set") // The name we call argument with
                .about("sets a VALUE for a given KEY") // The message displayed in "myapp -h"
                .arg(
                    Arg::with_name("KEY") // And their own arguments
                        .required(true),
                )
                .arg(
                    Arg::with_name("VALUE") // And their own arguments
                        .required(true),
                ),
        )
        .subcommand(
            App::new("get") // The name we call argument with
                .about("returns VALUE for given KEY") // The message displayed in "myapp -h"
                .arg(
                    Arg::with_name("KEY") // And their own arguments
                        .required(true),
                ),
        )
        .subcommand(
            App::new("rm") // The name we call argument with
                .about("remove entry for given KEY") // The message displayed in "myapp -h"
                .arg(
                    Arg::with_name("KEY") // And their own arguments
                        .required(true),
                ),
        )
        .get_matches();
    match matches.subcommand() {
        ("set", Some(cmd)) => {
            let key = cmd.value_of("KEY").unwrap().to_string();
            let value = cmd.value_of("VALUE").unwrap().to_string();
            kv_store.set(key, value)?;
        }
        ("get", Some(cmd)) => {
            let key = cmd.value_of("KEY").unwrap().to_string();
            if let Some(value) = kv_store.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        ("rm", Some(cmd)) => {
            let key = cmd.value_of("KEY").unwrap().to_string();
            match kv_store.remove(key) {
                Ok(()) => (),
                Err(KvsError::KeyNotFound) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(err) => return Err(err),
            }
        }
        _ => panic!("No command given"),
    }

    Ok(())
}
