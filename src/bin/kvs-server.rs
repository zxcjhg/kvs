use clap::Parser;
use kvs::{Command, Engine, KvStore, KvsEngine, KvsError, Response, Result, SledStore};
use slog::*;
use std::env;
use std::fs;
use std::io::{BufReader, BufWriter};
use std::net::{SocketAddr, TcpListener};
use std::process::exit;

const ENGINE_FILENAME: &str = ".engine";

#[derive(Parser, Debug, PartialEq)]
#[clap(name = "kvs-server", about = "Key-Value Storage Server", version)]
struct ApplicationArguments {
    #[clap(
        short,
        long = "addr",
        name = "addr",
        default_value = "127.0.0.1:4000",
        about = "Server address with format [IP:PORT]"
    )]
    address: SocketAddr,
    #[clap(
        arg_enum,
        short,
        long = "engine",
        name = "engine",
        default_value = "kvs",
        about = "Engine for key value storage"
    )]
    engine: Engine,
}

fn main() -> Result<()> {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stderr());
    let logger = Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!());

    let args = ApplicationArguments::parse();
    if let Some(engine) = get_current_engine(&args.engine)? {
        if engine != args.engine {
            eprintln!("Different engine");
            exit(1);
        }
    }

    info!(logger, "Storage version {}", env!["CARGO_PKG_VERSION"]);
    info!(logger, "Listening on: {}", args.address);
    info!(logger, "Backend engine: {}", args.engine);

    let mut kv_store: Box<dyn KvsEngine> = match args.engine {
        Engine::Kvs => Box::new(KvStore::open(env::current_dir()?.as_path())?),
        Engine::Sled => Box::new(SledStore::open(env::current_dir()?.as_path())?),
    };

    let listener = TcpListener::bind(args.address)?;

    for stream in listener.incoming() {
        let stream = stream?;
        let mut reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);

        match bincode::deserialize_from(&mut reader) {
            Ok(cmd) => match cmd {
                Command::Set { key, value } => {
                    match kv_store.set(key, value) {
                        Ok(()) => bincode::serialize_into(&mut writer, &Response::Ok(None))?,
                        Err(err) => bincode::serialize_into(
                            &mut writer,
                            &Response::Err(format!("{}", err)),
                        )?,
                    };
                }
                Command::Get { key } => {
                    match kv_store.get(key) {
                        Ok(value) => match value {
                            Some(value) => {
                                bincode::serialize_into(&mut writer, &Response::Ok(Some(value)))?
                            }
                            None => bincode::serialize_into(
                                &mut writer,
                                &Response::Ok(Some("Key not found".to_string())),
                            )?,
                        },
                        Err(err) => bincode::serialize_into(
                            &mut writer,
                            &Response::Err(format!("{}", err)),
                        )?,
                    };
                }
                Command::Rm { key } => {
                    match kv_store.remove(key) {
                        Ok(_) => bincode::serialize_into(&mut writer, &Response::Ok(None))?,
                        Err(KvsError::KeyNotFound) => bincode::serialize_into(
                            &mut writer,
                            &Response::Err("Key not found".to_string()),
                        )?,
                        Err(err) => bincode::serialize_into(
                            &mut writer,
                            &Response::Err(format!("{}", err)),
                        )?,
                    };
                }
            },
            Err(err) => {
                bincode::serialize_into(&mut writer, &Response::Err(format!("{}", err)))?;
            }
        }
    }

    Ok(())
}

fn get_current_engine(arg_engine: &Engine) -> Result<Option<Engine>> {
    match fs::read(ENGINE_FILENAME) {
        Err(_) => {
            fs::write(ENGINE_FILENAME, bincode::serialize(&arg_engine)?)?;
            Ok(Some(arg_engine.clone()))
        }
        Ok(buffer) => {
            let engine: Engine = bincode::deserialize(&buffer)?;
            Ok(Some(engine))
        }
    }
}
