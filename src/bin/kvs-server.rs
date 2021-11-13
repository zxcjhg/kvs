use clap::Parser;
use kvs::{Command, KvStore, KvsEngine, KvsError, Response, Result, SledStore, Engine};
use slog::*;
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
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

        let mut buffer = String::new();
        reader.read_line(&mut buffer)?;

        match serde_json::from_str(&buffer) {
            Ok(cmd) => match cmd {
                Command::Set { key, value } => {
                    match kv_store.set(key, value) {
                        Ok(()) => serde_json::to_writer(&mut writer, &Response::Ok(None))?,
                        Err(err) => {
                            serde_json::to_writer(&mut writer, &Response::Err(format!("{}", err)))?
                        }
                    };
                }
                Command::Get { key } => {
                    match kv_store.get(key) {
                        Ok(value) => match value {
                            Some(value) => {
                                serde_json::to_writer(&mut writer, &Response::Ok(Some(value)))?
                            }
                            None => serde_json::to_writer(
                                &mut writer,
                                &Response::Ok(Some("Key not found".to_string())),
                            )?,
                        },
                        Err(err) => {
                            serde_json::to_writer(&mut writer, &Response::Err(format!("{}", err)))?
                        }
                    };
                }
                Command::Rm { key } => {
                    match kv_store.remove(key) {
                        Ok(_) => serde_json::to_writer(&mut writer, &Response::Ok(None))?,
                        Err(KvsError::KeyNotFound) => serde_json::to_writer(
                            &mut writer,
                            &Response::Err("Key not found".to_string()),
                        )?,
                        Err(err) => {
                            serde_json::to_writer(&mut writer, &Response::Err(format!("{}", err)))?
                        }
                    };
                }
            },
            Err(err) => {
                serde_json::to_writer(&mut writer, &Response::Err(format!("{}", err)))?;
            }
        }
        writer.write_all(b"\n")?;
    }

    Ok(())
}

fn get_current_engine(arg_engine: &Engine) -> Result<Option<Engine>> {
    return match fs::read_to_string(ENGINE_FILENAME) {
        Err(_) => {
            fs::write(ENGINE_FILENAME, serde_json::to_string(&arg_engine)?)?;
            Ok(Some(arg_engine.clone()))
        },
        Ok(s) => {
            let engine: Engine = serde_json::from_str(&s)?;
            Ok(Some(engine))
        }
    };
}