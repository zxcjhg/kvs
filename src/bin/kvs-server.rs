use clap::Parser;
use kvs::common::{EngineType, Result};
use kvs::engine::{LogStructKVStore, SledStore};
use kvs::server::KvsServer;
use kvs::thread_pool::*;
use slog::*;
use std::env;
use std::fs;
use std::net::SocketAddr;
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
    engine: EngineType,
    #[clap(
        arg_enum,
        short,
        long = "thread_pool",
        name = "thread pool",
        default_value = "sharedq",
        about = "Engine for key value storage"
    )]
    thread_pool: ThreadPoolType,
    #[clap(
        short = 'n',
        long = "num_threads",
        name = "num of threads",
        default_value = "8",
        about = "Num of threads"
    )]
    num_threads: u32,
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
    info!(logger, "Thread pool: {:?}", args.thread_pool);

    match args.engine {
        EngineType::Kvs => {
            let kv_store = LogStructKVStore::open(env::current_dir()?.as_path())?;
            match args.thread_pool {
                ThreadPoolType::Rayon => KvsServer::<LogStructKVStore, RayonThreadPool>::new(
                    kv_store,
                    RayonThreadPool::new(args.num_threads as u32)?,
                )?
                .run(&args.address)?,
                ThreadPoolType::SharedQ => {
                    KvsServer::<LogStructKVStore, SharedQueueThreadPool>::new(
                        kv_store,
                        SharedQueueThreadPool::new(args.num_threads as u32)?,
                    )?
                    .run(&args.address)?
                }
            }
        }
        EngineType::Sled => {
            let kv_store = SledStore::open(env::current_dir()?.as_path())?;
            match args.thread_pool {
                ThreadPoolType::Rayon => KvsServer::<SledStore, RayonThreadPool>::new(
                    kv_store,
                    RayonThreadPool::new(args.num_threads as u32)?,
                )?
                .run(&args.address)?,
                ThreadPoolType::SharedQ => KvsServer::<SledStore, SharedQueueThreadPool>::new(
                    kv_store,
                    SharedQueueThreadPool::new(args.num_threads as u32)?,
                )?
                .run(&args.address)?,
            }
        }
    };

    Ok(())
}

fn get_current_engine(arg_engine: &EngineType) -> Result<Option<EngineType>> {
    match fs::read(ENGINE_FILENAME) {
        Err(_) => {
            fs::write(ENGINE_FILENAME, bincode::serialize(&arg_engine)?)?;
            Ok(Some(arg_engine.clone()))
        }
        Ok(buffer) => {
            let engine: EngineType = bincode::deserialize(&buffer)?;
            Ok(Some(engine))
        }
    }
}
