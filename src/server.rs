use crate::common::{Command, Response, Result};
use crate::engine::KvsEngine;
use crate::error::KvsError;
use crate::thread_pool::ThreadPool;
use std::io;
use std::io::{BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub struct KvsServer<T, F> {
    engine: T,
    pool: F,
    shutdown_flag: Arc<AtomicBool>,
}

impl<T, F> KvsServer<T, F>
where
    T: KvsEngine,
    F: ThreadPool,
{
    pub fn new(engine: T, pool: F) -> Result<KvsServer<T, F>> {
        Ok(KvsServer {
            engine,
            pool,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn run(&self, addr: &SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        listener
            .set_nonblocking(true)
            .expect("Cannot set non-blocking");
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let kv_store = self.engine.clone();
                    let shutdown_flag = Arc::clone(&self.shutdown_flag);
                    self.pool.spawn(move || {
                        handle_stream(kv_store, stream, shutdown_flag).unwrap();
                    });
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if self.shutdown_flag.load(Ordering::Relaxed) {
                        break;
                    }
                    continue;
                }
                // @TODO logging
                Err(_) => continue,
            };
        }
        println!("Shutting down");
        Ok(())
    }

    pub fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::Relaxed);
    }
}

fn handle_stream<E: KvsEngine>(
    kv_store: E,
    stream: TcpStream,
    shutdown_flag: Arc<AtomicBool>,
) -> Result<()> {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    while !shutdown_flag.load(Ordering::Relaxed) {
        match bincode::deserialize_from(&mut reader) {
            Ok(cmd) => match cmd {
                Command::Set { key, value } => match kv_store.set(key, value) {
                    Ok(()) => bincode::serialize_into(&mut writer, &Response::Ok(None)).unwrap(),
                    Err(err) => {
                        bincode::serialize_into(&mut writer, &Response::Err(format!("{}", err)))
                            .unwrap()
                    }
                },
                Command::Get { key } => match kv_store.get(key) {
                    Ok(value) => match value {
                        Some(value) => {
                            bincode::serialize_into(&mut writer, &Response::Ok(Some(value)))
                                .unwrap()
                        }
                        None => bincode::serialize_into(
                            &mut writer,
                            &Response::Ok(Some("Key not found".to_string())),
                        )
                        .unwrap(),
                    },
                    Err(err) => {
                        bincode::serialize_into(&mut writer, &Response::Err(format!("{}", err)))
                            .unwrap()
                    }
                },
                Command::Rm { key } => match kv_store.remove(key) {
                    Ok(_) => bincode::serialize_into(&mut writer, &Response::Ok(None)).unwrap(),
                    Err(KvsError::KeyNotFound) => bincode::serialize_into(
                        &mut writer,
                        &Response::Err("Key not found".to_string()),
                    )
                    .unwrap(),
                    Err(err) => {
                        bincode::serialize_into(&mut writer, &Response::Err(format!("{}", err)))
                            .unwrap()
                    }
                },
            },
            Err(err) => {
                bincode::serialize_into(&mut writer, &Response::Err(format!("{}", err)))?;
            }
        }
        writer.flush()?;
    }

    Ok(())
}
