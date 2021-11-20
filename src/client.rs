use crate::common::{Command, Response, Result};
use crate::error::KvsError;
use std::io::{BufReader, BufWriter, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};

pub struct KvsClient {
    stream: TcpStream,
    shutdown_flag: AtomicBool,
}

impl KvsClient {
    pub fn new(addr: &SocketAddr) -> Result<KvsClient> {
        Ok(KvsClient {
            stream: TcpStream::connect(&addr)?,
            shutdown_flag: AtomicBool::new(false),
        })
    }

    pub fn send(&self, cmd: &Command) -> Result<()> {
        if self.shutdown_flag.load(Ordering::Relaxed) {
            return Ok(());
        }
        let mut reader = BufReader::new(&self.stream);
        let mut writer = BufWriter::new(&self.stream);

        bincode::serialize_into(&mut writer, &cmd)?;
        writer.flush()?;
        match bincode::deserialize_from(&mut reader)? {
            Response::Ok(s) => {
                if let Some(s) = s {
                    println!("{}", s)
                }
            }
            Response::Err(s) => {
                eprintln!("{}", s);
                return Err(KvsError::UnexpectedError);
            }
        }
        Ok(())
    }

    pub fn shutdown(&self) -> Result<()> {
        self.stream.shutdown(Shutdown::Both).unwrap();
        self.shutdown_flag.store(true, Ordering::Relaxed);
        Ok(())
    }
}
