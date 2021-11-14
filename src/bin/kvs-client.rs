use clap::Parser;
use kvs::{Command, Response, Result};
use std::io::{BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::exit;

#[derive(Parser, Debug)]
#[clap(
    name = "kvs-client",
    about = "Client application to interact with Key-Value Storage",
    version
)]
struct ApplicationArguments {
    #[clap(subcommand)]
    command: Command,
    #[clap(
        global = true,
        short,
        long = "addr",
        name = "addr",
        default_value = "127.0.0.1:4000",
        about = "Remote server address IP:PORT"
    )]
    address: SocketAddr,
}

fn main() -> Result<()> {
    let args = ApplicationArguments::parse();

    let stream = TcpStream::connect(args.address)?;
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    bincode::serialize_into(&mut writer, &args.command)?;
    writer.flush()?;
    match bincode::deserialize_from(&mut reader)? {
        Response::Ok(s) => {
            if let Some(s) = s {
                println!("{}", s)
            }
        }
        Response::Err(s) => {
            eprintln!("{}", s);
            exit(1);
        }
    }
    Ok(())
}
