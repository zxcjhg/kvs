use clap::Parser;
use kvs::{Command, Response, Result};
use std::io::{BufRead, BufReader, BufWriter, Write};
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
    serde_json::to_writer(&mut writer, &args.command)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    let mut buffer = String::new();
    reader.read_line(&mut buffer)?;
    match serde_json::from_str(&buffer)? {
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
