use clap::Parser;
use kvs::client::KvsClient;
use kvs::common::{Command, Result};
use std::net::SocketAddr;

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
    let client = KvsClient::new(&args.address)?;
    client.send(&args.command)?;
    client.shutdown()?;
    Ok(())
}
