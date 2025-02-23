use clap::Parser;

use tokio::{
    io::{stdin, stdout, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    addr: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut stream = TcpStream::connect(args.addr).await?;

    let mut stdin = BufReader::new(stdin());
    let mut stdout = stdout();

    loop {
        let mut input = String::new();
        stdin.read_line(&mut input).await?;

        let inputbytes = input.into_bytes();
        stream.write(&inputbytes).await?;

        let mut response_buffer = [0; 1024];
        stream.read(&mut response_buffer).await?;

        stdout.write_all(&response_buffer).await?;
        stdout.write_all(&"\n".to_string().into_bytes()).await?;
        stdout.flush().await.unwrap();
    }
}
