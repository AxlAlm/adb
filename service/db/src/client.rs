use std::net::{TcpListener, TcpStream};

struct Client {}

fn handle_client(stream: TcpStream) {}

pub fn connect() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:80")?;

    // accept connections and process them serially
    for stream in listener.incoming() {
        handle_client(stream?);
    }

    Ok(())
}
