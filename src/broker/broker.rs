use std::error::Error;
use std::io::ErrorKind::WouldBlock;
use std::net::SocketAddr;
use std::process::exit;
use log::{error, info};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, Interest};
use tokio::net::{TcpListener, TcpStream};
use crate::broker::protocol::Protocol;

#[derive(Serialize, Deserialize, Clone)]
pub struct BrokerConfig {
    pub bind_addr: String,
}

async fn process_connection(sock: TcpStream, addr: SocketAddr) -> Result<(), Box<dyn Error + Sync + Send>> {
    info!("broker accepted connection from: {}", addr);

    /* TODO: get name and message type from client */
    let name = "DEFAULT_NAME";
    let msg_type = "DEFAULT_MSG_TYPE";

    let mut protocol = Protocol::new(name.to_string(), msg_type.to_string())?;

    let mut counter = 0;
    let mut m_sock = sock;
    loop {
        counter += 1;

        let ready;
        if (counter % 2) == 0 {
            /* XXX Note: the example says to use a bitwise | here, but that never works!!!. if READABLE | WRITABLE is used, only writable is ever set */
            ready = m_sock.ready(Interest::WRITABLE).await?;
        } else {
            ready = m_sock.ready(Interest::READABLE).await?;
        }

        if ready.is_readable() {
            let mut data = [0u8;1024];
            match m_sock.try_read(&mut data) {
                Ok(n) => {
                    let msgs = protocol.feed(&data);

                    /* TODO: send message to router */
                }
                Err(ref e) if e.kind() == WouldBlock => {
                    continue;
                }
                Err(e) => {
                    break;
                }
            }
        }

        if ready.is_writable() && (counter % 100000000) == 0{
            match m_sock.try_write(b"hello world\n") {
                Ok(n) => {
                    //println!("wrote {} bytes", n);
                }
                Err(ref e) if e.kind() == WouldBlock => {
                    continue;
                }
                Err(e) => {
                    break;
                }
            }
        }
    }

    println!("Socket closing for {}", name);
    Ok(())
}

pub async fn broker_main(cfg: BrokerConfig) -> Result<(), Box<dyn Error + Sync + Send>>{
    let listener = TcpListener::bind(&cfg.bind_addr).await?;
    loop {
        let (sock, addr) = listener.accept().await?;
        tokio::spawn(process_connection(sock, addr));
    }
}