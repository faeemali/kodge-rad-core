use std::error::Error;
use std::io::ErrorKind::WouldBlock;
use std::net::SocketAddr;
use std::time::Duration;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, Interest};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{channel, Sender};
use tokio::time::sleep;
use crate::broker::auth::{authenticate, AuthMessage, MSG_TYPE_AUTH};
use crate::broker::broker::States::{Authenticate, Register};
use crate::broker::control::{ControlMessages, ctrl_main};
use crate::broker::protocol::{Message, Protocol};
use crate::error::RadError;

#[derive(Serialize, Deserialize, Clone)]
pub struct BrokerConfig {
    pub bind_addr: String,
}

#[derive(Eq, PartialEq)]
enum States {
    Authenticate,
    Register,
}

async fn process_messages(conn_ctx: &mut ConnectionCtx,
                          msgs: &[Message])
                          -> Result<(), Box<dyn Error + Sync + Send>> {
    loop {
        match conn_ctx.state {
            Authenticate => {
                if msgs.len() != 1 {
                    return Err(Box::new(RadError::from("Invalid number of messages for authentication")));
                }

                let auth_msg_wrapper = &msgs[0];
                if auth_msg_wrapper.msg_type != MSG_TYPE_AUTH || !auth_msg_wrapper.routing_keys.is_empty() {
                    return Err(Box::new(RadError::from("Invalid message for authentication")));
                }

                let auth_msg = serde_json::from_slice::<AuthMessage>(auth_msg_wrapper.body.as_slice())?;

                if authenticate(&auth_msg).await? {
                    conn_ctx.state = Register;
                    continue;
                } else {
                    return Err(Box::new(RadError::from("Authentication error")));
                }
            }

            Register => {
                //must register with the control system
            }
        }

        //TODO: do this only when there's no activity
        sleep(Duration::from_millis(1)).await;
    }


    Ok(())
}

struct ConnectionCtx {
    pub state: States,
    pub name: Option<String>,
    pub protocol: Protocol,
    pub ctrl_tx: Sender<ControlMessages>,
}

async fn process_connection(sock: TcpStream, addr: SocketAddr, ctrl_tx: Sender<ControlMessages>) -> Result<(), Box<dyn Error + Sync + Send>> {
    info!("broker accepted connection from: {}", addr);

    let mut protocol = Protocol::new()?;

    let mut conn_ctx = ConnectionCtx {
        state: Authenticate,
        name: None,
        protocol,
        ctrl_tx,
    };

    let mut must_read = false;
    loop {
        must_read = !must_read;

        let ready = if !must_read {
            /* XXX Note: the example says to use a bitwise | here, but that never works!!!. if READABLE | WRITABLE is used, only writable is ever set */
            sock.ready(Interest::WRITABLE).await?
        } else {
            sock.ready(Interest::READABLE).await?
        };

        if ready.is_readable() {
            let mut data = [0u8; 1024];
            match sock.try_read(&mut data) {
                Ok(n) => {
                    if n == 0 {
                        break;
                    }
                    let msgs = &conn_ctx.protocol.feed(&data);
                    process_messages(&mut conn_ctx, &msgs).await;
                }
                Err(ref e) if e.kind() == WouldBlock => {
                    continue;
                }
                Err(e) => {
                    break;
                }
            }
        }

        if ready.is_writable() && !must_read {
            match sock.try_write(b"hello world\n") {
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

    println!("Socket closing for",);
    Ok(())
}

pub async fn broker_main(cfg: BrokerConfig) -> Result<(), Box<dyn Error + Sync + Send>> {
    let (ctrl_tx, ctrl_rx) = channel(32);
    tokio::spawn(ctrl_main(ctrl_rx));

    let listener = TcpListener::bind(&cfg.bind_addr).await?;
    loop {
        let (sock, addr) = listener.accept().await?;
        tokio::spawn(process_connection(sock, addr, ctrl_tx.clone()));
    }
}