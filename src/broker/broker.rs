use std::error::Error;
use std::io::ErrorKind::WouldBlock;
use std::net::SocketAddr;
use std::time::Duration;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, Interest};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time::sleep;
use crate::broker::auth::{authenticate, AuthMessage, MSG_TYPE_AUTH};
use crate::broker::broker::Actions::{MustDisconnect, NoAction};
use crate::broker::broker::States::{Authenticate, Process, Register};
use crate::broker::control::{ControlConnData, ControlMessages, ctrl_main, RegisterMessageReq};
use crate::broker::control::ControlMessages::RegisterMessage;
use crate::broker::protocol::{Message, Protocol};
use crate::broker::router::router_main;
use crate::error::RadError;

#[derive(Serialize, Deserialize, Clone)]
pub struct BrokerConfig {
    pub bind_addr: String,
}

#[derive(Eq, PartialEq)]
enum States {
    Authenticate,
    Register,
    Process,
}

#[derive(Eq, PartialEq)]
enum Actions {
    MustDisconnect,
    NoAction,
}

async fn process_control_messages(conn_ctx: &mut ConnectionCtx) -> Result<Actions, Box<dyn Error + Sync + Send>> {
    if let Some(ctrl_rx) = &mut conn_ctx.ctrl_rx {
        match ctrl_rx.try_recv() {
            Ok(msg) => {
                match &msg {
                    ControlMessages::DisconnectMessage(m) => {
                        return Ok(MustDisconnect);
                    }

                    _ => {
                        //ignore
                    }
                }
            }
            Err(e) => {
                if e == TryRecvError::Empty {} else if e == TryRecvError::Disconnected {
                    return Err(Box::new(e));
                }
            }
        }

        Ok(NoAction)
    } else {
        Err(Box::new(RadError::from("Unexpected error. ctrl_rx is None")))
    }
}

async fn process_messages(conn_ctx: &mut ConnectionCtx,
                          msgs: &[Message])
                          -> Result<bool, Box<dyn Error + Sync + Send>> {
    let mut register_details_opt = None;
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
                    register_details_opt = Some(auth_msg);
                    conn_ctx.state = Register;
                    continue;
                } else {
                    return Err(Box::new(RadError::from("Authentication error")));
                }
            }

            Register => {
                //must register with the control system
                match &register_details_opt {
                    Some(r) => {
                        /* create a means for the control plane to communicate back */
                        let (conn_tx, ctrl_rx) = channel(32);

                        /* create a means for the router to send back messages */
                        let (conn_router_tx, router_rx) = channel(32);

                        let reg = RegisterMessageReq {
                            data: ControlConnData {
                                conn_ctrl_tx: conn_tx, //for control plane to send messages to connection
                                name: r.name.clone(),
                                rx_msg_types: r.rx_msg_types.clone(),
                                tx_msg_types: r.tx_msg_types.clone(),
                            },
                            conn_router_tx, //for router to send messages to connection
                        };
                        register_details_opt = None;
                        conn_ctx.ctrl_rx = Some(ctrl_rx);
                        conn_ctx.router_rx = Some(router_rx);

                        conn_ctx.ctrl_tx.send(RegisterMessage(reg)).await?;
                        conn_ctx.state = Process;
                    }
                    None => {
                        return Err(Box::new(RadError::from("Failed to get register details (unexpected)")));
                    }
                }
            }

            Process => {
                /* check for messages from the control plane */
                let action = process_control_messages(conn_ctx).await?;
                if action == MustDisconnect {
                    let name = match &conn_ctx.name {
                        Some(n) => { n }
                        None => "[unknown]",
                    };
                    info!("Received disconnect message for {}. Disconnecting.", name);
                    return Ok(false);
                }
            }
        }

        //TODO: do this only when there's no activity
        sleep(Duration::from_millis(1)).await;
    }
}

struct ConnectionCtx {
    pub state: States,
    pub name: Option<String>,
    pub protocol: Protocol,
    pub ctrl_tx: Sender<ControlMessages>, //send messages to the control plane
    pub ctrl_rx: Option<Receiver<ControlMessages>>, //receive messages from the control plane
    pub router_tx: Sender<Message>, //send messages to router
    pub router_rx: Option<Receiver<Message>>, //for the router to send messages to the connection
}

async fn process_connection(sock: TcpStream,
                            addr: SocketAddr,
                            ctrl_tx: Sender<ControlMessages>,
                            router_tx: Sender<Message>)
                            -> Result<(), Box<dyn Error + Sync + Send>> {
    info!("broker accepted connection from: {}", addr);

    let protocol = Protocol::new()?;

    let mut conn_ctx = ConnectionCtx {
        state: Authenticate,
        name: None,
        protocol,
        ctrl_tx,
        ctrl_rx: None,
        router_tx,
        router_rx: None,
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
                    let process_res = process_messages(&mut conn_ctx, &msgs).await;
                    match process_res {
                        Ok(must_continue) => {
                            if !must_continue {
                                info!("Must disconnect. Aborting connecting processing");
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Error processing messages: {}", &e);
                            break;
                        }
                    }
                }
                Err(ref e) if e.kind() == WouldBlock => {
                    continue;
                }
                Err(e) => {
                    error!("Read error: {}", &e);
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

    println!("Socket closing for", );
    Ok(())
}

pub async fn broker_main(cfg: BrokerConfig) -> Result<(), Box<dyn Error + Sync + Send>> {
    let (router_ctrl_tx, router_ctrl_rx) = channel(32);
    let (router_conn_tx, router_conn_rx) = channel(32);

    tokio::spawn(router_main(router_ctrl_rx, router_conn_rx));

    let (ctrl_tx, ctrl_rx) = channel(32);
    tokio::spawn(ctrl_main(ctrl_rx, router_ctrl_tx.clone()));

    let listener = TcpListener::bind(&cfg.bind_addr).await?;
    loop {
        let (sock, addr) = listener.accept().await?;
        tokio::spawn(process_connection(sock, addr,
                                        ctrl_tx.clone(), //for sending messages to the control plane
                                        router_conn_tx.clone())); //for sending messages to the router
    }
}