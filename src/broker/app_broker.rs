use std::error::Error;
use std::io::ErrorKind::WouldBlock;
use std::net::SocketAddr;
use std::sync::{Arc};
use std::time::Duration;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::io::{Interest};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time::sleep;
use crate::AppCtx;
use crate::broker::auth::{authenticate};
use crate::broker::auth_types::{AuthMessageReq, MSG_TYPE_AUTH};
use crate::broker::app_broker::States::{Authenticate, Process, Register};
use crate::broker::protocol::{Message, MessageHeader, Protocol};
use crate::control::message_types::ControlMessages::{Disconnected, NewConnection, RegisterMessage};
use crate::control::message_types::{ControlMessages, RegisterMessageReq};
use crate::error::{raderr};
use crate::utils::timer::Timer;
use crate::utils::rad_utils;

#[derive(Serialize, Deserialize, Clone)]
pub struct BrokerConfig {
    pub bind_addr: String,
}

impl BrokerConfig {
    pub fn new(bind_addr: String) -> Self {
        Self {
            bind_addr,
        }
    }
}

#[derive(Eq, PartialEq)]
enum States {
    Authenticate,
    Register,
    Process,
}

async fn authenticate_client(conn_ctx: &mut ConnectionCtx, msg: &Message) -> Result<(), Box<dyn Error + Sync + Send>> {
    if msg.header.msg_type != MSG_TYPE_AUTH {
        return raderr("Invalid message for authentication");
    }

    let auth_msg = serde_json::from_slice::<AuthMessageReq>(msg.body.as_slice())?;

    if authenticate(&auth_msg).await? {
        conn_ctx.auth_message = Some(auth_msg);
        conn_ctx.state = Register;
        Ok(())
    } else {
        raderr("Authentication error")
    }
}

/* send a registration message to the control plane */
async fn register_client(conn_ctx: &mut ConnectionCtx) -> Result<(), Box<dyn Error + Sync + Send>> {
    //must register with the control system
    if let Some(r) = &conn_ctx.auth_message {
        let reg = RegisterMessageReq {
            instance_id: r.name.clone(),
            rx_msg_types: r.rx_msg_types.clone(),
            tx_msg_types: r.tx_msg_types.clone(),
        };
        conn_ctx.instance_id = Some(r.name.clone());

        conn_ctx.ctrl_tx.send(RegisterMessage((conn_ctx.addr.clone(), reg))).await?;
        Ok(())
    } else {
        raderr("Failed to get register details (unexpected)")
    }
}

async fn process_messages(conn_ctx: &mut ConnectionCtx,
                          conn_rx: &mut Receiver<ControlMessages>,
                          msgs: Vec<Message>)
                          -> Result<bool, Box<dyn Error + Sync + Send>> {
    /* TODO: check for messages from the control plane */
    

    /* TODO: pass all messages to the appropriate connection */
    
    Ok(true)
}

struct ConnectionCtx {
    pub addr: SocketAddr,
    //timer for authentication
    pub auth_timer: Timer,
    pub auth_message: Option<AuthMessageReq>,
    pub state: States,
    pub instance_id: Option<String>,
    pub protocol: Protocol,
    pub ctrl_tx: Sender<ControlMessages>,
}

async fn process_connection(mut sock: TcpStream,
                            addr: SocketAddr,
                            ctrl_tx: Sender<ControlMessages>)
                            -> Result<(), Box<dyn Error + Sync + Send>> {
    info!("broker accepted connection from: {}", addr);

    let protocol = Protocol::new()?;

    //create a means for the control plane to send back messages
    let (conn_tx, mut conn_rx) = channel::<ControlMessages>(32);
    ctrl_tx.send(NewConnection((addr, conn_tx))).await?;

    let mut conn_ctx = ConnectionCtx {
        addr,
        auth_timer: Timer::new(Duration::from_millis(10000)),
        auth_message: None,
        state: Authenticate,
        instance_id: None,
        protocol,
        ctrl_tx, //to send messages to the control plane
    };

    let mut done = false;
    while !done {
        if conn_ctx.auth_timer.timed_out() && conn_ctx.state == Authenticate {
            error!("Closing connection. Authentication timeout");
            break;
        }

        /* todo handle must die */

        let mut busy = false;

        let ready = sock.ready(Interest::WRITABLE | Interest::READABLE).await?;

        if ready.is_error() {
            error!("Error determining socket readiness. Aborting connection");
            break;
        }

        if ready.is_read_closed() || ready.is_write_closed() {
            error!("Reader/writer closed. Aborting connection");
            break;
        }

        if ready.is_readable() {
            //println!("readable");

            let mut data = [0u8; 1024];
            match sock.try_read(&mut data) {
                Ok(n) => {
                    if n == 0 {
                        warn!("Socket connection closed");
                        break;
                    }

                    //debug!("Broker: read {} bytes", n);
                    busy = true;
                    let msgs = conn_ctx.protocol.feed(&data);
                    if msgs.is_empty() {
                        continue;
                    }

                    match conn_ctx.state {
                        Authenticate => {
                            if authenticate_client(&mut conn_ctx, &msgs[0]).await.is_err() {
                                done = true;
                                continue;
                            }
                            conn_ctx.state = Register;
                        }
                        Register => {
                            if register_client(&mut conn_ctx).await.is_err() {
                                done = true;
                                continue;
                            }

                            /* TODO wait for register response or the client will not have any response */
                            conn_ctx.state = Process;
                        }
                        Process => {
                            let process_res = process_messages(&mut conn_ctx, &mut conn_rx, msgs).await;
                            match process_res {
                                Ok(must_continue) => {
                                    if !must_continue {
                                        info!("Must disconnect. Aborting connecting processing");
                                        done = true;
                                        break;
                                    }
                                }
                                Err(e) => {
                                    error!("Error processing messages: {}", &e);
                                }
                            }
                        } //process
                    }
                }
                Err(ref e) if e.kind() == WouldBlock => {}
                Err(e) => {
                    error!("Read error: {}", &e);
                    done = true;
                }
            }
        }

        if ready.is_writable() {
            //println!("writable");

            /* must read messages from the control plane and forward to the app */
            match conn_rx.try_recv() {
                Ok(msg) => {
                    /* TODO: finish/fix me */
                    // busy = true;
                    // let encoded_msg_res = Protocol::format(&msg);
                    // if let Err(e) = encoded_msg_res {
                    //     debug!("Error encoding message: {}. Ignoring.", &e);
                    //     continue;
                    // }
                    // let encoded_msg = encoded_msg_res.unwrap();
                    // let encoded_slice = encoded_msg.as_slice();
                    // 
                    // //debug!("Broker writing: {:?}", &encoded_slice);
                    // 
                    // let mut pos = 0usize;
                    // loop {
                    //     let res = sock.try_write(&encoded_slice[pos..]);
                    //     match res {
                    //         Ok(n) => {
                    //             pos = n;
                    //             if pos == encoded_slice.len() {
                    //                 break; //break out of the write loop
                    //             }
                    //         }
                    //         Err(ref e) if e.kind() == WouldBlock => {
                    //             continue;
                    //         }
                    // 
                    //         Err(e) => {
                    //             error!("Write error detected. Aborting. Error: {}", &e);
                    //             break;
                    //         }
                    //     }
                    // }
                }

                Err(TryRecvError::Empty) => {
                    //println!("no messages from router");
                }
                Err(TryRecvError::Disconnected) => {
                    error!("Router channel closed. Aborting");
                    break;
                }
            }
        }

        if !busy {
            sleep(Duration::from_millis(10)).await;
        }
    } //loop

    let name = rad_utils::get_value_or_unknown(&conn_ctx.instance_id);
    conn_ctx.ctrl_tx.send(Disconnected(addr)).await?;

    Ok(())
}

pub fn broker_init() -> (Sender<ControlMessages>, Receiver<ControlMessages>) {
    channel(32)
}

///start the broker, which includes the socket listener, router, and control plane
pub async fn broker_main(app_ctx: Arc<AppCtx>,
                         broker_rx: Receiver<ControlMessages>,
                         ctrl_tx: Sender<ControlMessages>) {
    let listener = match TcpListener::bind(&app_ctx.config.broker.bind_addr).await {
        Ok(listener) => listener,
        Err(e) => {
            let msg = format!("Error starting tcp listener: {}", &e);
            error!("{}", &msg);
            /* todo handle must die */
            panic!("{}", msg);
        }
    };

    loop {
        let mut must_sleep = false;
        select! {
            res = listener.accept() => {
                if let Err(e) = &res {
                    let msg = format!("Error accepting connection: {}. Aborting", &e);
                    error!("{}", msg);

                    /* todo handle must die */
                    //return?
                }

                let (sock, addr) = res.unwrap();
                        tokio::spawn(process_connection(sock,
                                        addr,
                                        ctrl_tx.clone()));
            }


            /* todo handle must die */
        } //select

        if must_sleep {
            sleep(Duration::from_millis(10)).await;
        }
    }
}