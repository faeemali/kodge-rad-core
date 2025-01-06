use std::error::Error;
use std::io::ErrorKind::WouldBlock;
use std::net::SocketAddr;
use std::sync::{Arc};
use std::time::Duration;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncWriteExt, Interest};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::RwLock;
use tokio::time::sleep;
use crate::broker::auth::{authenticate};
use crate::broker::auth_types::{AuthMessageReq, AuthMessageResp, MSG_TYPE_AUTH, MSG_TYPE_AUTH_RESP};
use crate::broker::app_broker::Actions::{MustDisconnect, NoAction};
use crate::broker::app_broker::States::{Authenticate, Process, Register};
use crate::broker::control::{ControlConnData, ControlMessages, ctrl_main, RegisterMessageReq};
use crate::broker::control::ControlMessages::{DisconnectMessage, RegisterMessage};
use crate::broker::protocol::{Message, MessageHeader, Protocol};
use crate::broker::router::router_main;
use crate::config::config::Config;
use crate::error::{raderr};
use crate::utils::timer::Timer;
use crate::utils::utils;

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

#[derive(Eq, PartialEq)]
enum Actions {
    MustDisconnect,
    NoAction,
}

async fn __get_control_message(conn_ctx: &mut ConnectionCtx) -> Result<Actions, Box<dyn Error + Sync + Send>> {
    if let Some(ctrl_rx) = &mut conn_ctx.ctrl_rx {
        match ctrl_rx.try_recv() {
            Ok(msg) => {
                match &msg {
                    DisconnectMessage(m) => {
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
        raderr("Unexpected error. ctrl_rx is None")
    }
}

async fn __authenticate_client(conn_ctx: &mut ConnectionCtx, msgs: &[Message]) -> Result<(), Box<dyn Error + Sync + Send>> {
    if msgs.len() != 1 {
        return raderr("Invalid number of messages for authentication");
    }

    let auth_msg_wrapper = &msgs[0];
    if auth_msg_wrapper.header.msg_type != MSG_TYPE_AUTH {
        return raderr("Invalid message for authentication");
    }

    let auth_msg = serde_json::from_slice::<AuthMessageReq>(auth_msg_wrapper.body.as_slice())?;

    if authenticate(&auth_msg).await? {
        conn_ctx.auth_message = Some(auth_msg);
        conn_ctx.state = Register;
        Ok(())
    } else {
        raderr("Authentication error")
    }
}

async fn __register_client(conn_ctx: &mut ConnectionCtx) -> Result<(), Box<dyn Error + Sync + Send>> {
    //must register with the control system
    if let Some(r) = &conn_ctx.auth_message {
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

        conn_ctx.name = Some(r.name.clone());
        conn_ctx.ctrl_rx = Some(ctrl_rx);
        conn_ctx.router_rx = Some(router_rx);

        conn_ctx.ctrl_tx.send(RegisterMessage(reg)).await?;
        conn_ctx.state = Process;

        Ok(())
    } else {
        raderr("Failed to get register details (unexpected)")
    }
}

async fn register_connection(conn_ctx: &mut ConnectionCtx,
                             msgs: Vec<Message>) -> Result<AuthMessageResp, Box<dyn Error + Sync + Send>> {
    if conn_ctx.state == Authenticate {
        __authenticate_client(conn_ctx, &msgs).await?;
    }
    debug!("Client successfully authenticated");

    if conn_ctx.state == Register {
        __register_client(conn_ctx).await?;
    }
    debug!("Client successfully registered");

    Ok(AuthMessageResp {
        success: true
    })
}

async fn process_messages(conn_ctx: &mut ConnectionCtx,
                          msgs: Vec<Message>)
                          -> Result<bool, Box<dyn Error + Sync + Send>> {
    /* check for messages from the control plane */
    let action = __get_control_message(conn_ctx).await?;
    if action == MustDisconnect {
        let name = match &conn_ctx.name {
            Some(n) => { n }
            None => "[unknown]",
        };
        info!("Received disconnect message for {}. Disconnecting.", name);
        return Ok(false);
    }

    /* pass all messages to the router */
    for msg in msgs {
        conn_ctx.router_tx.send(msg).await?;
    }

    Ok(true)
}

struct ConnectionCtx {
    //timer for authentication
    pub auth_timer: Timer,
    pub auth_message: Option<AuthMessageReq>,
    pub state: States,
    pub name: Option<String>,
    pub protocol: Protocol,
    pub ctrl_tx: Sender<ControlMessages>, //send messages to the control plane
    pub ctrl_rx: Option<Receiver<ControlMessages>>, //receive messages from the control plane
    pub router_tx: Sender<Message>, //send messages to router
    pub router_rx: Option<Receiver<Message>>, //for the router to send messages to the connection
}

async fn process_connection(mut sock: TcpStream,
                            addr: SocketAddr,
                            ctrl_tx: Sender<ControlMessages>,
                            router_tx: Sender<Message>,
                            am_must_die: Arc<RwLock<bool>>)
                            -> Result<(), Box<dyn Error + Sync + Send>> {
    info!("broker accepted connection from: {}", addr);

    let protocol = Protocol::new()?;

    let mut conn_ctx = ConnectionCtx {
        auth_timer: Timer::new(Duration::from_millis(10000)),
        auth_message: None,
        state: Authenticate,
        name: None,
        protocol,
        ctrl_tx,
        ctrl_rx: None,
        router_tx,
        router_rx: None,
    };

    let mut registered = false;
    loop {
        if conn_ctx.auth_timer.timed_out() && conn_ctx.state == Authenticate {
            error!("Closing connection. Authentication timeout");
            break;
        }

        if utils::get_must_die(am_must_die.clone()).await {
            warn!("Connection {} caught must die flag. Aborting.", utils::get_value_or_unknown(&conn_ctx.name));
            break;
        }


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
                    
                    //debug!("broker: after feed: num messages = {}", msgs.len());
                    if !registered {
                        match register_connection(&mut conn_ctx, msgs.clone()).await {
                            Ok(r) => {
                                let name = if let Some(n) = &conn_ctx.name {
                                    n.to_string()
                                } else {
                                    return raderr("Unexpected error: connection is none after registration");
                                };

                                let b = serde_json::to_vec(&r)?;
                                let msg = Message {
                                    header: MessageHeader {
                                        name,
                                        rks: None,
                                        rks_match_type: None,
                                        message_id: String::new(),
                                        msg_type: MSG_TYPE_AUTH_RESP.to_string(),
                                        length: b.len() as u32,
                                        extras: None,
                                    },
                                    body: b,
                                };
                                let bytes = Protocol::format(&msg)?;
                                sock.write_all(bytes.as_slice()).await?;
                                registered = true;
                            }
                            Err(e) => {
                                error!("Registration error: {}", &e);
                                break;
                            }
                        }
                        continue;
                    }

                    let process_res = process_messages(&mut conn_ctx, msgs).await;
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
                Err(ref e) if e.kind() == WouldBlock => {}
                Err(e) => {
                    error!("Read error: {}", &e);
                    break;
                }
            }
        }

        if ready.is_writable() {
            //println!("writable");

            /* must read messages from the router and forward to the app */
            if let Some(rx) = &mut conn_ctx.router_rx {
                match rx.try_recv() {
                    Ok(msg) => {
                        busy = true;
                        let encoded_msg_res = Protocol::format(&msg);
                        if let Err(e) = encoded_msg_res {
                            debug!("Error encoding message: {}. Ignoring.", &e);
                            continue;
                        }
                        let encoded_msg = encoded_msg_res.unwrap();
                        let encoded_slice = encoded_msg.as_slice();
                        
                        //debug!("Broker writing: {:?}", &encoded_slice);

                        let mut pos = 0usize;
                        loop {
                            let res = sock.try_write(&encoded_slice[pos..]);
                            match res {
                                Ok(n) => {
                                    pos = n;
                                    if pos == encoded_slice.len() {
                                        break; //break out of the write loop
                                    }
                                }
                                Err(ref e) if e.kind() == WouldBlock => {
                                    continue;
                                }

                                Err(e) => {
                                    error!("Write error detected. Aborting. Error: {}", &e);
                                    break;
                                }
                            }
                        }
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
        }

        if !busy {
            sleep(Duration::from_millis(10)).await;
        }
    } //loop

    let name = utils::get_value_or_unknown(&conn_ctx.name);
    info!("Socket closing for {}", &name);

    if let Some(name) = &conn_ctx.name {
        /* send a disconnect message to the control plane */
        conn_ctx.ctrl_tx.send(DisconnectMessage(name.to_string())).await?;
    }

    Ok(())
}

///start the broker, which includes the socket listener, router, and control plane
pub async fn start_broker(base_dir: String,
                         a_cfg: Arc<Config>,
                         am_must_die: Arc<RwLock<bool>>)
                         -> Result<(), Box<dyn Error + Sync + Send>> {
    let (router_ctrl_tx, router_ctrl_rx) = channel(32);
    let (router_conn_tx, router_conn_rx) = channel(32);

    tokio::spawn(router_main(base_dir,
                             a_cfg.clone(),
                             router_ctrl_rx,
                             router_conn_rx,
                             am_must_die.clone()));

    let (ctrl_tx, ctrl_rx) = channel(32);
    tokio::spawn(ctrl_main(ctrl_rx, router_ctrl_tx.clone(), am_must_die.clone()));

    let listener = TcpListener::bind(&a_cfg.broker.bind_addr).await?;
    loop {
        let mut must_sleep = false;
        select! {
            res = listener.accept() => {
                if let Err(e) = &res {
                    let msg = format!("Error accepting connection: {}. Aborting", &e);
                    error!("{}", msg);
                    utils::set_must_die(am_must_die.clone()).await;
                }
                let (sock, addr) = res.unwrap();
                        tokio::spawn(process_connection(sock,
                                        addr,
                                        ctrl_tx.clone(), //for sending messages to the control plane
                                        router_conn_tx.clone(),
                                        am_must_die.clone())); //for sending messages to the router
            }

            must_die = utils::get_must_die(am_must_die.clone()) => {
                if must_die {
                    warn!("Broker caught must die flag. Aborting");
                    return Ok(());
                }
                must_sleep = true;
            }
        } //select

        if must_sleep {
            sleep(Duration::from_millis(10)).await;
        }
    }
}