use std::error::Error;
use std::io::ErrorKind::WouldBlock;
use std::net::SocketAddr;
use std::sync::{Arc};
use std::time::Duration;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::io::{Interest, Ready};
use tokio::net::{TcpListener, TcpStream};
use tokio::{select, time};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time::sleep;
use crate::AppCtx;
use crate::broker::auth::{authenticate};
use crate::broker::auth_types::{AuthMessageReq, MSG_TYPE_AUTH};
use crate::broker::app_broker::States::{AuthenticateAndRegister, Process, WaitForRegistrationResponse};
use crate::broker::protocol::{Message, MessageHeader, Protocol};
use crate::control::message_types::ControlMessages::{AppExit, Disconnected, NewConnection, NewMessage, RegisterMessage};
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
    AuthenticateAndRegister,
    WaitForRegistrationResponse,
    Process,
}

async fn authenticate_client(conn_ctx: &mut ConnectionCtx, msg: &Message) -> Result<(), Box<dyn Error + Sync + Send>> {
    if msg.header.msg_type != MSG_TYPE_AUTH {
        return raderr("Invalid message for authentication");
    }

    let auth_msg = serde_json::from_slice::<AuthMessageReq>(msg.body.as_slice())?;

    if authenticate(&auth_msg).await? {
        conn_ctx.auth_message = Some(auth_msg);
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

        conn_ctx.ctrl_tx.send(RegisterMessage((conn_ctx.addr, reg))).await?;
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
    pub instance_id: Option<String>,
    pub protocol: Protocol,
    pub ctrl_tx: Sender<ControlMessages>,
}

async fn read_socket_data(conn_ctx: &mut ConnectionCtx,
                          sock: &mut TcpStream,
                          state: &mut States,
                          conn_rx: &mut Receiver<ControlMessages>)
                          -> Result<bool, Box<dyn Error + Sync + Send>> {
    let mut busy = false;
    let mut data = [0u8; 1024];
    match sock.try_read(&mut data) {
        Ok(n) => {
            if n == 0 {
                return raderr("Socket connection closed");
            }

            //debug!("Broker: read {} bytes", n);
            busy = true;
            let msgs = conn_ctx.protocol.feed(&data);
            if msgs.is_empty() {
                return Ok(false);
            }
            //println!("broker read {} messages: msg0={:?}", msgs.len(), &msgs[0]);

            match state {
                AuthenticateAndRegister => {
                    if authenticate_client(conn_ctx, &msgs[0]).await.is_err() {
                        return raderr("Authentication error");
                    }

                    if register_client(conn_ctx).await.is_err() {
                        return raderr("Registration error");
                    }

                    *state = WaitForRegistrationResponse;
                }
                
                WaitForRegistrationResponse => {
                }

                Process => {
                    let process_res = process_messages(conn_ctx, conn_rx, msgs).await;
                    match process_res {
                        Ok(must_continue) => {
                            if !must_continue {
                                return raderr("Must disconnect. Aborting connecting processing");
                            }
                        }
                        Err(e) => {
                            return raderr(format!("Error processing messages: {}", &e).as_str());
                        }
                    }
                } //process
            }
        }
        Err(ref e) if e.kind() == WouldBlock => {}
        Err(e) => {
            return raderr(format!("read error: {}", &e).as_str());
        }
    }

    Ok(busy)
}

async fn write_socket_data(sock: &mut TcpStream, msgs: &mut Vec<Message>) -> Result<bool, Box<dyn Error + Sync + Send>> {
    if msgs.is_empty() {
        return Ok(false);
    }

    //write all messages to the socket
    for j in 0..msgs.len() {
        let msg = &msgs[j];

        /* TODO: finish/fix me */
        let encoded_msg_res = Protocol::format(&msg);
        if let Err(e) = encoded_msg_res {
            debug!("Error encoding message: {}. Ignoring.", &e);
            return Ok(false);
        }
        let encoded_msg = encoded_msg_res?;
        let encoded_slice = encoded_msg.as_slice();

        //debug!("Broker writing: {:?}", &encoded_slice);

        let mut pos = 0usize;
        loop {
            /* todo consider replacing this with write_all() */
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
                    return raderr(format!("Write error detected. Aborting. Error: {}", &e).as_str());
                }
            }
        }
    }

    /* cleanup */
    msgs.clear();

    Ok(true)
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
        instance_id: None,
        protocol,
        ctrl_tx, //to send messages to the control plane
    };

    let mut tx_msgs = vec![];
    let mut done = false;
    let mut state = AuthenticateAndRegister;
    while !done {
        if conn_ctx.auth_timer.timed_out() && state == AuthenticateAndRegister {
            error!("Closing connection. Authentication timeout");
            break;
        }

        /* read messages from the control plane */
        match conn_rx.try_recv() {
            Ok(m) => {
                match m {
                    AppExit(s) => {
                        let mut m = format!("Connection caught must die signal. {}", &s);
                        if let Some(instance_id) = &conn_ctx.instance_id {
                            m.push_str(format!(" instance_id={}", instance_id).as_str());
                        }
                        error!("{}", m.as_str());
                        done = true;
                        continue;
                    }

                    NewMessage(msg) => {
                        tx_msgs.push(msg);
                    }
                    
                    /* todo listen for a registration success/failure here */

                    _ => {}
                }
            }

            Err(e) => {
                if e == TryRecvError::Disconnected {
                    /* don't send anything to the control plane because it's disconnected */
                    let msg = "Control plane disconnected. Aborting connection processing";
                    error!("{}", msg);
                    done = true;
                    continue;
                } //else ignore if empty
            }
        }

        let ready = sock.ready(Interest::WRITABLE | Interest::READABLE).await?;

        if ready.is_error() {
            error!("Error determining socket readiness. Aborting connection");
            break;
        }

        if ready.is_read_closed() || ready.is_write_closed() {
            error!("Reader/writer closed. Aborting connection");
            break;
        }

        let read_busy = if ready.is_readable() {
            match read_socket_data(&mut conn_ctx, &mut sock, &mut state, &mut conn_rx).await {
                Ok(b) => {
                    b
                }
                Err(e) => {
                    error!("Error reading socket data: {}", e);
                    done = true;
                    continue;
                }
            }
        } else {
            false
        };

        let write_busy = if ready.is_writable() {
            match write_socket_data(&mut sock, &mut tx_msgs).await {
                Ok(b) => b,
                Err(e) => {
                    error!("Error writing socket data: {}", e);
                    done = true;
                    continue;
                }
            }
        } else {
            false
        };

        if !read_busy && !write_busy {
            sleep(Duration::from_millis(10)).await;
        }
    } //loop

    conn_ctx.ctrl_tx.send(Disconnected(addr)).await?;

    Ok(())
}

pub fn broker_init() -> (Sender<ControlMessages>, Receiver<ControlMessages>) {
    channel(32)
}

async fn accept_connection(listener: &TcpListener, ctrl_tx: Sender<ControlMessages>) -> Result<(), Box<dyn Error + Sync + Send>> {
    let res = listener.accept().await;
    if let Err(e) = &res {
        let msg = format!("Error accepting connection: {}. Aborting", &e);
        error!("{}", &msg);
        let _ = ctrl_tx.send(AppExit(msg.to_string())).await;
        return raderr(msg);
    }


    let (sock, addr) = res.unwrap();
    tokio::spawn(process_connection(sock,
                                    addr,
                                    ctrl_tx.clone()));
    Ok(())
}

async fn handle_ctrl_messages(broker_rx: &mut Receiver<ControlMessages>) -> bool {
    let msg_opt = broker_rx.recv().await;
    let msg = if let Some(m) = &msg_opt {
        m
    } else {
        return false;
    };

    match msg {
        AppExit(s) => {
            info!("Broker caught must_die flag. Exiting");
            return true;
        }
        _ => {}
    }

    false
}

///start the broker, which includes the socket listener, router, and control plane
pub async fn broker_main(app_ctx: Arc<AppCtx>,
                         broker_rx: Receiver<ControlMessages>,
                         ctrl_tx: Sender<ControlMessages>) {
    info!("Listening on {}", &app_ctx.config.broker.bind_addr);
    let listener = match TcpListener::bind(&app_ctx.config.broker.bind_addr).await {
        Ok(listener) => listener,
        Err(e) => {
            let msg = format!("Error starting tcp listener: {}", &e);
            error!("{}", &msg);
            let _ = ctrl_tx.send(ControlMessages::AppExit(msg.to_string())).await;
            return;
        }
    };

    let mut broker_rx = broker_rx;
    let mut done = false;
    while !done {
        println!("looping");
        select! {
            accept_res = accept_connection(&listener, ctrl_tx.clone()) => {
                if let Err(e) = accept_res {
                    println!("Accept error: {}", e);
                }
            }

            must_exit = handle_ctrl_messages(&mut broker_rx) => {
                if must_exit {
                    done = true;
                }
            }
        } //select
    } //while

    info!("Broker cleanup in progress");
    drop(listener);
}