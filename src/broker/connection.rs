use std::error::Error;
use std::io::ErrorKind::WouldBlock;
use std::net::SocketAddr;
use std::time::Duration;
use log::{debug, error, info, warn};
use tokio::io::Interest;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time::sleep;
use crate::broker::auth::authenticate;
use crate::broker::auth_types::{AuthMessageReq, MSG_TYPE_AUTH};
use crate::broker::broker_types::{ConnectionCtx, States};
use crate::broker::broker_types::States::{WaitForRegistrationResponse, Process, AuthenticateAndRegister};
use crate::broker::protocol::{Message, Protocol};
use crate::control::message_types::{ControlMessages, RegisterMessageReq};
use crate::control::message_types::ControlMessages::{Disconnected, MustDie, NewConnection, NewMessage, RegisterMessage, Registered};
use crate::error::raderr;
use crate::utils::rad_utils::get_value_or_unknown;
use crate::utils::timer::Timer;

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

async fn read_socket_data(conn_ctx: &mut ConnectionCtx,
                          sock: &mut TcpStream,
                          state: &mut States)
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

                WaitForRegistrationResponse => {}

                Process => {
                    if !msgs.is_empty() {
                        /* send received messages to control plane */
                        for msg in msgs {
                            if conn_ctx.ctrl_tx.send(NewMessage(msg)).await.is_err() {
                                let instance_id = get_value_or_unknown(&conn_ctx.instance_id);
                                panic!("Error sending message to control plane for {}", &instance_id);
                            }
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

        let encoded_msg_res = Protocol::format(msg);
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

// process 1 connection only
pub async fn connection_main(mut sock: TcpStream,
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
                    MustDie(s) => {
                        let mut m = format!("Connection caught must die signal. {}", &s);
                        if let Some(instance_id) = &conn_ctx.instance_id {
                            m.push_str(format!(" instance_id={}", instance_id).as_str());
                        }
                        error!("{}", m.as_str());
                        done = true;
                        continue;
                    }

                    Registered => {
                        let instance_id = get_value_or_unknown(&conn_ctx.instance_id);
                        info!("{} registered. Moving to the process state.", instance_id);
                        state = Process
                    }

                    NewMessage(msg) => {
                        /* track messages to be sent back to the client */
                        tx_msgs.push(msg);
                    }

                    Disconnected(addr ) => {
                        let instance_id = get_value_or_unknown(&conn_ctx.instance_id);
                        warn!("Disconnected message received for {} ({}). Closing connection", addr, instance_id);
                        done = true;
                        continue;
                    }

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
            match read_socket_data(&mut conn_ctx, &mut sock, &mut state).await {
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
