use std::error::Error;
use std::sync::{Arc};
use log::{error, info};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener};
use tokio::{select};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use crate::AppCtx;
use crate::broker::broker_types::ConnectionCtx;
use crate::broker::connection::connection_main;
use crate::control::message_types::{ControlMessages};
use crate::control::message_types::ControlMessages::{BrokerReady, MustDie};
use crate::error::{raderr};
use crate::utils::rad_utils::{send_must_die};

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

pub fn broker_init() -> (Sender<ControlMessages>, Receiver<ControlMessages>) {
    channel(32)
}

async fn accept_connection(listener: &TcpListener, ctrl_tx: Sender<ControlMessages>) -> Result<(), Box<dyn Error + Sync + Send>> {
    let res = listener.accept().await;
    if let Err(e) = &res {
        let msg = format!("Error accepting connection: {}. Aborting", &e);
        error!("{}", &msg);
        let _ = ctrl_tx.send(MustDie(msg.to_string())).await;
        return raderr(msg);
    }


    let (sock, addr) = res.unwrap();
    tokio::spawn(connection_main(sock,
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

    if let MustDie(msg) = msg {
        info!("Broker caught must_die flag ({}). Exiting", msg);
        return true;
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
            send_must_die(ctrl_tx.clone(), &msg).await;
            return;
        }
    };

    if ctrl_tx.send(BrokerReady).await.is_err() {
        send_must_die(ctrl_tx.clone(), "Error sending broker ready message").await;
    }

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