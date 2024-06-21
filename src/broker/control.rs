use std::collections::HashMap;
use std::error::Error;
use log::{info, warn};
use tokio::sync::mpsc::{Receiver, Sender};
use crate::broker::control::ControlMessages::DisconnectMessage;

pub struct RegisterMessageReq {
    pub name: String,
    pub rx_msg_types: Vec<String>,
    pub tx_msg_types: Vec<String>,
    pub conn_tx: Sender<ControlMessages>,
}

pub struct DisconnectMessageReq;

pub enum ControlMessages {
    RegisterMessage(RegisterMessageReq),
    DisconnectMessage(DisconnectMessageReq),
}

struct CtrlCtx {
    connections: HashMap<String, RegisterMessageReq>,
}

async fn disconnect(ctx: &mut CtrlCtx, name: &str) -> Result<(), Box<dyn Error + Sync + Send>> {
    match ctx.connections.get(name) {
        Some(conn) => {
            info!("Disconnecting connection: {}", name);
            conn.conn_tx.send(DisconnectMessage(DisconnectMessageReq{})).await?;
        }
        None => {
            return Ok(());
        }
    }

    ctx.connections.remove(name);

    Ok(())
}

async fn register_connection(ctx: &mut CtrlCtx, new_conn: RegisterMessageReq) -> Result<(), Box<dyn Error + Sync + Send>>{
    let key = new_conn.name.trim();
    if ctx.connections.contains_key(key) {
        /* send disconnect messages */
        disconnect(ctx, key).await?;
    }

    info!("Registered connection: {}", &new_conn.name);

    Ok(())
}

pub async fn ctrl_main(rx: Receiver<ControlMessages>) {
    info!("Broker command receiver running");

    let mut ctx = CtrlCtx {
      connections: HashMap::new(),
    };

    let mut m_rx = rx;
    loop {
        let msg_opt = m_rx.recv().await;
        match msg_opt {
            Some(m) => {
                match m {
                    ControlMessages::RegisterMessage(new_conn) => {
                        if let Err(e) = register_connection(&mut ctx, new_conn).await {
                            break;
                        }
                    }

                    DisconnectMessage(d) => {
                    }
                }
            }
            None => {
                break;
            }
        }

    }

    warn!("Broker receiver closed. Aborting");

    /* TODO: must abort application here */
}
