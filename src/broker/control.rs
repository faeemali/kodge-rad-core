use std::collections::HashMap;
use std::error::Error;
use log::{error, info, warn};
use tokio::sync::mpsc::{Receiver, Sender};
use crate::broker::control::ControlMessages::{DisconnectMessage};
use crate::broker::router::{AddRoutesReq, RouterControlMessages};
use crate::broker::router::RouterControlMessages::RemoveRoutes;

pub struct RegisterMessageReq {
    pub name: String,
    pub rx_msg_types: Vec<String>,
    pub tx_msg_types: Vec<String>,
    pub conn_tx: Sender<ControlMessages>,
}

pub enum ControlMessages {
    RegisterMessage(RegisterMessageReq),
    DisconnectMessage(String),
}

struct CtrlCtx {
    connections: HashMap<String, RegisterMessageReq>,
    router_ctrl_tx: Sender<RouterControlMessages>,
}

async fn remove_routes(ctx: &mut CtrlCtx, name: &str) -> Result<(), Box<dyn Error + Sync + Send>> {
    ctx.router_ctrl_tx.send(RemoveRoutes(name.to_string())).await?;
    Ok(())
}

async fn disconnect(ctx: &mut CtrlCtx, name: &str) -> Result<(), Box<dyn Error + Sync + Send>> {
    match ctx.connections.get(name) {
        Some(conn) => {
            info!("Disconnecting connection: {}", name);
            conn.conn_tx.send(DisconnectMessage(name.to_string())).await?;
            remove_routes(ctx, name).await?;
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

    let name = new_conn.name.to_string();
    ctx.connections.insert(name.clone(), new_conn);
    info!("Registered connection: {}", name);
    
    ctx.router_ctrl_tx.send(RouterControlMessages::AddRoutes(AddRoutesReq {
        /* TODO ??? */    
    })).await?;
    
    Ok(())
}

pub async fn ctrl_main(rx: Receiver<ControlMessages>,
                        router_ctrl_tx: Sender<RouterControlMessages>) {
    info!("Broker command receiver running");

    let mut ctx = CtrlCtx {
      connections: HashMap::new(),
        router_ctrl_tx,
    };

    let mut m_rx = rx;
    loop {
        let msg_opt = m_rx.recv().await;
        match msg_opt {
            Some(m) => {
                match m {
                    ControlMessages::RegisterMessage(new_conn) => {
                        if let Err(e) = register_connection(&mut ctx, new_conn).await {
                            error!("Error registering connection: {}", &e);
                            break;
                        }
                    }

                    DisconnectMessage(name) => {
                        if let Err(e) = ctx.router_ctrl_tx.send(RemoveRoutes(name.clone())).await {
                            error!("Error notifying router to remove routes for: {}. Error: {}", name, &e);
                            break;
                        }
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
