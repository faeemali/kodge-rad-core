use std::collections::HashMap;
use std::error::Error;
use log::{debug, error, info, warn};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use crate::control::message_types::{ControlMessages, RegisterMessageReq};
use crate::control::control_plane::ControlMessages::DisconnectMessage;
use crate::control::message_types::RegisterConnectionReq;
use crate::control::message_types::ControlMessages::RemoveRoutes;


/* data shared between the control plane and the connection */
pub struct ControlConnData {
    pub name: String,
    pub rx_msg_types: Vec<String>,
    pub tx_msg_types: Vec<String>,
    pub conn_ctrl_tx: Sender<ControlMessages>,
}

pub struct CtrlCtx {
    connections: HashMap<String, ControlConnData>,
    router_tx: Sender<ControlMessages>,
    app_runner_tx: Sender<ControlMessages>,
    broker_tx: Sender<ControlMessages>,
}

impl CtrlCtx {
    pub fn new(router_tx: Sender<ControlMessages>,
               app_runner_tx: Sender<ControlMessages>,
               broker_tx: Sender<ControlMessages>) -> Self {
        CtrlCtx {
            connections: HashMap::new(),
            router_tx,
            app_runner_tx,
            broker_tx,
        }
    }
}

pub fn control_init() -> (Sender<ControlMessages>, Receiver<ControlMessages>) {
    channel(32)
}

async fn remove_routes(ctx: &mut CtrlCtx, name: &str) -> Result<(), Box<dyn Error + Sync + Send>> {
    ctx.router_tx.send(RemoveRoutes(name.to_string())).await?;
    Ok(())
}

async fn disconnect(ctx: &mut CtrlCtx, name: &str) -> Result<(), Box<dyn Error + Sync + Send>> {
    match ctx.connections.get(name) {
        Some(conn) => {
            info!("Disconnecting connection: {}", name);
            conn.conn_ctrl_tx.send(DisconnectMessage(name.to_string())).await?;
            remove_routes(ctx, name).await?;
        }
        None => {
            return Ok(());
        }
    }

    ctx.connections.remove(name);

    Ok(())
}

async fn register_connection(ctx: &mut CtrlCtx,
                             new_conn: RegisterMessageReq)
                             -> Result<(), Box<dyn Error + Sync + Send>> {
    let key = new_conn.data.name.trim();
    if ctx.connections.contains_key(key) {
        /* send disconnect messages */
        disconnect(ctx, key).await?;
    }

    let name = new_conn.data.name.to_string();
    ctx.connections.insert(name.clone(), new_conn.data);
    info!("Registered connection: {}", name);

    ctx.router_tx.send(ControlMessages::RegisterConnection(RegisterConnectionReq {
        name,
        conn_tx: new_conn.conn_router_tx,
    })).await?;

    Ok(())
}

pub async fn ctrl_main(ctx: CtrlCtx,
                       rx: Receiver<ControlMessages>) {
    info!("Broker command receiver running");

    let mut ctx = ctx;
    let mut rx = rx;
    loop {
        /* todo mandle must die */
        let msg_opt = rx.recv().await;
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
                        debug!("Disconnecting {} from control plane", name);
                        ctx.connections.remove(&name);

                        if let Err(e) = ctx.router_tx.send(RemoveRoutes(name.clone())).await {
                            error!("Error notifying router to remove routes for: {}. Error: {}", name, &e);
                            break;
                        }
                    }

                    _ => {
                        todo!();
                    }
                }
            }
            None => {
                break;
            }
        } //match
    }

    warn!("Broker receiver closed. Aborting");
    /* todo handle must die */
}
