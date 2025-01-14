use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use log::{error, info, warn};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use crate::control::message_types::{ControlConn, ControlMessages, RegisterMessageReq};
use crate::error::raderr;
use crate::utils::rad_utils::get_datetime_as_utc_millis;


pub struct CtrlCtx {
    connections: HashMap<SocketAddr, ControlConn>,
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

fn disconnect(ctx: &mut CtrlCtx, key: &SocketAddr) -> Result<(), Box<dyn Error + Sync + Send>> {
    match ctx.connections.get(key) {
        Some(conn) => {
            /* todo fix me */
            //conn.conn_ctrl_tx.send(DisconnectMessage(name.to_string())).await?;

            /* todo: fixme send a message to the router to remove routes */
            //ctx.router_tx.send(RemoveRoutes(name.to_string())).await?;
        }
        None => {
            return Ok(());
        }
    }

    ctx.connections.remove(key);

    Ok(())
}

fn find_connection_by_instance_id<'a>(ctx: &'a CtrlCtx, instance_id: &str) -> Option<(&'a SocketAddr, &'a ControlConn)>{
    for key in ctx.connections.keys() {
        let value = ctx.connections.get(key);
        if value.is_none() {
            continue;
        }
        let value = value.unwrap();

        if let Some(d) = &value.data {
            if d.instance_id == instance_id {
                return Some((key, value));
            }
        } else {
            continue;
        }
    }
    None
}

fn find_connection_key_by_instance_id(ctx: &CtrlCtx, instance_id: &str) -> Option<(SocketAddr)>{
    for key in ctx.connections.keys() {
        let value = ctx.connections.get(key);
        if value.is_none() {
            continue;
        }
        let value = value.unwrap();

        if let Some(d) = &value.data {
            if d.instance_id == instance_id {
                return Some(key.clone());
            }
        } else {
            continue;
        }
    }
    None
}

//disconnects an existing connection if one exists
fn disconnect_existing_connection(ctx: &mut  CtrlCtx, instance_id: &str) -> Result<(), Box<dyn Error + Sync + Send>> {
    let kv_opt = find_connection_key_by_instance_id(ctx, instance_id);
    if let Some(key) = kv_opt {
        disconnect(ctx, &key)?;
    }
    
    Ok(())
}

async fn register_connection(ctx: &mut CtrlCtx,
                             addr: SocketAddr,
                             new_conn: RegisterMessageReq)
                             -> Result<(), Box<dyn Error + Sync + Send>> {
    let instance_id = new_conn.instance_id.trim();
    disconnect_existing_connection(ctx, instance_id)?;
    
    if let Some(val) = ctx.connections.get_mut(&addr) {
            val.data = Some(new_conn.clone());
    } else {
        return raderr("Unexpected error. Unable to register connection. Connection socket info does not exist");
    }
    info!("Registered connection: {}", instance_id);

    /* notify the router */
    ctx.router_tx.send(ControlMessages::RegisterConnection(new_conn)).await?;

    Ok(())
}

fn add_new_connection(ctx: &mut CtrlCtx, addr: SocketAddr, conn_tx: Sender<ControlMessages>) {
    ctx.connections.insert(addr, ControlConn {
        start_time: get_datetime_as_utc_millis(),
        conn_tx,
        data: None,
    });
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
                    ControlMessages::RegisterMessage((addr, new_conn)) => {
                        if let Err(e) = register_connection(&mut ctx, addr, new_conn).await {
                            error!("Error registering connection: {}", &e);
                            break;
                        }
                    }

                    /* new connection from the broker */
                    ControlMessages::NewConnection((addr, conn_tx)) => {
                        add_new_connection(&mut ctx, addr, conn_tx);
                    }

                    ControlMessages::Disconnected(addr) => {
                        info!("Disconnecting connection: {}", &addr);

                        /* todo must notify router to remove routes */
                        ctx.connections.remove(&addr);
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
