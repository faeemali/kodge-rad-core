use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use log::{error, info, warn};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use crate::broker::auth_types::{AuthMessageResp, MSG_TYPE_AUTH_RESP};
use crate::broker::protocol::{Message};
use crate::control::message_types::{ControlConn, ControlMessages, RegisterMessageReq};
use crate::control::message_types::ControlMessages::{BrokerReady, Disconnected, MustDie, NewConnection, NewMessage, RegisterMessage, Registered, RemoveRoutes, RouteDstMessage};
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

fn find_connection_by_instance_id<'a>(ctx: &'a CtrlCtx, instance_id: &str) -> Option<(&'a SocketAddr, &'a ControlConn)> {
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

fn find_connection_key_by_instance_id(ctx: &CtrlCtx, instance_id: &str) -> Option<(SocketAddr)> {
    for key in ctx.connections.keys() {
        let value = ctx.connections.get(key);
        if value.is_none() {
            continue;
        }
        let value = value.unwrap();

        if let Some(d) = &value.data {
            if d.instance_id == instance_id {
                return Some(*key);
            }
        } else {
            continue;
        }
    }
    None
}

async fn disconnect(ctx: &mut CtrlCtx, key: &SocketAddr)  {
    if let Some(conn) = ctx.connections.remove(key) {
        //notify connection. If the disconnect originated
        //from the connection, the channel may be closed, so
        //ignore errors
        let _ = conn.conn_tx.send(Disconnected(*key)).await;

        if let Some(data) = &conn.data {                let instance_id = &data.instance_id;
            if ctx.router_tx.send(RemoveRoutes(instance_id.to_string())).await.is_err() {
                send_must_die_msgs(ctx, format!("Error removing routes for {} on disconnect", instance_id).as_str()).await;
            }
        }
    }
}

//disconnects an existing connection if one exists
async fn disconnect_existing_connection(ctx: &mut CtrlCtx, instance_id: &str) -> Result<(), Box<dyn Error + Sync + Send>> {
    let kv_opt = find_connection_key_by_instance_id(ctx, instance_id);
    if let Some(key) = kv_opt {
        disconnect(ctx, &key).await;
    }

    Ok(())
}

async fn register_connection(ctx: &mut CtrlCtx,
                             addr: SocketAddr,
                             new_conn: RegisterMessageReq)
                             -> Result<(), Box<dyn Error + Sync + Send>> {
    let instance_id = new_conn.instance_id.clone();
    disconnect_existing_connection(ctx, &instance_id).await?;

    if let Some(val) = ctx.connections.get_mut(&addr) {
        val.data = Some(new_conn.clone());
        info!("Registered connection: {}", &instance_id);

        /* notify the router */
        ctx.router_tx.send(ControlMessages::RegisterRoutes(new_conn)).await?;

        /* notify the broker that registration is complete */
        val.conn_tx.send(Registered).await?;

        /* send an auth response */
        let resp = Message::new_from_type(&instance_id, "", MSG_TYPE_AUTH_RESP, AuthMessageResp { success: true })?;
        val.conn_tx.send(NewMessage(resp)).await?;

        Ok(())
    } else {
        raderr("Unexpected error. Unable to register connection. Connection socket info does not exist")
    }
}

fn add_new_connection(ctx: &mut CtrlCtx, addr: SocketAddr, conn_tx: Sender<ControlMessages>) {
    ctx.connections.insert(addr, ControlConn {
        start_time: get_datetime_as_utc_millis(),
        conn_tx,
        data: None,
    });
}

// notify all subsystems that they must die
async fn send_must_die_msgs(ctx: &mut CtrlCtx, msg: &str) {
    for (_, conn) in &ctx.connections {
        let _ = conn.conn_tx.send(MustDie(msg.into())).await;
    }
    
    let _ = ctx.broker_tx.send(MustDie(msg.into())).await;
    let _ = ctx.router_tx.send(MustDie(msg.into())).await;
    let _ = ctx.app_runner_tx.send(MustDie(msg.into())).await;
    warn!("must_die notification sent to all subsystems");
}

pub async fn ctrl_main(ctx: CtrlCtx,
                       rx: Receiver<ControlMessages>) {
    info!("Broker command receiver running");

    let mut ctx = ctx;
    let mut rx = rx;
    let mut done = false;
    while !done {
        let msg_opt = rx.recv().await;
        let m = match msg_opt {
            Some(m) => m,
            None => {
                continue;
            }
        };

        match m {
            MustDie(msg) => {
                info!("Received MustDie message: {}", msg);
                send_must_die_msgs(&mut ctx, &msg).await;
            }

            /* new connection from the broker */
            NewConnection((addr, conn_tx)) => {
                add_new_connection(&mut ctx, addr, conn_tx);
            }

            RegisterMessage((addr, msg_info)) => {
                if let Err(e) = register_connection(&mut ctx, addr, msg_info).await {
                    panic!("Error registering connection: {}", &e);
                }
            }

            Disconnected(addr) => {
                info!("Disconnecting connection: {}", &addr);

                if let Some(conn) = ctx.connections.get(&addr) {
                    if let Some(data) = &conn.data {
                        let instance_id = data.instance_id.clone();
                        if ctx.router_tx.send(RemoveRoutes(instance_id.clone())).await.is_err() {
                            send_must_die_msgs(&mut ctx, format!("Error removing routes for {}", &instance_id).as_str()).await;
                            done = true;
                        }
                    }
                }
                
                ctx.connections.remove(&addr);
            }
            
            NewMessage(msg) => {
                /* forward to router */
                if ctx.router_tx.send(NewMessage(msg)).await.is_err() {
                    send_must_die_msgs(&mut ctx, "Error forwarding message to router").await;
                    done = true;
                }
            }
            
            RouteDstMessage((instance_id, msg)) => {
                if let Some((_, conn)) = find_connection_by_instance_id(&ctx, &instance_id) {
                    if let Err(e) = conn.conn_tx.send(NewMessage(msg)).await {
                        let msg = format!("Error sending message to {}: {}", &instance_id, &e);
                        error!("{}", &msg);
                        send_must_die_msgs(&mut ctx, &msg).await;
                        done = true;
                    }
                } else {
                    error!("Connection not found for instance_id: {}", &instance_id);
                }
            }

            BrokerReady => {
                if ctx.app_runner_tx.send(BrokerReady).await.is_err() {
                    let msg = "Error sending broker ready message";
                    send_must_die_msgs(&mut ctx, msg).await;
                }
            }

            _ => {
            }
        }
    }

    info!("Control plane terminated");
}
