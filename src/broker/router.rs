use std::collections::HashMap;
use std::error::Error;
use std::process::exit;
use log::{debug, error, warn};
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::TryRecvError;
use crate::broker::protocol::Message;

/**
The router accepts messages from all connections, then decides how best to route those
messages to other applications
 */

pub struct RegisterConnectionReq {
    pub name: String, //connection name
    pub conn_tx: Sender<Message>, //to send messages to connection
}

pub enum RouterControlMessages {
    RegisterConnection(RegisterConnectionReq),
    RemoveRoutes(String),
}

async fn process_control_message(ctx: &mut RouterCtx, msg: RouterControlMessages) {
    match msg {
        RouterControlMessages::RegisterConnection(conn) => {
            ctx.connections.insert(conn.name.clone(), conn);
        }

        RouterControlMessages::RemoveRoutes(name) => {}
        _ => {}
    }
}

async fn __get_route_dst<'a>(ctx: &'a RouterCtx, msg: &Message) -> Option<&'a Vec<String>> {
    let key = format!("{}/*", &msg.header.name);
    let val_opt = ctx.routes_map.get(&key);
    if let Some(val) = val_opt {
        return Some(val);
    }

    let key = format!("{}/{}", &msg.header.name, &msg.header.msg_type);
    let val_opt = ctx.routes_map.get(&key);
    if let Some(val) = val_opt {
        return Some(val);
    }

    None
}

fn find_connection_by_name<'a>(ctx: &'a RouterCtx, name: &str) -> Option<&'a RegisterConnectionReq> {
    ctx.connections.get(name)
}

///processes messages received from the various network connections
async fn process_connection_message(ctx: &RouterCtx, msg: Message) {
    match __get_route_dst(ctx, &msg).await {
        Some(dsts) => {
            /* send message to all dsts */
            for dst in dsts {
                if let Some(c) = find_connection_by_name(ctx, dst) {
                    if let Err(e) = c.conn_tx.send(msg.clone()).await {
                        warn!("Unable to send message to dst: {}. Receiver dropped. Error: {}", dst, &e);
                    }
                } else {
                    debug!("dst {} does not exist. Unable to forward message", dst);
                }
            }
        }
        None => {
            debug!("Ignoring route for: {}/{}. Not found", &msg.header.name, &msg.header.msg_type);
        }
    }
}

struct RouterCtx {
    pub connections: HashMap<String, RegisterConnectionReq>,
    pub route_config: RouteConfig,
    pub routes_map: HashMap<String, Vec<String>>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq)]
struct RouteSrc {
    pub app: String, //application name
    pub types: Option<Vec<String>>, //the message types to route
}

#[derive(Serialize, Deserialize, Eq, PartialEq)]
struct Route {
    pub src: RouteSrc,
    pub dst: Vec<String>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq)]
struct RouteConfig {
    pub routes: Vec<Route>,
}

async fn read_config(base_dir: String, workflow: &str) -> Result<RouteConfig, Box<dyn Error>> {
    let path = format!("{}/workflows/{}/routes.yaml", &base_dir, workflow);
    let mut f = File::open(&path).await?;

    let mut contents = String::new();
    f.read_to_string(&mut contents).await?;
    let routes: RouteConfig = serde_yaml::from_str(&contents)?;
    Ok(routes)
}

///Convert routes into key=="name/message_type" and val==dst
async fn preprocess_routes(route_config: &RouteConfig) -> HashMap<String, Vec<String>> {
    let mut routes_map = HashMap::new();
    for route in &route_config.routes {
        if let Some(types) = &route.src.types {
            for t in types {
                let route_key = format!("{}/{}", route.src.app, t);
                routes_map.insert(route_key, route.dst.clone());
            }
        } else {
            /* applicable for all types */
            let route_key = format!("{}/*", route.src.app);
            routes_map.insert(route_key, route.dst.clone());
        }
    }

    routes_map
}

pub async fn router_main(base_dir: String,
                         workflow: String,
                         ctrl_rx: Receiver<RouterControlMessages>,
                         conn_rx: Receiver<Message>) {
    let routes_res = read_config(base_dir.clone(), &workflow).await;
    if let Err(e) = routes_res {
        error!("Error loading routes: {}", &e);
        exit(1);
    }
    let route_config = routes_res.unwrap();
    let routes_map = preprocess_routes(&route_config).await;

    let mut ctx = RouterCtx {
        connections: HashMap::new(),
        route_config,
        routes_map,
    };

    let mut m_ctrl_rx = ctrl_rx; //for control messages
    let mut m_conn_rx = conn_rx; //for connection messages
    loop {
        //process messages from the control plane
        match m_ctrl_rx.try_recv() {
            Ok(msg) => {
                process_control_message(&mut ctx, msg).await;
            }
            Err(e) => {
                if e == TryRecvError::Disconnected {
                    error!("Router control disconnect detected. Aborting");
                    break;
                }
            }
        }

        //process messages from connections
        match m_conn_rx.try_recv() {
            Ok(msg) => {
                process_connection_message(&ctx, msg).await;
            }

            Err(e) => {
                if e == TryRecvError::Disconnected {
                    error!("Router connection disconnect detected. Aborting");
                    break;
                }
            }
        }
    }

    error!("Router error detected. Aborting");
    /* TODO: kill application here */
}