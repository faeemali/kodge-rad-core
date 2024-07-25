use std::collections::HashMap;
use std::error::Error;
use std::process::exit;
use std::sync::Arc;
use std::time::Duration;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::RwLock;
use tokio::time::sleep;
use crate::app::STDOUT;
use crate::broker::protocol::{Message, RK_MATCH_TYPE_ALL, RK_MATCH_TYPE_ANY, RK_MATCH_TYPE_NONE};
use crate::error::{raderr};
use crate::utils::utils;

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

        RouterControlMessages::RemoveRoutes(name) => {
            info!("Disconnecting {} from the router", &name);
            ctx.connections.remove(&name);

            //do not remove the static routes. They are "static", unlike connections
            //which are dynamic
        }
    }
}

/// checks if the message can be sent to the destinations specified in rksdsts by
/// matching the routing keys in the message against the routing keys in the route,
/// depending on the routing key match type specified in the message
fn message_matches_routing_keys(msg: &Message, rksdsts: &RksDsts) -> bool {
    if msg.header.rks_match_type == RK_MATCH_TYPE_NONE {
        /* no routing keys to be matched. This message is valid */
        return true;
    }

    if msg.header.rks_match_type == RK_MATCH_TYPE_ALL {
        if msg.header.rks.len() != rksdsts.rks.len() {
            return false;
        }

        for rk in &msg.header.rks {
            if !rksdsts.rks.contains(rk) {
                return false;
            }
        }

        return true;
    }

    /* we only need one routing key to match */
    if msg.header.rks_match_type == RK_MATCH_TYPE_ANY {
        for rk in &msg.header.rks {
            if rksdsts.rks.contains(rk) {
                return true;
            }
        }

        return false;
    }

    warn!("Invalid message match type: {}", &msg.header.rks_match_type);
    false
}


///gets the destination for a message. This searches the routes_map in ctx and looks for
/// a route with a specific name, or a route marked as "*" which means accept all messages
async fn __get_route_dst<'a>(ctx: &'a RouterCtx, msg: &Message) -> Option<&'a Vec<String>> {
    let key = format!("{}/*", &msg.header.name);
    let val_opt = ctx.routes_map.get(&key);
    if let Some(val) = val_opt {
        if message_matches_routing_keys(msg, val) {
            //println!("returning routes (0): {:?}", val);
            return Some(&val.dsts);
        }
    }

    let key = format!("{}/{}", &msg.header.name, &msg.header.msg_type);
    let val_opt = ctx.routes_map.get(&key);
    if let Some(val) = val_opt {
        //println!("returning routes: {:?}", val);
        if message_matches_routing_keys(msg, val) {
            return Some(&val.dsts)
        }
    }

    None
}

fn find_connection_by_name<'a>(ctx: &'a RouterCtx, name: &str) -> Option<&'a RegisterConnectionReq> {
    ctx.connections.get(name)
}

/// processes messages received from the various network connections, or messages
/// destined for network connections
async fn process_connection_message(ctx: &RouterCtx, msg: Message) {
    match __get_route_dst(ctx, &msg).await {
        Some(dsts) => {
            /* send message to all dsts */
            for dst in dsts {
                if dst == STDOUT {
                    if let Some(stdout) = &ctx.stdout_chan_opt {
                        if let Err(e) = stdout.send(msg.clone()).await {
                            error!("Error sending message of type {} to stdout. Error: {}", &msg.header.msg_type, &e);
                            return;
                        }
                    } else {
                        error!("Message should be routed to stdout but stdout channel is None");
                        return;
                    }
                } else if let Some(c) = find_connection_by_name(ctx, dst) {
                    if let Err(e) = c.conn_tx.send(msg.clone()).await {
                        error!("Unable to send message to dst: {}. Receiver dropped. Error: {}", dst, &e);
                        return;
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
    /// the registration requests (name, allowed tx/rx messages)
    pub connections: HashMap<String, RegisterConnectionReq>,

    /// the router configuration, as read from the config file
    pub route_config: RouteConfig,

    /// a map of message types and their respective destinations
    pub routes_map: HashMap<String, RksDsts>,

    //for sending data to stdout
    pub stdout_chan_opt: Option<Sender<Message>>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq)]
struct RouteSrc {
    pub app: String, //application name
    pub types: Option<Vec<String>>, //the message types to route
    pub rks: Option<Vec<String>>,
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

async fn read_config(workflow_base_dir: String) -> Result<RouteConfig, Box<dyn Error>> {
    let path = format!("{}/routes.yaml", &workflow_base_dir);
    let mut f = File::open(&path).await?;

    let mut contents = String::new();
    f.read_to_string(&mut contents).await?;
    let routes: RouteConfig = serde_yaml::from_str(&contents)?;
    Ok(routes)
}

struct RksDsts {
    /// Routing keys
    pub rks: Vec<String>,

    /// Destinations
    pub dsts: Vec<String>,
}

/// Convert routes into key=="name/message_type" and val=={\[rks],\[dst]}
/// The special source \[stdin] and the special dst \[stdout]
/// link the the route to stdin/stdout
async fn preprocess_routes(route_config: &RouteConfig) -> HashMap<String, RksDsts> {
    let mut routes_map = HashMap::new();
    for route in &route_config.routes {
        let rks = if let Some(r) = &route.src.rks {
            r.clone()
        } else {
            vec![]
        };

        if let Some(types) = &route.src.types {
            for t in types {
                let route_key = format!("{}/{}", route.src.app, t);
                routes_map.insert(route_key, RksDsts {
                    rks: rks.clone(),
                    dsts: route.dst.clone(),
                });
            }
        } else {
            /* applicable for all types */
            let route_key = format!("{}/*", route.src.app);
            routes_map.insert(route_key, RksDsts {
                rks: rks.clone(),
                dsts: route.dst.clone(),
            });
        }
    }

    routes_map
}

pub async fn get_message_from_src(conn: &mut Receiver<Message>,
                                  stdin_opt: &mut Option<Receiver<Message>>)
                                  -> Result<Vec<Message>, Box<dyn Error + Sync + Send>> {
    let mut msgs = vec![];

    match conn.try_recv() {
        Ok(msg) => {
            msgs.push(msg);
        }

        Err(e) => {
            if e == TryRecvError::Disconnected {
                let msg = "Router connection disconnect detected. Aborting";
                error!("{}", msg);
                return raderr(msg);
            }
        }
    }

    if let Some(stdin) = stdin_opt {
        match stdin.try_recv() {
            Ok(msg) => {
                msgs.push(msg);
            }
            Err(e) => {
                if e == TryRecvError::Disconnected {
                    let msg = "Router stdin disconnect detected. Aborting";
                    error!("{}", msg);
                    return raderr(msg);
                }
            }
        }
    }

    Ok(msgs)
}

pub async fn router_main(workflow_base_dir: String,
                         ctrl_rx: Receiver<RouterControlMessages>,
                         conn_rx: Receiver<Message>,
                         stdin_chan_opt: Option<Receiver<Message>>,
                         stdout_chan_opt: Option<Sender<Message>>,
                         am_must_die: Arc<RwLock<bool>>) {
    let routes_res = read_config(workflow_base_dir.clone()).await;
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
        stdout_chan_opt,
    };

    let mut m_stdin_chan_opt = stdin_chan_opt;
    let mut m_ctrl_rx = ctrl_rx; //for control messages
    let mut m_conn_rx = conn_rx; //for connection messages
    loop {
        if utils::get_must_die(am_must_die.clone()).await {
            warn!("Router caught must die flag. Aborting");
            return;
        }

        let mut busy = false;

        //process messages from the control plane
        match m_ctrl_rx.try_recv() {
            Ok(msg) => {
                process_control_message(&mut ctx, msg).await;
                busy = true;
            }
            Err(e) => {
                if e == TryRecvError::Disconnected {
                    error!("Router control disconnect detected. Aborting");
                    break;
                }
            }
        }

        //process messages from stdin or one of the connections
        let msgs_res = get_message_from_src(&mut m_conn_rx, &mut m_stdin_chan_opt).await;
        if let Err(e) = msgs_res {
            let msg = format!("Error retrieving source messages: {}", &e);
            error!("{}", &msg);
        } else {
            let msgs = msgs_res.unwrap();
            for msg in msgs {
                process_connection_message(&ctx, msg).await;
                busy = true;
            }
        }

        if !busy {
            sleep(Duration::from_millis(10)).await;
        }
    }

    error!("Router error detected. Aborting");
    utils::set_must_die(am_must_die).await;
}