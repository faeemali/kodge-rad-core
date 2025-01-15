use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use log::{debug, error, info};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time::sleep;
use crate::AppCtx;
use crate::control::message_types::{ControlMessages, RegisterMessageReq};
use crate::broker::protocol::{Message, RK_MATCH_TYPE_ALL, RK_MATCH_TYPE_ANY};
use crate::config::config::{Route};
use crate::control::message_types::ControlMessages::{NewMessage, RegisterRoutes, RemoveRoutes};
use crate::error::raderr;

pub fn router_init() -> (Sender<ControlMessages>, Receiver<ControlMessages>) {
    channel::<ControlMessages>(32)
}

async fn process_control_message(ctx: &mut RouterCtx, msg: ControlMessages) {
    match msg {
        RegisterRoutes(req) => {
            let instance_id = req.instance_id.clone();
            ctx.connections.insert(instance_id.clone(), req);
            info!("Registered routes for {}", &instance_id);
        }
        
        RemoveRoutes(instance_id) => {
            info!("Disconnecting {} from the router", &instance_id);
            ctx.connections.remove(&instance_id);

            //do not remove the static routes. They are "static", unlike connections
            //which are dynamic
        }

        _ => {
            todo!()
        }
    }
}

/// checks if the message can be sent to the destinations specified in rksdsts by
/// matching the routing keys in the message against the routing keys in the route,
/// depending on the routing key match type specified in the message.
/// Rules:
///  - All routing keys in the message and the router must be unique or else this algorithm
///    may fail when matching all. This *flaw* is allowed in the interests of speed. A better
///    algorithm can definitely be used that also checks for duplicates, but that is slower on
///    a per-message basis. TODO: eliminate duplicates during pre-processing. That should fix all
///    problems.
///  - If match type is invalid, the message is not matched
///  - If match type is None, the message is matched successfully, regardless of the value of rks
///  - If match type is Some, and rks is None, then the message is not matched
///  - If match type is Some, and rks is Some, then matching proceeds as normal
fn message_matches_routing_keys<'a>(msg: &Message, rt_rks_dsts_list: &'a [RksDsts]) -> Option<&'a Vec<String>> {
    if let Some(msg_match_type) = &msg.header.rks_match_type {
        //match_type is some

        if let Some(msg_rks) = &msg.header.rks {
            /* process all i.e. all keys must match */
            if msg_match_type == RK_MATCH_TYPE_ALL {
                /*
                    match for all "routing keys"/"list of destinations" combinations.
                    We're looking for one entry from the Vec that matches.
                 */
                for rt_rks_dsts in rt_rks_dsts_list {
                    if rt_rks_dsts.rks.is_empty() {
                        /* no routing keys in the routing table */
                        continue;
                    }

                    let rt_rks = &rt_rks_dsts.rks;
                    if msg_rks.len() != rt_rks.len() {
                        //invalid lengths means they cannot possibly match all
                        continue;
                    }

                    /*
                       use the cheap algorithm and just confirm every item in the routing table rks
                       exists in the message as well.
                     */
                    let mut found_all = true;
                    for rt_rk in rt_rks {
                        if !msg_rks.contains(rt_rk) {
                            found_all = false;
                            break;
                        }
                    }

                    if found_all {
                        /* found a match! */
                        return Some(&rt_rks_dsts.dsts);
                    } else {
                        continue;
                    }
                }
                None
            } else if msg_match_type == RK_MATCH_TYPE_ANY {
                /* we only need one routing key to match from any item in the list */
                for rt_rks_dsts in rt_rks_dsts_list {
                    if rt_rks_dsts.rks.is_empty() {
                        continue;
                    }

                    let rt_rks = &rt_rks_dsts.rks;
                    for rt_rk in rt_rks {
                        if msg_rks.contains(rt_rk) {
                            return Some(&rt_rks_dsts.dsts);
                        }
                    }
                }
                None
            } else {
                debug!("Invalid message match type: {}", msg_match_type);
                None
            }
        } else {
            /* we have a routing key match type by no routing keys. Disallow message */
            None
        }
    } else {
        /* match_type is none no routing keys to be matched. This message is valid, provided there's only one set of destinations */
        if rt_rks_dsts_list.len() != 1 {
            debug!("Message being discarded because routing is ambiguous. {}/{} has multiple destination groups, but no routing key match type", &msg.header.instance_id, &msg.header.msg_type);
            return None;
        }
        Some(&rt_rks_dsts_list[0].dsts)
    }
}

fn __match_route<'a>(msg: &Message, key: &str, val_opt: &Option<&'a Vec<RksDsts>>) -> Option<&'a Vec<String>> {
    if let Some(val) = val_opt {
        if let Some(dsts) = message_matches_routing_keys(msg, val) {
            debug!("returning route dsts for {}: {:?}", &key, dsts);
            return Some(dsts);
        }
    }
    None
}

///gets the destination for a message. This searches the routes_map in ctx and looks for
/// a route with a specific name, or a route marked as "*" which means accept all messages
async fn __get_route_dst<'a>(ctx: &'a RouterCtx, msg: &Message) -> Option<&'a Vec<String>> {
    let key = format!("{}/*", &msg.header.instance_id);
    let val_opt = ctx.routes_map.get(&key);
    let ret = __match_route(msg, &key, &val_opt);
    if ret.is_some() {
        return ret;
    }

    let key = format!("{}/{}", &msg.header.instance_id, &msg.header.msg_type);
    let val_opt = ctx.routes_map.get(&key);
    let ret = __match_route(msg, &key, &val_opt);
    if ret.is_some() {
        return ret;
    }

    None
}

fn find_connection_by_name<'a>(ctx: &'a RouterCtx, name: &str) -> Option<&'a RegisterMessageReq> {
    ctx.connections.get(name)
}

/// processes messages received from the various network connections, or messages
/// destined for network connections
async fn process_connection_message(ctx: &RouterCtx, msg: Message) {
    match __get_route_dst(ctx, &msg).await {
        Some(dsts) => {
            /* send message to all dsts */
            for dst in dsts {
                if let Some(c) = find_connection_by_name(ctx, dst) {
                    /* TODO: fixme, must send message to control plane */
                    // if let Err(e) = c.conn_tx.send(msg.clone()).await {
                    //     error!("Unable to send message to dst: {}. Receiver dropped. Error: {}", dst, &e);
                    //     return;
                    // }
                } else {
                    debug!("dst {} does not exist. Unable to forward message", dst);
                }
            }
        }
        None => {
            debug!("Ignoring route for: {}/{}. Not found", &msg.header.instance_id, &msg.header.msg_type);
        }
    }
}

struct RouterCtx {
    /// the registration requests (name, allowed tx/rx messages)
    pub connections: HashMap<String, RegisterMessageReq>,

    /// the router configuration, as read from the config file
    pub routes: Vec<Route>,

    /// a map of message types and their respective destinations
    pub routes_map: HashMap<String, Vec<RksDsts>>,
}

#[derive(Debug)]
struct RksDsts {
    /// Routing keys. empty means allow everything
    pub rks: Vec<String>,

    /// Destinations
    pub dsts: Vec<String>,
}

///checks if rks (all items in the array) are found in  rks_dsts.rks.
/// if found, the relevant item is returned, else None is returned
fn __find_existing_rks_dsts<'a>(rks: &[String], rks_dsts: &'a mut [RksDsts]) -> Option<&'a mut RksDsts> {
    /*
        add the destination to an item with matching route keys,
        or create a new item
     */
    for rk_dst in rks_dsts {
        if rks.len() != rk_dst.rks.len() {
            return None;
        }

        /* compare all routing keys, irrespective of order */

        /* put all items of one array into a map */
        let mut map = HashMap::new();
        for existing_rk in &rk_dst.rks {
            map.insert(existing_rk.clone(), existing_rk.clone());
        }

        /* check all items of the second array are in the map */
        let mut found = true;
        for rk in rks {
            if !map.contains_key(rk) {
                found = false;
                break;
            }
        }

        if found {
            return Some(rk_dst);
        }
    }

    None
}

fn __add_to_routes_map(route_key: &str,
                       routes_map: &mut HashMap<String, Vec<RksDsts>>,
                       rks: Vec<String>,
                       dst: &str) {
    if routes_map.contains_key(route_key) {
        let rks_dsts = routes_map.get_mut(route_key).unwrap();
        let existing_opt = __find_existing_rks_dsts(&rks, rks_dsts);
        if let Some(rk_dst) = existing_opt {
            /* append */
            rk_dst.dsts.push(dst.to_string());
        } else {
            /* create new */
            rks_dsts.push(RksDsts {
                rks: rks.clone(),
                dsts: vec![dst.to_string()],
            })
        }
    } else {
        /* insert new */
        routes_map.insert(route_key.to_string(), vec![RksDsts {
            rks,
            dsts: vec![dst.to_string()],
        }]);
    }
}

/// Convert routes into key=="instance_id/message_type" and val=={\[rks],\[dst]}
async fn preprocess_routes(routes: &[Route]) -> HashMap<String, Vec<RksDsts>> {
    let mut routes_map = HashMap::new();
    for route in routes {
        if route.src.msg_types.is_empty() {
            /* applicable for all types */
            let route_key = format!("{}/*", route.src.instance_id);
            __add_to_routes_map(&route_key, &mut routes_map, route.src.routing_keys.clone(), &route.dst);
        } else {
            for t in &route.src.msg_types {
                let route_key = format!("{}/{}", route.src.instance_id, t);
                __add_to_routes_map(&route_key, &mut routes_map, route.src.routing_keys.clone(), &route.dst);
            }
        }
    }

    debug!("routes after pre-processing: {:?}", &routes_map);

    routes_map
}

pub async fn get_message_from_src(conn: &mut Receiver<ControlMessages>)
                                  -> Result<Vec<Message>, Box<dyn Error + Sync + Send>> {
    let mut msgs = vec![];

    match conn.try_recv() {
        Ok(msg) => {
            if let NewMessage(m) = msg {
                msgs.push(m);
            }

            /* TODO: what about the other message types? */
        }

        Err(e) => {
            if e == TryRecvError::Disconnected {
                let msg = "Router connection disconnect detected. Aborting";
                error!("{}", msg);
                return raderr(msg);
            }
        }
    }

    Ok(msgs)
}

pub async fn router_main(app_ctx: Arc<AppCtx>,
                         router_rx: Receiver<ControlMessages>,
                         ctrl_tx: Sender<ControlMessages>) {
    let routes_map = preprocess_routes(&app_ctx.config.routes).await;

    let mut ctx = RouterCtx {
        connections: HashMap::new(),
        routes: app_ctx.config.routes.clone(),
        routes_map,
    };

    let mut router_rx = router_rx;
    loop {
        /* todo handle must die */
        let mut busy = false;


        //process messages from the control plane
        match router_rx.try_recv() {
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
        let msgs_res = get_message_from_src(&mut router_rx).await;
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

    /* todo handle must die */
}