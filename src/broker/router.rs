use std::collections::HashMap;
use std::error::Error;
use std::process::exit;
use log::error;
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

///processes messages received from the various network connections
async fn process_connection_message(msg: Message) {
    
}

struct RouterCtx {
    pub connections: HashMap<String, RegisterConnectionReq>,
    pub routes: Routes,
}

#[derive(Serialize, Deserialize, Eq, PartialEq)]
struct RouteSrc {
    pub app: String, //application name
    pub types: Option<Vec<String>>, //the message types to route
}

#[derive(Serialize, Deserialize, Eq, PartialEq)]
struct Routes {
    pub src: RouteSrc,
    pub dst: String,
}

async fn read_config(base_dir: String) -> Result<Routes, Box<dyn Error>> {
    let path = format!("{}/routes.yaml", &base_dir);
    let mut f = File::open(&path).await?;
    
    let mut contents = String::new();
    f.read_to_string(&mut contents).await?;
    let routes: Routes = serde_yaml::from_str(&contents)?;
    Ok(routes)
}

pub async fn router_main(base_dir: String,
                         ctrl_rx: Receiver<RouterControlMessages>,
                         conn_rx: Receiver<Message>) {
    let routes_res = read_config(base_dir.clone()).await;
    if let Err(e) = routes_res {
        error!("Error loading routes: {}", &e);
        exit(1);
    }
    let routes = routes_res.unwrap();

    let mut ctx = RouterCtx {
        connections: HashMap::new(),
        routes,
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
                process_connection_message(msg).await;
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