use std::collections::HashMap;
use log::error;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::TryRecvError;
use crate::broker::protocol::Message;

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

async fn process_connection_message(msg: Message) {}

struct RouterCtx {
    pub connections: HashMap<String, RegisterConnectionReq>,
}

pub async fn router_main(ctrl_rx: Receiver<RouterControlMessages>,
                         conn_rx: Receiver<Message>) {
    let mut ctx = RouterCtx {
        connections: HashMap::new(),
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