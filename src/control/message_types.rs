use std::net::SocketAddr;
use tokio::sync::mpsc::Sender;
use crate::broker::protocol::Message;
use crate::control::control_plane::ControlConnData;

pub enum ControlMessages {
    NewConnection(SocketAddr),
    Disconnected(SocketAddr),
    RegisterMessage(RegisterMessageReq),
    RegisterConnection(RegisterConnectionReq),
    RemoveRoutes(String),
    AppExit(String),

}

pub struct RegisterMessageReq {
    pub data: ControlConnData,
    pub conn_router_tx: Sender<Message>, //for connection to send messages to router
}

/**
    The router accepts messages from all connections, then decides how best to route those
    messages to other applications
 */
pub struct RegisterConnectionReq {
    pub name: String, //connection name
    pub conn_tx: Sender<Message>, //to send messages to connection
}