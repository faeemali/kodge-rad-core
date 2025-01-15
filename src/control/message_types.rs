use std::net::SocketAddr;
use tokio::sync::mpsc::Sender;
use crate::broker::protocol::Message;

pub enum ControlMessages {
    NewConnection((SocketAddr, Sender<ControlMessages>)),
    Disconnected(SocketAddr),
    RegisterMessage((SocketAddr, RegisterMessageReq)),
    Registered,
    RegisterRoutes(RegisterMessageReq),
    RemoveRoutes(String),
    AppExit(String),
    NewMessage(Message),
    RouteDstMessage((String, Message)) //route to dst based on instance id
}

#[derive(Clone)]
pub struct RegisterMessageReq {
    pub instance_id: String,
    pub rx_msg_types: Vec<String>,
    pub tx_msg_types: Vec<String>,
}

pub struct ControlConn {
    pub start_time: i64,
    pub conn_tx: Sender<ControlMessages>,
    pub data: Option<RegisterMessageReq>,
}
