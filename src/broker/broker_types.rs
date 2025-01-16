use std::net::SocketAddr;
use tokio::sync::mpsc::Sender;
use crate::broker::auth_types::AuthMessageReq;
use crate::broker::protocol::Protocol;
use crate::control::message_types::ControlMessages;
use crate::utils::timer::Timer;

pub struct ConnectionCtx {
    pub addr: SocketAddr,
    //timer for authentication
    pub auth_timer: Timer,
    pub auth_message: Option<AuthMessageReq>,
    pub instance_id: Option<String>,
    pub protocol: Protocol,
    pub ctrl_tx: Sender<ControlMessages>,
}

#[derive(Eq, PartialEq)]
pub enum States {
    AuthenticateAndRegister,
    WaitForRegistrationResponse,
    Process,
}
