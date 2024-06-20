use std::error::Error;
use serde::{Deserialize, Serialize};

pub const MSG_TYPE_AUTH: &str = "rad-auth";

#[derive(Serialize, Deserialize)]
pub struct AuthMessage {
    //the types of messages this service can receive
    pub rx_msg_types: Vec<String>,

    //the types of messages this service can send
    pub tx_msg_types: Vec<String>,

    pub name: String,
}

pub async fn authenticate(msg: &AuthMessage) -> Result<bool, Box<dyn Error + Sync + Send>> {
    /* TODO: do something here */
    Ok(true)
}