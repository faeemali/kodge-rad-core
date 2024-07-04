use serde::{Deserialize, Serialize};

pub const MSG_TYPE_AUTH: &str = "rad-auth";
pub const MSG_TYPE_AUTH_RESP: &str = "rad-auth-resp";

#[derive(Serialize, Deserialize, Eq, PartialEq)]
pub struct AuthMessageReq {
    //the types of messages this service can receive
    pub rx_msg_types: Vec<String>,

    //the types of messages this service can send
    pub tx_msg_types: Vec<String>,

    pub name: String,
}

#[derive(Serialize, Deserialize, Eq, PartialEq)]
pub struct AuthMessageResp {
    pub success: bool,
}
