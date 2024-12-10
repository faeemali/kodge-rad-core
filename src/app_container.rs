use std::collections::HashMap;
use std::error::Error;
use std::io::ErrorKind::WouldBlock;
use std::sync::Arc;
use std::time::Duration;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock};
use tokio::io::{AsyncWriteExt, Interest};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time::sleep;
use crate::bin::{BinConfig, load_bin};
use crate::broker::auth_types::{AuthMessageReq, AuthMessageResp, MSG_TYPE_AUTH, MSG_TYPE_AUTH_RESP};
use crate::broker::protocol::{Message, MessageHeader, Protocol};
use crate::error::{raderr};
use crate::process::{run_bin_main, spawn_process};
use crate::utils::timer::Timer;
use crate::utils::utils;

async fn authenticate_client_connection(conn: &mut TcpStream,
                                        conn_name: String,
                                        protocol: &mut Protocol)
                                        -> Result<(), Box<dyn Error + Sync + Send>> {
        let req = AuthMessageReq {
            name: conn_name.clone(),
            tx_msg_types: vec![], //TODO: fix me
            rx_msg_types: vec![], //TODO: fix me
        };
        let req_bytes = serde_json::to_vec(&req)?;

        let auth_msg = Message {
            header: MessageHeader {
                name: conn_name.clone(),
                rks: None,
                rks_match_type: None,
                message_id: String::new(),
                msg_type: MSG_TYPE_AUTH.to_string(),
                length: req_bytes.len() as u32,
                extras: None,
            },
            body: req_bytes,
        };

        let b = Protocol::format(&auth_msg)?;
        conn.write_all(&b).await?;

        /* wait for response */
        let response_timer = Timer::new(Duration::from_millis(3000));
        let mut b = [0; 1024];
        while !response_timer.timed_out() {
            match conn.try_read(&mut b) {
                Ok(size) => {
                    if size == 0 {
                        //eof
                        break;
                    }

                    let msgs = protocol.feed(&b[0..size]);
                    if msgs.is_empty() {
                        sleep(Duration::from_millis(5)).await;
                        continue;
                    }

                    if msgs.len() != 1 {
                        return raderr(format!("auth error for container {}. Expected one message response", &conn_name));
                    }

                    if msgs[0].header.msg_type != MSG_TYPE_AUTH_RESP {
                        return raderr(format!("Invalid message response type to authentication request for container {}", &conn_name));
                    }

                    let resp = serde_json::from_slice::<AuthMessageResp>(&msgs[0].body)?;
                    if resp.success {
                        debug!("Authentication successful for container {}", &conn_name);
                        return Ok(());
                    }
                }

                Err(e) => {
                    if e.kind() == WouldBlock {
                        sleep(Duration::from_millis(5)).await;
                        continue;
                    } else {
                        return raderr(format!("Error during authentication response read: {}", &e));
                    }
                }
            }
        } //while

        raderr(format!("Authentication response timeout for container {}", &conn_name))
}