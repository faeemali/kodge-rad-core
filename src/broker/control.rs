use log::{info, warn};
use tokio::sync::mpsc::Receiver;
use crate::broker::control::ControlMessages::{DisconnectMessage, RegisterMessage};

pub enum ControlMessages {
    RegisterMessage(String),
    DisconnectMessage(String),
}

pub async fn ctrl_main(rx: Receiver<ControlMessages>) {
    info!("Broker command receiver running");

    let mut m_rx = rx;
    loop {
        let msg_opt = m_rx.recv().await;
        match msg_opt {
            Some(m) => {
                match m {
                    RegisterMessage(s) => {
                    }
                    DisconnectMessage(d) => {
                        
                    }
                }
            }
            None => {
                break;
            }
        }

    }

    warn!("Broker receiver closed. Aborting");

    /* TODO: must abort application here */
}
