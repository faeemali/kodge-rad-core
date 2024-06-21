use log::error;
use tokio::sync::mpsc::{Receiver};
use tokio::sync::mpsc::error::TryRecvError;

pub struct AddRoutesReq {

}

pub enum RouterControlMessages {
    AddRoutes(AddRoutesReq),
    RemoveRoutes(String),
}

async fn process_control_message(msg: RouterControlMessages) {
    match msg {
        RouterControlMessages::RemoveRoutes(name) => {

        }
        _ => {

        }
    }
}

pub async fn router_main(ctrl_rx: Receiver<RouterControlMessages>) {
    let mut m_ctrl_rx = ctrl_rx;
    loop {
        match m_ctrl_rx.try_recv() {
            Ok(msg) => {
                process_control_message(msg).await;
            }
            Err(e) => {
                if e == TryRecvError::Disconnected {
                    error!("Router control disconnect detected. Aborting");
                    break;
                }
            }
        }
    }

    error!("Router error detected. Aborting");
    /* TODO: kill application here */
}