use std::env::args;
use std::error::Error;
use std::process::exit;
use std::sync::Arc;
use futures::StreamExt;
use tokio::sync::{RwLock};
use log::{error, info, warn};
use crate::broker::broker::{broker_main, BrokerConfig};
use crate::config::config::{Config, config_load};
use crate::error::{raderr};
use crate::utils::utils::set_must_die;

mod utils;
mod error;
mod config;
mod bin;
mod process;
mod broker;
mod app_container;

#[derive(Clone)]
pub struct AppCtx {
    pub base_dir: String,
    pub must_die: Arc<RwLock<bool>>,
    pub config: Config,
}

fn show_help(app_name: &str) {
    println!("Usage: {} <config_dir>", app_name);
}

async fn handle_signal(am_must_die: Arc<RwLock<bool>>) {
    warn!("Caught signal. Aborting app");
    set_must_die(am_must_die).await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let args: Vec<String> = args().collect();
    if args.len() != 2 {
        show_help(&args[0]);
        exit(1);
    }

    let base_dir = args[1].as_str();

    let config = config_load(base_dir)?;

    log4rs::init_file(format!("{}/log_conf.yaml", base_dir), Default::default())?;

    info!("Application Starting");
    info!("base_dir: {}", base_dir);
    let am_must_die = Arc::new(RwLock::new(false));

    let app_ctx = AppCtx {
        base_dir: base_dir.to_string(),
        must_die: am_must_die.clone(),
        config: config.clone(),
    };

    let am_must_die_clone = am_must_die.clone();
    ctrlc_async::set_async_handler(handle_signal(am_must_die_clone))?;

    let handle = tokio::spawn(broker_main(base_dir.to_string(),
                             BrokerConfig::new(config.broker.bind_addr.clone()),
                             am_must_die.clone(),
    ));

    let res = tokio::join!(handle);
    if let Err(e) = &res.0 {
        error!("Error joining with broker: {}", e);
        exit(1);
    }

    info!("Main app exiting");
    exit(0);
}
