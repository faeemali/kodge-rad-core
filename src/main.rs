use std::env::args;
use std::error::Error;
use std::process::exit;
use std::sync::Arc;
use tokio::sync::{RwLock};
use log::{info, warn};
use crate::broker::app_broker::{start_broker, BrokerConfig};
use crate::config::config::{Config};
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
    println!("Usage: {} <config_dir> <config_file>", app_name);
}

async fn handle_signal(am_must_die: Arc<RwLock<bool>>) {
    warn!("Caught signal. Aborting app");
    set_must_die(am_must_die).await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let args: Vec<String> = args().collect();
    if args.len() != 3 {
        show_help(&args[0]);
        exit(1);
    }

    let base_dir = args[1].as_str();
    let runtime_config = args[2].to_string();

    let config = Config::load(&runtime_config)?;
    let a_config = Arc::new(config);

    log4rs::init_file(format!("{}/log_conf.yaml", base_dir), Default::default())?;

    info!("Application Starting");
    info!("base_dir: {}", base_dir);
    let am_must_die = Arc::new(RwLock::new(false));

    let am_must_die_clone = am_must_die.clone();
    ctrlc_async::set_async_handler(handle_signal(am_must_die_clone))?;

    start_broker(base_dir.to_string(), a_config, am_must_die).await?;

    info!("Main app exiting");
    exit(0);
}
