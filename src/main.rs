use std::env::args;
use std::error::Error;
use std::process::exit;
use std::sync::Arc;
use tokio::sync::{RwLock};
use log::{info, warn};
use tokio::spawn;
use crate::broker::app_broker::{start_broker, BrokerConfig};
use crate::client::apps::{update_manifest, init_cache, show_manifest_summary, read_manifest, get_app, get_apps};
use crate::config::config::{Config};
use crate::utils::app_runner::{app_runner_init, app_runner_main};
use crate::utils::rad_utils::{get_system_info, set_must_die, SystemInfo};

mod utils;
mod error;
mod config;
mod process;
mod broker;
mod client;

#[derive(Clone)]
pub struct AppCtx {
    pub base_dir: String,
    pub must_die: Arc<RwLock<bool>>,
    pub config: Config,
    pub system_info: SystemInfo,
}

fn show_help(app_name: &str) {
    /* TODO: make <config_dir> something like ${HOME}/.rad by default */
    println!("Usage: {} <config_dir> <config_file> [options]", app_name);
    println!("Options:");
    println!("  update           Retrieves and saves the latest app information");
    println!("  list             Lists a summary of all available applications");
    println!("  get              Downloads all apps for the specified workflow");
}

async fn handle_signal(app_ctx: Arc<AppCtx>) {
    warn!("Caught signal. Aborting app");
    set_must_die(app_ctx.must_die.clone()).await;
}

async fn process_commands(app_ctx: Arc<AppCtx>, opts: &[String]) -> Result<(), Box<dyn Error + Send + Sync>> {
    match opts[0].as_str() {
        "update" => {
            update_manifest(app_ctx.clone()).await?;
            info!("manifest updated");            
        }
        
        "list" => {
            let manifests = read_manifest(app_ctx.clone()).await?;
            show_manifest_summary(manifests);
        }
        
        "get" => {
            get_apps(app_ctx.clone()).await?;
            info!("Apps downloaded");
        }
        
        _ => {
            return Err("Unknown command".into());
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let args: Vec<String> = args().collect();
    if args.len() < 3 {
        show_help(&args[0]);
        exit(1);
    }

    let base_dir = args[1].as_str();
    let runtime_config = args[2].to_string();

    let am_must_die = Arc::new(RwLock::new(false));
    let config = Config::load(&runtime_config)?;
    log4rs::init_file(format!("{}/log_conf.yaml", base_dir), Default::default())?;


    let app_ctx = AppCtx {
        base_dir: base_dir.to_string(),
        must_die: am_must_die,
        config,
        system_info: get_system_info(),
    };
    let a_app_ctx = Arc::new(app_ctx);
    
    init_cache(a_app_ctx.clone())?;

    if args.len() > 3 {
        if args[3] == "help" || args[3] == "--help" || args[3] == "-h" {
            show_help(&args[0]);
            exit(1);
        }

        process_commands(a_app_ctx.clone(), &args[3..]).await?;
        exit(0);
    }

    ctrlc_async::set_async_handler(handle_signal(a_app_ctx.clone()))?;
    
    let (app_runner_tx, app_runner_rx) = app_runner_init();
    
    info!("Application Starting");
    info!("base_dir: {}", base_dir);
    
    
    spawn(app_runner_main(a_app_ctx.clone(), app_runner_rx));
    
    start_broker(a_app_ctx).await?;
    info!("Main app exiting");
    exit(0);
}
