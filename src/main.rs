use std::env::args;
use std::error::Error;
use std::process::exit;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock};
use log::{info, warn};
use tokio::spawn;
use tokio::time::sleep;
use crate::broker::app_broker::{broker_init, broker_main};
use crate::client::apps::{update_manifest, init_cache, show_manifest_summary, read_manifest, get_app, get_apps};
use crate::config::config::{Config};
use crate::control::control_plane::{control_init, ctrl_main, CtrlCtx};
use runner::app_runner::{app_runner_init, app_runner_main};
use crate::router::router::{router_init, router_main};
use crate::utils::rad_utils::{get_system_info, SystemInfo};

mod utils;
mod error;
mod config;
mod process;
mod broker;
mod client;
mod control;
mod runner;
mod router;

#[derive(Clone)]
pub struct AppCtx {
    pub base_dir: String,
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

    let config = Config::load(&runtime_config)?;
    log4rs::init_file(format!("{}/log_conf.yaml", base_dir), Default::default())?;


    let app_ctx = AppCtx {
        base_dir: base_dir.to_string(),
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
    let (router_tx, router_rx) = router_init();
    let (broker_tx, broker_rx) = broker_init();
    let (ctrl_tx, ctrl_rx) = control_init();

    info!("Application Starting");
    info!("base_dir: {}", base_dir);

    /* start all modules */
    spawn(broker_main(a_app_ctx.clone(),
                      broker_rx,
                      ctrl_tx.clone()));
    spawn(app_runner_main(a_app_ctx.clone(),
                          app_runner_rx,
                          ctrl_tx.clone()));
    spawn(router_main(a_app_ctx.clone(),
                      router_rx,
                      ctrl_tx.clone()));

    let ctrl_ctx = CtrlCtx::new(router_tx,
                                app_runner_tx,
                                broker_tx);
    spawn(ctrl_main(ctrl_ctx, ctrl_rx));

    loop {
        sleep(Duration::from_millis(1000)).await;
    }


    info!("Main app exiting");
    exit(0);
}
