use std::env::args;
use std::error::Error;
use std::ops::DerefMut;
use std::process::exit;
use std::sync::Arc;
use tokio::sync::Mutex;
use log::{info, warn};

use crate::config::config::{Config, config_load};
use crate::error::RadError;
use crate::workflow::{execute_workflow, get_all_workflows, workflow_exists};

mod utils;
mod error;
mod config;
mod app;
mod workflow;
mod process;
mod broker;

pub struct AppCtx {
    pub base_dir: String,
    pub must_die: Arc<Mutex<bool>>,
    pub config: Config,
}

fn show_help(app_name: &str) {
    println!("Usage: {} <config_dir> <command> [args...]", app_name);
    println!(r#"Commands:
    list_workflows - list all workflows
    run <app_name> [args] - runs the configured application
    "#)
}

fn list_workflows(app_ctx: &AppCtx) -> Result<(), Box<dyn Error>> {
    let workflows = get_all_workflows(&app_ctx.base_dir)?;
    for workflow_id in &workflows {
        workflow_id.print();
    }
    Ok(())
}

async fn run_app(app_ctx: &AppCtx, app: &str, args: &[String]) -> Result<(), Box<dyn Error>> {
    if !workflow_exists(&app_ctx.base_dir, app)? {
        return Err(Box::new(RadError::from("App not found")));
    }
    execute_workflow(app_ctx, app, args).await?;
    Ok(())
}

async fn process_cmd(app_ctx: &AppCtx, cmd_line: &[String]) -> Result<(), Box<dyn Error>> {
    let app_name = &cmd_line[0];
    let cmd = cmd_line[2].trim().to_lowercase();
    match cmd.as_str() {
        "list_workflows" => {
            if cmd_line.len() != 3 {
                show_help(app_name);
            }
            list_workflows(app_ctx)?;
        }
        "run" => {
            if cmd_line.len() < 4 {
                show_help(app_name);
            }
            let args = if cmd_line.len() == 4 {
                &[]
            } else {
                &cmd_line[4..]
            };
            run_app(app_ctx, &cmd_line[3], args).await?;
        }
        _ => {
            show_help(&cmd_line[0]);
        }
    }

    Ok(())
}

async fn handle_signal(am_must_die: Arc<Mutex<bool>>) {
    warn!("Caught signal. Aborting app");
    let mut must_die_mg = am_must_die.lock().await;
    let must_die = must_die_mg.deref_mut();
    *must_die = true;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = args().collect();
    if args.len() < 3 {
        show_help(&args[0]);
        exit(1);
    }

    let base_dir = args[1].as_str();

    let config = config_load(base_dir)?;

    log4rs::init_file(format!("{}/log_conf.yaml", base_dir), Default::default())?;

    info!("Application Starting");
    info!("base_dir: {}", base_dir);

    let am_must_die = Arc::new(Mutex::new(false));

    let app_ctx = AppCtx {
        base_dir: base_dir.to_string(),
        must_die: am_must_die.clone(),
        config: config.clone(),
    };

    let am_must_die_clone = am_must_die.clone();
    ctrlc_async::set_async_handler(handle_signal(am_must_die_clone))?;

    process_cmd(&app_ctx, &args).await?;

    Ok(())
}
