use std::env::args;
use std::error::Error;
use std::ops::DerefMut;
use std::process::exit;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use log::{info, warn};
use crate::app::{execute_app, get_all_apps};

use crate::config::config::{Config, config_load};
use crate::error::RadError;
use crate::utils::utils::set_must_die;
use crate::workflow::{execute_workflow, get_all_workflows, workflow_exists};

mod utils;
mod error;
mod config;
mod bin;
mod workflow;
mod process;
mod broker;
mod app;
mod app_container;

#[derive(Clone)]
pub struct AppCtx {
    pub base_dir: String,
    pub must_die: Arc<RwLock<bool>>,
    pub config: Config,
}

fn show_help(app_name: &str) {
    println!("Usage: {} <config_dir> <command> [args...]", app_name);
    println!(r#"Commands:
    list_workflows - list all workflows
    list_apps - list all applications
    run_wf <workflow_name> [args] - runs the configured workflow
    run_app <app_name> - runs the configured application
    "#)
}

fn list_workflows(app_ctx: &AppCtx) -> Result<(), Box<dyn Error + Sync + Send>> {
    let workflows = get_all_workflows(&app_ctx.base_dir)?;
    for (workflow, _) in &workflows {
        workflow.id.print();
    }
    Ok(())
}

fn list_apps(app_ctx: &AppCtx) -> Result<(), Box<dyn Error + Sync + Send>> {
    let apps = get_all_apps(&app_ctx.base_dir)?;
    for app in apps {
        app.id.print();
    }
    Ok(())
}

/// Run a workflow that's not linked to stdin or stdout (an app is required for that)
async fn run_workflow(app_ctx: AppCtx, wf_name: &str, args: Vec<String>) -> Result<(), Box<dyn Error + Sync + Send>> {
    if !workflow_exists(&app_ctx.base_dir, wf_name)? {
        return Err(Box::new(RadError::from("App not found")));
    }
    execute_workflow(app_ctx, wf_name.to_string(), args, None, None).await?;
    Ok(())
}

async fn run_app(app_ctx: AppCtx, app: &str) -> Result<(), Box<dyn Error + Sync + Send>> {
    execute_app(app_ctx, app).await?;
    Ok(())
}

async fn process_cmd(app_ctx: AppCtx, cmd_line: &[String]) -> Result<(), Box<dyn Error + Sync + Send>> {
    let app_name = &cmd_line[0];
    let cmd = cmd_line[2].trim().to_lowercase();
    match cmd.as_str() {
        "list_workflows" => {
            if cmd_line.len() != 3 {
                show_help(app_name);
            }
            list_workflows(&app_ctx)?;
        }
        "list_apps" => {
            if cmd_line.len() != 3 {
                show_help(app_name);
            }
            list_apps(&app_ctx)?;
        }

        "run_wf" => {
            if cmd_line.len() < 4 {
                show_help(app_name);
            }
            let args = if cmd_line.len() == 4 {
                vec![]
            } else {
                cmd_line[4..].to_vec()
            };
            run_workflow(app_ctx, &cmd_line[3], args).await?;
        }

        "run_app" => {
            if cmd_line.len() < 3 {
                show_help(app_name);
            }
            run_app(app_ctx, &cmd_line[3]).await?;
        }

        _ => {
            show_help(&cmd_line[0]);
        }
    }

    Ok(())
}

async fn handle_signal(am_must_die: Arc<RwLock<bool>>) {
    warn!("Caught signal. Aborting app");
    set_must_die(am_must_die).await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
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

    let am_must_die = Arc::new(RwLock::new(false));

    let app_ctx = AppCtx {
        base_dir: base_dir.to_string(),
        must_die: am_must_die.clone(),
        config: config.clone(),
    };

    let am_must_die_clone = am_must_die.clone();
    ctrlc_async::set_async_handler(handle_signal(am_must_die_clone))?;

    process_cmd(app_ctx, &args).await?;

    Ok(())
}
