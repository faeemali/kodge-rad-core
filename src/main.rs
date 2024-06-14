use std::env::args;
use std::error::Error;
use std::ops::DerefMut;
use std::process::exit;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::app::{app_exists, get_all_apps};
use crate::error::RadError;
use crate::workflow::execute_workflow;

mod utils;
mod error;
mod config;
mod app;
mod workflow;
mod process;

pub struct AppCtx {
    pub base_dir: String,
    pub must_die: Arc<Mutex<bool>>,
}

fn show_help(app_name: &str) {
    println!("Usage: {} <config_dir> <command> [args...]", app_name);
    println!(r#"Commands:
    list - list all applications
    run <app_name> [args] - runs the configured application
    "#)
}

fn list_apps(app_ctx: &AppCtx) -> Result<(), Box<dyn Error>> {
    let apps = get_all_apps(&app_ctx.base_dir)?;
    for app in apps {
        println!("{}", app);
    }
    Ok(())
}

async fn run_app(app_ctx: &AppCtx, app: &str, args: &[String]) -> Result<(), Box<dyn Error>> {
    if !app_exists(&app_ctx.base_dir, app)? {
        return Err(Box::new(RadError::from("App not found")));
    }
    execute_workflow(app_ctx, app, args).await?;
    Ok(())
}

async fn process_cmd(app_ctx: &AppCtx, cmd_line: &[String]) -> Result<(), Box<dyn Error>> {
    let app_name = &cmd_line[0];
    let cmd = cmd_line[2].trim().to_lowercase();
    match cmd.as_str() {
        "list" => {
            if cmd_line.len() != 3 {
                show_help(app_name);
            }
            list_apps(app_ctx)?;
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
    println!("Caught signal. Aborting app");
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

    println!("Application Starting");
    println!("base_dir: {}", base_dir);

    let am_must_die = Arc::new(Mutex::new(false));


    let app_ctx = AppCtx {
        base_dir: base_dir.to_string(),
        must_die: am_must_die.clone(),
    };

    let am_must_die_clone = am_must_die.clone();
    ctrlc_async::set_async_handler(handle_signal(am_must_die_clone))?;

    process_cmd(&app_ctx, &args).await?;
    
    Ok(())
}
