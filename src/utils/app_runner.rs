use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use log::{error, info};
use tokio::process::{Child, Command};
use tokio::sync::mpsc::{channel, Sender, Receiver};
use tokio::time::sleep;
use crate::AppCtx;
use crate::client::apps::{get_app_base_directory, get_manifest_entry_for_app};
use crate::config::config::App;

pub enum AppRunnerCmds {
    Exit(String),
}

pub struct RunningAppInfo<'a> {
    pub app: &'a App,
    pub child: Child,
}

pub fn app_runner_init() -> (Sender<AppRunnerCmds>, Receiver<AppRunnerCmds>) {
    channel::<AppRunnerCmds>(10)
}

const ENV_RAD_INSTANCE_ID: &str = "RAD_INSTANCE_ID";
async fn start_app(app_ctx: Arc<AppCtx>, app: &App) -> Result<RunningAppInfo, Box<dyn Error + Sync + Send>> {
    /* find the manifest for this app */
    let app_info = get_manifest_entry_for_app(app_ctx.clone(), app).await?;
    let app_base_dir = get_app_base_directory(app_ctx, app);

    let manifest = &app_info.manifest;
    let app_dir = format!("{}/{}", app_base_dir, manifest.version_code);

    /* TODO: must use absolute path or this won't work */
    let cmd = format!("{}/{}", &app_dir, &manifest.execution.cmd);
    
    info!("Starting app: {} with instance id: {} and command {}", &app.name, &app.instance_id, &cmd);
    let mut command = Command::new(cmd);

    // Set the command-line arguments
    if let Some(args) = &manifest.execution.args {
        command.args(args);
    }

    // Set the working directory
    command.current_dir(app_dir.as_str());

    //specify environment variable
    command.env(ENV_RAD_INSTANCE_ID, &app.instance_id);

    // Execute the command and capture the output
    let child = command.spawn()?;
    Ok(RunningAppInfo {
        app,
        child
    })
}

async fn start_apps<'a>(app_ctx: &'a Arc<AppCtx>) -> Result<Vec<RunningAppInfo<'a>>, Box<dyn Error + Sync + Send>> {
    let mut children = vec![];
    for app in &app_ctx.config.apps {
        let child = start_app(app_ctx.clone(), app).await?;
        children.push(child);
    }

    Ok(children)
}

pub async fn app_runner_main(app_ctx: Arc<AppCtx>, rx: Receiver<AppRunnerCmds>) {
    info!("Starting App Runner");

    info!("SLEEPING BEFORE STARTING APP. MUST CHANGE THIS!!!");
    sleep(Duration::from_secs(5)).await;

    let mut app_infos = match start_apps(&app_ctx).await {
        Ok(children) => children,
        Err(e) => {
            panic!("Failed to start apps: {}", e);
        }
    };

    loop {
        for app_info in &mut app_infos {
            let child = &mut app_info.child;
            match child.try_wait() {
                Ok(status_opt) => {
                    if let Some(status) = status_opt {
                        info!("App {} exited with status {}", app_info.app.name, status);

                        /* TODO: notify all other apps that they must die */
                    } //else app has not yet exited
                }
                Err(e) => {
                    error!("Error determining status of app instance: {}", &app_info.app.instance_id);
                }
            }
        }
    }
}