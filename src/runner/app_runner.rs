use std::error::Error;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use log::{error, info};
use tokio::process::{Child, Command};
use tokio::sync::mpsc::{channel, Sender, Receiver};
use tokio::time::sleep;
use crate::AppCtx;
use crate::client::apps::{get_app_base_directory, get_manifest_entry_for_app};
use crate::config::config::App;
use crate::control::message_types::ControlMessages;

pub struct RunningAppInfo<'a> {
    pub app: &'a App,
    pub child: Child,
}

pub fn app_runner_init() -> (Sender<ControlMessages>, Receiver<ControlMessages>) {
    channel::<ControlMessages>(10)
}

const ENV_RAD_INSTANCE_ID: &str = "RAD_INSTANCE_ID";
async fn start_app(app_ctx: Arc<AppCtx>, app: &App) -> Result<RunningAppInfo, Box<dyn Error + Sync + Send>> {
    /* find the manifest for this app */
    let app_info = get_manifest_entry_for_app(app_ctx.clone(), app).await?;
    let app_base_dir = get_app_base_directory(app_ctx, app);

    let manifest = &app_info.manifest;
    let app_dir = format!("{}/{}", app_base_dir, manifest.version_code);

    let cmd_str = format!("{}/{}", &app_dir, &manifest.execution.cmd);
    let cmd = Path::new(cmd_str.as_str());
    let abs_cmd = fs::canonicalize(&cmd)?;

    info!("Starting app: {} with instance id: {} and command {}", &app.name, &app.instance_id, &abs_cmd.display());
    let mut command = Command::new(abs_cmd);

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
        child,
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

pub async fn app_runner_main(app_ctx: Arc<AppCtx>,
                             rx: Receiver<ControlMessages>,
                             ctrl_tx: Sender<ControlMessages>) {
    info!("Starting App Runner");

    info!("SLEEPING BEFORE STARTING APP. MUST CHANGE THIS!!!");
    sleep(Duration::from_secs(3)).await;

    let mut app_infos = match start_apps(&app_ctx).await {
        Ok(children) => children,
        Err(e) => {
            panic!("Failed to start apps: {}", e);
        }
    };

    let mut done = false;
    while !done {
        for app_info in &mut app_infos {
            let child = &mut app_info.child;
            match child.try_wait() {
                Ok(status_opt) => {
                    if let Some(status) = status_opt {
                        info!("App {} exited with status {}", app_info.app.name, status);

                        /* TODO: notify all other apps that they must die */
                        done = true;
                    } //else app has not yet exited
                }
                Err(e) => {
                    error!("Error determining status of app instance: {}", &app_info.app.instance_id);
                }
            }
        }

        sleep(Duration::from_millis(500)).await;
    }
}