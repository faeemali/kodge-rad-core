use std::error::Error;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use log::{error, info, warn};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use tokio::process::{Child, Command};
use tokio::sync::mpsc::{channel, Sender, Receiver};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time::sleep;
use crate::AppCtx;
use crate::client::apps::{get_app_base_directory, get_manifest_entry_for_app};
use crate::config::config::App;
use crate::control::message_types::ControlMessages;
use crate::control::message_types::ControlMessages::{MustDie, BrokerReady};
use crate::utils::timer::Timer;

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

//wait until the broker is ready, then return true. If a must_die message was received,
//return false
async fn wait_for_broker_ready(rx: &mut Receiver<ControlMessages>) -> bool {
    loop {
        if let Some(msg) = rx.recv().await {
            match msg {
                BrokerReady => {
                    return true;
                }
                MustDie(m) => {
                    info!("App runner caught must die message: {}", &m);
                    return false;
                }
                _ => {}
            }
            sleep(Duration::from_millis(10)).await;
        }
    }
}

async fn force_stop_apps<'a>(apps: &mut [RunningAppInfo<'a>], duration: Duration) {
    let timer = Timer::new(duration);
    loop {
        let mut running = false;
        for j in 0..apps.len() {
            let app = &mut apps[j];
            let child = &mut app.child;
            if let Ok(res) = child.try_wait() {
                if res.is_none() && timer.timed_out() {
                    running = true;
                    warn!("Force killing app: {}", app.app.instance_id);
                    let _ = child.kill().await;
                }
            }
        } //for

        if !running {
            info!("All apps stopped");
            break;
        }
    }
}

async fn signal_all_apps_to_stop(apps: &mut [RunningAppInfo<'_>]) {
    for j in 0..apps.len() {
        let app = &apps[j];
        let child = &app.child;
        let pid_opt = child.id();
        if let Some(pid) = pid_opt {
            let unix_pid = Pid::from_raw(pid as i32);

            info!("Sending SIGHUP to {}, pid={}", &app.app.instance_id, unix_pid);
            if let Err(e) = kill(unix_pid, Signal::SIGHUP) {
                error!("Failed to send SIGHUP signal to process {}. Error: {}", unix_pid, e);
            }
        }
    }
}

pub async fn app_runner_main(app_ctx: Arc<AppCtx>,
                             rx: Receiver<ControlMessages>,
                             ctrl_tx: Sender<ControlMessages>) {
    info!("Starting App Runner");

    let mut rx = rx;
    if !wait_for_broker_ready(&mut rx).await {
        warn!("App runner caught must die flag while waiting for broker to become ready. Aborting");
        return;
    }

    let mut app_infos = match start_apps(&app_ctx).await {
        Ok(children) => children,
        Err(e) => {
            panic!("Failed to start apps: {}", e);
        }
    };

    let mut done = false;
    while !done {
        match rx.try_recv() {
            Ok(msg) => {
                if let MustDie(msg) = msg {
                    info!("app runner caught must die flag ({}). Aborting", &msg);
                    signal_all_apps_to_stop(&mut app_infos).await;
                    done = true;
                    continue;
                }
            }

            Err(e) => {
                if e == TryRecvError::Disconnected {
                    panic!("App runner detected control plane disconnected. Aborting");
                }
            }
        }

        let mut must_stop_all_apps = false;
        for app_info in &mut app_infos {
            let child = &mut app_info.child;
            match child.try_wait() {
                Ok(status_opt) => {
                    if let Some(status) = status_opt {
                        info!("App {} exited with status {}", app_info.app.instance_id, status);
                        must_stop_all_apps = true;
                        break;
                    } //else app has not yet exited
                }
                Err(e) => {
                    error!("Error determining status of app instance: {}. Error: {}", &app_info.app.instance_id, &e);
                }
            }
        }

        if must_stop_all_apps {
            signal_all_apps_to_stop(&mut app_infos).await;
            force_stop_apps(&mut app_infos, Duration::from_millis(5000)).await;
            done = true;
            continue;
        }

        sleep(Duration::from_millis(50)).await;
    }

    info!("App runner stopped");
}