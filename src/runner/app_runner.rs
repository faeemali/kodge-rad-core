use std::collections::HashMap;
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

//given arg, which is a string potentially containing one or
//more variables (where the variable names are the keys of "replacements",
//this function replaces all variables with the corresponding
//values
fn replace_variables(arg: &str, replacements: HashMap<String, String>) -> String {
    println!("processing: {}", &arg);
    let mut ret = String::from(arg);
    for (key, value) in &replacements {
        /* create a variable string eg. XXX becomes ${XXX} */
        let key_var = format!("${{{}}}", key.as_str());
        println!("replacing: {}. ret is: {}", &key_var, &ret);
        loop {
            /* replace all instances of the variable */
            if !ret.contains(key_var.as_str()) {
                break;
            }
            ret = ret.replace(&key_var, value);
        }
    }

    println!("Returning: {}", &ret);
    ret
}

const VAR_OS: &str = "RAD_OS";
const VAR_ARCH: &str = "RAD_ARCH";
const VAR_BROKER_ADDR: &str = "RAD_BROKER_ADDR";
const VAR_BASE_DIR: &str = "RAD_BASE_DIR";
const ENV_RAD_INSTANCE_ID: &str = "RAD_INSTANCE_ID";
fn create_variables(app_ctx: Arc<AppCtx>, app: &App) -> HashMap<String, String> {
    let mut map = HashMap::new();
    map.insert(ENV_RAD_INSTANCE_ID.to_string(), app.instance_id.clone());
    map.insert(VAR_BASE_DIR.to_string(), app_ctx.base_dir.clone());
    map.insert(VAR_BROKER_ADDR.to_string(), app_ctx.config.broker.bind_addr.clone());
    map.insert(VAR_OS.to_string(), app_ctx.system_info.os.clone());
    map.insert(VAR_ARCH.to_string(), app_ctx.system_info.arch.clone());
    map
}

/* for each argument, look for variables and replace them */
fn pre_process_args(args: &[String], vars: HashMap<String, String>) -> Vec<String> {
    let mut ret = vec![];
    for arg in args {
        let v = replace_variables(arg, vars.clone());
        ret.push(v);
    }
    ret
}

async fn start_app(app_ctx: Arc<AppCtx>, app: &App) -> Result<RunningAppInfo, Box<dyn Error + Sync + Send>> {
    /* find the manifest for this app */
    let app_info = get_manifest_entry_for_app(app_ctx.clone(), app).await?;
    let app_base_dir = get_app_base_directory(app_ctx.clone(), app);

    let manifest = &app_info.manifest;
    let app_dir = format!("{}/{}", app_base_dir, manifest.version_code);

    let cmd_str = format!("{}/{}", &app_dir, &manifest.execution.cmd);
    let cmd = Path::new(cmd_str.as_str());
    let abs_cmd = fs::canonicalize(&cmd)?;

    info!("Starting app: {} with instance id: {} and command {}", &app.name, &app.instance_id, &abs_cmd.display());
    let mut command = Command::new(abs_cmd);

    // Set the command-line arguments
    if let Some(args) = &manifest.execution.args {
        let vars = create_variables(app_ctx.clone(), app);
        let processed_args = pre_process_args(args, vars);
        command.args(processed_args);
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


#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_pre_process_args_with_valid_variables() {
        let args = vec![
            "os ${RAD_OS} ${RAD_ARCH}".to_string(),
            "${RAD_INSTANCE_ID} is your instance id".to_string(),
            "and ${RAD_BASE_DIR} is your base dir".to_string(),
            "${RAD_BROKER_ADDR}".to_string(),
            "no variables here".to_string(),
            "${UNKNOWN_VARIABLE} is an unknown variable".to_string(),
            "${} will not be processed".to_string(),
        ];

        let mut vars = HashMap::new();
        vars.insert(ENV_RAD_INSTANCE_ID.to_string(), "test-app-1".to_string());
        vars.insert(VAR_BASE_DIR.to_string(), "foo/blah".to_string());
        vars.insert(VAR_BROKER_ADDR.to_string(), "localhost:8080".to_string());
        vars.insert(VAR_OS.to_string(), "macos".to_string());
        vars.insert(VAR_ARCH.to_string(), "aarch64".to_string());

        let result = pre_process_args(&args, vars);

        assert_eq!(result.len(), 7);
        assert_eq!(result[0], "os macos aarch64");
        assert_eq!(result[1], "test-app-1 is your instance id");
        assert_eq!(result[2], "and foo/blah is your base dir");
        assert_eq!(result[3], "localhost:8080");
        assert_eq!(result[4], "no variables here");
        assert_eq!(result[5], "${UNKNOWN_VARIABLE} is an unknown variable");
        assert_eq!(result[6], "${} will not be processed");
    }
}
