use std::error::Error;
use tokio::process::{Child, Command};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;
use log::{debug, error, info, warn};
use tokio::sync::{RwLock};
use tokio::time::sleep;
use crate::bin::{BinConfig};
use crate::utils::utils;

async fn __check_app_exit(child: &mut Child, app_id: &str) -> Result<Option<ExitStatus>, Box<dyn Error + Sync + Send>> {
    let exit_opt_res = child.try_wait();
    if let Err(e) = exit_opt_res {
        error!("Error checking if app {} exited: {}", app_id, &e);
        return Err(Box::new(e));
    }

    let exit_opt = exit_opt_res.unwrap();
    if let Some(s) = exit_opt {
        return Ok(Some(s));
    } //else still running

    Ok(None)
}

pub fn spawn_process(base_dir: &str,
                     bin_config: &BinConfig,
                     capture_stdin: &Option<String>,
                     capture_stdout: &Option<String>) -> Result<Child, Box<dyn Error + Sync + Send>> {
    let exec_cmd = &bin_config.execution.cmd;
    let path = if exec_cmd.starts_with('/') {
        //debug!("Executing external app: {}", exec_cmd);
        exec_cmd.to_string()
    } else {
        //debug!("Executing included app: {}", exec_cmd);
        format!("{}/cache/{}/{}", base_dir, &bin_config.id.id, exec_cmd)
    };

    let mut cmd = Command::new(path);
    let mut process = cmd.kill_on_drop(true);

    if let Some(args) = &bin_config.execution.args {
        debug!("Using args: {:?} for {}", args, exec_cmd);
        process = process.args(args);
    }

    if let Some(working_dir) = &bin_config.execution.working_dir {
        debug!("Using working dir: {:?} for {}", working_dir, exec_cmd);
        process.current_dir(working_dir);
    }

    if capture_stdin.is_some() {
        process = process.stdin(Stdio::piped());
    } else {
        process = process.stdin(Stdio::null());
    }

    if capture_stdout.is_some() {
        process = process.stdout(Stdio::piped());
    } else {
        process = process.stdout(Stdio::null());
    }

    let child = process.spawn()?;
    Ok(child)
}

/// runs the app as specified in the app configuration, but does not
/// capture stdio. The app is monitored over its lifetime until it
/// it terminated, or the must_die flag is set
pub async fn run_bin_main(base_dir: String,
                          app: BinConfig,
                          am_must_die: Arc<RwLock<bool>>) -> Result<(), Box<dyn Error + Sync + Send>> {
    info!("Managing app task for: {}", &app.id.id);
    let mut child = spawn_process(&base_dir,
                                  &app,
                                  &None,
                                  &None)?;

    loop {
        match __check_app_exit(&mut child, &app.id.id).await {
            Ok(r_opt) => {
                if let Some(status) = &r_opt {
                    info!("App {} exited with code: {}", &app.id.id, status.code().unwrap_or(-1));
                    return Ok(());
                } //else app is still running
            }
            Err(e) => {
                error!("Error checking app exit status: {:?}. Aborting", e);
                break;
            }
        }

        let must_die = utils::get_must_die(am_must_die.clone()).await;
        if must_die {
            warn!("Caught must die flag for app: {}", &app.id.id);
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }

    warn!("Killing the child for app: {}", &app.id.id);
    child.kill().await?;

    Ok(())
}
