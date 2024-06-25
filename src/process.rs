use std::error::Error;
use std::ops::DerefMut;
use tokio::process::{Child, Command};
use std::process::{ExitStatus};
use std::sync::Arc;
use std::time::Duration;
use log::{error, info, warn};
use tokio::sync::Mutex;
use tokio::time::sleep;
use crate::app::{App};

async fn __get_must_die(am_must_die: Arc<Mutex<bool>>) -> bool {
    let must_die_mg = am_must_die.lock().await;
    *must_die_mg
}

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

fn spawn_process(base_dir: &str, app: &App) -> Result<Child, Box<dyn Error + Sync + Send>> {
    let path = format!("{}/cache/{}/{}", base_dir, &app.id.id, &app.execution.cmd);
    let mut cmd = Command::new(path);
    let mut process = cmd.kill_on_drop(true);

    if let Some(args) = &app.execution.args {
        process = process.args(args);
    }

    let child = process.spawn()?;
    Ok(child)
}

async fn set_must_die(am_must_die: Arc<Mutex<bool>>) {
    let mut must_die_mg = am_must_die.lock().await;
    let must_die = must_die_mg.deref_mut();
    *must_die = true;
}

pub async fn run_app_main(base_dir: String,
                          app: App,
                          am_must_die: Arc<Mutex<bool>>) -> Result<(), Box<dyn Error + Sync + Send>> {
    info!("Managing app task for: {}", &app.id.id);
    let mut child = spawn_process(&base_dir, &app)?;

    loop {
        match __check_app_exit(&mut child, &app.id.id).await {
            Ok(r_opt) => {
                if let Some(status) = &r_opt {
                    info!("App {} exited with code: {}", &app.id.id, status.code().unwrap_or(-1));
                } //else app is still running
            }
            Err(e) => {
                error!("Error checking app exit status: {:?}. Aborting", e);
                break;
            }
        }
            
        let must_die = __get_must_die(am_must_die.clone()).await;
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
