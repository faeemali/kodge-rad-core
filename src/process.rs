use std::error::Error;
use tokio::process::{Child};
use std::process::{ExitStatus};
use log::{error};

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