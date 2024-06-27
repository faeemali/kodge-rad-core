use std::error::Error;
use std::fs;
use std::ops::DerefMut;
use std::time::Duration;
use log::{error, info};
use serde::{Deserialize, Serialize};
use tokio::select;
use tokio::task::JoinSet;
use tokio::time::sleep;
use crate::AppCtx;
use crate::config::config_common::ConfigId;
use crate::error::RadError;
use crate::utils::utils;
use crate::utils::utils::load_yaml;
use crate::workflow::execute_workflow;

#[derive(Serialize, Deserialize, Clone)]
pub struct App {
    pub id: ConfigId,
    pub workflows: Vec<AppWorkflowItem>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AppWorkflowItem {
    pub name: String,
    pub args: Vec<String>,
}

pub fn get_all_apps(base_dir: &str) -> Result<Vec<App>, Box<dyn Error + Sync + Send>> {
    let mut ret = vec![];

    let path = format!("{}/apps", base_dir);
    let files = fs::read_dir(&path)?;
    for file_res in files {
        if let Err(e) = file_res {
            error!("Error reading app file: {}. Skipping", e);
            continue;
        }
        let file = file_res.unwrap();
        if let Ok(file_type) = file.file_type() {
            if file_type.is_file() {
                match file.file_name().to_str() {
                    Some(f) => {
                        let name = format!("{}/{}", &path, f);
                        let app = load_yaml::<App>(&name)?;
                        ret.push(app);
                    }
                    None => {
                        error!("Error reading application file. Skipping");
                        continue;
                    }
                }
            }
        }
    }
    Ok(ret)
}

fn find_app_by_id(base_dir: &str, app_id: &str) -> Result<Option<App>, Box<dyn Error + Sync + Send>> {
    let apps = get_all_apps(base_dir)?;
    let app = apps.iter().find(|a| a.id.id == app_id).cloned();
    Ok(app)
}

pub async fn execute_app(app_ctx: AppCtx, app_id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let app = match find_app_by_id(&app_ctx.base_dir, app_id)? {
        Some(a) => { a }
        None => {
            return Err(Box::new(RadError::from(format!("Application not found: {}", app_id))));
        }
    };

    /* start all workflows */
    let mut join_set = JoinSet::new();

    for wf in app.workflows {
        join_set.spawn(execute_workflow(app_ctx.clone(), wf.name.clone(), wf.args.clone()));
    }

    // loop {
    //     sleep(Duration::from_millis(10)).await;
    // }

    match join_set.join_next().await {
        Some(res) => {
            match res {
                Ok(r_res) => {
                    match r_res {
                        Ok(_) => {
                            info!("Workflow terminated without error. Aborting remaining workflows");
                            utils::set_must_die(app_ctx.must_die.clone()).await;
                        }
                        Err(e) => {
                            error!("Workflow aborted with error: {}. Aborting all", &e);
                            utils::set_must_die(app_ctx.must_die.clone()).await;
                        }
                    }
                }
                Err(e) => {
                    error!("Join error detected. Aborting everything. Error: {}", &e);
                    utils::set_must_die(app_ctx.must_die.clone()).await;
                }
            }
        }
        
        None => {
            return Err(Box::new(RadError::from("Unexpected error. Got none while waiting for workflows to complete")));
        }
    }
    
    Ok(())
}