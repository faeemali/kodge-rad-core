use std::error::Error;
use std::path::Path;
use serde::{Deserialize, Serialize};
use crate::config::config_common::{ConfigId};
use crate::utils::utils::{get_dirs, load_yaml};

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct AppExecution {
    pub cmd: String,
    pub args: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct App {
    pub id: ConfigId,
    pub execution: AppExecution,
}

impl App {
    pub fn verify(&self) -> Result<(), Box<dyn Error>> {
        self.id.print();
        Ok(())
    }
}

pub fn load_app(base_dir: &str, app_name: &str) -> Result<App, Box<dyn Error>> {
    let filename = format!("{}/cache/{}/config.yaml", base_dir, app_name);
    let app = load_yaml::<App>(&filename)?;
    Ok(app)
}

pub fn get_all_workflows(base_dir: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let filename = format!("{}/workflows", base_dir);
    let path = Path::new(filename.as_str());
    if !Path::exists(path) {
        return Ok(vec![]); //no apps
    }

    let apps = get_dirs(path)?;
    Ok(apps)
}

pub fn workflow_exists(base_dir: &str, app_name: &str) -> Result<bool, Box<dyn Error>> {
    let apps = get_all_workflows(base_dir)?;
    for app in &apps {
        if app == app_name {
            return Ok(true);
        }
    }
    Ok(false)
}


