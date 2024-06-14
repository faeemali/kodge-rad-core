use std::error::Error;
use std::path::Path;
use log::info;
use serde::{Deserialize, Serialize};
use crate::config::config_common::{ConfigId, KVPair};
use crate::error::RadError;
use crate::utils::utils::{get_dirs, load_yaml};
use crate::workflow::AppIoDirection;

pub const STDIN: &str = "stdin";
pub const STDOUT: &str = "stdout";
pub const STDERR: &str = "stderr";

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct AppExecution {
    pub cmd: String,
    pub args: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct AppIoInOut {
    #[serde(rename = "in")]
    pub input: Option<Vec<AppIoDefinition>>,

    #[serde(rename = "out")]
    pub output: Option<Vec<AppIoDefinition>>,
}

impl AppIoInOut {
    pub fn verify(&self) -> Result<(), Box<dyn Error>> {
        if let Some(input) = &self.input {
            for io in input {
                io.verify(true)?;
            }
        }

        if let Some(output) = &self.output {
            for io in output {
                io.verify(false)?;
            }
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct AppIoDefinition {
    pub id: ConfigId,
    #[serde(rename = "type")]
    pub io_type: String,
    pub integration: AppIoIntegration,
}

impl AppIoDefinition {
    pub fn verify(&self, input: bool) -> Result<(), Box<dyn Error>> {
        self.print();
        if self.io_type != "single" {
            return Err(Box::from("Invalid IO type"));
        }

        self.integration.verify(input)?;

        Ok(())
    }

    pub fn print(&self) {
        self.id.print();
        info!("Type: {}", self.io_type);
        self.integration.print();
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct AppIoIntegration {
    #[serde(rename = "type")]
    pub integration_type: String,
    pub extras: Option<Vec<KVPair>>,
}

impl AppIoIntegration {
    pub fn verify(&self, input: bool) -> Result<(), Box<dyn Error>> {
        self.print();
        if input {
            if self.integration_type != STDIN {
                return Err(Box::new(RadError::from("Invalid input type specified")));
            }
        } else if self.integration_type != STDOUT && self.integration_type != STDERR {
            return Err(Box::new(RadError::from("Invalid output type specified")));
        }
        Ok(())
    }

    pub fn print(&self) {
        info!("Integration:");
        info!("type: {}", self.integration_type);
        if let Some(extras) = &self.extras {
            info!("Extras:");
            for extra in extras {
                info!("k: {}, v: {}", &extra.key, &extra.value);
            }
        }
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct App {
    pub id: ConfigId,
    pub io: AppIoInOut,
    pub execution: AppExecution,
}

impl App {
    pub fn verify(&self) -> Result<(), Box<dyn Error>> {
        self.id.print();
        self.io.verify()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn find_connector_by_id<'a>(&self, id: &str, ios: &'a Option<Vec<AppIoDefinition>>) -> Option<&'a AppIoDefinition> {
        match ios {
            Some(s) => {
                for def in s {
                    if def.id.id == id {
                        return Some(def);
                    }
                }
                None
            }
            None => {
                None
            }
        }
    }

    #[allow(dead_code)]
    pub fn find_connector(&self, id: &str, direction: AppIoDirection) -> Option<&AppIoDefinition> {
        return if direction == AppIoDirection::In {
            self.find_connector_by_id(id, &self.io.input)
        } else {
            self.find_connector_by_id(id, &self.io.output)
        };
    }
}

pub fn load_app(base_dir: &str, app_name: &str) -> Result<App, Box<dyn Error>> {
    let filename = format!("{}/cache/{}/config.yaml", base_dir, app_name);
    let app = load_yaml::<App>(&filename)?;
    Ok(app)
}

pub fn get_all_apps(base_dir: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let filename = format!("{}/apps", base_dir);
    let path = Path::new(filename.as_str());
    if !Path::exists(path) {
        return Ok(vec![]); //no apps
    }

    let apps = get_dirs(path)?;
    Ok(apps)
}

pub fn app_exists(base_dir: &str, app_name: &str) -> Result<bool, Box<dyn Error>> {
    let apps = get_all_apps(base_dir)?;
    for app in &apps {
        if app == app_name {
            return Ok(true);
        }
    }
    Ok(false)
}


