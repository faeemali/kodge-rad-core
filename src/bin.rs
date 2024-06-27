use std::error::Error;
use std::path::Path;
use serde::{Deserialize, Serialize};
use crate::config::config_common::{ConfigId};
use crate::utils::utils::{get_dirs, load_yaml};

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct BinExecution {
    pub cmd: String,
    pub args: Option<Vec<String>>,
    pub working_dir: Option<String>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct Bin {
    pub id: ConfigId,
    pub execution: BinExecution,
}

impl Bin {
    pub fn verify(&self) -> Result<(), Box<dyn Error>> {
        self.id.print();
        Ok(())
    }
}

pub fn load_bin(base_dir: &str, app_name: &str) -> Result<Bin, Box<dyn Error>> {
    let filename = format!("{}/cache/{}/config.yaml", base_dir, app_name);
    let bin = load_yaml::<Bin>(&filename)?;
    Ok(bin)
}



