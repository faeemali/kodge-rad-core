use std::error::Error;
use std::fs::File;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Config {
}

pub fn config_load(base_dir: &str) -> Result<Config, Box<dyn Error>> {
    let path = format!("{}/config.yaml", base_dir);
    let f = File::open(&path)?;
    let config = serde_yaml::from_reader::<&File, Config>(&f)?;
    Ok(config)
}
