use std::error::Error;
use std::fs::File;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub input: String,
    pub output: String,
}

pub fn config_load(base_dir: &str) -> Result<Config, Box<dyn Error>> {
    let path = format!("{}/config.json", base_dir);
    let f = File::open(&path)?;
    let config = serde_json::from_reader::<&File, Config>(&f)?;
    Ok(config)
}
