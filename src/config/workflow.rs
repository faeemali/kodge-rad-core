use serde::{Deserialize, Serialize};
use crate::config::config_common::ConfigId;

#[derive(Serialize, Deserialize)]
pub struct Workflow {
    pub id: ConfigId,
    pub apps: Vec<String>,
    pub connections: Vec<OutIn>,
}

#[derive(Serialize, Deserialize)]
pub struct OutIn {
    #[serde(rename = "out")]
    pub output: String,
    #[serde(rename = "in")]
    pub input: String,
}