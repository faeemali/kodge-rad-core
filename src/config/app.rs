use serde::{Deserialize, Serialize};
use crate::config::config_common::{ConfigId, KVPair};

#[derive(Serialize, Deserialize)]
pub struct App {
    pub id: ConfigId,
    #[serde(rename = "type")]
    pub app_type: String,
    pub io: Vec<AppIo>,
    pub execution: AppExecution,
}

#[derive(Serialize, Deserialize)]
pub struct AppExecution {
    pub cmd: String,
    pub args: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct AppIo {
    pub id: ConfigId,
    #[serde(rename="type")]
    pub io_type: String,
    pub integration: AppIoIntegration,
}

#[derive(Serialize, Deserialize)]
pub struct AppIoIntegration {
    #[serde(rename = "type")]
    pub integration_type: String,
    pub extras: Option<KVPair>,
}