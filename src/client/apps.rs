use std::error::Error;
use std::sync::Arc;
use log::error;
use serde::{Deserialize, Serialize};
use crate::AppCtx;

#[derive(Serialize, Deserialize, Debug)]
pub struct ManifestItem {
    pub name: String,       
    pub summary: String,    
    pub description: String,

    /// VersionName: some name like v0.0.1-test2. Basically, a text name
    #[serde(rename = "version-name")]
    pub version_name: String,

    /// An integer representing the app. Every app must have a unique integer value.
    /// It is not a requirement that the value is always increasing, only that it is unique.
    /// This allows, eg. for values 0-1000 to be stable releases, while 1001-1999 are test
    /// releases or whatever.
    #[serde(rename = "version-code")]
    pub version_code: u64,

    pub usage: String,      
    #[serde(rename = "rx-types")]
    pub rx_types: Vec<String>,
    #[serde(rename = "tx-types")]
    pub tx_types: Vec<String>,  
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppItem {
    pub name: String,                  // Name of the application
    pub manifests: Vec<ManifestItem>, // A list of the application versions
}

pub async fn get_manifests(app_ctx: Arc<AppCtx>) -> Result<Vec<AppItem>, Box<dyn Error + Sync + Send>> {
    let system = app_ctx.system_info.get_url_encoded_system_string();
    let resp = reqwest::get(format!("{}/rest/get-manifests?system={}", &app_ctx.config.server.addr, system)).await?;
    if !resp.status().is_success() {
        error!("Failed to get manifests: {}", resp.status());
        return Err(format!("Failed to get manifests. status={}", resp.status()).into());
    }

    let items = resp.json::<Vec<AppItem>>().await?;
    Ok(items)
}