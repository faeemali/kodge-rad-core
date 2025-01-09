use std::error::Error;
use std::fs;
use std::fs::File;
use std::sync::Arc;
use log::{error, info};
use serde::{Deserialize, Serialize};
use crate::AppCtx;
use crate::config::config::App;

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
pub struct AppDownloadInfo {
    pub manifest: ManifestItem,
    pub checksum: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppItem {
    pub name: String,                  // Name of the application

    #[serde(rename = "app-info")]
    pub app_info: Vec<AppDownloadInfo>, // A list of the application versions
}

pub async fn apps_init(app_ctx: Arc<AppCtx>) -> Result<(), Box<dyn Error + Sync + Send>> {
    init_cache(app_ctx)?;
    Ok(())
}

const CACHE_DIR_NAME: &str = "cache";
const CACHE_TMP_DIR_NAME: &str = "tmp";
pub fn init_cache(app_ctx: Arc<AppCtx>) -> Result<(), Box<dyn Error + Sync + Send>>{
    let path = format!("{}/{}/{}", app_ctx.base_dir, CACHE_DIR_NAME, CACHE_TMP_DIR_NAME);
    match fs::metadata(&path) {
        Ok(metadata) => {
            if !metadata.is_dir() {
                return Err("cache file exists but is not a directory".into());                
            }
        }
        
        Err(_) => {
            info!("Cache directory does not exist. Creating");
            if let Err(e) = fs::create_dir_all(&path) {
                return Err(format!("Failed to create cache directory: {}", e).into());
            }
        }
    }
    
    Ok(())
}

const APP_MANIFEST_FILENAME: &str = "manifest.json";

fn get_manifest_path(base_dir: &str) -> String {
    format!("{}/{}/{}", base_dir, CACHE_DIR_NAME, APP_MANIFEST_FILENAME)
}

///update_manifests retrieves the latest manifest data and
/// saves to the cache.
pub async fn update_manifest(app_ctx: Arc<AppCtx>) -> Result<Vec<AppItem>, Box<dyn Error + Sync + Send>> {
    let system = app_ctx.system_info.get_url_encoded_system_string();
    let resp = reqwest::get(format!("{}/rest/get-manifests?system={}", &app_ctx.config.server.addr, system)).await?;
    if !resp.status().is_success() {
        error!("Failed to get manifests: {}", resp.status());
        return Err(format!("Failed to get manifests. status={}", resp.status()).into());
    }

    let items = resp.json::<Vec<AppItem>>().await?;
    
    let path = get_manifest_path(&app_ctx.base_dir);
    let file = File::create(&path)?;
    serde_json::to_writer_pretty(&file, &items)?;
        
    Ok(items)
}

///Reads and returns the manifest from the local filesystem
pub async fn read_manifest(app_ctx: Arc<AppCtx>) -> Result<Vec<AppItem>, Box<dyn Error + Sync + Send>> {
    let path = get_manifest_path(&app_ctx.base_dir);
    match fs::metadata(&path) {
        Ok(_) => {
            /* looks like the file exists */
        }
        Err(e) => {
            error!("Error reading manifest file: {}. Attempting to retrieve from server", e);
            if let Err(update_err) = update_manifest(app_ctx.clone()).await {
                return Err(update_err.into());    
            }
        }
    }
    
    let file = File::open(&path)?;
    let ret: Vec<AppItem> = serde_json::from_reader(file)?;
    Ok(ret)
}

pub fn show_manifest_summary(apps: Vec<AppItem>) {
    let mut version = "";
    let mut code = 0u64;
    let mut summary = "";
    (0..apps.len()).for_each(|i| {
        let app = &apps[i];
        app.app_info.iter().for_each(|info| {
           if info.manifest.version_code > code {
               let manifest = &info.manifest;
               code = manifest.version_code;
               version = manifest.version_name.as_str();
               summary = manifest.summary.as_str();
           } 
        });
        
        let version_info = format!("[{}, ({})]", version, apps[i].app_info.len());
        println!("{}. {:<15} {:<10} - {:<40}", i + 1, app.name, version_info, summary);
    })
}

async fn app_exists(app_ctx: Arc<AppCtx>, app: &App) {
    let path = format!("{}/{}/{}", app_ctx.base_dir, CACHE_DIR_NAME, app.name);    
}

async fn get_latest_app_version(app_ctx: Arc<AppCtx>, app: &App) -> Result<u64, Box<dyn Error + Sync + Send>> {
    let manifest = read_manifest(app_ctx.clone()).await?;
    
    let mut found = false;
    for a in &manifest {
        if a.name != app.name {
            continue;
        }
        
        /* find the latest version */
        let mut latest = 0;
        for a_info in &a.app_info {
            let m = &a_info.manifest;
            if m.version_code > latest {
                latest = m.version_code;
                found = true;
            }
        }
        
        if found {
            return Ok(latest);
        }
    }
    
    Err("App not found".into())
}

async fn app_exists_in_manifest(app_ctx: Arc<AppCtx>, app: &App) -> Result<u64, Box<dyn Error + Sync + Send>> {
    let manifest = read_manifest(app_ctx.clone()).await?;
    for a in &manifest {
        if a.name != app.name {
            continue;
        }
        
        for a_info in &a.app_info {
            let manifest = &a_info.manifest;
            if manifest.version_name == app.version {
                return Ok(manifest.version_code);
            }
        }
    }
    
    Err("App not found".into())
}

//checks if the app exists in the cache i.e. the actual files exist.
//This does not just look at the manifest
async fn app_exists_in_cache(app_ctx: Arc<AppCtx>, app: &App, version_code: u64) -> Result<bool, Box<dyn Error + Sync + Send>> {
    Ok(true)
}

const APP_VERSION_LATEST: &str = "latest";
pub async fn get_app(app_ctx: Arc<AppCtx>, app: &App) -> Result<(), Box<dyn Error + Sync + Send>> {
    let latest = if app.version == APP_VERSION_LATEST {
        get_latest_app_version(app_ctx.clone(), app).await?
    } else {
        app_exists_in_manifest(app_ctx.clone(), app).await?
    };
    
    Ok(())
}

pub async fn get_apps(app_ctx: Arc<AppCtx>) -> Result<(), Box<dyn Error + Sync + Send>> {
    for app in &app_ctx.config.apps {
        get_app(app_ctx.clone(), app).await?;
    }
    Ok(())
}