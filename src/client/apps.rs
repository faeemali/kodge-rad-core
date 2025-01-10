use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use futures::TryStreamExt;
use log::{error, info};
use serde::{Deserialize, Serialize};
use crate::AppCtx;
use crate::config::config::App;
use crate::utils::utils;
use tokio;
use crate::utils::utils::{clean_directory, extract_tar_gz};

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

fn create_cache_dir(path: &str) -> Result<(), Box<dyn Error + Sync + Send>> {
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

const CACHE_DIR_NAME: &str = "cache";
const CACHE_TMP_DIR_NAME: &str = "tmp";
const CACHE_APPS_DIR_NAME: &str = "apps";
pub fn init_cache(app_ctx: Arc<AppCtx>) -> Result<(), Box<dyn Error + Sync + Send>> {
    let path = format!("{}/{}/{}", app_ctx.base_dir, CACHE_DIR_NAME, CACHE_TMP_DIR_NAME);
    create_cache_dir(&path)?;

    let path = format!("{}/{}/{}", app_ctx.base_dir, CACHE_DIR_NAME, CACHE_APPS_DIR_NAME);
    create_cache_dir(&path)?;

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

//given an app name, this searches through the manifest to find the latest version of the app
//and returns the app code and the associated checksum
async fn get_latest_app_from_manifest(app_ctx: Arc<AppCtx>, app: &App) -> Result<(u64, String), Box<dyn Error + Sync + Send>> {
    let manifest = read_manifest(app_ctx.clone()).await?;

    let mut found = false;
    for a in &manifest {
        if a.name != app.name {
            continue;
        }

        /* find the latest version */
        let mut latest = 0;
        let mut checksum = String::new();
        for a_info in &a.app_info {
            let m = &a_info.manifest;
            if m.version_code > latest {
                latest = m.version_code;
                checksum = a_info.checksum.clone();
                found = true;
            }
        }

        if found {
            return Ok((latest, checksum));
        }
    }

    Err("App not found".into())
}

//check if an app exists in the manifest and on success, returns the
//version code and checksum associated with the app
async fn app_exists_in_manifest(app_ctx: Arc<AppCtx>, app: &App) -> Result<(u64, String), Box<dyn Error + Sync + Send>> {
    let manifest = read_manifest(app_ctx.clone()).await?;
    for a in &manifest {
        if a.name != app.name {
            continue;
        }

        for a_info in &a.app_info {
            let manifest = &a_info.manifest;
            if manifest.version_name == app.version {
                return Ok((manifest.version_code, a_info.checksum.clone()));
            }
        }
    }

    Err("App not found".into())
}

const CHECKSUM_FILENAME: &str = "checksum.sha256";

//path must be the path to some specific version of the app eg. echo/1
//This function then gets the checksum from the checksum file for that app
fn get_app_checksum(path: &str) -> Result<String, Box<dyn Error + Sync + Send>> {
    let checksum_file = format!("{}/{}", path, CHECKSUM_FILENAME);

    /* read the checksum file */
    let mut cf = File::open(&checksum_file)?;

    let mut checksum = String::new();
    cf.read_to_string(&mut checksum)?;

    let trimmed = checksum.trim();
    if trimmed.len() != 64 {
        return Err(format!("checksum does not have 64 characters: {}", trimmed).into());
    }

    Ok(trimmed.to_string())
}

//checks if the app exists in the cache i.e. the actual files exist.
//This does not just look at the manifest. On success, the checksum of the
//app is returned
async fn app_exists_in_cache(app_ctx: Arc<AppCtx>, app: &App, version_code: u64) -> Result<Option<String>, Box<dyn Error + Sync + Send>> {
    let base_app_path = format!("{}/{}/{}/{}/{}", &app_ctx.base_dir, CACHE_DIR_NAME, CACHE_APPS_DIR_NAME, app.name, version_code);
    let path = Path::new(&base_app_path);
    if !path.exists() {
        return Ok(None);
    }

    let checksum = get_app_checksum(&base_app_path)?;
    Ok(Some(checksum))
}

const DOWNLOAD_FILENAME: &str = "download.tar.gz";
pub async fn download_app(app_ctx: Arc<AppCtx>, app: &App, tmp_dir: &str, cache_checksum: &str) -> Result<Option<String>, Box<dyn Error + Sync + Send>> {
    clean_directory(&tmp_dir)?;

    let system = utils::get_system_info().get_url_encoded_system_string();
    let url = format!("{}/rest/get-app?system={}&app={}&checksum={}&version={}",
                      &app_ctx.config.server.addr,
                      &system,
                      urlencoding::encode(&app.name),
                      urlencoding::encode(cache_checksum),
                      urlencoding::encode(&app.version));
    let response = reqwest::get(url).await?; // Await the response

    if response.status() == reqwest::StatusCode::NO_CONTENT {
        info!("Download not required for {}", &app.name);
        return Ok(None)
    }

    // Check if the response status is success
    let filename = if response.status().is_success() {
        // Get the response body as a stream
        let mut content = response.bytes_stream();

        // Create the output file
        let output_path = format!("{}/{}", &tmp_dir, DOWNLOAD_FILENAME);
        let path = Path::new(&output_path);
        let mut file = File::create(&path)?;

        // Save the contents to the file
        while let Some(chunk) = content.try_next().await? {
            file.write_all(&chunk)?;
        }
        info!("App {} downloaded successfully", output_path);

        output_path
    } else {
        return Err(format!("Download failed. Status={}", response.status()).into());
    };

    Ok(Some(filename))
}

const APP_VERSION_LATEST: &str = "latest";
pub async fn get_app(app_ctx: Arc<AppCtx>, app: &App) -> Result<(), Box<dyn Error + Sync + Send>> {
    let (version_code, manifest_checksum) = if app.version == APP_VERSION_LATEST {
        get_latest_app_from_manifest(app_ctx.clone(), app).await?
    } else {
        app_exists_in_manifest(app_ctx.clone(), app).await?
    };

    let cache_checksum = match app_exists_in_cache(app_ctx.clone(), app, version_code).await? {
        Some(checksum) => {
            checksum
        }

        None => {
            "".to_string()
        }
    };

    if manifest_checksum == cache_checksum {
        //looks like we already have the app. no need to download anything
        return Ok(())
    }

    let tmp_dir = format!("{}/{}/{}", &app_ctx.base_dir, CACHE_DIR_NAME, CACHE_TMP_DIR_NAME);
    let filename_opt = download_app(app_ctx.clone(), app, &tmp_dir, &cache_checksum).await?;
    if let Some(downloaded_filename) = filename_opt {
        /* create the app + version directory */
        let app_dir = format!("{}/{}/{}/{}/{}", &app_ctx.base_dir, CACHE_DIR_NAME, CACHE_APPS_DIR_NAME, &app.name, version_code);
        create_cache_dir(&app_dir)?;

        extract_tar_gz(&downloaded_filename, &app_dir)?;

        /* cleanup */
        clean_directory(&tmp_dir)?;
    } else {
        /*
            nothing was downloaded because there was nothing to download.
            But there was no error.
         */
    }

    Ok(())
}

pub async fn get_apps(app_ctx: Arc<AppCtx>) -> Result<(), Box<dyn Error + Sync + Send>> {
    /* get a unique list of apps to prevent unnecessary downloading */
    let mut map = HashMap::new();
    for app in &app_ctx.config.apps {
        map.insert(app.name.clone(), app.clone());
    }

    for app in map.values() {
        get_app(app_ctx.clone(), app).await?;
    }
    Ok(())
}