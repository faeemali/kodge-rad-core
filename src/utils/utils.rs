use std::error::Error;
use std::fs::{File, read_dir};
use std::ops::DerefMut;
use std::path::Path;
use std::sync::Arc;
use base64::DecodeError;
use base64::prelude::*;
use serde::de::DeserializeOwned;
use tokio::sync::{Mutex, RwLock};

pub fn get_value_or_unknown(opt: &Option<String>) -> String {
   match opt {
       Some(s) => {
           s.to_string()
       }
       None => {
           "[unknown]".to_string()
       }
   } 
}

pub fn get_dirs(dir_path: &Path) -> Result<Vec<String>, std::io::Error> {
    let mut dirs = Vec::new();
    for entry in read_dir(dir_path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            let name_os = entry.file_name();
            let name_opt = name_os.to_str();
            if name_opt.is_none() {
                continue;
            }
            let name = name_opt.unwrap();
            dirs.push(name.to_string());
        }
    }
    Ok(dirs)
}

pub fn load_yaml<T: DeserializeOwned>(filename: &str) -> Result<T, Box<dyn Error + Sync + Send>>{
    let path = Path::new(filename);
    let f = File::open(path)?;
    let yaml: T = serde_yaml::from_reader(&f)?;
    Ok(yaml)
}

#[allow(dead_code)]
pub fn decode_base64_byte_stream(encoded_data: &str) -> Result<Vec<u8>, DecodeError> {
    BASE64_STANDARD.decode(encoded_data)
}

pub async fn get_must_die(am_must_die: Arc<RwLock<bool>>) -> bool {
    let must_die_mg = am_must_die.read().await;
    *must_die_mg
}

pub async fn set_must_die(am_must_die: Arc<RwLock<bool>>) {
    let mut must_die_mg = am_must_die.write().await;
    let must_die = must_die_mg.deref_mut();
    *must_die = true;
}