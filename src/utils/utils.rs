use std::error::Error;
use std::fs::{File, read_dir};
use std::path::Path;
use base64::DecodeError;
use base64::prelude::*;

use serde::de::DeserializeOwned;
use serde::Deserialize;

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

pub fn load_yaml<T: DeserializeOwned>(filename: &str) -> Result<T, Box<dyn Error>>{
    let path = Path::new(filename);
    let f = File::open(path)?;
    let yaml: T = serde_yaml::from_reader(&f)?;
    Ok(yaml)
}

pub fn decode_base64_byte_stream(encoded_data: &str) -> Result<Vec<u8>, DecodeError> {
    BASE64_STANDARD.decode(encoded_data)
}