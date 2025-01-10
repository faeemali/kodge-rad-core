use std::error::Error;
use std::fs::{File};
use std::{fs, io};
use std::io::{BufReader, Read};
use std::ops::{DerefMut};
use std::path::Path;
use std::sync::Arc;
use base64::DecodeError;
use base64::prelude::*;
use flate2::read::GzDecoder;
use serde::de::DeserializeOwned;
use tar::Archive;
use tokio::sync::{RwLock};
use crate::error::raderr;
use crate::utils::utils::TokenType::{Name, Variable, Version};
use sha2::{Sha256, Digest};

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

#[allow(dead_code)]
pub fn load_yaml<T: DeserializeOwned>(filename: &str) -> Result<T, Box<dyn Error + Sync + Send>> {
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

pub const MIN_NAME_LEN: usize = 1;
pub const MAX_NAME_LEN: usize = 32;
pub const MIN_VERSION_LEN: usize = 1;
pub const MAX_VERSION_LEN: usize = 16;

#[derive(Debug, Eq, PartialEq)]
pub enum TokenType {
    //alphanumeric, -,_,. allowed. Must start with letter and end
    //with alphanumeric
    Name,

    //alphanumeric, _ allowed. Must start with letter and end with
    //alphanumeric
    Variable,

    //same chars as name. must start with alphanumeric. must end with
    //alphanumeric
    Version,
}

pub struct Validation {}

impl Validation {
    //alphanumeric, dash, underscore allowed
    pub fn is_valid_name_char(c: char) -> bool {
        c.is_alphanumeric() || c == '-' || c == '_' || c == '.'
    }

    pub fn is_valid_variable_char(c: char) -> bool {
        c.is_alphanumeric() || c == '_'
    }

    pub fn is_valid_version_char(c: char) -> bool {
        Self::is_valid_name_char(c)
    }

    ///checks if a name is valid. Names must be alphanumeric and contain
    /// underscore or dash. Max length is 32, min is 1.
    /// Name must start with a letter and terminate with an alphanumeric char
    pub fn is_valid_token(name: &str, token_type: TokenType) -> bool {
        let (min, max) = match token_type {
            TokenType::Name => (MIN_NAME_LEN, MAX_NAME_LEN),
            TokenType::Variable => (MIN_NAME_LEN, MAX_NAME_LEN),
            TokenType::Version => (MIN_VERSION_LEN, MAX_VERSION_LEN),
        };

        let n = name.trim();
        if n.len() < min || n.len() > max {
            return false;
        }

        let nb: Vec<char> = name.chars().collect();
        for c in &nb {
            if token_type == Variable && !Validation::is_valid_variable_char(*c) ||
                token_type == Name && !Validation::is_valid_name_char(*c) ||
                token_type == Version && !Validation::is_valid_version_char(*c) {
                return false;
            }
        }

        match token_type {
            Name | Variable  => {
                if !nb[0].is_alphabetic() {
                    return false;
                }
            }

            Version => {
                if !nb[0].is_alphanumeric() {
                    return false;
                }
            }
        }

        if !nb[nb.len() - 1].is_alphanumeric() {
            return false;
        }
        
        true
    }

    pub fn is_valid_msg_type(msg_type: &str) -> bool {
        Validation::is_valid_token(msg_type, Name)
    }

    #[allow(dead_code)]
    pub fn is_valid_name(name: &str) -> bool {
        Validation::is_valid_token(name, Name)
    }

    pub fn is_valid_variable(var: &str) -> bool {
        Validation::is_valid_token(var, Variable)
    }

    pub fn is_valid_version(ver: &str) -> bool {
        Validation::is_valid_token(ver, Version)
    }

    ///checks if a full instance id is valid
    pub fn is_valid_instance_id(instance: &str) -> bool {
        Validation::is_valid_name(instance)
    }

    ///checks if just the suffix of an instance id is valid. This may
    /// start with a - or . or _ for example
    pub fn is_valid_instance_suffix(instance: &str) -> bool {
        let chars: Vec<char> = instance.chars().collect();
        for c in chars {
            if !Self::is_valid_name_char(c) {
                return false;
            }
        }

        true
    }

    ///splits a comma separated list of variables into a vec and returns
    pub fn get_tokens_from_list(vars: &str, token_type: TokenType) -> Result<Vec<String>, Box<dyn Error + Sync + Send>> {
        let mut split = vars.split(",").collect::<Vec<&str>>();
        
        //if there's only one item, split() sets the last item to "", so remove it
        if split[split.len()-1].is_empty() {
            split.pop();
        }
        
        let mut res = vec![];
        for v in split {
            match token_type {
                Name => {
                    if !Validation::is_valid_name(v) {
                        return raderr(format!("Invalid name found ({}) in {}", v, vars));
                    }
                }
                Variable => {
                    if !Validation::is_valid_variable(v) {
                        return raderr(format!("Invalid variable found ({}) in {}", v, vars));
                    }
                }
                Version => {
                    if !Validation::is_valid_version(v) {
                        return raderr(format!("Invalid version found ({}) in {}", v, vars));
                    }
                }
            }


            res.push(v.to_string());
        }

        Ok(res)
    }
}

#[derive(Debug, Clone)]
pub struct SystemInfo {
    pub os: String,
    pub arch: String,
}

impl SystemInfo {
    pub fn get_system_string(&self) -> String {
        format!("{}/any/{}", self.os, self.arch)
    }
    
    pub fn get_url_encoded_system_string(&self) -> String {
        let system = self.get_system_string();
        urlencoding::encode(&system).to_string()
    }
}

pub fn get_system_info() -> SystemInfo {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    SystemInfo {
        os: os.to_string(),
        arch: arch.to_string(),
    }
}

//given a directory, removes all files and subdirectories within that directory.
//the directory itself is not removed. AI generated
pub fn clean_directory(path: &str) -> io::Result<()> {
    let dir_path = Path::new(path);

    // Check if the directory exists and is actually a directory
    if dir_path.exists() && dir_path.is_dir() {
        // Iterate through the directory's contents
        for entry in fs::read_dir(dir_path)? {
            let entry = entry?;
            let entry_path = entry.path();

            // Remove files and empty directories
            if entry_path.is_file() {
                fs::remove_file(entry_path)?;
            } else if entry_path.is_dir() {
                fs::remove_dir_all(entry_path)?;
            }
        }
    } else {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "Specified path is not a directory or does not exist",
        ));
    }

    Ok(())
}

/// Extracts a `.tar.gz` archive into the specified output directory.
/// AI generated
///
/// # Arguments
/// - `archive_path` - Path to the `.tar.gz` archive file.
/// - `output_path` - Path to the directory where the archive contents should be extracted.
///
/// # Returns
/// - `Ok(())` on success.
/// - `Err(io::Error)` on failure.
pub fn extract_tar_gz(archive_path: &str, output_path: &str) -> io::Result<()> {
    // Open the `.tar.gz` file
    let archive_file = File::open(archive_path)?;

    // Create a GzDecoder to decompress the file
    let decompressed = GzDecoder::new(archive_file);

    // Create a tar Archive to handle the decompressed data
    let mut archive = Archive::new(decompressed);

    // Extract the archive into the output directory
    archive.unpack(output_path)?;

    Ok(())
}

/// Calculates the SHA-256 hash of a file.
/// AI generated
///
/// # Arguments
/// - `file_path`: The path of the file to hash.
///
/// # Returns
/// - `Ok<String>`: The SHA-256 hash as a hexadecimal string.
/// - `Err(io::Error)`: If there is an error reading the file.
pub fn calculate_sha256(file_path: &str) -> io::Result<String> {
    // Open the file
    let file = File::open(file_path)?;
    let mut reader = BufReader::new(file);

    // Create a Sha256 hasher
    let mut hasher = Sha256::new();

    // Read the file in chunks and update the hasher
    let mut buffer = [0u8; 4096];
    while let Ok(bytes_read) = reader.read(&mut buffer) {
        if bytes_read == 0 {
            break; // EOF reached
        }
        hasher.update(&buffer[..bytes_read]);
    }

    // Finalize the hash and convert it to a hexadecimal string
    let hash_result = hasher.finalize();
    Ok(format!("{:x}", hash_result).to_lowercase())
}
