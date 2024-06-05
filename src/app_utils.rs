use std::error::Error;
use std::fs::{read_dir};
use std::path::Path;

pub fn get_all_apps(base_dir: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let filename = format!("{}/apps", base_dir);
    let path = Path::new(filename.as_str());
    if (!Path::exists(path)) {
        return Ok(vec![]); //no apps
    }

    let apps = get_dirs(path)?;
    Ok(apps)
}

pub fn app_exists(base_dir: &str, app_name: &str) -> Result<bool, Box<dyn Error>> {
    let apps = get_all_apps(base_dir)?;
    for app in &apps {
        if app == app_name {
            return Ok(true)
        }
    }
    Ok(false)
}

fn get_dirs(dir_path: &Path) -> Result<Vec<String>, std::io::Error> {
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

