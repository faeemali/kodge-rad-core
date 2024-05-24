mod utils;
mod error;
mod config;
mod app_manager;

use std::env::args;
use std::error::Error;
use std::process::exit;

pub struct AppCtx {
    pub base_dir: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = args().collect();
    if args.len() != 2 {
        println!("Usage: {} config_dir", args[0]);
        exit(1);
    }

    let base_dir = args[1].as_str();

    println!("Application Starting");
    println!("base_dir: {}", base_dir);

    let app_ctx = AppCtx {
        base_dir: base_dir.to_string(),
    };
    
    
    
    Ok(())
}
