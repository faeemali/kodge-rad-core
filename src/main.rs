mod utils;
mod config;

use std::env::args;
use std::error::Error;
use std::process::exit;
use crate::config::config_load;
use crate::utils::utils::capture_streaming_output_async;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = args().into_iter().collect();
    if args.len() != 2 {
        println!("Usage: {} config_dir", args[0]);
        exit(1);
    }

    let base_dir = args[1].as_str();

    println!("Application Starting");
    println!("base_dir: {}", base_dir);

    let config = config_load(base_dir)?;
    println!("input: {}, output: {}", &config.input, &config.output);

    let output_res = capture_streaming_output_async(&config.input, &[]).await;
    if let Err(e) = output_res {
        println!("Error running command: {}", e);
    } else {
        println!("Command run successfully!");
    }

    Ok(())
}
