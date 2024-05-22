mod utils;
mod config;
mod error;

use std::env::args;
use std::error::Error;
use std::process::exit;
use std::sync::Arc;
use std::time::Duration;
use futures::future::join_all;
use tokio::sync::mpsc::channel;
use tokio::sync::Mutex;
use tokio::time::sleep;
use crate::config::config_load;
use crate::utils::utils::{run_app_sink_async, run_app_source_async};

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

    let config = config_load(base_dir)?;
    println!("input: {}, output: {}", &config.input, &config.output);

    let (source_tx, source_rx) = channel::<u8>(1024);
    let rx_am = Arc::new(Mutex::new(source_rx));

    let source_handle = tokio::spawn(run_app_source_async(config.input.clone(), &[], source_tx));
    let sink_handle = tokio::spawn(run_app_sink_async(config.output.clone(), &[], rx_am.clone()));

    let t1 = tokio::spawn(async {
        let (r_res,) = tokio::join!(source_handle);
        if r_res.is_err() {
            println!("Error processing source handle");
            return;
        }
        
        let r = r_res.unwrap();
        if let Err(e) = r {
            println!("Error processing source task: {}", e);
        } else {
            println!("Successfully processed source task");
        }
    });

    let t2 = tokio::spawn(async {
       let (r_res,) = tokio::join!(sink_handle);
        if r_res.is_err() {
            println!("Error processing sink handle");
            return;
        }
        
        let r = r_res.unwrap();
        if let Err(e) = r {
            println!("Error processing sink task: {}", e);
        } else {
            println!("Sink task processed successfully");
        }
    });
    
    join_all(vec![t1, t2]).await;
    
    Ok(())
}
