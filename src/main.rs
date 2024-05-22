mod utils;
mod config;
mod error;

use std::env::args;
use std::error::Error;
use std::process::exit;
use std::sync::mpsc::sync_channel;
use std::time::Duration;
use futures::future::join_all;
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

    let (source_tx, source_rx) = sync_channel::<u8>(1024);
    let source_handle = tokio::spawn(run_app_source_async(config.input.clone(), &[], source_tx.clone()));
    let sink_handle = tokio::spawn(run_app_sink_async(config.output.clone(), &[], source_rx));


    println!("joining sink");
    let sink_res = tokio::join!(sink_handle);
    println!("sink joined");

    // let results = join_all(vec![source_handle, sink_handle]).await;
    // for result in results {
    //     if result.is_err() {
    //         println!("got error");
    //     }
    // }

    Ok(())
}
