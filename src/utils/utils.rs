use std::error::Error;
use std::fs::{File, read_dir};
use std::ops::DerefMut;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::time::sleep;
use crate::error::RadError;
use crate::workflow::Workflow;


fn check_app_exit_status(status: &Option<ExitStatus>) -> Result<bool, Box<RadError>> {
    if let Some(s) = status {
        /* exited */
        return if s.success() {
            Ok(true) //app exited successfully
        } else if s.code().is_some() {
            //app failed with an error code
            Err(Box::new(RadError::from(format!("Error code: {}", s.code().unwrap()))))
        } else {
            //app failed without an error code
            Err(Box::new(RadError::from("Error code: [unknown]")))
        }
    }

    //app is still running
    Ok(false)
}

async fn read_from_source(process: &mut Child, tx: Sender<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
    let stdout = process.stdout.as_mut().ok_or(RadError::from("Error accessing stdout"))?;
    let mut buffer: [u8; 1024] = [0; 1024];
    loop {
        let size = stdout.read(&mut buffer).await?;
        if size == 0 {
            println!("read from source exiting function");
            return Ok(());
        }

        println!("Sending {} bytes", size);
        for j in 0..size {
            tx.send(buffer[j]).await?;
        }
    } //loop
}

pub async fn run_app_source_async(command: String, args: &[&str], tx: Sender<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
    let mut process = Command::new(command)
        .args(args)
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .spawn()?;

    if process.stdout.is_none() {
        return Ok(());
    }

    loop {
        read_from_source(&mut process, tx.clone()).await?;
        let status = check_app_stopped(&mut process).await?;
        let exited = check_app_exit_status(&status)?;
        if exited {
           break;
        }
        sleep(Duration::from_millis(1)).await;
    }

    println!("dropping tx");
    drop(tx);

    Ok(())
}

async fn sink_write(process: &mut Child, rx_am: Arc<Mutex<Receiver<u8>>>) -> Result<(), Box<dyn Error + Sync + Send>> {
    let mut rx_mg = rx_am.lock().await;
    let rx = rx_mg.deref_mut();

    let stdin = process.stdin.as_mut().ok_or(RadError::from("Error obtaining stdin"))?;
    loop {
        println!("reading from channel");
        let data_res = rx.recv().await;
        println!("read from channel complete");
        if data_res.is_none() {
            println!("sink detected channel closed");
            let _ = process.kill().await;
            return Err(Box::new(RadError::from("sink write error: channel closed")));
        }
        let data = data_res.unwrap();

        //println!("Writing 1 byte");
        stdin.write_u8(data).await?;
    } //loop
}

async fn check_app_stopped(process: &mut Child) -> Result<Option<ExitStatus>, Box<dyn Error + Sync + Send>> {
    let exited = process.try_wait()?;
    if exited.is_none() {
        return Ok(None);
    }

    let status = exited.unwrap();
    println!("app status: {:?}", status);
    Ok(Some(status))
}

pub async fn run_app_sink_async(command: String, args: &[&str], rx_am: Arc<Mutex<Receiver<u8>>>) -> Result<(), Box<dyn Error + Sync + Send>> {
    let mut process = Command::new(command)
        .args(args)
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .spawn()?;

    if process.stdin.is_none() {
        return Ok(());
    }

    loop {
        sink_write(&mut process, rx_am.clone()).await?;
        let status = check_app_stopped(&mut process).await?;
        let exited = check_app_exit_status(&status)?;
        if exited {
            break;
        }
        sleep(Duration::from_millis(2)).await;
    }

    Ok(())
}

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
