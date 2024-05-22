use std::error::Error;
use std::process::{ExitStatus, Stdio};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::time::sleep;
use crate::error::RadError;


fn return_app_err(status: &ExitStatus) -> Result<(), Box<RadError>> {
    if status.code().is_none() {
        return Err(Box::new(RadError::from("Exit code: [unknown]")));
    }

    let code = status.code().unwrap();
    Err(Box::new(RadError::from(format!("Exit code: {}", code))))
}

pub async fn run_app_source_async(command: String, args: &[&str], tx: SyncSender<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
    let mut process = Command::new(command)
        .args(args)
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .spawn()?;

    if process.stdout.is_none() {
        return Ok(());
    }
    let stdout = process.stdout.as_mut().ok_or(RadError::from("Error accessing stdout"))?;
    let mut buffer: [u8; 1024] = [0; 1024];
    loop {
        let size = stdout.read(&mut buffer).await?;
        if size == 0 {
            break;
        }

        println!("Sending {} bytes", size);
        for j in 0..size {
            tx.send(buffer[j])?;
        }
    } //loop

    println!("waiting for source to exit");
    let exited = process.wait().await?;
    println!("source exited");
    
    if !exited.success() {
        return_app_err(&exited)?;
    }

    Ok(())
}

pub async fn run_app_sink_async(command: String, args: &[&str], rx: Receiver<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
    let mut process = Command::new(command)
        .args(args)
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .spawn()?;

    if process.stdin.is_none() {
        return Ok(());
    }
    
    let stdin = process.stdin.as_mut().ok_or(RadError::from("Error obtaining stdin"))?;
    loop {
        let data_res = rx.recv();
        if let Err(e) = data_res {
            break;
        }
        
        let data = data_res.unwrap();
        
        //println!("Writing 1 byte");
        stdin.write_u8(data).await?;
    } //loop

    //if the receiver is disconnected, kill the running app
    process.kill().await?;

    let exited = process.wait().await?;
    if !exited.success() {
        return_app_err(&exited)?;
    }
    Ok(())
}
