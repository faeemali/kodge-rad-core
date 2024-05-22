use std::error::Error;
use std::process::Stdio;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

pub async fn capture_streaming_output_async(command: &str, args: &[&str]) -> Result<(), Box<dyn Error>> {
    let mut process = Command::new(command)
        .args(args)
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .spawn()?;

    let mut buffer: [u8; 1024] = [0; 1024];
    match &mut process.stdout {
        None => {return Ok(());}
        Some(stdout) => {
           loop {
               let size = stdout.read(&mut buffer).await?;
               if size == 0 {
                   break;
               }
               
               let output = String::from_utf8_lossy(&buffer[..size]);
               println!("{}", output);
           } //loop
        } //Some
    }
    Ok(())
}
