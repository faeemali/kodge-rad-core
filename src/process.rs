use std::error::Error;
use std::ops::DerefMut;
use tokio::process::{Child, ChildStdin, Command, ChildStdout, ChildStderr};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::Mutex;
use tokio::time::sleep;
use crate::app::{App, STDERR, STDIN, STDOUT};
use crate::error::RadError;
use crate::workflow::{ConnectorChannel, Message, MessageTypes};

fn must_grab_stdin_out_err(connector: &[ConnectorChannel]) -> (bool, bool, bool) {
    let mut stdin = false;
    let mut stdout = false;
    let mut stderr = false;
    for cc in connector {
        match cc.connector.integration.integration_type.as_str() {
            STDIN => {
                stdin = true;
            }
            STDOUT => {
                stdout = true;
            }
            STDERR => {
                stderr = true;
            }
            _ => {
                continue;
            }
        }
    }

    (stdin, stdout, stderr)
}

/*
    this handles input TO an application. This means we will WRITE to the application input.
 */
async fn handle_app_input<T: ProcessWriter>(output: &mut T, connector: &mut ConnectorChannel) -> Result<bool, Box<dyn Error + Sync + Send>> {
    /* read from the receiver, write to stdin */
    if let Some(rx) = &mut connector.rx {
        let data_res = rx.try_recv();
        if let Err(e) = data_res {
            return if e == TryRecvError::Empty {
                Ok(false)
            } else {
                Err(Box::new(RadError::from(format!("Channel closed for cc: {}", connector.connector.id.id))))
            };
        }
        let data = data_res.unwrap();

        /* TODO: do something with msg_type here */

        /* write to stdin */
        output.write(&data.data).await?;

        Ok(true)
    } else {
        Err(Box::new(RadError::from(format!("rx channel non-existent for connector: {}", &connector.connector.id.id))))
    }
}

/*
    This handles output FROM the application. This means we will READ from the application output.
 */
async fn handle_app_output<T: ProcessReader>(input: &mut T, connector: &mut ConnectorChannel) -> Result<bool, Box<dyn Error + Send + Sync>> {
    if let Some(tx) = &mut connector.tx {
        let data = input.read().await?;
        if data.is_empty() {
            return Ok(false);
        }

        /* write to channel */
        tx.send(Message {
            msg_type: MessageTypes::Process,
            data,
        }).await?;

        Ok(true)
    } else {
        Err(Box::new(RadError::from(format!("tx channel non-existent for connector: {}", &connector.connector.id.id))))
    }
}

async fn check_must_die(am_must_die: &Arc<Mutex<bool>>) -> bool {
    let must_die_mg = am_must_die.lock().await;
    *must_die_mg
}

async fn manage_running_process(child: &mut Child, app: App, connectors: &mut [ConnectorChannel], must_die: Arc<Mutex<bool>>) -> Result<ExitStatus, Box<dyn Error + Sync + Send>> {
    loop {
        let mut data_processed = false;
        for connector in connectors.iter_mut() {
            if connector.connector.integration.integration_type == STDIN {
                if let Some(s) = &mut child.stdin {
                    handle_app_input(s, connector).await?;
                    data_processed = true;
                } else {
                    return Err(Box::new(RadError::from(format!("no available stdin for app: {}", app.id.id))));
                }
            }

            if connector.connector.integration.integration_type == STDOUT {
                if let Some(s) = &mut child.stdout {
                    handle_app_output(s, connector).await?;
                    data_processed = true;
                } else {
                    return Err(Box::new(RadError::from(format!("no available stdout for app: {}", app.id.id))));
                }
            }

            if connector.connector.integration.integration_type == STDERR {
                if let Some(s) = &mut child.stderr {
                    handle_app_output(s, connector).await?;
                    data_processed = true;
                } else {
                    return Err(Box::new(RadError::from(format!("no available stderr for app: {}", app.id.id))));
                }
            }
        }

        let exit_opt = child.try_wait()?;
        if let Some(s) = exit_opt {
            return Ok(s);
        } //else still running

        if check_must_die(&must_die).await {
            println!("Caught must die flag. Aborting app: {}", app.id.id);

            /*
                TODO: 
                    This issues a sigterm, which may not always be advisable. Consider this
                    message from a tokio bugpost:                
                    ```
                    Tokio does expose a Child::id method which could be used in conjunction with libc::kill(child.id(), libc::SIGTERM) 
                    ```
             */
            
            child.kill().await?;
            println!("Kill issued for app {}", app.id.id);
        }

        //only sleep if no data was processed
        if !data_processed {
            sleep(Duration::from_millis(100)).await;
        }
    }
}

//do sync/async reads from anywhere
pub trait ProcessReader {
    async fn read(&mut self) -> Result<Vec<u8>, Box<dyn Error + Sync + Send>>;
}

//do sync/async writes from anywhere
pub trait ProcessWriter {
    async fn write(&mut self, data: &[u8]) -> Result<(), Box<dyn Error + Sync + Send>>;
}

async fn io_read<T: AsyncReadExt + Unpin>(src: &mut T) -> Result<Vec<u8>, Box<dyn Error + Sync + Send>> {
    let mut buf = [0u8; 1024];
    let size = src.read(&mut buf).await?;
    if size == 0 {
        return Ok(vec![]);
    }
    Ok(buf[0..size].to_vec())
}

impl ProcessReader for ChildStdout {
    async fn read(&mut self) -> Result<Vec<u8>, Box<dyn Error + Sync + Send>> {
        io_read(self).await
    }
}

impl ProcessReader for ChildStderr {
    async fn read(&mut self) -> Result<Vec<u8>, Box<dyn Error + Sync + Send>> {
        io_read(self).await
    }
}

impl ProcessWriter for ChildStdin {
    async fn write(&mut self, data: &[u8]) -> Result<(), Box<dyn Error + Sync + Send>> {
        let res = self.write_all(data).await?;
        Ok(res)
    }
}

fn spawn_process(base_dir: &str, app: &App, connectors: &[ConnectorChannel]) -> Result<Child, Box<dyn Error + Sync + Send>> {
    let (stdin, stdout, stderr) = must_grab_stdin_out_err(connectors);
    
    let path = format!("{}/cache/{}/{}", base_dir, &app.id.id, &app.execution.cmd);
    let mut cmd = Command::new(path);
    let mut process = cmd.kill_on_drop(true);

    if let Some(args) = &app.execution.args {
        process = process.args(args);
    }

    if stdin {
        process = process.stdin(Stdio::piped());
    }

    if stdout {
        process = process.stdout(Stdio::piped());
    }

    if stderr {
        process = process.stderr(Stdio::piped());
    }

    let child = process.spawn()?;
    Ok(child)
}

pub async fn run_app_main(base_dir: String,
                          app: App,
                          connectors: Vec<ConnectorChannel>,
                          am_must_die: Arc<Mutex<bool>>) -> Result<ExitStatus, Box<dyn Error + Sync + Send>> {
    let mut child = spawn_process(&base_dir, &app, &connectors)?;
    
    let mut mut_connectors = connectors;
    let exit_status = manage_running_process(&mut child, app.clone(), &mut mut_connectors, am_must_die.clone()).await?;
    let code = if let Some(c) = exit_status.code() {
        c
    } else {
        -1
    };

    if exit_status.success() {
        println!("App {} exited successfully with code: {}", &app.id.id, code);
    } else {
        println!("App {} exited, but not successfully, with code: {}", &app.id.id, code);
    }

    let mut must_die_mg = am_must_die.lock().await;
    *must_die_mg = true;

    Ok(exit_status)
}
