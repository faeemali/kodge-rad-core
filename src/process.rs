use std::error::Error;
use std::io::{Read, stdin, stdout, Write};
use tokio::process::{Child, ChildStdin, Command, ChildStdout, ChildStderr};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;
use log::{error, info, warn};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::Mutex;
use tokio::task::yield_now;
use tokio::time::sleep;
use crate::app::{App, STDERR, STDIN, STDOUT};
use crate::error::RadError;
use crate::workflow::{ConnectorChannel, StdioHolder, StdioInOut};

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
async fn handle_app_input(output: &mut (impl AsyncWriteExt + Unpin), connector: &mut ConnectorChannel) -> Result<bool, Box<dyn Error + Sync + Send>> {
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
        output.write_all(&data).await?;

        Ok(true)
    } else {
        Err(Box::new(RadError::from(format!("rx channel non-existent for connector: {}", &connector.connector.id.id))))
    }
}

/*
    This handles output FROM the application. This means we will READ from the application output.
 */
async fn handle_app_output(input: &mut (impl AsyncReadExt + Unpin),
                                             connector: &mut ConnectorChannel) -> Result<bool, Box<dyn Error + Send + Sync>> {
    if let Some(tx) = &mut connector.tx {
        let mut b = [0u8;1024];
        let size = input.read(&mut b).await?;
        if size == 0 {
            return Ok(false);
        }

        /* write to channel */
        tx.send(b[0..size].to_vec()).await?;

        Ok(true)
    } else {
        Err(Box::new(RadError::from(format!("tx channel non-existent for connector: {}", &connector.connector.id.id))))
    }
}

async fn check_must_die(am_must_die: &Arc<Mutex<bool>>) -> bool {
    let must_die_mg = am_must_die.lock().await;
    *must_die_mg
}

async fn manage_connector_data_passthrough(child: &mut Child, connectors: &mut [ConnectorChannel], app: &App) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let mut data_processed = false;

    for connector in connectors.iter_mut() {
        if app.id.id == "input1" {
            println!("gh1.1 {}", app.id.id);
        }

        if connector.connector.integration.integration_type == STDIN {
            if let Some(s) = &mut child.stdin {
                data_processed |= handle_app_input(s, connector).await?;
            } else {
                return Err(Box::new(RadError::from(format!("no available stdin for app: {}", app.id.id))));
            }
        }

        if app.id.id == "input1" {
            println!("gh1.2 {}", app.id.id);
        }

        if connector.connector.integration.integration_type == STDOUT {
            if let Some(s) = &mut child.stdout {
                data_processed |= handle_app_output(s, connector).await?;
            } else {
                return Err(Box::new(RadError::from(format!("no available stdout for app: {}", app.id.id))));
            }
        }

        if app.id.id == "input1" {
            println!("gh1.3 {}", app.id.id);
        }

        if connector.connector.integration.integration_type == STDERR {
            if let Some(s) = &mut child.stderr {
                data_processed |= handle_app_output(s, connector).await?;
            } else {
                return Err(Box::new(RadError::from(format!("no available stderr for app: {}", app.id.id))));
            }
        }

        if app.id.id == "input1" {
            println!("gh1.4 {}", app.id.id);
        }

    }

    Ok(data_processed)
}

async fn __handle_stdin_passthrough(child: &mut Child, app_id: &str) -> Result<bool, Box<dyn Error + Sync + Send>> {
    let mut b = [0u8; 1024];

    /* read from stdio, pass to app's stdio */
    let size = stdin().read(&mut b)?;
    if let Some(stdin) = &mut child.stdin {
        if size > 0 {
            ChildStdin::write_all(stdin, &b[0..size]).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    } else {
        Err(Box::new(RadError::from(format!("stdin not available for app {}. passthrough not possible", app_id))))
    }
}

async fn __handle_stdout_stderr_passthrough(child: &mut Child, app_id: &str, out: bool) -> Result<bool, Box<dyn Error + Sync + Send>> {
    let mut b = [0u8; 1024];

    /* read from app's stdio, pass to main stdio */
    if out {
        if let Some(stdout) = &mut child.stdout {
            let size = AsyncReadExt::read(stdout, &mut b).await?;
            if size > 0 {
                std::io::stdout().write_all(&b[0..size])?;
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Err(Box::new(RadError::from(format!("stdout not available for app {}. passthrough (stdout) not possible", app_id))))
        }
    } else if let Some(stderr) = &mut child.stderr {
        let size = AsyncReadExt::read(stderr, &mut b).await?;
        if size > 0 {
            std::io::stderr().write_all(&b[0..size])?;
            Ok(true)
        } else {
            Ok(false)
        }
    } else {
        Err(Box::new(RadError::from(format!("stderr not available for app {}. passthrough (stderr) not possible", app_id))))
    }
}

async fn manage_stdio_passthrough(child: &mut Child, stdio: &StdioHolder, app: &App) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let mut data_processed = false;


    if stdio.input {
        data_processed |= __handle_stdin_passthrough(child, &app.id.id).await?;
    }

    if stdio.output {
        data_processed |= __handle_stdout_stderr_passthrough(child, &app.id.id, true).await?;
    }

    if stdio.error {
        data_processed |= __handle_stdout_stderr_passthrough(child, &app.id.id, false).await?;
    }

    Ok(data_processed)
}

async fn manage_running_process(child: &mut Child, app: App, connectors: &mut [ConnectorChannel], stdio: &StdioHolder, must_die: Arc<Mutex<bool>>) -> Result<ExitStatus, Box<dyn Error + Sync + Send>> {
    info!("Monitoring app: {}", &app.id.id);
    loop {
        if app.id.id == "input1" {
            println!("processing: {}", &app.id.id);
        }
        let mut data_processed = manage_connector_data_passthrough(child, connectors, &app).await?;
       // data_processed |= manage_stdio_passthrough(child, stdio, &app).await?;

        if app.id.id == "input1" {
            println!("gh1 {}", &app.id.id);
        }

        let exit_opt = child.try_wait()?;
        if let Some(s) = exit_opt {
            return Ok(s);
        } //else still running

        if app.id.id == "input1" {
            println!("gh2 {}", &app.id.id);
        }

        if check_must_die(&must_die).await {
            warn!("Caught must die flag. Aborting app: {}", app.id.id);

            /*
                TODO:
                    This issues a sigterm, which may not always be advisable. Consider this
                    message from a tokio bugpost:
                    ```
                    Tokio does expose a Child::id method which could be used in conjunction with libc::kill(child.id(), libc::SIGTERM)
                    ```
             */

            child.kill().await?;
            warn!("Kill issued for app {}", app.id.id);
        }

        if app.id.id == "input1" {
            println!("gh3 {}", &app.id.id);
        }


        //only sleep if no data was processed
        if !data_processed {
            sleep(Duration::from_millis(100)).await;
        }
        //force a task switch
        //println!("yielding on app: {}", &app.id.id);
        yield_now().await;
    }
}

async fn io_read(src: &mut (impl AsyncReadExt + Unpin)) -> Result<Vec<u8>, Box<dyn Error + Sync + Send>> {
    let mut buf = [0u8; 1024];
    let size = src.read(&mut buf).await?;
    if size == 0 {
        return Ok(vec![]);
    }
    Ok(buf[0..size].to_vec())
}

fn spawn_process(base_dir: &str,
                 app: &App,
                 connectors: &[ConnectorChannel],
                 stdio: &StdioHolder) -> Result<Child, Box<dyn Error + Sync + Send>> {
    let (c_stdin, c_stdout, c_stderr) = must_grab_stdin_out_err(connectors);

    let path = format!("{}/cache/{}/{}", base_dir, &app.id.id, &app.execution.cmd);
    let mut cmd = Command::new(path);
    let mut process = cmd.kill_on_drop(true);

    if let Some(args) = &app.execution.args {
        process = process.args(args);
    }

    if c_stdin || stdio.input {
        process = process.stdin(Stdio::piped());
    }

    if c_stdout || stdio.output {
        process = process.stdout(Stdio::piped());
    }

    if c_stderr || stdio.error {
        process = process.stderr(Stdio::piped());
    }

    let child = process.spawn()?;
    Ok(child)
}

pub async fn run_app_main(base_dir: String,
                          app: App,
                          connectors: Vec<ConnectorChannel>,
                          stdio: StdioHolder,
                          am_must_die: Arc<Mutex<bool>>) -> Result<ExitStatus, Box<dyn Error + Sync + Send>> {
    info!("Managing app task for: {}", &app.id.id);
    let mut child = spawn_process(&base_dir, &app, &connectors, &stdio)?;

    let mut mut_connectors = connectors;
    let exit_status = manage_running_process(&mut child, app.clone(), &mut mut_connectors, &stdio, am_must_die.clone()).await?;
    let code = if let Some(c) = exit_status.code() {
        c
    } else {
        -1
    };

    if exit_status.success() {
        info!("App {} exited successfully with code: {}", &app.id.id, code);
    } else {
        error!("App {} exited, but not successfully, with code: {}", &app.id.id, code);
    }

    let mut must_die_mg = am_must_die.lock().await;
    *must_die_mg = true;

    Ok(exit_status)
}
