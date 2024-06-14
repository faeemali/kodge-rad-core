use std::error::Error;
use std::io::{Read, stdin, Write};
use std::ops::DerefMut;
use tokio::process::{Child, ChildStdin, Command};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;
use log::{error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::time::sleep;
use crate::app::{App, STDERR, STDIN, STDOUT};
use crate::workflow::{ConnectorChannel, StdioHolder};

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
async fn handle_app_input(am_child: Arc<Mutex<Child>>, connector: ConnectorChannel, input_type: &str) {
    let mut m_connector = connector;

    /* read from the receiver, write to stdin */
    if let Some(rx) = &mut m_connector.rx {
        loop {
            let data_opt = rx.recv().await;
            if let Some(data) = data_opt {
                /* write to stdin */
                if input_type == STDIN {
                    /* write to stdin of the app */
                    let mut child_mg = am_child.lock().await;
                    let child = child_mg.deref_mut();
                    if let Some(s) = &mut child.stdin {
                        if s.write_all(&data).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }
    } else {
        /* TODO: abort here */
        error!("rx channel non-existent for connector: {}", &m_connector.connector.id.id);
    }

    /* TODO: abort */
    warn!("stdin task shutting down for channel: {}", &m_connector.connector.id.id);
}

/*
    This handles output FROM the application. This means we will READ from the application output.
 */
async fn handle_app_output(am_child: Arc<Mutex<Child>>,
                           connector: ConnectorChannel,
                           output_type: &str) {
    let mut m_connector = connector;

    if let Some(tx) = &mut m_connector.tx {
        let mut b = [0u8; 1024];

        loop {
            let mut child_mg = am_child.lock().await;
            let child = child_mg.deref_mut();

            if output_type == STDOUT {
                if let Some(s) = &mut child.stdout {
                    let res = s.read(&mut b).await;
                    if let Ok(size) = res {
                        let send_res = tx.send(b[0..size].to_vec()).await;
                        if send_res.is_err() {
                            error!("Error sending data to stdout channel for connector {}", &m_connector.connector.id.id);
                            break;
                        }
                    } else {
                        error!("Error reading from stdout for connector {}", &m_connector.connector.id.id);
                        break;
                    }
                }
            } else if let Some(s) = &mut child.stderr {
                let res = s.read(&mut b).await;
                if let Ok(size) = res {
                    let send_res = tx.send(b[0..size].to_vec()).await;
                    if send_res.is_err() {
                        error!("Error sending data to stderr channel for connector {}", &m_connector.connector.id.id);
                        break;
                    }
                } else {
                    error!("Error reading from stderr for connector {}", &m_connector.connector.id.id);
                    break;
                }
            }
        }
    } else {
        error!("tx channel non-existent for connector: {}", &m_connector.connector.id.id);
    }

    /* TODO: must abort here */
    error!("Channel ({}) terminated for connector: {}", output_type, &m_connector.connector.id.id);
}

async fn check_must_die(am_must_die: Arc<Mutex<bool>>) -> bool {
    let must_die_mg = am_must_die.lock().await;
    *must_die_mg
}

async fn start_connector_tasks(am_child: Arc<Mutex<Child>>, am_connectors: Arc<Mutex<Vec<ConnectorChannel>>>, app: &App) {
    let mut connectors_mg = am_connectors.lock().await;
    let connectors = connectors_mg.deref_mut();

    while !connectors.is_empty() {
        let connector = connectors.remove(0);

        let integration_type = &connector.connector.integration.integration_type as &str;
        if integration_type == STDIN {
            tokio::spawn(handle_app_input(am_child.clone(), connector, STDIN));
        } else if integration_type == STDOUT {
            tokio::spawn(handle_app_output(am_child.clone(), connector, STDOUT));
        } else if integration_type == STDERR {
            tokio::spawn(handle_app_output(am_child.clone(), connector, STDERR));
        } else {
            error!("Unable to start connector. Unknown connector type detected: {}", integration_type);
        }
    }
}

async fn __handle_stdin_passthrough(am_child: Arc<Mutex<Child>>, app_id: String) {
    let mut b = [0u8; 1024];

    loop {
        /* read from stdio, pass to app's stdio */
        let size_res = stdin().read(&mut b);
        if let Ok(size) = size_res {
            let mut child_mg = am_child.lock().await;
            let child = child_mg.deref_mut();
            if let Some(stdin) = &mut child.stdin {
                if size == 0 {
                    error!("Aborting stdin passthrough due to EOF");
                    break;
                }

                let res = ChildStdin::write_all(stdin, &b[0..size]).await;
                if let Err(e) = res {
                    error!("error writing to child stdin (passthrough): {}", e);
                    break;
                }
            } else {
                error!("stdin not available for app {}. passthrough not possible", app_id);
                break;
            }
        } else {
            error!("Error reading from stdin");
            break;
        }
    }

    error!("stdin passthrough aborting");
}

async fn __handle_stdout_stderr_passthrough(am_child: Arc<Mutex<Child>>, app_id: String, out: bool) {
    let mut b = [0u8; 1024];

    loop {
        /* read from app's stdio, pass to main stdio */
        let mut child_mg = am_child.lock().await;
        let child = child_mg.deref_mut();
        if out {
            if let Some(stdout) = &mut child.stdout {
                let size_res = AsyncReadExt::read(stdout, &mut b).await;
                if let Ok(size) = size_res {
                    if size == 0 {
                        error!("Aborting due to EOF read from child stdout (passthrough)");
                        break;
                    }

                    let res = std::io::stdout().write_all(&b[0..size].to_vec());
                    if let Err(e) = res {
                        error!("Error writing to stdout (passthrough): {}", e);
                        break;
                    }
                } else {
                    error!("error reading from child's stdout (passthrough)");
                    break;
                }
            } else {
                error!("stdout not available for app {}. passthrough (stdout) not possible", app_id);
                break;
            }
        } else if let Some(stderr) = &mut child.stderr {
            let size_res = AsyncReadExt::read(stderr, &mut b).await;
            if let Ok(size) = size_res {
                if size == 0 {
                    error!("Aborting due to EOF from child stderr (passthrough");
                    break;
                }

                let res = std::io::stderr().write_all(&b[0..size].to_vec());
                if let Err(e) = res {
                    error!("Error writing to stderr (passthrough): {}", e);
                    break;
                }
            } else {
                error!("Error reading child's stderr (passthrough)");
                break;
            }
        } else {
            error!("stderr not available for app {}. passthrough (stderr) not possible", app_id);
        }
    }

    /* todo must abort here */
    error!("stdout/stderr passthrough aborted. out={}", out);
}

async fn start_passthrough_tasks(child: Arc<Mutex<Child>>, stdio: StdioHolder, app: &App) {
    if stdio.input {
        tokio::spawn(__handle_stdin_passthrough(child.clone(), app.id.id.clone()));
    }

    if stdio.output {
        tokio::spawn(__handle_stdout_stderr_passthrough(child.clone(), app.id.id.clone(), true));
    }

    if stdio.error {
        tokio::spawn(__handle_stdout_stderr_passthrough(child.clone(), app.id.id.clone(), false));
    }
}

async fn __check_app_exit(am_child: Arc<Mutex<Child>>) -> Result<Option<ExitStatus>, Box<dyn Error + Sync + Send>> {
    let mut child_mg = am_child.lock().await;
    let child = child_mg.deref_mut();

    let exit_opt = child.try_wait()?;
    if let Some(s) = exit_opt {
        return Ok(Some(s));
    } //else still running
    
    Ok(None)
}

async fn manage_running_process(am_child: Arc<Mutex<Child>>,
                                app: App,
                                connectors: Arc<Mutex<Vec<ConnectorChannel>>>,
                                stdio: StdioHolder,
                                must_die: Arc<Mutex<bool>>) -> Result<ExitStatus, Box<dyn Error + Sync + Send>> {

    start_connector_tasks(am_child.clone(), connectors.clone(), &app).await;
    start_passthrough_tasks(am_child.clone(), stdio, &app).await;

    info!("Monitoring app: {}", &app.id.id);
    loop {
        if app.id.id == "input1" {
            println!("gh1 {}", &app.id.id);
        }
        
        let exit_res = __check_app_exit(am_child.clone()).await?;
        if let Some(s) = exit_res {
            return Ok(s);
        }      
    
        if app.id.id == "input1" {
            println!("gh2 {}", &app.id.id);
        }

        if check_must_die(must_die.clone()).await {
            warn!("Caught must die flag. Aborting app: {}", app.id.id);

            /*
                TODO:
                    This issues a sigterm, which may not always be advisable. Consider this
                    message from a tokio bugpost:
                    ```
                    Tokio does expose a Child::id method which could be used in conjunction with libc::kill(child.id(), libc::SIGTERM)
                    ```
             */

            let mut child_mg = am_child.lock().await;
            let child = child_mg.deref_mut();
            
            child.kill().await?;
            warn!("Kill issued for app {}", app.id.id);
        }

        if app.id.id == "input1" {
            println!("gh3 {}", &app.id.id);
        }

        sleep(Duration::from_millis(100)).await;
    }
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
    let child = spawn_process(&base_dir, &app, &connectors, &stdio)?;
    let am_child = Arc::new(Mutex::new(child));

    let am_connectors = Arc::new(Mutex::new(connectors));
    let exit_status = manage_running_process(am_child, app.clone(), am_connectors, stdio, am_must_die.clone()).await?;
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
