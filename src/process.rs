use std::error::Error;
use std::io::{Read, stdin, Write};
use std::ops::DerefMut;
use tokio::process::{Child, ChildStdin, Command};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;
use log::{error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::select;
use tokio::sync::Mutex;
use tokio::task::yield_now;
use tokio::time::sleep;
use crate::app::{App, STDERR, STDIN, STDOUT};
use crate::error::RadError;
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
async fn handle_app_input(am_child: Arc<Mutex<Child>>,
                          connector: ConnectorChannel,
                          input_type: &str,
                          am_must_die: Arc<Mutex<bool>>) {
    let mut m_connector = connector;

    /* read from the receiver, write to stdin */
    if let Some(rx) = &mut m_connector.rx {
        loop {
            let data_opt = rx.recv().await;
            if let Some(data) = data_opt {
                /* write to stdin */
                if input_type == STDIN {
                    /* write to stdin of the app */
                    println!("handle_app_input getting lock");
                    let mut child_mg = am_child.lock().await;
                    let child = child_mg.deref_mut();
                    println!("handle_app_input got lock");
                    if let Some(s) = &mut child.stdin {
                        if s.write_all(&data).await.is_err() {
                            break;
                        }
                    }
                }
            } else {
                error!("rx channel closed for connector: {}", &m_connector.connector.id.id);
                break;
            }
        } //loop
    } else {
        error!("rx channel non-existent for connector: {}", &m_connector.connector.id.id);
    }

    warn!("stdin task shutting down for channel: {}", &m_connector.connector.id.id);
    set_must_die(am_must_die.clone()).await;
}

/*
    This handles output FROM the application. This means we will READ from the application output.
 */
async fn handle_app_output(am_child: Arc<Mutex<Child>>,
                           connector: ConnectorChannel,
                           output_type: &str,
                           am_must_die: Arc<Mutex<bool>>) {
    let mut m_connector = connector;

    if let Some(tx) = &mut m_connector.tx {
        let mut b = [0u8; 1024];

        loop {
            let mut data_processed = false;

            if output_type == STDOUT {
                let mut child_mg = am_child.lock().await;
                let child = child_mg.deref_mut();

                //println!("reading from stdout");
                if let Some(s) = &mut child.stdout {
                    let res = select! {
                        res = s.read(&mut b) => {
                            res
                        }
                        _ = sleep(Duration::from_micros(100)) => {
                            yield_now().await;
                            continue;
                        }
                    };

                    if let Ok(size) = res {
                        println!("read {} bytes from output", size);

                        let send_res = tx.send(b[0..size].to_vec()).await;
                        if send_res.is_err() {
                            error!("Error sending data to stdout channel for connector {}", &m_connector.connector.id.id);
                            break;
                        }

                        data_processed = true;
                    } else {
                        error!("Error reading from stdout for connector {}", &m_connector.connector.id.id);
                        break;
                    }
                }
            } else {
                let mut child_mg = am_child.lock().await;
                let child = child_mg.deref_mut();

                if let Some(s) = &mut child.stderr {
                    let res = select! {
                        res = s.read(&mut b) => {
                            res
                        }
                        _ = sleep(Duration::from_micros(100)) => {
                            yield_now().await;
                            continue;
                        }
                    };

                    if let Ok(size) = res {
                        let send_res = tx.send(b[0..size].to_vec()).await;
                        if send_res.is_err() {
                            error!("Error sending data to stderr channel for connector {}", &m_connector.connector.id.id);
                            break;
                        }

                        data_processed = true;
                    } else {
                        error!("Error reading from stderr for connector {}", &m_connector.connector.id.id);
                        break;
                    }
                }
            }

            if !data_processed {
                sleep(Duration::from_millis(1)).await;
            }
        }
    } else {
        error!("tx channel non-existent for connector: {}", &m_connector.connector.id.id);
    }

    error!("Channel ({}) terminated for connector: {}", output_type, &m_connector.connector.id.id);
    set_must_die(am_must_die.clone()).await;
}

async fn __get_must_die(am_must_die: Arc<Mutex<bool>>) -> bool {
    let must_die_mg = am_must_die.lock().await;
    *must_die_mg
}

async fn start_connector_tasks(am_child: Arc<Mutex<Child>>,
                               am_connectors:
                               Arc<Mutex<Vec<ConnectorChannel>>>,
                               app: &App,
                               am_must_die: Arc<Mutex<bool>>) {
    let mut connectors_mg = am_connectors.lock().await;
    let connectors = connectors_mg.deref_mut();

    while !connectors.is_empty() {
        let connector = connectors.remove(0);

        let integration_type = &connector.connector.integration.integration_type as &str;
        if integration_type == STDIN {
            tokio::spawn(handle_app_input(am_child.clone(), connector, STDIN, am_must_die.clone()));
        } else if integration_type == STDOUT {
            tokio::spawn(handle_app_output(am_child.clone(), connector, STDOUT, am_must_die.clone()));
        } else if integration_type == STDERR {
            tokio::spawn(handle_app_output(am_child.clone(), connector, STDERR, am_must_die.clone()));
        } else {
            error!("Unable to start connector. Unknown connector type detected: {}", integration_type);
        }
    }
}

async fn __write_to_child_stdin(am_child: Arc<Mutex<Child>>, data: &[u8], app_id: &str) -> Result<(), Box<dyn Error + Sync + Send>> {
    let size = data.len();

    println!("read {} bytes from stdin", size);
    let mut child_mg = am_child.lock().await;
    let child = child_mg.deref_mut();
    println!("got lock");

    if let Some(stdin) = &mut child.stdin {
        if size == 0 {
            let msg = "Aborting stdin passthrough due to EOF".to_string();
            error!("{}", &msg);
            return Err(Box::new(RadError::from(msg)));
        }

        println!("writing {} bytes to child's stdin", size);
        let res = ChildStdin::write_all(stdin, data).await;
        if let Err(e) = res {
            error!("error writing to child stdin (passthrough): {}", e);
            return Err(Box::new(e));
        }
        Ok(())
    } else {
        let msg = format!("stdin not available for app {}. passthrough not possible", app_id);
        error!("{}", &msg);
        Err(Box::new(RadError::from(msg)))
    }
}

async fn __handle_stdin_passthrough(am_child: Arc<Mutex<Child>>,
                                    app_id: String,
                                    am_must_die: Arc<Mutex<bool>>) {
    let mut b = [0u8; 1024];

    loop {
        /* read from stdio, pass to app's stdio */
        let size_res = stdin().read(&mut b);
        if let Ok(size) = size_res {
            if __write_to_child_stdin(am_child.clone(), &b[0..size], &app_id).await.is_err() {
                break;
            }
        } else {
            error!("Error reading from stdin");
            break;
        }
    }

    error!("stdin passthrough aborting");
    set_must_die(am_must_die.clone()).await;
}

async fn __handle_stdout_stderr_passthrough(am_child: Arc<Mutex<Child>>,
                                            app_id: String,
                                            out: bool,
                                            am_must_die: Arc<Mutex<bool>>) {
    let mut b = [0u8; 1024];
    loop {
        let mut data_processed = false;

        /* read from app's stdio, pass to main stdio */
        if out {
            let mut child_mg = am_child.lock().await;
            let child = child_mg.deref_mut();

            if let Some(stdout) = &mut child.stdout {
                let size_res = AsyncReadExt::read(stdout, &mut b).await;
                if let Ok(size) = size_res {
                    if size == 0 {
                        error!("Aborting due to EOF read from child stdout (passthrough)");
                        break;
                    }

                    let res = std::io::stdout().write_all(&b[0..size]);
                    if let Err(e) = res {
                        error!("Error writing to stdout (passthrough): {}", e);
                        break;
                    }
                    data_processed = true;
                } else {
                    error!("error reading from child's stdout (passthrough)");
                    break;
                }
            } else {
                error!("stdout not available for app {}. passthrough (stdout) not possible", app_id);
                break;
            }
        } else {
            let mut child_mg = am_child.lock().await;
            let child = child_mg.deref_mut();

            if let Some(stderr) = &mut child.stderr {
                let size_res = AsyncReadExt::read(stderr, &mut b).await;
                if let Ok(size) = size_res {
                    if size == 0 {
                        error!("Aborting due to EOF from child stderr (passthrough");
                        break;
                    }

                    let res = std::io::stderr().write_all(&b[0..size]);
                    if let Err(e) = res {
                        error!("Error writing to stderr (passthrough): {}", e);
                        break;
                    }

                    data_processed = true;
                } else {
                    error!("Error reading child's stderr (passthrough)");
                    break;
                }
            } else {
                error!("stderr not available for app {}. passthrough (stderr) not possible", app_id);
                break;
            }
        }

        if !data_processed {
            sleep(Duration::from_millis(1)).await;
        }
    }

    error!("stdout/stderr passthrough aborted. out={}", out);
    set_must_die(am_must_die.clone()).await;
}

async fn start_passthrough_tasks(child: Arc<Mutex<Child>>,
                                 stdio: StdioHolder,
                                 app: &App,
                                 am_must_die: Arc<Mutex<bool>>) {
    if stdio.input {
        tokio::spawn(__handle_stdin_passthrough(child.clone(), app.id.id.clone(), am_must_die.clone()));
    }

    if stdio.output {
        tokio::spawn(__handle_stdout_stderr_passthrough(child.clone(), app.id.id.clone(), true, am_must_die.clone()));
    }

    if stdio.error {
        tokio::spawn(__handle_stdout_stderr_passthrough(child.clone(), app.id.id.clone(), false, am_must_die.clone()));
    }
}

async fn __check_app_exit(am_child: Arc<Mutex<Child>>, app_id: &str) -> Result<Option<ExitStatus>, Box<dyn Error + Sync + Send>> {
    if app_id == "input1" {
        println!("checking app exit for input1");
    }
    let mut child_mg = am_child.lock().await;
    let child = child_mg.deref_mut();

    if app_id == "input1" {
        println!("gh7 input1");
    }


    let exit_opt_res = child.try_wait();
    if let Err(e) = exit_opt_res {
        error!("Error checking if app {} exited: {}", app_id, &e);
        return Err(Box::new(e));
    }

    let exit_opt = exit_opt_res.unwrap();
    if let Some(s) = exit_opt {
        if app_id == "input1" {
            println!("gh7.1 input1");
        }

        return Ok(Some(s));
    } //else still running

    if app_id == "input1" {
        println!("gh7.2 input1");
    }

    Ok(None)
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

async fn set_must_die(am_must_die: Arc<Mutex<bool>>) {
    let mut must_die_mg = am_must_die.lock().await;
    let must_die = must_die_mg.deref_mut();
    *must_die = true;
}

pub async fn run_app_main(base_dir: String,
                          app: App,
                          connectors: Vec<ConnectorChannel>,
                          stdio: StdioHolder,
                          am_must_die: Arc<Mutex<bool>>) -> Result<(), Box<dyn Error + Sync + Send>> {
    info!("Managing app task for: {}", &app.id.id);
    let child = spawn_process(&base_dir, &app, &connectors, &stdio)?;
    let am_child = Arc::new(Mutex::new(child));

    let am_connectors = Arc::new(Mutex::new(connectors));

    start_connector_tasks(am_child.clone(), am_connectors.clone(), &app, am_must_die.clone()).await;
    start_passthrough_tasks(am_child.clone(), stdio, &app, am_must_die.clone()).await;

    loop {
        if __get_must_die(am_must_die.clone()).await {
            break;
        }

        sleep(Duration::from_millis(5)).await;
    }

    warn!("Killing the child for app: {}", &app.id.id);
    let mut child_mg = am_child.lock().await;
    let c = child_mg.deref_mut();
    c.kill().await?;

    Ok(())
}
