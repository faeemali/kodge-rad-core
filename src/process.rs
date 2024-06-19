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
use tokio::sync::mpsc::{channel, Receiver};
use tokio::sync::mpsc::error::TryRecvError;
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
async fn handle_app_input(child: &mut Child,
                          connector: &mut ConnectorChannel,
                          input_type: &str) -> Result<(), Box<dyn Error + Sync + Send>> {
    /* read from the receiver, write to stdin */
    if let Some(rx) = &mut connector.rx {
        let data_res = rx.try_recv();
        if let Err(e) = data_res {
            return if e == TryRecvError::Empty {
                Ok(())
            } else {
                error!("rx channel closed for connector: {}", &connector.connector.id.id);
                Err(Box::new(e))
            };
        }

        let data = data_res.unwrap();
        
        /* write to stdin */
        if input_type == STDIN {
            /* write to stdin of the app */
            if let Some(s) = &mut child.stdin {
                s.write_all(&data).await?;
            } else {
                return Err(Box::new(RadError::from("unexpected error. stdin not available for child")));
            }
        }

        Ok(())
    } else {
        let msg = format!("unexpected error: rx channel non-existent for connector: {}", &connector.connector.id.id);
        error!("{}", &msg);
        Err(Box::new(RadError::from(msg)))
    }
}

/*
    This handles output FROM the application. This means we will READ from the application output.
 */
async fn handle_app_output(child: &mut Child,
                           connector: &mut ConnectorChannel,
                           output_type: &str,
                           timeout: Duration) -> Result<(), Box<dyn Error + Sync + Send>> {
    if let Some(tx) = &connector.tx {
        let mut b = [0u8; 1024];

        if output_type == STDOUT {
            if let Some(s) = &mut child.stdout {
                let res = select! {
                        res = s.read(&mut b) => {
                            res
                        }
                        _ = sleep(timeout) => {
                            return Ok(());
                        }
                    };

                if let Ok(size) = res {
                    tx.send(b[0..size].to_vec()).await?;
                } else {
                    let msg = format!("Error reading from stdout for connector {}", &connector.connector.id.id);
                    error!("{}", &msg);
                    return Err(Box::new(RadError::from(msg)));
                }
            } else {
                return Err(Box::new(RadError::from("Unexpected error. stdout not found in child")));
            }

            Ok(())
        } else if let Some(s) = &mut child.stderr {
            let res = select! {
                    res = s.read(&mut b) => {
                        res
                    }
                    _ = sleep(timeout) => {
                        yield_now().await;
                        return Ok(());
                    }
                };

            if let Ok(size) = res {
                tx.send(b[0..size].to_vec()).await?;
            } else {
                let msg = format!("Error reading from stderr for connector {}", &connector.connector.id.id);
                error!("{}", &msg);
                return Err(Box::new(RadError::from(msg)));
            }

            Ok(())
        } else {
            Err(Box::new(RadError::from(format!("Unexpected error. stderr not available for connector: {}", &connector.connector.id.id))))
        }
    } else {
        let msg = format!("tx channel non-existent for connector: {}", &connector.connector.id.id);
        error!("{}", &msg);
        Err(Box::new(RadError::from(msg)))
    }
}

async fn __get_must_die(am_must_die: Arc<Mutex<bool>>) -> bool {
    let must_die_mg = am_must_die.lock().await;
    *must_die_mg
}

async fn process_connectors(child: &mut Child,
                            connectors: &mut [ConnectorChannel])
                            -> Result<(), Box<dyn Error + Sync + Send>> {
    for connector in connectors {
        let integration_type = &connector.connector.integration.integration_type as &str;
        if integration_type == STDIN {
            handle_app_input(child, connector, STDIN).await?;
        } else if integration_type == STDOUT {
            handle_app_output(child, connector, STDOUT, Duration::from_micros(10)).await?;
        } else if integration_type == STDERR {
            handle_app_output(child, connector, STDERR, Duration::from_micros(1)).await?;
        } else {
            let msg = format!("Unable to start connector. Unknown connector type detected: {}", integration_type);
            error!("{}", &msg);
            return Err(Box::new(RadError::from(msg)));
        }
    }

    Ok(())
}

async fn __write_to_child_stdin(child: &mut Child, data: &[u8], app_id: &str) -> Result<(), Box<dyn Error + Sync + Send>> {
    let size = data.len();

    if let Some(stdin) = &mut child.stdin {
        if size == 0 {
            let msg = "Aborting stdin passthrough due to EOF".to_string();
            error!("{}", &msg);
            return Err(Box::new(RadError::from(msg)));
        }

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

async fn __handle_stdin_passthrough(child: &mut Child,
                                    app_id: &str,
                                    stdin_rx: &mut Receiver<Vec<u8>>) -> Result<(), Box<dyn Error + Sync + Send>> {
    /* read from stdio, pass to app's stdio */
    let data_res = stdin_rx.try_recv();
    if let Err(e) = data_res {
        return if e == TryRecvError::Empty {
            Ok(())
        } else {
            let msg = format!("stdin rx channel shut down: {}", &e);
            error!("{}", &msg);
            Err(Box::new(RadError::from(msg)))
        };
    }

    let data = data_res.unwrap();
    __write_to_child_stdin(child, &data, app_id).await?;
    Ok(())
}

async fn __handle_stdout_stderr_passthrough(child: &mut Child,
                                            app_id: &str,
                                            out: bool,
                                            timeout: Duration) -> Result<(), Box<dyn Error + Sync + Send>> {
    let mut b = [0u8; 1024];
    /* read from app's stdio, pass to main stdio */
    if out {
        if let Some(stdout) = &mut child.stdout {
            let size_res = select! {
                r = AsyncReadExt::read(stdout, &mut b) => {
                    r
                }
                _ = sleep(timeout) => {
                    return Ok(());
                }
            };
            if let Ok(size) = size_res {
                if size == 0 {
                    let msg = "Aborting due to EOF read from child stdout (passthrough)";
                    error!("{}", msg);
                    return Err(Box::new(RadError::from(msg)));
                }

                std::io::stdout().write_all(&b[0..size])?;
                Ok(())
            } else {
                let msg = "error reading from child's stdout (passthrough)";
                error!("{}", msg);
                Err(Box::new(RadError::from(msg)))
            }
        } else {
            let msg = format!("stdout not available for app {}. passthrough (stdout) not possible", app_id);
            error!("{}", &msg);
            Err(Box::new(RadError::from(msg)))
        }
    } else if let Some(stderr) = &mut child.stderr {
        let size_res = select! {
            r = AsyncReadExt::read(stderr, &mut b) => {
                r
            }
            _ = sleep(timeout) => {
                return Ok(());
            }
        };

        if let Ok(size) = size_res {
            if size == 0 {
                let msg = "Aborting due to EOF from child stderr (passthrough)";
                error!("{}", msg);
                return Err(Box::new(RadError::from(msg)));
            }

            std::io::stderr().write_all(&b[0..size])?;
            Ok(())
        } else {
            let msg = "Error reading child's stderr (passthrough)";
            error!("{}", msg);
            Err(Box::new(RadError::from(msg)))
        }
    } else {
        let msg = format!("stderr not available for app {}. passthrough (stderr) not possible", app_id);
        error!("{}", msg);
        Err(Box::new(RadError::from(msg)))
    }
}

async fn process_stdio_passthrough(child: &mut Child,
                                   stdio: &StdioHolder,
                                   app: &App,
                                   stdin_rx_opt: &mut Option<Receiver<Vec<u8>>>) -> Result<(), Box<dyn Error + Sync + Send>> {
    if stdio.input {
        if let Some(stdin_rx) = stdin_rx_opt {
            __handle_stdin_passthrough(child, &app.id.id, stdin_rx).await?;
        } else {
            return Err(Box::new(RadError::from("Unexpected error: stdin_rx is None!!!")));
        }
    }

    if stdio.output {
        __handle_stdout_stderr_passthrough(child, &app.id.id, true, Duration::from_micros(10)).await?;
    }

    if stdio.error {
        __handle_stdout_stderr_passthrough(child, &app.id.id, false, Duration::from_micros(1)).await?;
    }

    Ok(())
}

async fn __check_app_exit(am_child: Arc<Mutex<Child>>, app_id: &str) -> Result<Option<ExitStatus>, Box<dyn Error + Sync + Send>> {
    let mut child_mg = am_child.lock().await;
    let child = child_mg.deref_mut();

    let exit_opt_res = child.try_wait();
    if let Err(e) = exit_opt_res {
        error!("Error checking if app {} exited: {}", app_id, &e);
        return Err(Box::new(e));
    }

    let exit_opt = exit_opt_res.unwrap();
    if let Some(s) = exit_opt {
        return Ok(Some(s));
    } //else still running

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
    let mut child = spawn_process(&base_dir, &app, &connectors, &stdio)?;

    let mut m_connectors = connectors;

    /* spawn a stdin task if required, since this isn't a async operation */
    let mut stdio_rx_opt = None;
    if stdio.input {
        let am_must_die_clone = am_must_die.clone();

        let (tx, rx) = channel(32);
        stdio_rx_opt = Some(rx);
        tokio::spawn(async move {
            let mut b = [0u8; 1024];
            loop {
                let size_res = stdin().read(&mut b);
                if let Err(e) = size_res {
                    error!("Error reading from stdin(). Error: {}", &e);
                    break;
                }

                let size = size_res.unwrap();
                let send_res = tx.send(b[0..size].to_vec()).await;
                if let Err(e) = send_res {
                    error!("Error sending stdin() data to channel: {}", &e);
                    break;
                }

                if __get_must_die(am_must_die_clone.clone()).await {
                    error!("stdio() reader task detected must_die flag");
                    break;
                }
            }

            error!("stdio() reader aborting");
            set_must_die(am_must_die_clone).await;
        });
    }


    loop {
        if process_connectors(&mut child, &mut m_connectors).await.is_err() {
            set_must_die(am_must_die.clone()).await;
            break;
        }

        if process_stdio_passthrough(&mut child, &stdio, &app, &mut stdio_rx_opt).await.is_err() {
            set_must_die(am_must_die.clone()).await;
            break;
        }

        sleep(Duration::from_millis(5)).await;
    }

    warn!("Killing the child for app: {}", &app.id.id);
    child.kill().await?;

    Ok(())
}
