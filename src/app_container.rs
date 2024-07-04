use std::error::Error;
use std::io::ErrorKind::WouldBlock;
use std::process::ExitStatus;
use std::sync::Arc;
use std::time::Duration;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tokio::try_join;
use rand::random;
use tokio::io::{AsyncReadExt, AsyncWriteExt, Interest};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time::sleep;
use crate::bin::{BinConfig, load_bin};
use crate::broker::protocol::{Message, MessageHeader, Protocol};
use crate::error::RadError;
use crate::process::{run_bin_main, spawn_process};
use crate::utils::utils;

/// Run (i.e. start) an app once only. App exit means container
/// failure. The app may only exit cleanly if signaled to do so
/// by the container. The app is expected to be long-running app
pub const RUN_TYPE_ONCE: &str = "once";

/// Run the app, which is expected to be long-running. If
/// the app dies, restart it
pub const RUN_TYPE_REPEATED: &str = "repeated";

/// The app is not long-running. It must be started for each
/// message, and it will then process the data and exit. It
/// must be restarted for the next message, etc.
pub const RUN_TYPE_START_STOP: &str = "start_stop";


#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct OnceOptions {
}

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct RepeatedOptions {
    pub restart_delay: u64,
}

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct StartStopOptions {
    /// messages for this app will be read from the can and passed
    /// to stdin
    pub redirect_msgs_to_stdin: bool,

    /// data from stdout will be converted to a message and passed
    /// to a chan
    pub redirect_stdout_to_msgs: bool,

    /// if redirect_stdout_to_msgs is true, this must contain
    /// the message type for the stdout message
    pub stdout_msg_type: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BinOptions {
    /// name of the binary, as specified in the cache directory
    /// i.e. from the id.id field
    pub name: String,

    /// optional args
    pub args: Option<Vec<String>>,

    /// if the optional args are specified, this determines
    /// if the args must be appended to the args specified in
    /// the config (as specified in the cache), or if these
    /// args must override the args specified in the cache.
    /// Defaults to false if not specified.
    pub append_args: Option<bool>,

    /// if specified, this overrides the settings in the
    /// config file for the app (from the cache dir)
    pub working_dir: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Container {
    pub run_type: String,
    pub once_options: Option<OnceOptions>,
    pub repeated_options: Option<RepeatedOptions>,
    pub start_stop_options: Option<StartStopOptions>,
    pub bin: BinOptions,
}

impl Container {
    pub fn verify(&self) -> Result<(), Box<dyn Error + Sync + Send>> {
        match self.run_type.as_str() {
            RUN_TYPE_ONCE => {
                /* no options here, so some or None is valid */
                Ok(())
            }

            RUN_TYPE_REPEATED => {
                if self.repeated_options.is_some() {
                    Ok(())
                } else {
                    Err(Box::new(RadError::from("Invalid options specified for repeated type")))
                }
            }

            RUN_TYPE_START_STOP => {
                if self.start_stop_options.is_some() {
                    Ok(())
                } else {
                    Err(Box::new(RadError::from("Invalid options specified for start_stop type")))
                }
            }

            _ => {
                Err(Box::new(RadError::from(format!("Invalid run type specified for container. Type: {}", &self.run_type))))
            }
        }
    }
}

async fn handle_once_and_repeated_containers(base_dir: &str,
                                             container_id: &str,
                                             container: Container,
                                             bin_config: BinConfig,
                                             am_must_die: Arc<RwLock<bool>>) -> Result<(), Box<dyn Error + Sync + Send>> {
    loop {
        let join_handle = tokio::spawn(run_bin_main(base_dir.to_string(), bin_config.clone(), am_must_die.clone()));
        if let Err(e) = join_handle.await? {
            error!("Error running bin: {:?} for container: {}", e, &container_id);
        } else {
            info!("App exited successfully for container: {}", &container_id);
        }

        if container.run_type == RUN_TYPE_ONCE {
            return if container.once_options.is_some() {
                error!("Run-once container aborting after app exit ({})", &container_id);
                Ok(())
            } else {
                Err(Box::new(RadError::from("Unexpected error: invalid options found for run-once container")))
            }
        } else if container.run_type == RUN_TYPE_REPEATED {
            if let Some(opts) = &container.repeated_options {
                info!("Delaying for {}ms and restarting app for container: {}", opts.restart_delay, &container_id);
                sleep(Duration::from_millis(opts.restart_delay)).await;
                continue;
            } else {
                return Err(Box::new(RadError::from("Unexpected error: invalid options found for repeated container")));
            }
        } else {
            return Err(Box::new(RadError::from(format!("Invalid run type detected. Don't know what to do after app exit. Run-type: {}", &container.run_type))));
        }
    }
}

async fn authenticate_connection(conn: &mut TcpStream) -> Result<(), Box<dyn Error + Sync + Send>> {
    /* TODO: finish me */
    Ok(())
}

/// creates a loopback connection to the broker for sending/receiving
/// messages. These messages are then passed back to the container for
/// conversion to stdio
async fn handle_start_stop_container_client(broker_listen_port: u16,
                                            container_id: String,
                                            to_container: Sender<Message>,
                                            from_container: Receiver<Message>,
                                            am_must_die: Arc<RwLock<bool>>) -> Result<(), Box<dyn Error + Sync + Send>> {
    let mut from_container = from_container;
    let mut protocol = Protocol::new()?;

    let mut b = [0u8; 1024];
    let mut conn = TcpStream::connect(format!("localhost:{}", broker_listen_port)).await?;
    authenticate_connection(&mut conn).await?;

    loop {
        if utils::get_must_die(am_must_die.clone()).await {
            warn!("container client for container {} caught must die flag. Aborting", &container_id);
            return Ok(());
        }

        let mut busy = false;

        let ready = conn.ready(Interest::READABLE | Interest::WRITABLE).await?;
        if ready.is_readable() {
            match conn.try_read(&mut b) {
                Ok(size) => {
                    if size == 0 {
                        //eof
                        return Ok(());
                    }

                    let msgs = protocol.feed(&b[0..size]);
                    for msg in msgs {
                        to_container.send(msg).await?;
                    }
                    busy = true;
                }
                Err(ref e) if e.kind() == WouldBlock => {
                    /* ignore */
                }

                Err(e) => {
                    return Err(Box::new(e));
                }
            }
        }

        if ready.is_writable() {
            match from_container.try_recv() {
                Ok(msg) => {
                    let formatted = Protocol::format(&msg)?;
                    conn.write_all(&formatted).await?;
                    busy = true;
                }

                Err(e) => {
                    if e == TryRecvError::Empty {} else {
                        return Err(Box::new(e));
                    }
                }
            }
        }

        if !busy {
            sleep(Duration::from_millis(5)).await;
        }
    }
}


async fn handle_start_stop_container(base_dir: &str,
                                     container_id: String,
                                     container: Container,
                                     bin_config: BinConfig,
                                     broker_listen_port: u16,
                                     am_must_die: Arc<RwLock<bool>>)
                                     -> Result<(), Box<dyn Error + Sync + Send>> {
    if let Some(opts) = &container.start_stop_options {
        //naming is from our perspecting
        let (tx_stdio_to_broker, rx_from_container) = tokio::sync::mpsc::channel(32);
        let (tx_from_broker, mut rx_broker_to_stdio) = tokio::sync::mpsc::channel(32);
        let broker_listen_port_clone = broker_listen_port.clone();

        info!("Starting loopback stdio<->broker bridge for container {}", &container_id);
        let am_must_die_clone = am_must_die.clone();
        let container_id_clone = container_id.clone();
        tokio::spawn(async move {
            let res = handle_start_stop_container_client(broker_listen_port_clone,
                                                         container_id_clone,
                                                         tx_from_broker,
                                                         rx_from_container,
                                                         am_must_die_clone).await;
            if let Err(e) = res {
                error!("Error in start-stop container client: {}", e);
            }
        });

        loop {
            if utils::get_must_die(am_must_die.clone()).await {
                warn!("Container {} caught must_die flag. Aborting", container_id);
                return Ok(());
            }

            match rx_broker_to_stdio.try_recv() {
                Ok(msg) => {
                    /* spawn a task and pass the message to stdio */
                    let mut child = spawn_process(base_dir,
                                                  &bin_config,
                                                  opts.redirect_msgs_to_stdin,
                                                  opts.redirect_stdout_to_msgs)?;

                    if opts.redirect_msgs_to_stdin {
                        /* write the message contents to stdin */
                        if let Some(stdin) = &mut child.stdin {
                            stdin.write_all(&msg.body).await?;
                        }
                    }
                    /*
                        else ignore the message eg.
                        if we don't care about the message contents,
                        but just want a trigger to start the app
                     */

                    let mut data = vec![];

                    if opts.redirect_stdout_to_msgs {
                        /* grab stdout */
                        if let Some(stdout) = &mut child.stdout {
                            let mut b = [0; 1024];
                            loop {
                                let size = stdout.read(&mut b).await?;
                                if size == 0 {
                                    //eof
                                    break;
                                }

                                data.extend_from_slice(&b[0..size]);
                            }
                        }
                    }

                    match child.wait().await {
                        Ok(status) => {
                            if let Some(code) = status.code() {
                                if code == 0 {
                                    debug!("Container {} app exited successfully. Continuing", &container_id);
                                } else {
                                    warn!("Container {} app exited with code {}. Stdout output will be ignored", &container_id, code);
                                }
                            }

                            if opts.redirect_stdout_to_msgs {
                                /* send data to router */
                                if let Some(msg_type) = &opts.stdout_msg_type {
                                    let msg = Message {
                                        header: MessageHeader {
                                            name: container.bin.name.clone(),
                                            msg_type: msg_type.to_string(),
                                            length: data.len() as u32,
                                        },
                                        body: data,
                                    };
                                    tx_stdio_to_broker.send(msg).await?;
                                } else {
                                    error!("Unable to send stdout data to router for container {}. No message type specified", container_id);
                                }
                            }
                        }

                        Err(e) => {
                            error!("Error waiting for app to exit in container {}. Stdout output ignored, but continuing. Error: {}", container_id, e);
                        }
                    }
                }

                Err(e) => {
                    if e == TryRecvError::Empty {
                        sleep(Duration::from_millis(5)).await;
                        continue;
                    } else {
                        let msg = format!("Chan to broker closed for container {}", container_id);
                        error!("{}", &msg);
                        return Err(Box::new(RadError::from(msg)));
                    }
                }
            }
        }
    } else {
        Err(Box::new(RadError::from("Unexpected error: invalid options found for start-stop container")))
    }
}

pub async fn run_container_main(base_dir: String,
                                container: Container,
                                broker_listen_port: u16,
                                am_must_die: Arc<RwLock<bool>>) -> Result<(), Box<dyn Error + Sync + Send>> {
    let bin_config = load_bin(&base_dir, &container.bin.name)?;

    let container_id = format!("{}-{}", chrono::Utc::now().timestamp_millis(), random::<u64>());
    info!("Running container {} ({}) for app: {}",
        &container_id,
        &container.run_type,
        &container.bin.name);

    let res = if container.run_type == RUN_TYPE_ONCE || container.run_type == RUN_TYPE_REPEATED {
        handle_once_and_repeated_containers(&base_dir, &container_id, container, bin_config, am_must_die.clone()).await
    } else if container.run_type == RUN_TYPE_START_STOP {
        handle_start_stop_container(&base_dir,
                                    container_id.clone(),
                                    container,
                                    bin_config,
                                    broker_listen_port,
                                    am_must_die.clone()).await
    } else {
        return Err(Box::new(RadError::from(format!("Invalid container type ({}) detected", &container.run_type))));
    };

    if let Err(e) = res {
        let msg = format!("Error in container {}. Aborting application. Error: {}", &container_id, &e);
        error!("{}", &msg);
        utils::set_must_die(am_must_die).await;
        return Err(Box::new(RadError::from(msg)));
    }

    Ok(())
}