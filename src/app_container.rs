use std::collections::HashMap;
use std::error::Error;
use std::io::ErrorKind::WouldBlock;
use std::sync::Arc;
use std::time::Duration;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock};
use tokio::io::{AsyncWriteExt, Interest};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time::sleep;
use crate::bin::{BinConfig, load_bin};
use crate::broker::auth_types::{AuthMessageReq, AuthMessageResp, MSG_TYPE_AUTH, MSG_TYPE_AUTH_RESP};
use crate::broker::protocol::{Message, MessageHeader, Protocol};
use crate::error::{raderr};
use crate::process::{run_bin_main, spawn_process};
use crate::utils::timer::Timer;
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
    /// if true, the app will not be started by the container. The container will just sit
    /// in an endless loop waiting for the app to abort. The app will need to be manually started.
    /// This is useful for debugging
    pub disabled: bool,
}

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct RepeatedOptions {
    /// see OnceOptions::disabled for explanation
    pub disabled: bool,
    pub restart_delay: u64,
}

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct StartStopOptions {
    /// messages for this app will be read from the can and passed
    /// to stdin using the specified type if Some
    pub redirect_msgs_to_stdin: Option<String>,

    /// data from stdout will be converted to a message and passed
    /// to a chan if Some
    pub redirect_stdout_to_msgs: Option<String>,
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
    //a unique name for this container. No 2 containers may share a name
    pub name: String,
    pub run_type: String,
    pub once_options: Option<OnceOptions>,
    pub repeated_options: Option<RepeatedOptions>,
    pub start_stop_options: Option<StartStopOptions>,
    pub bin: BinOptions,
}

impl Container {
    pub fn verify(&self) -> Result<(), Box<dyn Error + Sync + Send>> {
        if !utils::is_valid_name(&self.name) {
            return raderr(format!("Invalid name for container. Name={}", &self.name));
        }

        match self.run_type.as_str() {
            RUN_TYPE_ONCE => {
                /* no options here, so some or None is valid */
                Ok(())
            }

            RUN_TYPE_REPEATED => {
                if self.repeated_options.is_some() {
                    Ok(())
                } else {
                    raderr("Invalid options specified for repeated type")
                }
            }

            RUN_TYPE_START_STOP => {
                if self.start_stop_options.is_some() {
                    Ok(())
                } else {
                    raderr("Invalid options specified for start_stop type")
                }
            }

            _ => {
                raderr(format!("Invalid run type specified for container. Type: {}", &self.run_type))
            }
        }
    }
}

const APP_INSTANCE_ID_KEY: &str = "RAD_INSTANCE_ID";

fn is_once_or_repeated_container_disabled(container: &Container) -> bool {
    if container.run_type == RUN_TYPE_ONCE {
        if let Some(options) = &container.once_options {
            options.disabled
        } else {
            false
        }
    } else if let Some(options) = &container.repeated_options {
        options.disabled
    } else {
        false
    }
}

async fn handle_once_and_repeated_containers(base_dir: &str,
                                             container: Container,
                                             bin_config: BinConfig,
                                             am_must_die: Arc<RwLock<bool>>) -> Result<(), Box<dyn Error + Sync + Send>> {
    /* if disabled, just run in a loop forever */
    if is_once_or_repeated_container_disabled(&container) {
        warn!("Container {} marked as disabled. App must be manually started", &container.name);
        loop {
            sleep(Duration::from_millis(100)).await;
            if utils::get_must_die(am_must_die.clone()).await {
                warn!("Container {} (disabled) caught must die flag. Exiting", &container.name);
                return Ok(());
            }
        }
    }


    loop {
        //let the instance id be the same as the container id, since the container id must be unique
        let mut env = HashMap::new();
        env.insert(APP_INSTANCE_ID_KEY.to_string(), container.name.to_string());

        let join_handle = tokio::spawn(
            run_bin_main(
                base_dir.to_string(),
                bin_config.clone(),
                env,
                am_must_die.clone()));
        if let Err(e) = join_handle.await? {
            error!("Error running bin: {:?} for container: {}", e, &container.name);
        } else {
            info!("App exited successfully for container: {}", &container.name);
        }

        if container.run_type == RUN_TYPE_ONCE {
            return if container.once_options.is_some() {
                error!("Run-once container aborting after app exit ({})", &container.name);
                Ok(())
            } else {
                raderr("Unexpected error: invalid options found for run-once container")
            };
        } else if container.run_type == RUN_TYPE_REPEATED {
            if let Some(opts) = &container.repeated_options {
                info!("Delaying for {}ms and restarting app for container: {}", opts.restart_delay, &container.name);
                sleep(Duration::from_millis(opts.restart_delay)).await;
                continue;
            } else {
                return raderr("Unexpected error: invalid options found for repeated container");
            }
        } else {
            return raderr(format!("Invalid run type detected. Don't know what to do after app exit. Run-type: {}", &container.run_type));
        }
    }
}

async fn authenticate_client_connection(conn: &mut TcpStream,
                                        container: &Container,
                                        protocol: &mut Protocol)
                                        -> Result<(), Box<dyn Error + Sync + Send>> {
    /* do simple unwraps here. Checks have already been performed */
    if let Some(opts) = &container.start_stop_options {
        /* data types from stdout */
        let tx_types = match &opts.redirect_stdout_to_msgs {
            Some(t) => {
                vec![t.to_string()]
            }
            None => {
                vec![]
            }
        };

        let rx_types = match &opts.redirect_msgs_to_stdin {
            Some(t) => {
                vec![t.to_string()]
            }
            None => {
                vec![]
            }
        };

        /* data types to stdin */
        //let rx_types = if opts.

        let req = AuthMessageReq {
            name: container.name.clone(),
            tx_msg_types: tx_types,
            rx_msg_types: rx_types,
        };
        let req_bytes = serde_json::to_vec(&req)?;

        let auth_msg = Message {
            header: MessageHeader {
                name: container.name.to_string(),
                rks: None,
                rks_match_type: None,
                message_id: String::new(),
                msg_type: MSG_TYPE_AUTH.to_string(),
                length: req_bytes.len() as u32,
                extras: None,
            },
            body: req_bytes,
        };

        let b = Protocol::format(&auth_msg)?;
        conn.write_all(&b).await?;

        /* wait for response */
        let response_timer = Timer::new(Duration::from_millis(3000));
        let mut b = [0; 1024];
        while !response_timer.timed_out() {
            match conn.try_read(&mut b) {
                Ok(size) => {
                    if size == 0 {
                        //eof
                        break;
                    }

                    let msgs = protocol.feed(&b[0..size]);
                    if msgs.is_empty() {
                        sleep(Duration::from_millis(5)).await;
                        continue;
                    }

                    if msgs.len() != 1 {
                        return raderr(format!("auth error for container {}. Expected one message response", &container.name));
                    }

                    if msgs[0].header.msg_type != MSG_TYPE_AUTH_RESP {
                        return raderr(format!("Invalid message response type to authentication request for container {}", &container.name));
                    }

                    let resp = serde_json::from_slice::<AuthMessageResp>(&msgs[0].body)?;
                    if resp.success {
                        debug!("Authentication successful for container {}", &container.name);
                        return Ok(());
                    }
                }

                Err(e) => {
                    if e.kind() == WouldBlock {
                        sleep(Duration::from_millis(5)).await;
                        continue;
                    } else {
                        return raderr(format!("Error during authentication response read: {}", &e));
                    }
                }
            }
        } //while

        raderr(format!("Authentication response timeout for container {}", &container.name))
    } else {
        raderr(format!("Error authenticating client for container {}", &container.name))
    }
}

/// creates a loopback connection to the broker for sending/receiving
/// messages. These messages are then passed back to the container for
/// conversion to stdio
async fn handle_start_stop_container_client(broker_listen_port: u16,
                                            container: Container,
                                            to_container: Sender<Message>,
                                            from_container: Receiver<Message>,
                                            am_must_die: Arc<RwLock<bool>>) -> Result<(), Box<dyn Error + Sync + Send>> {
    let mut from_container = from_container;
    let mut protocol = Protocol::new()?;

    let mut b = [0u8; 1024];
    let mut conn = TcpStream::connect(format!("localhost:{}", broker_listen_port)).await?;
    authenticate_client_connection(&mut conn, &container, &mut protocol).await?;

    loop {
        if utils::get_must_die(am_must_die.clone()).await {
            warn!("container client for container {} caught must die flag. Aborting", &container.name);
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
                                     container: Container,
                                     bin_config: BinConfig,
                                     broker_listen_port: u16,
                                     am_must_die: Arc<RwLock<bool>>)
                                     -> Result<(), Box<dyn Error + Sync + Send>> {
    if let Some(opts) = &container.start_stop_options {
        //naming is from our perspecting
        let (tx_stdio_to_broker, rx_from_container) = tokio::sync::mpsc::channel(32);
        let (tx_from_broker, mut rx_broker_to_stdio) = tokio::sync::mpsc::channel(32);

        info!("Starting loopback stdio<->broker bridge for container {}", &container.name);
        let am_must_die_clone = am_must_die.clone();
        let container_clone = container.clone();
        tokio::spawn(async move {
            let res = handle_start_stop_container_client(broker_listen_port,
                                                         container_clone,
                                                         tx_from_broker,
                                                         rx_from_container,
                                                         am_must_die_clone).await;
            if let Err(e) = res {
                error!("Error in start-stop container client: {}", e);
            }
        });

        loop {
            if utils::get_must_die(am_must_die.clone()).await {
                warn!("Container {} caught must_die flag. Aborting", &container.name);
                return Ok(());
            }

            match rx_broker_to_stdio.try_recv() {
                Ok(msg) => {
                    debug!("Container {} received new message", &container.name);

                    /* spawn a task and pass the message to stdio */
                    let mut child = spawn_process(base_dir,
                                                  &bin_config,
                                                  None,
                                                  &opts.redirect_msgs_to_stdin,
                                                  &opts.redirect_stdout_to_msgs)?;


                    if opts.redirect_msgs_to_stdin.is_some() {
                        /* write the message contents to stdin */
                        if let Some(ref mut stdin) = child.stdin {
                            debug!("Writing to stdin: {}", std::str::from_utf8(&msg.body.as_slice()).unwrap_or("[decode_error]"));
                            stdin.write_all(&msg.body).await?;
                        }
                    }

                    /*
                        else ignore the message eg.
                        if we don't care about the message contents,
                        but just want a trigger to start the app
                     */

                    if opts.redirect_stdout_to_msgs.is_some() {
                        let output = child.wait_with_output().await?;
                        if let Some(msg_type) = &opts.redirect_stdout_to_msgs {
                            /* send data to router */
                            let msg = Message {
                                header: MessageHeader {
                                    name: container.name.clone(),
                                    rks: None,
                                    rks_match_type: None,
                                    message_id: msg.header.message_id.clone(),
                                    msg_type: msg_type.to_string(),
                                    length: output.stdout.len() as u32,
                                    extras: None,
                                },
                                body: output.stdout,
                            };
                            tx_stdio_to_broker.send(msg).await?;
                            //debug!("Message sent to broker");
                        }
                    } else {
                        /* just wait for exit, don't capture output */
                        if let Err(e) = child.wait().await {
                            let msg = format!("Error waiting for app to exit in container {}. Stdout output ignored, but continuing. Error: {}", &container.name, e);
                            error!("{}", &msg);
                        }
                    }
                }

                Err(e) => {
                    if e == TryRecvError::Empty {
                        sleep(Duration::from_millis(5)).await;
                        continue;
                    } else {
                        let msg = format!("Chan to broker closed for container {}", &container.name);
                        error!("{}", &msg);
                        return raderr(msg);
                    }
                }
            }
        } //loop
    } else {
        raderr("Unexpected error: invalid options found for start-stop container")
    }
}

pub async fn run_container_main(base_dir: String,
                                container: Container,
                                broker_listen_port: u16,
                                am_must_die: Arc<RwLock<bool>>) -> Result<(), Box<dyn Error + Sync + Send>> {
    let bin_config = load_bin(&base_dir, &container.bin.name)?;

    info!("Running container {} ({}) for app: {}",
        &container.name,
        &container.run_type,
        &container.bin.name);

    let container_name = container.name.clone();
    let res = if container.run_type == RUN_TYPE_ONCE || container.run_type == RUN_TYPE_REPEATED {
        handle_once_and_repeated_containers(&base_dir,
                                            container,
                                            bin_config,
                                            am_must_die.clone()).await
    } else if container.run_type == RUN_TYPE_START_STOP {
        handle_start_stop_container(&base_dir,
                                    container,
                                    bin_config,
                                    broker_listen_port,
                                    am_must_die.clone()).await
    } else {
        return raderr(format!("Invalid container type ({}) detected", &container.run_type));
    };

    if let Err(e) = res {
        let msg = format!("Error in container {}. Aborting application. Error: {}", &container_name, &e);
        error!("{}", &msg);
        utils::set_must_die(am_must_die).await;
        return raderr(msg);
    }

    Ok(())
}