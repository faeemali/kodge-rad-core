use std::error::Error;
use std::fs;
use std::io::{Read, stdin, stdout, Write};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinSet;
use crate::AppCtx;
use crate::broker::protocol::{Message, MessageHeader};
use crate::config::config_common::ConfigId;
use crate::error::{raderr};
use crate::utils::utils;
use crate::utils::utils::load_yaml;
use crate::workflow::execute_workflow;

#[derive(Serialize, Deserialize, Clone)]
pub struct App {
    pub id: ConfigId,
    pub workflows: Vec<AppWorkflowItem>,
    pub stdio: AppStdio,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AppWorkflowItem {
    pub name: String,
    pub args: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AppStdio {
    pub stdin: Option<StdioItem>,
    pub stdout: Option<StdioItem>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StdioItem {
    pub workflow: String,
    pub msg_type: String,
}

pub fn get_all_apps(base_dir: &str) -> Result<Vec<App>, Box<dyn Error + Sync + Send>> {
    let mut ret = vec![];

    let path = format!("{}/apps", base_dir);
    let files = fs::read_dir(&path)?;
    for file_res in files {
        if let Err(e) = file_res {
            error!("Error reading app file: {}. Skipping", e);
            continue;
        }
        let file = file_res.unwrap();
        if let Ok(file_type) = file.file_type() {
            if file_type.is_file() {
                match file.file_name().to_str() {
                    Some(f) => {
                        let name = format!("{}/{}", &path, f);
                        let app = load_yaml::<App>(&name)?;
                        ret.push(app);
                    }
                    None => {
                        error!("Error reading application file. Skipping");
                        continue;
                    }
                }
            }
        }
    }
    Ok(ret)
}

fn find_app_by_id(base_dir: &str, app_id: &str) -> Result<Option<App>, Box<dyn Error + Sync + Send>> {
    let apps = get_all_apps(base_dir)?;
    let app = apps.iter().find(|a| a.id.id == app_id).cloned();
    Ok(app)
}

pub const STDIN: &str = "stdin";
pub const STDOUT: &str = "stdout";

async fn handle_stdin_passthrough_main(stdin_tx: Sender<Message>, msg_type: String) -> Result<(), Box<dyn Error + Sync + Send>> {
    let mut b = [0u8; 1024];
    loop {
        /* TODO: how do i interrupt this? The app can't die correctly because this is a blocking read */
        let size_res = stdin().read(&mut b);
        if let Err(e) = size_res {
            let msg = format!("Error reading from stdin: {}. Aborting", &e);
            error!("{}", &msg);
            return raderr(msg);
        }

        let size = size_res.unwrap();
        if size == 0 {
            warn!("EOF on stdin. Aborting");
            break;
        }

        stdin_tx.send(Message {
            header: MessageHeader {
                name: STDIN.to_string(),
                msg_type: msg_type.to_string(),
                length: size as u32,
            },
            body: b[0..size].to_vec(),
        }).await?;
    }

    Ok(())
}

async fn handle_stdout_passthrough_main(stdout_rx: Receiver<Message>, msg_type: String) -> Result<(), Box<dyn Error + Sync + Send>> {
    let mut stdout_rx = stdout_rx;

    loop {
        let msg = match stdout_rx.recv().await {
            Some(m) => m,
            None => {
                error!("stdout receiver closed");
                break;
            }
        };

        if msg.header.msg_type != msg_type {
            warn!("stdout ignoring message of type: {}", &msg_type);
            continue;
        }

        match stdout().write_all(msg.body.as_slice()) {
            Ok(r) => {

            }
            Err(e) => {
                let msg = format!("stdout tx error: {}", &e);
                error!("{}", &msg);
                return raderr(msg);
            }
        }
    }

    info!("stdout passthrough terminating");
    Ok(())
}

pub async fn execute_app(app_ctx: AppCtx, app_id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let app = match find_app_by_id(&app_ctx.base_dir, app_id)? {
        Some(a) => { a }
        None => {
            return raderr(format!("Application not found: {}", app_id));
        }
    };

    /* start all workflows */
    let mut join_set = JoinSet::new();

    for wf in app.workflows {
        /* handle stdin passthrough */
        let stdin_chan = if let Some(stdin) = &app.stdio.stdin {
            if wf.name == stdin.workflow {
                let (stdin_tx, stdin_rx) = channel(32);
                join_set.spawn(handle_stdin_passthrough_main(stdin_tx, stdin.msg_type.clone()));
                Some(stdin_rx)
            } else {
                None
            }
        } else {
            None
        };

        /* handle stdout passthrough */
        let stdout_chan = if let Some(stdout) = &app.stdio.stdout {
            if wf.name == stdout.workflow {
                let (stdout_tx, stdout_rx) = channel(32);
                join_set.spawn(handle_stdout_passthrough_main(stdout_rx, stdout.msg_type.clone()));
                Some(stdout_tx)
            } else {
                None
            }
        } else {
            None
        };

        join_set.spawn(execute_workflow(app_ctx.clone(),
                                        wf.name.clone(),
                                        wf.args.clone(),
                                        stdin_chan,
                                        stdout_chan));
    }

    match join_set.join_next().await {
        Some(res) => {
            match res {
                Ok(r_res) => {
                    match r_res {
                        Ok(_) => {
                            info!("Workflow terminated. Aborting remaining workflows");
                            utils::set_must_die(app_ctx.must_die.clone()).await;
                            Ok(())
                        }
                        Err(e) => {
                            let msg = format!("Workflow aborted with error: {}. Aborting all", &e);
                            error!("{}", &msg);
                            utils::set_must_die(app_ctx.must_die.clone()).await;
                            raderr(msg)
                        }
                    }
                }
                Err(e) => {
                    let msg = format!("Join error detected. Aborting everything. Error: {}", &e);
                    error!("{}", &msg);
                    utils::set_must_die(app_ctx.must_die.clone()).await;
                    raderr(msg)
                }
            }
        }

        None => {
            raderr("Unexpected error. Got none while waiting for workflows to complete")
        }
    } //for
}