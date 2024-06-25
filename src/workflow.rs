use std::collections::HashMap;
use std::error::Error;
use std::ops::{Deref};
use std::sync::Arc;
use std::time::Duration;
use log::{info, warn};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::time::sleep;

use crate::{AppCtx};
use crate::app::{App, AppIoDefinition, load_app, STDERR, STDIN, STDOUT};
use crate::broker::broker::{broker_main};
use crate::config::config_common::ConfigId;
use crate::error::RadError;
use crate::process::run_app_main;
use crate::utils::utils::load_yaml;

/* 
    the io direction from the app's point of view. The typical use case
    is eg. to read from the app and write to a channel, or read from a channel
    and write to the app.
    
    Since everything is from the app's point of view, reading from the app
    means we must read from the app's output, therefore the direction
    will be Out. Similarly, writing to the app means writing to it's input,
    therefore the direction will be In.
 */
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub enum AppIoDirection {
    Out,
    In,
}

struct ConnectorHolder<'a> {
    pub app_id: String,
    pub definition: &'a AppIoDefinition,
    pub connected: bool, //has this connection been used?
    pub direction: AppIoDirection,
}

#[derive(Serialize, Deserialize)]
pub struct Workflow {
    pub id: ConfigId,
    pub apps: Vec<String>,
}

impl Workflow {
    fn verify_apps(&self, wf_ctx: &WorkflowCtx) -> Result<(), Box<dyn Error>> {
        for app_name in &self.apps {
            info!("Found app: {}", app_name);
            let app = load_app(&wf_ctx.base_dir, app_name)?;
            app.verify()?;
        }
        Ok(())
    }

    fn __verify_stdin(&self, wf_ctx: &WorkflowCtx, in_name: &str) -> Result<(), Box<dyn Error>> {
        let app = load_app(&wf_ctx.base_dir, in_name)?;

        /* ensure this app does not have anything connected to its input */
        if let Some(inputs) = app.io.input {
            for input in &inputs {
                if input.integration.integration_type == STDIN {
                    return Ok(());
                }
            }
        }

        Err(Box::new(RadError::from(format!("stdin not specified for this app: {}", &app.id.id))))
    }

    fn __verify_stdout_err(&self, wf_ctx: &WorkflowCtx, in_name: &str, out: bool) -> Result<(), Box<dyn Error>> {
        let app = load_app(&wf_ctx.base_dir, in_name)?;

        /* ensure this app does not have anything connected to its input */
        if let Some(outputs) = app.io.output {
            for output in &outputs {
                if out {
                    if output.integration.integration_type == STDOUT {
                        return Ok(());
                    }
                } else if output.integration.integration_type == STDERR {
                    return Ok(());
                }
            }
        }

        if out {
            Err(Box::new(RadError::from(format!("stdout not a required output in app {}", &app.id.id))))
        } else {
            Err(Box::new(RadError::from(format!("stderr not a required output in app {}", &app.id.id))))
        }
    }

    fn verify(&self, wf_ctx: &WorkflowCtx) -> Result<(), Box<dyn Error>> {
        self.id.print();

        info!("verifying apps");
        self.verify_apps(wf_ctx)?;

        Ok(())
    }
}

pub struct WorkflowCtx {
    pub base_dir: String,
    pub workflow: Workflow,
}

fn __find_connector<'a>(conn_id: &str, app: &'a App) -> Result<&'a AppIoDefinition, Box<dyn Error>> {
    let connector = match find_connector_in_app(conn_id, app) {
        Some(c) => c,
        None => {
            return Err(Box::new(RadError::from(format!("Connector {} not found in app {}. Unexpected", &conn_id, &app.id.id))));
        }
    };
    Ok(connector)
}

fn find_connector_in_app<'a>(connector_id: &str, app: &'a App) -> Option<&'a AppIoDefinition> {
    if let Some(defs) = &app.io.input {
        for def in defs {
            if &def.id.id == connector_id {
                return Some(def);
            }
        }
    }

    if let Some(defs) = &app.io.output {
        for def in defs {
            if &def.id.id == connector_id {
                return Some(def);
            }
        }
    }

    None
}

fn __get_connectors<'a>(app_id: &str, io_opt: &'a Option<Vec<AppIoDefinition>>, direction: AppIoDirection)
                        -> Vec<ConnectorHolder<'a>> {
    let mut io_defs = vec![];
    return if let Some(ios) = io_opt {
        ios.iter().for_each(|i| {
            let holder = ConnectorHolder {
                app_id: app_id.to_string(),
                definition: i,
                connected: false,
                direction,
            };
            io_defs.push(holder);
        });

        io_defs
    } else {
        vec![]
    };
}

fn get_connectors_for_apps(apps: &[App]) -> Vec<ConnectorHolder> {
    let mut connectors: Vec<ConnectorHolder> = vec![];
    for app in apps {
        let mut input_connectors = __get_connectors(&app.id.id,
                                                    &app.io.input,
                                                    AppIoDirection::In);
        connectors.append(&mut input_connectors);

        let mut output_connectors = __get_connectors(&app.id.id,
                                                     &app.io.output,
                                                     AppIoDirection::Out);
        connectors.append(&mut output_connectors);
    }

    connectors
}

struct ExecutionCtx {
    pub base_dir: String,
    pub apps: Arc<Vec<App>>,

    //if true, the workflow must die. Not using AtomicBool because of a note that says
    //it's only supported on certain platforms
    pub must_die: Arc<Mutex<bool>>,
}

impl ExecutionCtx {
    /* checks the list of connectors to see if we must grab stdin, stdout, stderr */
    pub async fn run_apps(&mut self) {
        let apps = self.apps.deref();
        for app in apps {
            tokio::spawn(run_app_main(self.base_dir.to_string(),
                                      app.clone(),
                                      self.must_die.clone()));
        }

        self.monitor_apps().await;
        warn!("apps terminated");
    }

    async fn must_die(&self) -> bool {
        *self.must_die.lock().await
    }

    async fn monitor_apps(&self) {
        loop {
            if self.must_die().await {
                warn!("Workflow detected must_die flag. Aborting after 1s");
                sleep(Duration::from_millis(1000)).await;
                break;
            }

            sleep(Duration::from_millis(100)).await;
        }
    }
}

pub async fn execute_workflow(app_ctx: &AppCtx, workflow_name: &str, args: &[String]) -> Result<(), Box<dyn Error>> {
    tokio::spawn(broker_main(app_ctx.base_dir.clone(),
                             workflow_name.to_string(),
                             app_ctx.config.broker.clone()));

    let filename = format!("{}/workflows/{}/workflow.yaml", app_ctx.base_dir, workflow_name);
    let workflow: Workflow = load_yaml(&filename)?;

    let mut apps = vec![];
    for app_name in &workflow.apps {
        let app = load_app(&app_ctx.base_dir, app_name)?;
        apps.push(app);
    }
    let a_apps = Arc::new(apps);

    let wf_ctx = WorkflowCtx {
        base_dir: app_ctx.base_dir.to_string(),
        workflow,
    };

    wf_ctx.workflow.verify(&wf_ctx)?;
    info!("All app connections verified");

    let mut exec_ctx = ExecutionCtx {
        base_dir: app_ctx.base_dir.to_string(),
        apps: a_apps,
        must_die: app_ctx.must_die.clone(),
    };

    /* start each app in the map */
    exec_ctx.run_apps().await;

    Ok(())
}


