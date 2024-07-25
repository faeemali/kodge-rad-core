use std::collections::HashMap;
use std::error::Error;
use std::ops::{Deref};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use log::{info, warn};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{RwLock};
use tokio::time::sleep;

use crate::{AppCtx};
use crate::app_container::{Container, run_container_main};
use crate::bin::{load_bin};
use crate::broker::broker::{broker_main};
use crate::broker::protocol::Message;
use crate::config::config_common::ConfigId;
use crate::error::{raderr};
use crate::utils::utils;
use crate::utils::utils::{get_dirs, load_yaml};

#[derive(Serialize, Deserialize, Clone)]
pub struct Workflow {
    pub id: ConfigId,
    pub apps: Vec<Container>,
}

impl Workflow {
    fn verify_apps(&self, wf_ctx: &WorkflowCtx) -> Result<(), Box<dyn Error + Sync + Send>> {
        //use the hashmap to determine if a container name is duplicated
        let mut container_name_map = HashMap::new();

        for container in &self.apps {
            info!("Found (container name)/[container type]/binary: ({})/[{}]/{}", &container.name, &container.run_type, &container.bin.name);
            container.verify()?;

            if !utils::is_valid_name(&container.name) {
                return raderr(format!("Invalid container name: {}", &container.name));
            }

            let val = container_name_map.get(&container.name);
            if val.is_some() {
                return raderr(format!("Duplicate container name detected: {}", &container.name));
            }
            container_name_map.insert(container.name.clone(), container.name.clone());

            let bin_name = &container.bin.name;
            let bin = load_bin(&wf_ctx.base_dir, bin_name)?;
            bin.verify()?;
        }
        Ok(())
    }

    fn verify(&self, wf_ctx: &WorkflowCtx) -> Result<(), Box<dyn Error + Sync + Send>> {
        info!("Workflow: ");
        self.id.print();

        info!("verifying binaries");
        self.verify_apps(wf_ctx)?;

        Ok(())
    }
}

pub struct WorkflowCtx {
    pub base_dir: String,
    pub workflow: Workflow,
    pub containers: Arc<Vec<Container>>,
    pub must_die: Arc<RwLock<bool>>,
    pub broker_listen_port: u16,
}

impl WorkflowCtx {
    /* checks the list of connectors to see if we must grab stdin, stdout, stderr */
    pub async fn run_containers(&mut self) {
        let containers = self.containers.deref();
        for container in containers {
            tokio::spawn(run_container_main(self.base_dir.to_string(),
                                            self.workflow.id.id.clone(),
                                            container.clone(),
                                            self.broker_listen_port,
                                            self.must_die.clone()));
        }

        self.monitor_bins().await;
        warn!("bins terminated");
    }

    async fn monitor_bins(&self) {
        loop {
            if utils::get_must_die(self.must_die.clone()).await {
                warn!("Workflow detected must_die flag. Aborting after 1s");
                sleep(Duration::from_millis(1000)).await;
                break;
            }

            sleep(Duration::from_millis(100)).await;
        }
    }
}

fn find_broker_listen_port(bind_addr: &str) -> Result<u16, Box<dyn Error + Send + Sync>> {
    let pos_opt = bind_addr.find(':');
    match pos_opt {
        Some(pos) => {
            if pos == (bind_addr.len() - 1) {
                return raderr("Invalid format for broker bind address");
            }
            let port_s = &bind_addr[pos + 1..];
            let port = port_s.parse::<u16>()?;
            Ok(port)
        }
        None => {
            raderr("Unable to determine port from broker bind address")
        }
    }
}

pub async fn execute_workflow(app_ctx: AppCtx,
                              workflow_id: String,
                              args: Vec<String>,
                              stdin_chan_opt: Option<Receiver<Message>>,
                              stdout_chan_opt: Option<Sender<Message>>)
                              -> Result<(), Box<dyn Error + Sync + Send>> {
    info!("Executing workflow: {}", &workflow_id);

    let (workflow, wf_dir) = match find_workflow_by_id(&app_ctx.base_dir, &workflow_id)? {
        Some(wf_dir) => {
            wf_dir
        }
        None => {
            return raderr(format!("Workflow with id: {} does not exist", &workflow_id));
        }
    };

    tokio::spawn(broker_main(wf_dir,
                             workflow_id.clone(),
                             app_ctx.config.broker.clone(),
                             stdin_chan_opt,
                             stdout_chan_opt,
                             app_ctx.must_die.clone()));

    let containers = workflow.apps.clone();
    let a_containers = Arc::new(containers);

    let broker_listen_port = find_broker_listen_port(&app_ctx.config.broker.bind_addr)?;
    let mut wf_ctx = WorkflowCtx {
        base_dir: app_ctx.base_dir.to_string(),
        containers: a_containers,
        workflow,
        broker_listen_port,
        must_die: app_ctx.must_die.clone(),
    };

    wf_ctx.workflow.verify(&wf_ctx)?;

    /* start each binary in the map */
    wf_ctx.run_containers().await;

    Ok(())
}

/// returns a list of all workflow objects, and their associated directories
pub fn get_all_workflows(base_dir: &str) -> Result<Vec<(Workflow, String)>, Box<dyn Error + Sync + Send>> {
    let filename = format!("{}/workflows", base_dir);
    let path = Path::new(filename.as_str());
    if !Path::exists(path) {
        return Ok(vec![]); //no binaries
    }

    let mut ret = vec![];
    let workflows = get_dirs(path)?;
    for workflow in &workflows {
        let wf_dir = format!("{}/{}", &filename, workflow);
        let wf_path = format!("{}/workflow.yaml", &wf_dir);
        let wf = load_yaml::<Workflow>(&wf_path)?;
        ret.push((wf, wf_dir));
    }

    Ok(ret)
}

pub fn find_workflow_by_id(base_dir: &str, wf_id: &str) -> Result<Option<(Workflow, String)>, Box<dyn Error + Sync + Send>> {
    let workflows = get_all_workflows(base_dir)?;
    Ok(workflows.iter().find(|(wf, _dir)| wf.id.id.eq(wf_id)).cloned())
}

pub fn workflow_exists(base_dir: &str, wf_id: &str) -> Result<bool, Box<dyn Error + Sync + Send>> {
    let wf_ids = get_all_workflows(base_dir)?;
    for (wf, _) in &wf_ids {
        if wf.id.id == wf_id {
            return Ok(true);
        }
    }
    Ok(false)
}
