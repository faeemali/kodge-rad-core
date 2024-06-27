use std::error::Error;
use std::ops::{Deref};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use log::{info, warn};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::time::sleep;

use crate::{AppCtx};
use crate::bin::{Bin, load_bin};
use crate::broker::broker::{broker_main};
use crate::config::config_common::ConfigId;
use crate::process::run_bin_main;
use crate::utils::utils::{get_dirs, load_yaml};

#[derive(Serialize, Deserialize, Clone)]
pub struct Workflow {
    pub id: ConfigId,
    pub bins: Vec<String>,
}

impl Workflow {
    fn verify_bins(&self, wf_ctx: &WorkflowCtx) -> Result<(), Box<dyn Error>> {
        for bin_name in &self.bins {
            info!("Found binary: {}", bin_name);
            let bin = load_bin(&wf_ctx.base_dir, bin_name)?;
            bin.verify()?;
        }
        Ok(())
    }

    fn verify(&self, wf_ctx: &WorkflowCtx) -> Result<(), Box<dyn Error>> {
        info!("Workflow: ");
        self.id.print();

        info!("verifying binaries");
        self.verify_bins(wf_ctx)?;

        Ok(())
    }
}

pub struct WorkflowCtx {
    pub base_dir: String,
    pub workflow: Workflow,
    pub bins: Arc<Vec<Bin>>,
    pub must_die: Arc<Mutex<bool>>,
}

impl WorkflowCtx {
    /* checks the list of connectors to see if we must grab stdin, stdout, stderr */
    pub async fn run_bins(&mut self) {
        let bins = self.bins.deref();
        for bin in bins {
            tokio::spawn(run_bin_main(self.base_dir.to_string(),
                                      bin.clone(),
                                      self.must_die.clone()));
        }

        self.monitor_bins().await;
        warn!("bins terminated");
    }

    async fn must_die(&self) -> bool {
        *self.must_die.lock().await
    }

    async fn monitor_bins(&self) {
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

    let mut bins = vec![];
    for bin_name in &workflow.bins {
        let bin = load_bin(&app_ctx.base_dir, bin_name)?;
        bins.push(bin);
    }
    let a_bins = Arc::new(bins);

    let mut wf_ctx = WorkflowCtx {
        base_dir: app_ctx.base_dir.to_string(),
        bins: a_bins,
        workflow,
        must_die: app_ctx.must_die.clone(),
    };

    wf_ctx.workflow.verify(&wf_ctx)?;

    /* start each binary in the map */
    wf_ctx.run_bins().await;

    Ok(())
}


pub fn get_all_workflows(base_dir: &str) -> Result<Vec<ConfigId>, Box<dyn Error>> {
    let filename = format!("{}/workflows", base_dir);
    let path = Path::new(filename.as_str());
    if !Path::exists(path) {
        return Ok(vec![]); //no binaries
    }

    let mut ret = vec![];
    let workflows = get_dirs(path)?;
    for workflow in &workflows {
        let wf_path = format!("{}/{}/workflow.yaml", &filename, workflow);
        let wf = load_yaml::<Workflow>(&wf_path)?;
        ret.push(wf.id.clone());
    }

    Ok(ret)
}

pub fn workflow_exists(base_dir: &str, wf_id: &str) -> Result<bool, Box<dyn Error>> {
    let wf_ids = get_all_workflows(base_dir)?;
    for id in &wf_ids {
        if id.id == wf_id {
            return Ok(true);
        }
    }
    Ok(false)
}
