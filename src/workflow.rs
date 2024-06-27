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
use crate::app::{App, load_app};
use crate::broker::broker::{broker_main};
use crate::config::config_common::ConfigId;
use crate::process::run_app_main;
use crate::utils::utils::{get_dirs, load_yaml};

#[derive(Serialize, Deserialize, Clone)]
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

    fn verify(&self, wf_ctx: &WorkflowCtx) -> Result<(), Box<dyn Error>> {
        info!("Workflow: ");
        self.id.print();

        info!("verifying apps");
        self.verify_apps(wf_ctx)?;

        Ok(())
    }
}

pub struct WorkflowCtx {
    pub base_dir: String,
    pub workflow: Workflow,
    pub apps: Arc<Vec<App>>,
    pub must_die: Arc<Mutex<bool>>,
}

impl WorkflowCtx {
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

    let mut wf_ctx = WorkflowCtx {
        base_dir: app_ctx.base_dir.to_string(),
        apps: a_apps,
        workflow,
        must_die: app_ctx.must_die.clone(),
    };

    wf_ctx.workflow.verify(&wf_ctx)?;

    /* start each app in the map */
    wf_ctx.run_apps().await;

    Ok(())
}


pub fn get_all_workflows(base_dir: &str) -> Result<Vec<ConfigId>, Box<dyn Error>> {
    let filename = format!("{}/workflows", base_dir);
    let path = Path::new(filename.as_str());
    if !Path::exists(path) {
        return Ok(vec![]); //no apps
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
