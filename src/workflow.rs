use std::error::Error;
use serde::{Deserialize, Serialize};
use crate::app::{App, AppIoDefinition, load_app};
use crate::AppCtx;
use crate::config::config_common::ConfigId;
use crate::utils::utils::{load_yaml};

#[derive(Serialize, Deserialize)]
pub struct Workflow {
    pub id: ConfigId,
    pub apps: Vec<String>,
    pub connections: Vec<OutIn>,
}

#[derive(Serialize, Deserialize)]
pub struct OutIn {
    #[serde(rename = "out")]
    pub output: String,
    #[serde(rename = "in")]
    pub input: String,
}

struct ConnectorHolder<'a> {
    pub app_name: String,
    pub definition: &'a AppIoDefinition,
    pub connected: bool, //has this connection been used?
}


impl Workflow {
    fn verify_apps(&self, wf_ctx: &WorkflowCtx) -> Result<(), Box<dyn Error>> {
        for app_name in &self.apps {
            println!("Found app: {}", app_name);
            let app = load_app(&wf_ctx.base_dir, app_name)?;
            app.verify()?;
        }
        Ok(())
    }

    fn verify(&self, wf_ctx: &WorkflowCtx) -> Result<(), Box<dyn Error>> {
        self.id.print();
        self.verify_apps(wf_ctx)?;
        Ok(())
    }

    fn __get_connectors<'a>(&self, app_id: &str, io_opt: &'a Option<Vec<AppIoDefinition>>)
                            -> Vec<ConnectorHolder<'a>> {
        let mut io_defs = vec![];
        return if let Some(ios) = io_opt {
            ios.iter().for_each(|i| {
                let holder = ConnectorHolder {
                    app_name: app_id.to_string(),
                    definition: i,
                    connected: false,
                };
                io_defs.push(holder);
            });

            io_defs
        } else {
            vec![]
        };
    }

    fn get_connectors_for_apps<'a>(&self, wf_ctx: &'a WorkflowCtx) -> Vec<ConnectorHolder<'a>> {
        let mut connectors: Vec<ConnectorHolder> = vec![];
        for app in &wf_ctx.apps {
            let mut input_connectors = self.__get_connectors(&app.id.id, &app.io.input);
            connectors.append(&mut input_connectors);

            let mut output_connectors = self.__get_connectors(&app.id.id, &app.io.output);
            connectors.append(&mut output_connectors);
        }

        connectors
    }

    /* find connections (in/out) and for each app, ensure all connectors are used */
    fn verify_connections(&self, wf_ctx: &WorkflowCtx) -> Result<(), Box<dyn Error>> {
        let connectors = self.get_connectors_for_apps(wf_ctx);
        Ok(())
    }
}

pub struct WorkflowCtx {
    pub base_dir: String,
    pub workflow: Workflow,
    pub apps: Vec<App>,
}

pub fn execute_workflow(app_ctx: &AppCtx, app_name: &str, args: &[String]) -> Result<(), Box<dyn Error>> {
    let filename = format!("{}/apps/{}/workflow.yaml", app_ctx.base_dir, app_name);
    let workflow: Workflow = load_yaml(&filename)?;

    let mut apps = vec![];
    for app_name in &workflow.apps {
        let app = load_app(&app_ctx.base_dir, app_name)?;
        apps.push(app);
    }

    let wf_ctx = WorkflowCtx {
        base_dir: app_ctx.base_dir.to_string(),
        workflow,
        apps,
    };

    wf_ctx.workflow.verify(&wf_ctx)?;
    wf_ctx.workflow.verify_connections(&wf_ctx)?;

    Ok(())
}


