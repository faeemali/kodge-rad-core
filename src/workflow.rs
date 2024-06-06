use std::error::Error;
use serde::{Deserialize, Serialize};
use crate::app::load_app;
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

impl Workflow {
    fn verify_apps(&self, app_ctx: &AppCtx) -> Result<(), Box<dyn Error>>{
        for app_name in &self.apps {
            println!("Found app: {}", app_name);
            let app = load_app(app_ctx, app_name)?;
            app.verify()?;
        }
        Ok(())
    }

    fn verify(&self, app_ctx: &AppCtx) -> Result<(), Box<dyn Error>> {
        self.id.print();
        self.verify_apps(app_ctx)?;

        Ok(())
    }

}

pub fn execute_workflow(app_ctx: &AppCtx, app_name: &str, args: &[String]) -> Result<(), Box<dyn Error>> {
    let filename = format!("{}/apps/{}/workflow.yaml", app_ctx.base_dir, app_name);
    let workflow: Workflow = load_yaml(&filename)?;
    workflow.verify(app_ctx)?;
    Ok(())
}


