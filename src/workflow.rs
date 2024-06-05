use std::error::Error;
use std::fs::File;
use std::path::Path;
use crate::AppCtx;
use crate::config::workflow::Workflow;
use crate::utils::utils::get_value_or_unknown;

pub fn execute_workflow(app_ctx: &AppCtx, app_name: &str, args: &[String]) -> Result<(), Box<dyn Error>> {
    let workflow = load_workflow(app_ctx, app_name)?;
    verify_workflow(app_ctx, &workflow)?;
    Ok(())
}

fn verify_workflow(app_ctx: &AppCtx, workflow: &Workflow) -> Result<(), Box<dyn Error>> {
    let name = get_value_or_unknown(&workflow.id.name);
    let description = get_value_or_unknown(&workflow.id.description);
    
    println!(r#"Running:
    id:          {}
    name:        {}
    description: {}"#, &workflow.id.id, &name, &description);
    
    Ok(())
}

fn load_workflow(app_ctx: &AppCtx, app_name: &str) -> Result<Workflow, Box<dyn Error>>{
    let filename = format!("{}/apps/{}/workflow.yaml", &app_ctx.base_dir, app_name);
    let path = Path::new(filename.as_str());
    let f = File::open(path)?;
    let workflow: Workflow = serde_yaml::from_reader(&f)?;
    Ok(workflow)
}