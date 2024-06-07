use std::error::Error;
use serde::{Deserialize, Serialize};
use crate::app::{App, AppIoDefinition, load_app};
use crate::AppCtx;
use crate::config::config_common::ConfigId;
use crate::error::RadError;
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

#[derive(Copy, Clone, Eq, PartialEq)]
enum IoDirection {
    Out,
    In,
}

struct ConnectorHolder<'a> {
    pub app_id: String,
    pub definition: &'a AppIoDefinition,
    pub connected: bool, //has this connection been used?
    pub direction: IoDirection,
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

    fn __get_connectors<'a>(&self, app_id: &str, io_opt: &'a Option<Vec<AppIoDefinition>>, direction: IoDirection)
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

    fn get_connectors_for_apps<'a>(&self, wf_ctx: &'a WorkflowCtx) -> Vec<ConnectorHolder<'a>> {
        let mut connectors: Vec<ConnectorHolder> = vec![];
        for app in &wf_ctx.apps {
            let mut input_connectors = self.__get_connectors(&app.id.id,
                                                             &app.io.input,
                                                             IoDirection::In);
            connectors.append(&mut input_connectors);

            let mut output_connectors = self.__get_connectors(&app.id.id,
                                                              &app.io.output,
                                                              IoDirection::Out);
            connectors.append(&mut output_connectors);
        }

        connectors
    }

    /*
       expects a string of the form [app name]-connector, splits the string and returns
       "app name" and "connector"
     */
    fn get_app_and_connector(outin: &str) -> Result<(String, String), Box<dyn Error>> {
        let split: Vec<&str> = outin.split("-").collect();
        if split.len() != 2 {
            return Err(Box::new(RadError::from("Invalid outin string: badly formatted")));
        }

        if !split[0].starts_with("[") || !split[0].ends_with("]") || split[0].len() <= 2 {
            return Err(Box::new(RadError::from("Invalid app name specified in outin string: badly formatted")));
        }

        let app_name = &(split[0])[1..(split[0].len() - 1)];
        if app_name.len() != app_name.trim().len() {
            return Err(Box::new(RadError::from("Invalid app name specified in outin. Badly formatted (extra spaces)")));
        }

        let connector = split[1];
        if connector.len() != connector.trim().len() {
            return Err(Box::new(RadError::from("Invalid connector name in outin. Extra spaces")));
        }

        Ok((app_name.to_string(), connector.to_string()))
    }

    //verifies that an app and connector are matched to an application, and marks the
    //connector as "used" in the application
    fn verify_app_and_connector(&self,
                                connectors: &mut [ConnectorHolder],
                                app_id: &str,
                                connector_id: &str,
                                direction: IoDirection) -> Result<(), Box<dyn Error>> {
        for connector in connectors {
            if connector.app_id != app_id ||
                connector.direction != direction ||
                connector.connected ||
                connector.definition.id.id != connector_id {
                continue;
            }

            connector.connected = true;
            return Ok(())
        }

        Err(Box::from(RadError::from(format!("[{}]-{} not matched in any app", app_id, connector_id))))
    }

    /* find connections (in/out) and for each app, ensure all connectors are used */
    fn verify_connections(&self, wf_ctx: &WorkflowCtx) -> Result<(), Box<dyn Error>> {
        let mut connectors = self.get_connectors_for_apps(wf_ctx);

        for outin in &wf_ctx.workflow.connections {
            let (out_app, out_connector) = Self::get_app_and_connector(&outin.output)?;
            self.verify_app_and_connector(&mut connectors, &out_app, &out_connector, IoDirection::Out)?;

            let (in_app, in_connector) = Self::get_app_and_connector(&outin.input)?;
            self.verify_app_and_connector(&mut connectors, &in_app, &in_connector, IoDirection::In)?;
        }

        /* scan through the list of connectors for any unused items */
        for connector in &connectors {
            if !connector.connected {
                return Err(Box::new(RadError::from(format!("Found unused connector: [{}]-{}", connector.app_id, connector.definition.id.id))));
            }
        }

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
    println!("All app connections verified");

    Ok(())
}


