use std::collections::HashMap;
use std::error::Error;
use std::ops::{Deref, DerefMut};
use std::process::Stdio;
use std::rc::Rc;
use std::sync::Arc;
use futures::SinkExt;

use serde::{Deserialize, Serialize};
use tokio::process::{Child, Command};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;

use crate::app::{App, AppIoDefinition, load_app, STDERR, STDIN, STDOUT};
use crate::AppCtx;
use crate::config::config_common::ConfigId;
use crate::error::RadError;
use crate::utils::utils::load_yaml;

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
pub enum IoDirection {
    Out,
    In,
}

struct ConnectorHolder<'a> {
    pub app_id: String,
    pub definition: &'a AppIoDefinition,
    pub connected: bool, //has this connection been used?
    pub direction: IoDirection,
}

pub enum MessageTypes {
    Process,
    Kill,
}

//for messages sent across channels
pub struct Message {
    pub msg_type: MessageTypes,
    pub data: Vec<u8>,
}

//contains a list of connectors, and the channels that must map to them
struct ConnectorChannel{
    pub app: App,
    pub direction: IoDirection,
    pub connector: AppIoDefinition,

    //either rx or tx must be used, never both
    pub rx: Option<Receiver<Message>>,
    pub tx: Option<Sender<Message>>,
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
}

pub struct WorkflowCtx {
    pub base_dir: String,
    pub workflow: Workflow,
}

//creates channels between all outputs and inputs from the workflow
fn create_channel_io(apps: Arc<Vec<App>>, connections: &[OutIn]) -> Result<HashMap<String, ConnectorChannel>, Box<dyn Error>> {
    let mut map = HashMap::new();
    for connection in connections {
        let (out_app_id, out_conn_id) = get_app_and_connector(&connection.output)?;
        let (in_app_id, in_conn_id) = get_app_and_connector(&connection.input)?;

        let out_app = match find_app(apps.clone(), &out_app_id) {
            Some(a) => {a},
            None => {
                return Err(Box::new(RadError::from(format!("App {} not found (out) (Unexpected)", &out_app_id))));
            }
        };

        let in_app = match find_app(apps.clone(),  &in_app_id) {
            Some(a) => {a},
            None => {
                return Err(Box::new(RadError::from(format!("App {} not found (in) (Unexpected)", &in_app_id))));
            }
        };

        let out_connector = match find_connector_in_app(&out_conn_id, &out_app) {
            Some(c) => c,
            None => {
                return Err(Box::new(RadError::from(format!("Connector {} not found in app {}. Unexpected", &out_conn_id, &out_app_id))));
            }
        };

        let in_connector = match find_connector_in_app(&in_conn_id, &in_app) {
            Some(c) => c,
            None => {
                return Err(Box::new(RadError::from(format!("Connector {} not found in app {}. Unexpected", &in_conn_id, &in_app_id))));
            }
        };

        /* we can create a channel between in and out */
        let (tx, rx) = channel(32);

        /* we read from the output, and write to the channel */
        let out_connector = ConnectorChannel {
            app: out_app.clone(),
            direction: IoDirection::Out,
            connector: out_connector.clone(),
            rx: None,
            tx: Some(tx),
        };

        /* we write to the input, which then does something with the data */
        let in_connector = ConnectorChannel {
            app: in_app.clone(),
            direction: IoDirection::In,
            connector: in_connector.clone(),
            rx: Some(rx),
            tx: None,
        };

        if map.contains_key(&connection.output) {
            return Err(Box::new(RadError::from(format!("Duplicate connection found: {}", &connection.output))));
        }

        if map.contains_key(&connection.input) {
            return Err(Box::new(RadError::from(format!("Duplication connection found: {}", &connection.input))));
        }

        map.insert(connection.output.to_string(), out_connector);
        map.insert(connection.input.to_string(), in_connector);
    }

    Ok(map)
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

fn find_app(a_apps: Arc<Vec<App>>, app_id: &str) -> Option<App> {
    let apps = a_apps.deref();
    for app in apps {
        if app.id.id == app_id {
            let a = app.clone();
            return Some(a);
        }
    }
    None
}

/*
    removes the items from the applied map, and adds them to a new map, aranged by app.
    This is a destruction operation
*/
pub fn convert_connector_map_to_sort_by_app(a_connector_map: Arc<HashMap<String, ConnectorChannel>>) -> HashMap<App, Vec<ConnectorChannel>> {
    let mut app_map = HashMap::new();

    let mut keys = vec![];
    a_connector_map.keys().for_each(|k| keys.push(k.to_string()));

    let cm = a_connector_map;
    for key in keys {
        let v = cm.remove(&key).unwrap();

        if !app_map.contains_key(&v.app) {
            app_map.insert(v.app.clone(), vec![v]);
        } else {
            let list_opt = app_map.get_mut(&v.app.clone()).unwrap();
            list_opt.push(v);
        }
    }

    app_map
}

fn __get_connectors<'a>(app_id: &str, io_opt: &'a Option<Vec<AppIoDefinition>>, direction: IoDirection)
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

fn get_connectors_for_apps<'a>(apps: &'a [App]) -> Vec<ConnectorHolder<'a>> {
    let mut connectors: Vec<ConnectorHolder> = vec![];
    for app in apps {
        let mut input_connectors = __get_connectors(&app.id.id,
                                                         &app.io.input,
                                                         IoDirection::In);
        connectors.append(&mut input_connectors);

        let mut output_connectors = __get_connectors(&app.id.id,
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
fn verify_app_and_connector(connectors: &mut [ConnectorHolder],
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
        return Ok(());
    }

    Err(Box::from(RadError::from(format!("[{}]-{} not matched in any app", app_id, connector_id))))
}

/* find connections (in/out) and for each app, ensure all connectors are used */
fn verify_connections(wf_ctx: &WorkflowCtx, a_apps: Arc<Vec<App>>) -> Result<(), Box<dyn Error>> {
    let mut connectors = get_connectors_for_apps(a_apps.deref());

    for outin in &wf_ctx.workflow.connections {
        let (out_app, out_connector) = get_app_and_connector(&outin.output)?;
        verify_app_and_connector(&mut connectors, &out_app, &out_connector, IoDirection::Out)?;

        let (in_app, in_connector) = get_app_and_connector(&outin.input)?;
        verify_app_and_connector(&mut connectors, &in_app, &in_connector, IoDirection::In)?;
    }

    /* scan through the list of connectors for any unused items */
    for connector in &connectors {
        if !connector.connected {
            return Err(Box::new(RadError::from(format!("Found unused connector: [{}]-{}", connector.app_id, connector.definition.id.id))));
        }
    }

    Ok(())
}


/* checks the list of connectors to see if we must grab stdin, stdout, stderr */
fn must_grab_stdin_out_err(connector: &Vec<&ConnectorChannel>) -> (bool, bool, bool) {
    let mut stdin = false;
    let mut stdout = false;
    let mut stderr = false;
    for cc in connector {
        match cc.connector.integration.integration_type.as_str() {
            STDIN => {
                stdin = true;
            }
            STDOUT => {
                stdout = true;
            }
            STDERR => {
                stderr = true;
            }
            _ => {
                continue;
            }
        }
    }

    (stdin, stdout, stderr)
}

async fn monitor_child<'a>(child: &'a Child, app: &'a App, connectors: Vec<&'a ConnectorChannel>) {

}

async fn run_app<'a>(app: &'a App, connectors: Vec<&'a ConnectorChannel>) -> Result<(), Box<dyn Error + Sync + Send>> {
    let (stdin, stdout, stderr) = must_grab_stdin_out_err(&connectors);
    let mut cmd = Command::new(&app.execution.cmd);
    let mut process = cmd.kill_on_drop(true);

    if let Some(args) = &app.execution.args {
        process = process.args(args);
    }

    if stdin {
        process = process.stdin(Stdio::piped());
    }

    if stdout {
        process = process.stdout(Stdio::piped());
    }

    if stderr {
        process = process.stderr(Stdio::piped());
    }

    let child = process.spawn()?;

    monitor_child(&child, app, connectors).await;

    Ok(())
}

fn run_apps(app_map: &'static HashMap<&'static App, Vec<&'static ConnectorChannel>>) {
    // for (app, connectors) in &app_map {
    //     let app_clone = app.clone();
    //     let connectors_clone = connectors.clone();
    //     tokio::spawn(run_app(app_clone, connectors_clone));
    // }
}

struct ExecutionCtx {
    pub apps: Vec<App>,
    pub connector_map: Arc<HashMap<String, ConnectorChannel>>,
    pub app_map: HashMap<App, Vec<ConnectorChannel>>
}

pub fn execute_workflow(app_ctx: &AppCtx, app_name: &str, args: &[String]) -> Result<(), Box<dyn Error>> {
    let filename = format!("{}/apps/{}/workflow.yaml", app_ctx.base_dir, app_name);
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
    verify_connections(&wf_ctx, a_apps.clone())?;
    println!("All app connections verified");

    let connector_map = create_channel_io(&apps, &wf_ctx.workflow.connections)?;
    let a_connector_map = Arc::new(connector_map);

    let app_map = convert_connector_map_to_sort_by_app(a_connector_map.clone());

    let exec_ctx = ExecutionCtx {
        connector_map: a_connector_map.clone(),
        apps,
        app_map,
    };

    /* start each app in the map */
    //run_apps(&app_map);

    Ok(())
}


