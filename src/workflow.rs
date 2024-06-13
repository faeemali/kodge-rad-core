use std::collections::HashMap;
use std::error::Error;
use std::ops::{Deref, DerefMut};
use std::process::ExitStatus;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::{select, try_join};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::{AppCtx, process};
use crate::app::{App, AppIoDefinition, load_app};
use crate::config::config_common::ConfigId;
use crate::error::RadError;
use crate::utils::utils::load_yaml;

#[derive(Serialize, Deserialize)]
pub struct OutIn {
    #[serde(rename = "out")]
    pub output: String,
    #[serde(rename = "in")]
    pub input: String,
}

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

#[derive(Eq, PartialEq, Hash, Clone)]
pub enum MessageTypes {
    Process,
    Kill,
}

//for messages sent across channels
#[derive(Eq, PartialEq, Hash, Clone)]
pub struct Message {
    pub msg_type: MessageTypes,
    pub data: Vec<u8>,
}

//contains a list of connectors, and the channels that must map to them
pub struct ConnectorChannel {
    pub app: App,
    pub direction: AppIoDirection,
    pub connector: AppIoDefinition,

    //either rx or tx must be used, never both
    pub rx: Option<Receiver<Message>>,
    pub tx: Option<Sender<Message>>,
}

#[derive(Serialize, Deserialize)]
pub struct Workflow {
    pub id: ConfigId,
    pub apps: Vec<String>,
    pub connections: Vec<OutIn>,
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
            Some(a) => { a }
            None => {
                return Err(Box::new(RadError::from(format!("App {} not found (out) (Unexpected)", &out_app_id))));
            }
        };

        let in_app = match find_app(apps.clone(), &in_app_id) {
            Some(a) => { a }
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

        /* we read from the output (eg. stdout), and write to the channel */
        let out_connector = ConnectorChannel {
            app: out_app.clone(),
            direction: AppIoDirection::Out,
            connector: out_connector.clone(),
            rx: None,
            tx: Some(tx),
        };

        /* we read from the receiver and write to the input (stdin), which then does something with the data */
        let in_connector = ConnectorChannel {
            app: in_app.clone(),
            direction: AppIoDirection::In,
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
async fn convert_connector_map_to_sort_by_app(am_connector_map: Arc<Mutex<HashMap<String, ConnectorChannel>>>) -> HashMap<App, Vec<ConnectorChannel>> {
    let mut connector_map_mg = am_connector_map.lock().await;
    let connector_map = connector_map_mg.deref_mut();

    let mut app_map = HashMap::new();

    let mut keys = vec![];
    connector_map.keys().for_each(|k| keys.push(k.to_string()));

    let cm = connector_map;
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
                            direction: AppIoDirection) -> Result<(), Box<dyn Error>> {
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
        verify_app_and_connector(&mut connectors, &out_app, &out_connector, AppIoDirection::Out)?;

        let (in_app, in_connector) = get_app_and_connector(&outin.input)?;
        verify_app_and_connector(&mut connectors, &in_app, &in_connector, AppIoDirection::In)?;
    }

    /* scan through the list of connectors for any unused items */
    for connector in &connectors {
        if !connector.connected {
            return Err(Box::new(RadError::from(format!("Found unused connector: [{}]-{}", connector.app_id, connector.definition.id.id))));
        }
    }

    Ok(())
}

struct ExecutionCtx {
    pub base_dir: String,
    pub apps: Arc<Vec<App>>,
    pub app_map: HashMap<App, Vec<ConnectorChannel>>,
    
    //if true, the workflow must die. Not using AtomicBool because of a note that says
    //it's only supported on certain platforms
    pub must_die: Arc<Mutex<bool>>,
}

impl ExecutionCtx {
    /* checks the list of connectors to see if we must grab stdin, stdout, stderr */
    pub async fn run_apps(&mut self) {

        let app_map = &mut self.app_map;
        for (key, value) in app_map.drain() {   
            tokio::spawn(process::run_app_main(self.base_dir.to_string(), key, value, self.must_die.clone()));
        }

        self.monitor_apps().await;
        
        println!("apps terminated");
    }
    
    async fn must_die(&self) -> bool {
        *self.must_die.lock().await
    }

    async fn monitor_apps(&self) {
        loop {
            println!("Workflow running at: {}", chrono::Local::now().to_rfc3339());
            
            if self.must_die().await {
                println!("Workflow detected must_die flag. Aborting after 1s");
                sleep(Duration::from_millis(1000)).await;
                break;
            }
            
            sleep(Duration::from_millis(1000)).await;
        }
    }
}

pub async fn execute_workflow(app_ctx: &AppCtx, app_name: &str, args: &[String]) -> Result<(), Box<dyn Error>> {
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

    let connector_map = create_channel_io(a_apps.clone(), &wf_ctx.workflow.connections)?;
    let am_connector_map = Arc::new(Mutex::new(connector_map));

    let app_map = convert_connector_map_to_sort_by_app(am_connector_map.clone()).await;

    let mut exec_ctx = ExecutionCtx {
        base_dir: app_ctx.base_dir.to_string(),
        apps: a_apps,
        app_map,
        must_die: Arc::new(Mutex::new(false)),
    };

    /* start each app in the map */
    exec_ctx.run_apps().await;

    Ok(())
}


