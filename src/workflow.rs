use std::collections::HashMap;
use std::error::Error;
use std::ops::{Deref};
use std::sync::Arc;
use std::time::Duration;
use log::{info, warn};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{channel, Receiver, Sender, unbounded_channel};
use tokio::sync::Mutex;
use tokio::time::sleep;

use crate::{AppCtx};
use crate::app::{App, AppIoDefinition, load_app, STDERR, STDIN, STDOUT};
use crate::config::config_common::ConfigId;
use crate::error::RadError;
use crate::process::run_app_main;
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

//contains a list of connectors, and the channels that must map to them
pub struct ConnectorChannel {
    pub app: App,
    pub direction: AppIoDirection,
    pub connector: AppIoDefinition,

    //either rx or tx must be used, never both
    pub rx: Option<Receiver<Vec<u8>>>,
    pub tx: Option<Sender<Vec<u8>>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StdioInOut {
    #[serde(rename = "in")]
    pub input: Option<String>,

    #[serde(rename = "out")]
    pub output: Option<String>,

    #[serde(rename = "err")]
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct Workflow {
    pub id: ConfigId,
    pub apps: Vec<String>,
    pub connections: Vec<OutIn>,
    pub stdio: StdioInOut,
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
                    return Err(Box::new(RadError::from(format!("stdin already used by app {}. Cannot assign io", &app.id.id))));
                }
            }
        }

        Ok(())
    }

    fn __verify_stdout_err(&self, wf_ctx: &WorkflowCtx, in_name: &str, out: bool) -> Result<(), Box<dyn Error>> {
        let app = load_app(&wf_ctx.base_dir, in_name)?;

        /* ensure this app does not have anything connected to its input */
        if let Some(outputs) = app.io.output {
            for output in &outputs {
                if out {
                    if output.integration.integration_type == STDOUT {
                        return Err(Box::new(RadError::from(format!("stdout already used by app {}. Cannot assign io", &app.id.id))));
                    }
                } else if output.integration.integration_type == STDERR {
                    return Err(Box::new(RadError::from(format!("stderr already used by app {}. Cannot assign io", &app.id.id))));
                }
            }
        }

        Ok(())
    }

    fn verify_stdio(&self, wf_ctx: &WorkflowCtx) -> Result<(), Box<dyn Error>> {
        if let Some(name) = &self.stdio.input {
            self.__verify_stdin(wf_ctx, name)?;
        }

        if let Some(name) = &self.stdio.output {
            self.__verify_stdout_err(wf_ctx, name, true)?;
        }

        if let Some(name) = &self.stdio.error {
            self.__verify_stdout_err(wf_ctx, name, false)?;
        }

        Ok(())
    }

    fn verify(&self, wf_ctx: &WorkflowCtx) -> Result<(), Box<dyn Error>> {
        self.id.print();

        info!("verifying apps");
        self.verify_apps(wf_ctx)?;

        info!("verifying stdio");
        self.verify_stdio(wf_ctx)?;
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

fn __find_app(apps: Arc<Vec<App>>, app_id: &str, out: bool) -> Result<App, Box<dyn Error>> {
    match find_app(apps, &app_id) {
        Some(a) => {
            Ok(a)
        }

        None => {
            let out_or_in = if out {
                "out"
            } else {
                "in"
            };
            Err(Box::new(RadError::from(format!("App {} not found ({}) (Unexpected)", &app_id, out_or_in))))
        }
    }
}

//creates channels between all outputs and inputs from the workflow
fn create_channel_io(apps: Arc<Vec<App>>, connections: &[OutIn]) -> Result<HashMap<String, ConnectorChannel>, Box<dyn Error>> {
    let mut map = HashMap::new();
    for connection in connections {
        let (out_app_id, out_conn_id) = get_app_and_connector(&connection.output)?;
        let (in_app_id, in_conn_id) = get_app_and_connector(&connection.input)?;

        let out_app = __find_app(apps.clone(), &out_app_id, true)?;
        let in_app = __find_app(apps.clone(), &in_app_id, false)?;

        let out_connector = __find_connector(&out_conn_id, &out_app)?;
        let in_connector = __find_connector(&in_conn_id, &in_app)?;

        /* we can create a channel between in and out */
        let (tx, rx) = channel(1);

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
    removes the items from the supplied map, and adds them to a new map, arranged by app.
    This is a destructive operation
*/
async fn convert_connector_map_to_sort_by_app(connector_map: HashMap<String, ConnectorChannel>) -> HashMap<App, Vec<ConnectorChannel>> {
    let mut app_map = HashMap::new();

    let mut keys = vec![];
    connector_map.keys().for_each(|k| keys.push(k.to_string()));

    let mut cm = connector_map;
    for key in &keys {
        let v = cm.remove(key).unwrap();

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

/* used to link some app's stdin/out/err with the main app's stdin/out/err */
#[derive(Clone, Copy)]
pub struct StdioHolder {
    pub input: bool,
    pub output: bool,
    pub error: bool,
}

struct ExecutionCtx {
    pub base_dir: String,
    pub apps: Arc<Vec<App>>,
    pub app_map: HashMap<App, Vec<ConnectorChannel>>,

    pub stdio: StdioInOut,

    //if true, the workflow must die. Not using AtomicBool because of a note that says
    //it's only supported on certain platforms
    pub must_die: Arc<Mutex<bool>>,
}

impl ExecutionCtx {
    /* checks the list of connectors to see if we must grab stdin, stdout, stderr */
    pub async fn run_apps(&mut self) {
        let app_map = &mut self.app_map;
        for (app, connectors) in app_map.drain() {
            let mut use_stdio = StdioHolder {
                input: false,
                output: false,
                error: false,
            };

            /*
                note, checks should already have been performed to ensure stdin/out/err
                are free for use at the verify stage
             */

            if let Some(app_in) = &self.stdio.input {
                if app_in == &app.id.id {
                    /* we need to connect the main app's stdin to this app */
                    info!("Using terminal stdin for app {}", app_in);
                   use_stdio.input = true;
                }
            }

            if let Some(app_out) = &self.stdio.output {
                if app_out == &app.id.id {
                    /* connect this app's stdout to the main app */
                    info!("Using terminal stdout for app {}", app_out);
                    use_stdio.output = true;
                }
            }

            if let Some(app_err) = &self.stdio.error {
                if app_err == &app.id.id {
                    /* connect this app's stderr to the main app */
                    info!("Using terminal stderr for app {}", app_err);
                    use_stdio.error = true;
                }
            }

            tokio::spawn(run_app_main(self.base_dir.to_string(),
                                      app,
                                      connectors,
                                      use_stdio,
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
    info!("All app connections verified");

    let connector_map = create_channel_io(a_apps.clone(), &wf_ctx.workflow.connections)?;
    let app_map = convert_connector_map_to_sort_by_app(connector_map).await;

    let mut exec_ctx = ExecutionCtx {
        base_dir: app_ctx.base_dir.to_string(),
        apps: a_apps,
        app_map,
        stdio: wf_ctx.workflow.stdio,
        must_die: app_ctx.must_die.clone(),
    };

    /* start each app in the map */
    exec_ctx.run_apps().await;

    Ok(())
}


