use std::error::Error;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use crate::error::RadError;

/// Run (i.e. start) an app once only. App exit means container
/// failure. The app may only exit cleanly if signaled to do so
/// by the container. The app is expected to be long-running app
pub const RUN_TYPE_ONCE: &str = "once";

/// Run the app, which is expected to be long-running. If
/// the app dies, restart it
pub const RUN_TYPE_REPEATED: &str = "repeated";

/// The app is not long-running. It must be started for each
/// message, and it will then process the data and exit. It
/// must be restarted for the next message, etc.
pub const RUN_TYPE_START_STOP: &str = "start-stop";


#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct OnceOptions {
}

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct RepeatedOptions {
    pub restart_delay: u64,
}

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct StartStopOptions {
    /// messages for this app will be read from the can and passed
    /// to stdin
    pub redirect_msgs_to_stdin: bool,

    /// data from stdout will be converted to a message and passed
    /// to a chan
    pub redirect_stdout_to_msgs: bool,

    /// if redirect_stdout_to_msgs is true, this must contain
    /// the message type for the stdout message
    pub stdout_msg_type: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum ContainerOptions {
    OnceOptions(OnceOptions),
    RepeatedOptions(RepeatedOptions),
    StartStopOptions(StartStopOptions),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BinOptions {
    /// name of the binary, as specified in the cache directory
    /// i.e. from the id.id field
    pub name: String,

    /// optional args
    pub args: Option<Vec<String>>,

    /// if the optional args are specified, this determines
    /// if the args must be appended to the args specified in
    /// the config (as specified in the cache), or if these
    /// args must override the args specified in the cache.
    /// Defaults to false if not specified.
    pub append_args: Option<bool>,

    /// if specified, this overrides the settings in the
    /// config file for the app (from the cache dir)
    pub working_dir: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Container {
    pub run_type: String,
    pub options: ContainerOptions,
    pub bin: BinOptions,
}

impl Container {
    pub fn verify(&self) -> Result<(), Box<dyn Error + Sync + Send>> {
        match self.run_type.as_str() {
            RUN_TYPE_ONCE => {
                if let ContainerOptions::OnceOptions(_) = self.options {
                    Ok(())
                } else {
                    Err(Box::new(RadError::from("Invalid options specified for once type")))
                }
            }
            RUN_TYPE_REPEATED => {
                if let ContainerOptions::RepeatedOptions(_) = self.options {
                    Ok(())
                } else {
                    Err(Box::new(RadError::from("Invalid options specified for repeated type")))
                }
            }
            
            RUN_TYPE_START_STOP => {
                if let ContainerOptions::StartStopOptions(_) = self.options {
                    Ok(())
                } else {
                    Err(Box::new(RadError::from("Invalid options specified for start_stop type")))
                }
            }
            
            _ => {
                Err(Box::new(RadError::from(format!("Invalid run type specified for container. Type: {}", &self.run_type))))
            }
        }           
    }
}

pub async fn run_container_main(base_dir: String, container: Container, must_die: Arc<Mutex<bool>>) {

}