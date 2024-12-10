use serde::{Deserialize, Serialize};
use crate::config::config_common::{ConfigId};

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct BinExecution {
    pub cmd: String,
    pub args: Option<Vec<String>>,
    pub working_dir: Option<String>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct BinConfig {
    pub id: ConfigId,
    pub execution: BinExecution,
}



