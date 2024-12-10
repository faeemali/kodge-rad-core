use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct ConfigId {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct KVPair {
    pub key: String,
    pub value: String,
}