use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct KVPair {
    pub key: String,
    pub value: String,
}