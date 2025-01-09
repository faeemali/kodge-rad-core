use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct KVPair {
    pub key: String,
    pub value: String,
}