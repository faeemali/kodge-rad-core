use log::info;
use serde::{Deserialize, Serialize};
use crate::utils::utils::get_value_or_unknown;

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct ConfigId {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
}

impl ConfigId {
    pub fn print(&self) {
        let name = get_value_or_unknown(&self.name);
        let description = get_value_or_unknown(&self.description);

        info!(r#"
    ID->
    id:          {}
    name:        {}
    description: {}"#, &self.id, &name, &description);
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub struct KVPair {
    pub key: String,
    pub value: String,
}