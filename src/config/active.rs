use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Active {
    pub active: Vec<String>,
}