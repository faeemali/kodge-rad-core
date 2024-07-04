use std::error::Error;
use crate::broker::auth_types::AuthMessageReq;

pub async fn authenticate(msg: &AuthMessageReq) -> Result<bool, Box<dyn Error + Sync + Send>> {
    /* TODO: do something here */
    Ok(true)
}