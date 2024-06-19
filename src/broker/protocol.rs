use std::error::Error;
use log::{error, warn};
use serde::{Deserialize, Serialize};
use crate::broker::protocol::States::{GetBody, GetHeader};
use crate::error::RadError;
use base64::prelude::*;

#[derive(Serialize, Deserialize)]
pub struct Message {
    pub msg_type: String,
    pub body: String,
}

pub const HEADER: u8 = 0xAA;
pub const FOOTER: u8 = 0x55;

const MAX_MSG_LENGTH: usize = 1048576; //1MB
const MAX_NAME_LENGTH: usize = 32;

pub struct Protocol {
    name: String, //max 32 bytes
    msg_type: String, //max 32 bytew
    msg_buffer: Vec<u8>,
    state: States,
}

enum States {
    GetHeader,
    GetBody,

}

impl Protocol {
    pub fn new(name: String, msg_type: String) -> Result<Self, RadError> {
        if name.len() >  MAX_NAME_LENGTH {
            let partial_name = &name[0..MAX_NAME_LENGTH];
            let msg = format!("Unable to create protocol decoder. Name length too long: {}...", partial_name);
            error!("{}", &msg);
            return Err(RadError::from(msg));
        }

        if msg_type.len() >  MAX_NAME_LENGTH {
            let partial_name = &msg_type[0..MAX_NAME_LENGTH];
            let msg = format!("Unable to create protocol decoder. msg_type length too long: {}...", partial_name);
            error!("{}", &msg);
            return Err(RadError::from(msg));
        }

        Ok(Protocol {
            name,
            msg_type,
            msg_buffer: Vec::with_capacity(10240),
            state: GetHeader,
        })
    }

    fn reset(&mut self) {
        self.state = GetHeader;
        self.msg_buffer.clear();
    }

    pub fn feed(&mut self, data: &[u8]) -> Vec<Message> {
        let mut ret = vec![];
        for b in data {
            match self.state {
                GetHeader => {
                    if *b == HEADER {
                        self.state = GetBody;
                    }
                }

                GetBody => {
                    if *b == HEADER {
                        /* stay here */
                        self.reset();
                        self.state = GetBody;
                        continue;
                    } else if *b == FOOTER {
                        let encoded_body = BASE64_STANDARD.encode(&self.msg_buffer);

                        /* got a message */
                        let msg = Message {
                            msg_type: self.msg_type.to_string(),
                            body: encoded_body,
                        };
                        ret.push(msg);

                        self.reset();
                        continue;
                    }

                    /* collect bytes */
                    self.msg_buffer.push(*b);

                    if self.msg_buffer.len() >= MAX_MSG_LENGTH {
                        warn!("{}: Message discarded. Overflow", &self.name);
                        self.reset();
                        continue;
                    }
                }
            }
        }

        ret
    }
}