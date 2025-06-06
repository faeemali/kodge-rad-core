use std::error::Error;
use std::time::Duration;
use log::{debug};
use serde::{Deserialize, Serialize};
use crate::broker::protocol::States::{GetBody, GetCrc, GetFooter, GetHeader, GetMessageHeader};
use crate::utils::types::KVPair;
use crate::error::RadError;
use crate::utils::crc::{crc16, crc16_for_byte};
use crate::utils::timer::Timer;
use crate::utils::rad_utils::Validation;

/**
Message format is:
[0xAA]<message header><binary zero><binary data><crc>[0x55]
where:
    - message header is json data that conforms to the struct MessageHeader
    - binary zero is 0x00
    - binary data will then be read for <length> bytes, where length is
        specified in the header
    - crc is 16 bytes long, calculated with crc16. byte0 is bits 0-7, byte1 is bits-8-15.
        The crc is calculated for all data from <message_type> to <binary_data>
 */
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Message {
    pub header: MessageHeader,
    pub body: Vec<u8>,
}

impl Message {
    //creates a basic message instance without routing key information
    pub fn new(instance_id: &str, message_id: &str, msg_type: &str, body: &[u8]) -> Self {
        Message {
            header: MessageHeader {
                instance_id: instance_id.to_string(),
                rks: None,
                rks_match_type: None,
                message_id: message_id.to_string(),
                msg_type: msg_type.to_string(),
                extras: None,
                length: body.len() as u32,
            },
            body: body.to_vec(),
        }
    }

    //like new(), but expects body to be a serializable type
    pub fn new_from_type<T>(instance_id: &str, message_id: &str, msg_type: &str, body: T)
                            -> Result<Self, Box<dyn Error + Sync + Send>>
    where
        T: Serialize,
    {
        let body_bytes = serde_json::to_vec(&body)?;
        let msg = Message::new(instance_id, message_id, msg_type, &body_bytes);
        Ok(msg)
    }
}

pub const HEADER: u8 = 0xAA;
pub const FOOTER: u8 = 0x55;

const MAX_MSG_BODY_LENGTH: usize = 1048576; //1MB
const MAX_MSG_HEADER_LENGTH: usize = 10240 * 2;
const MSG_RX_TIMEOUT_MS: u64 = 10000;

pub struct Protocol {
    msg_buffer: Vec<u8>,
    header: Option<MessageHeader>,
    state: States,
    retrieved_crc: u32,
    calculated_crc: u16,
    timer: Timer,
}

/// All routing keys must be matched i.e. all routing keys in the message
/// and the routing keys in the router must be the same
pub const RK_MATCH_TYPE_ALL: &str = "all";

/// any of the routing keys in the message (i.e. just one) must match
/// the keys specified in the router
pub const RK_MATCH_TYPE_ANY: &str = "any";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MessageHeader {
    /// this must be a unique name for the app i.e. the instance
    /// id. It is important that the actual app name is not
    /// used. For instance, if an app is called "echo" and the "echo"
    /// app is used several times in a workflow, the router will not know which
    /// instance of "echo" to reference when routing messages. 
    pub instance_id: String,

    /// additional routing keys. May be empty
    pub rks: Option<Vec<String>>,

    /// how to match routing keys, if any are specified. Must be one of the
    /// RK_MATCH_TYPE_XXX constants
    pub rks_match_type: Option<String>,

    /// Used for synchronizing message requests and responses. Allows for comms
    /// to be synchronous
    pub message_id: String,

    pub msg_type: String,

    /// optional extras. For instance, a webserver may decide to include
    /// http headers here
    pub extras: Option<Vec<KVPair>>,

    pub length: u32,
}

#[derive(Eq, PartialEq)]
enum States {
    GetHeader,
    GetMessageHeader,
    GetBody,
    GetCrc,
    GetFooter,
}

impl Protocol {
    pub fn new() -> Result<Self, RadError> {
        Ok(Protocol {
            msg_buffer: Vec::with_capacity(10240),
            header: None,
            state: GetHeader,
            retrieved_crc: 0x00,
            calculated_crc: 0xFFFF,
            timer: Timer::new(Duration::from_millis(MSG_RX_TIMEOUT_MS)),
        })
    }

    fn partial_reset(&mut self) {
        self.msg_buffer.clear();
        self.timer.reset();
    }

    fn reset(&mut self) {
        self.partial_reset();
        self.header = None;
        self.timer.reset();
        self.calculated_crc = 0xFFFF;
        self.state = GetHeader;
    }

    pub fn feed(&mut self, data: &[u8]) -> Vec<Message> {
        let mut ret = vec![];
        for b in data {
            if self.state != GetHeader && self.timer.timed_out() {
                /* msg rx timeout */
                self.reset();
            } else {
                self.timer.reset();
            }


            match &self.state {
                GetHeader => {
                    if *b == HEADER {
                        self.reset();
                        self.state = GetMessageHeader;
                    }
                }

                GetMessageHeader => {
                    if *b == HEADER {
                        self.reset();
                        self.state = GetMessageHeader;
                    }

                    self.calculated_crc = crc16_for_byte(self.calculated_crc, *b);

                    if *b == 0x00 {
                        let msg_header_res: serde_json::error::Result<MessageHeader> = serde_json::from_slice(self.msg_buffer.as_slice());
                        if let Err(e) = msg_header_res {
                            debug!("Error processing message header: {}", &e);
                            self.reset();
                            continue;
                        }

                        let msg_header = msg_header_res.unwrap();
                        if !Validation::is_valid_name(&msg_header.instance_id) || !Validation::is_valid_msg_type(&msg_header.msg_type) {
                            debug!("Invalid message name or type (protocol)");
                            self.reset();
                            continue;
                        }

                        if msg_header.length > MAX_MSG_HEADER_LENGTH as u32 {
                            debug!("Invalid message length");
                            self.reset();
                            continue;
                        }

                        let body_len = msg_header.length;
                        self.header = Some(msg_header);

                        self.partial_reset();

                        if body_len == 0 {
                            self.state = GetCrc;
                        } else {
                            self.state = GetBody;
                        }
                        continue;
                    }

                    self.msg_buffer.push(*b);
                    if self.msg_buffer.len() >= MAX_MSG_BODY_LENGTH {
                        debug!("Overflow detected on message header. Ignoring");
                        self.reset();
                        continue;
                    }
                }

                GetBody => {
                    self.msg_buffer.push(*b);
                    self.calculated_crc = crc16_for_byte(self.calculated_crc, *b);

                    if self.msg_buffer.len() >= MAX_MSG_BODY_LENGTH {
                        debug!("Message body overflow detected");
                        self.reset();
                        continue;
                    }

                    if let Some(s) = &self.header {
                        if self.msg_buffer.len() == s.length as usize {
                            //do NOT reset the message buffer yet
                            self.state = GetCrc;
                            continue;
                        }
                    } else {
                        debug!("Unexpected error: message header is none while trying to retrieve body");
                        self.reset();
                        continue;
                    }
                }

                GetCrc => {
                    /* use the high byte as a counter */
                    if (self.retrieved_crc >> 24) == 0 {
                        //byte0
                        self.retrieved_crc = *b as u32;
                        self.retrieved_crc |= 1 << 24;
                    } else {
                        //byte1
                        self.retrieved_crc |= (*b as u32) << 8;
                        self.retrieved_crc &= 0x00FFFFFF;

                        if self.retrieved_crc != self.calculated_crc as u32 {
                            debug!("Ignoring message: invalid crc");
                            self.reset();
                            continue;
                        }

                        //do NOT reset the message buffer here
                        self.state = GetFooter;
                    }
                }

                GetFooter => {
                    if *b == FOOTER {
                        /* got a message */
                        match &self.header {
                            Some(h) => {
                                let msg = Message {
                                    header: h.clone(),
                                    body: self.msg_buffer[0..self.msg_buffer.len()].to_vec(),
                                };
                                ret.push(msg);
                            }
                            None => {
                                debug!("Unexpected error while processing footer. Header is None");
                                self.reset();
                                continue;
                            }
                        }
                    } else if *b == HEADER {
                        self.reset();
                        self.state = GetMessageHeader;
                    }
                }
            }
        }

        ret
    }

    pub fn format(msg: &Message) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let mut crc = 0xFFFFu16;

        let mut ret = vec![];
        ret.push(HEADER);

        let header = serde_json::to_vec(&msg.header)?;
        let header_slice = header.as_slice();
        ret.extend_from_slice(header_slice);
        crc = crc16(crc, header_slice, header_slice.len());

        ret.push(0x00);
        crc = crc16_for_byte(crc, 0x00);

        let body_slice = msg.body.as_slice();
        ret.extend_from_slice(body_slice);
        crc = crc16(crc, body_slice, body_slice.len());

        ret.push((crc & 0xFF) as u8);
        ret.push(((crc >> 8) & 0xFF) as u8);

        ret.push(FOOTER);
        Ok(ret)
    }
}