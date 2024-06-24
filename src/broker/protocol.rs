use std::error::Error;
use std::time::Duration;
use log::{debug, error, warn};
use serde::{Deserialize, Serialize};
use crate::broker::protocol::States::{GetBody, GetFooter, GetHeader, GetLength, GetMessageType, GetRoutingKeys};
use crate::error::RadError;
use base64::prelude::*;
use crate::utils::crc::{crc16, crc16_for_byte};
use crate::utils::timer::Timer;

/**
Message format is:
[0xAA]<message type>,[routing key, routing key, routing key/]<length><binary data><crc>[0x55]
where:
    - message type - required, is a string of no more than 16 chars. [a-zA-Z0-9-_] allowed
    - routing_key is optional. More than one can be specified, and routing keys are comma
        delimited. Routing keys will be trimmed before use. The last routing key (if any),
        must be followed by a forward slash (/). The slash is required even if no routing keys are
        specified. The message type and routing keys must be [a-zA-Z0-9-_.*], where . is
        a separator, and * is a wildcard. Max=128 chars, including
    - length is 3 bytes in LSB, byte0 (first byte after routing key comma) is bits 0-7,
        then 8-15, then 16-23
    - binary data will then be read for <length> bytes
    - crc is 16 bytes long, calculated with crc16. byte0 is bits 0-7, byte1 is bits-8-15.
        The crc is calculated for all data from <message_type> to <binary_data>

    //TODO: maybe add a checksum
 */
#[derive(Serialize, Deserialize)]
pub struct Message {
    pub msg_type: String,
    pub routing_keys: Vec<String>,
    pub body: Vec<u8>,
}

pub const HEADER: u8 = 0xAA;
pub const FOOTER: u8 = 0x55;

const MAX_MSG_LENGTH: usize = 1048576; //1MB
const MAX_NAME_LENGTH: usize = 32;
const MAX_RK_LENGTH: usize = 128;

const MSG_RX_TIMEOUT_MS: u64 = 10000;

pub struct Protocol {
    msg_buffer: Vec<u8>,
    state: States,
    msg_type: String, //msg type retrieved during processing
    routing_keys: Vec<String>, //retrieved during processing
    msg_length: u32, //retrieved during processing
    retrieved_crc: u32,
    calculated_crc: u16,
    timer: Timer,
}

#[derive(Eq, PartialEq)]
enum States {
    GetHeader,
    GetMessageType,
    GetRoutingKeys,
    GetLength,
    GetBody,
    GetCrc,
    GetFooter,
}

impl Protocol {
    pub fn new() -> Result<Self, RadError> {
        Ok(Protocol {
            msg_buffer: Vec::with_capacity(10240),
            state: GetHeader,
            msg_type: "".to_string(),
            routing_keys: vec![],
            msg_length: 0,
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
        self.msg_type = "".to_string();
        self.routing_keys = vec![];
        self.msg_length = 0;
        self.timer.reset();
        self.calculated_crc = 0xFFFF;
    }

    fn is_valid_msg_type_char(c: char) -> bool {
        c.is_alphanumeric() || c == '-' || c == '_'
    }

    fn is_valid_routing_key_char(c: char) -> bool {
        Self::is_valid_msg_type_char(c) || c == '.' || c == '*'
    }

    fn is_valid_msg_type(&self, msg_type: &str) -> bool {
        if msg_type.is_empty() {
            return false;
        }

        for b in msg_type.as_bytes() {
            if !Self::is_valid_msg_type_char(*b as char) {
                return false;
            }
        }

        true
    }

    fn is_valid_routing_key(&self, rk: &str) -> bool {
        let rk_bytes = rk.as_bytes();
        for b in rk_bytes {
            if !Self::is_valid_routing_key_char(*b as char) {
                return false;
            }
        }

        let mut t = rk_bytes[0] as char;
        if t == '*' || t == '.' {
            return false;
        }

        t = rk_bytes[rk_bytes.len() - 1] as char;
        if t == '*' || t == '.' {
            return false;
        }

        if rk.contains("**") || rk.contains("..") {
            return false;
        }

        true
    }

    /**
     * routing keys have the form rk1,rk2,rk3,..... This returns a Vec of the routing keys
     */
    fn split_routing_keys(&self, raw_rk: &str) -> Result<Vec<String>, Box<dyn Error + Sync + Send>> {
        let mut ret = vec![];
        let split = raw_rk.split(",");
        for rk in split {
            if self.is_valid_routing_key(rk.trim()) {
                ret.push(rk.to_string());
            } else {
                let msg = format!("Invalid routing key detected: {}", rk);
                error!("{}", &msg);
                return Err(Box::new(RadError::from(msg)));
            }
        }

        Ok(ret)
    }

    pub fn feed(&mut self, data: &[u8]) -> Vec<Message> {
        let mut ret = vec![];
        for b in data {
            if self.state != GetHeader && self.timer.timed_out() {
                /* msg rx timeout */
                self.reset();
            }


            match self.state {
                GetHeader => {
                    if *b == HEADER {
                        self.reset();
                        self.state = GetMessageType;
                    }
                }

                GetMessageType => {
                    if *b == HEADER {
                        self.reset();
                        self.state = GetMessageType;
                        continue;
                    }

                    let c = *b as char;
                    if c == ',' {
                        let name_buf = &self.msg_buffer[0..self.msg_buffer.len()];
                        let name = match std::str::from_utf8(name_buf) {
                            Ok(n) => {
                                n
                            }
                            Err(e) => {
                                error!("Error parsing message name: {}", &e);
                                self.reset();
                                continue;
                            }
                        };

                        if !self.is_valid_msg_type(name) {
                            error!("invalid msg type");
                            self.reset();
                            continue;
                        }

                        self.partial_reset();
                        self.state = GetRoutingKeys;
                    } else {
                        self.msg_buffer.push(*b);
                        self.calculated_crc = crc16_for_byte(self.calculated_crc, *b);
                        if self.msg_buffer.len() > MAX_NAME_LENGTH {
                            self.reset();
                            error!("message type too long");
                            continue;
                        }
                    }
                }

                GetRoutingKeys => {
                    if *b == HEADER {
                        self.reset();
                        self.state = GetMessageType;
                        continue;
                    }

                    let c = *b as char;
                    if c == '/' {
                        let keys_str = match std::str::from_utf8(&self.msg_buffer[0..self.msg_buffer.len()]) {
                            Ok(n) => {
                                n
                            }
                            Err(e) => {
                                error!("Error parsing routing keys: {}", &e);
                                self.reset();
                                continue;
                            }
                        };


                        let keys = match self.split_routing_keys(keys_str) {
                            Ok(k) => { k }
                            Err(e) => {
                                error!("Error splitting routing keys: {}", &e);
                                self.reset();
                                continue;
                            }
                        };
                        self.routing_keys = keys;

                        self.partial_reset();
                        self.msg_length = 0;
                        self.state = GetLength;
                    } else {
                        self.msg_buffer.push(*b);
                        self.calculated_crc = crc16_for_byte(self.calculated_crc, *b);
                        if self.msg_buffer.len() > MAX_RK_LENGTH {
                            error!("rk(s) too long");
                            self.partial_reset();
                            continue;
                        }
                    }
                }

                GetLength => {
                    /* we have a 3-byte length. use the high byte to track the number of length bytes received */
                    let pos = self.msg_length >> 24;
                    if pos == 0 {
                        self.msg_length = *b as u32; //byte0
                        self.calculated_crc = crc16_for_byte(self.calculated_crc, *b);

                        self.msg_length |= 1 << 24;
                    } else if pos == 1 {
                        self.msg_length |= (*b as u32) << 8; //byte1
                        self.calculated_crc = crc16_for_byte(self.calculated_crc, *b);

                        self.msg_length |= 2 << 24; //value is now 0x03 on the high byte
                    } else if pos == 3 {
                        self.msg_length |= (*b as u32) << 16;
                        self.calculated_crc = crc16_for_byte(self.calculated_crc, *b);

                        self.msg_length &= 0x00FFFFFF; //clear the length byte. we have everything we need

                        if self.msg_length == 0 || (self.msg_length as usize) > MAX_MSG_LENGTH {
                            error!("Invalid message length: {}", &self.msg_length);
                            self.reset();
                            continue;
                        }

                        self.partial_reset();
                        self.state = GetBody;
                    } else {
                        error!("Invalid length pos. Invalid value: {}", pos);
                        self.partial_reset();
                        continue;
                    }
                }

                GetBody => {
                    self.msg_buffer.push(*b);
                    self.calculated_crc = crc16_for_byte(self.calculated_crc, *b);

                    if self.msg_buffer.len() == self.msg_length as usize {
                        self.partial_reset();
                        self.state = GetFooter;
                        continue;
                    }
                }

                States::GetCrc=> {
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

                        self.partial_reset();
                        self.state = GetFooter;
                    }
                }

                GetFooter => {
                    if *b == FOOTER {
                        /* got a message */
                        let msg = Message {
                            body: self.msg_buffer[0..self.msg_buffer.len()].to_vec(),
                            msg_type: self.msg_type.to_string(),
                            routing_keys: self.routing_keys.clone(),
                        };

                        ret.push(msg);
                    } else if *b == HEADER {
                        self.reset();
                        self.state = GetMessageType;
                    }
                }
            }
        }

        ret
    }

    pub fn format(msg: &Message) -> Vec<u8> {
        let mut crc = 0xFFFFu16;

        let mut ret = vec![];
        ret.push(HEADER);

        let msg_type_slice = msg.msg_type.as_bytes();
        ret.extend_from_slice(msg_type_slice);
        crc = crc16(crc, msg_type_slice, msg_type_slice.len());

        ret.extend_from_slice(",".as_bytes());
        crc = crc16_for_byte(crc, ',' as u8);

        for rk in &msg.routing_keys {
            let rk_slice = rk.as_bytes();
            ret.extend_from_slice(rk_slice);
            crc = crc16(crc, rk_slice, rk_slice.len());

            ret.extend_from_slice(",".as_bytes());
            crc = crc16_for_byte(crc, ',' as u8);
        }

        ret.extend_from_slice("/".as_bytes());
        crc = crc16_for_byte(crc, '/' as u8);

        let len = msg.body.len();
        let mut lb = (len & 0xFF) as u8;
        ret.push(lb);
        crc = crc16_for_byte(crc, lb);

        lb = ((len >> 8) & 0xFF) as u8;
        ret.push(lb);
        crc = crc16_for_byte(crc, lb);

        lb = ((len >> 16) & 0xFF) as u8;
        ret.push(lb);
        crc = crc16_for_byte(crc, lb);

        let body_slice = msg.body.as_slice();
        ret.extend_from_slice(body_slice);
        crc = crc16(crc, body_slice, body_slice.len());

        ret.push((crc & 0xFF) as u8);
        ret.push(((crc >> 8) & 0xFF) as u8);

        ret.push(FOOTER);
        ret
    }
}