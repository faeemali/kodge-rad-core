use std::error::Error;
use std::time::Duration;
use log::{debug, error};
use serde::{Deserialize, Serialize};
use crate::broker::protocol::States::{GetBody, GetFooter, GetHeader, GetMessageHeader};
use crate::error::RadError;
use crate::utils::crc::{crc16, crc16_for_byte};
use crate::utils::timer::Timer;

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
#[derive(Serialize, Deserialize, Clone)]
pub struct Message {
    pub header: MessageHeader,
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
    header: Option<MessageHeader>,
    state: States,
    retrieved_crc: u32,
    calculated_crc: u16,
    timer: Timer,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct MessageHeader {
    pub name: String,
    pub msg_type: String,
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
    }

    fn is_valid_msg_type_char(c: char) -> bool {
        c.is_alphanumeric() || c == '-' || c == '_'
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

    fn is_valid_name(&self, name: &str) -> bool {
        if name.is_empty() {
            return false;
        }

        for b in name.as_bytes() {
            if !Self::is_valid_msg_type_char(*b as char) {
                return false;
            }
        }

        true
    }

    pub fn feed(&mut self, data: &[u8]) -> Vec<Message> {
        let mut ret = vec![];
        for b in data {
            if self.state != GetHeader && self.timer.timed_out() {
                /* msg rx timeout */
                self.reset();
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
                            debug!("Error prcessing message header: {}", &e);
                            self.reset();
                            continue;
                        }

                        let msg_header = msg_header_res.unwrap();
                        if !self.is_valid_name(&msg_header.name) ||  !self.is_valid_msg_type(&msg_header.msg_type) {
                            debug!("Invalid message name or type");
                            self.reset();
                            continue;
                        }

                        if msg_header.length > MAX_MSG_LENGTH as u32 {
                            debug!("Invalid message length");
                            self.reset();
                            continue;
                        }

                        self.header = Some(msg_header);

                        self.partial_reset();
                        self.state = GetBody;
                    }

                    self.msg_buffer.push(*b);
                }

                GetBody => {
                    self.msg_buffer.push(*b);
                    self.calculated_crc = crc16_for_byte(self.calculated_crc, *b);

                    if let Some(s) = &self.header {
                        if self.msg_buffer.len() == s.length as usize {
                            self.partial_reset();
                            self.state = GetFooter;
                            continue;
                        }

                    } else {
                        debug!("Unexpected error: message header is none while trying to retrieve body");
                        self.reset();
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

    pub fn format(msg: &Message) -> Result<Vec<u8>, Box<dyn Error>> {
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