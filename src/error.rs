use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

pub struct RadError {
    pub message: String,
}

impl RadError {
    pub fn new(msg: &str) -> RadError {
        RadError {
            message: String::from(msg),
        }
    }
}

impl Error for RadError {
}

impl From<&str> for RadError {
    fn from(value: &str) -> Self {
        RadError {
            message: String::from(value),
        }
    }
}

impl From<String> for RadError {
    fn from(value: String) -> Self {
        RadError {
            message: value,
        }
    }
}

impl Debug for RadError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RadError: [{}]", &self.message)
    }
}

impl Display for RadError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RadError: [{}]", &self.message)
    }
}