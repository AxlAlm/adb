use crate::db::DB;

use std::error::Error;
use std::fmt;

use super::general::Operation;

pub struct FindError {
    message: String,
}

impl FindError {
    fn new(message: &str) -> Self {
        FindError {
            message: message.to_string(),
        }
    }
}

impl fmt::Display for FindError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl fmt::Debug for FindError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FindError: {}", self.message)
    }
}

impl Error for FindError {}

pub fn find(op: Operation, db: &DB) -> Result<String, FindError> {
    let query_result = String::from("query_result");
    return Ok(query_result);
}

#[cfg(test)]
mod find_tests {

    #[test]
    fn parse_find_general() {}
}
