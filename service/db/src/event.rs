use crate::ast;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub stream: String,
    pub key: String,
    pub event: String,
    pub version: u64,
    pub timestamp: u128,
    pub attributes: Vec<Attribute>,
}

impl Event {
    pub fn new(mutation: ast::mutation::AddEventMutation, version: u64) -> Result<Self, String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?;

        return Ok(Event {
            stream: mutation.stream.to_string(),
            key: mutation.key.to_string(),
            event: mutation.event.to_string(),
            version,
            timestamp: now.as_millis(),
            attributes: mutation.attributes.into_iter().map(Into::into).collect(),
        });
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    pub name: String,
    pub value: String,
}

impl From<ast::mutation::Attribute> for Attribute {
    fn from(a: ast::mutation::Attribute) -> Self {
        Attribute {
            name: a.name,
            value: a.value,
        }
    }
}
