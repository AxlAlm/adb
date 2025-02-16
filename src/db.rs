use core::fmt;
use std::collections::HashMap;

use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::ast::mutation;
// use crate::ast::{mutation::AddEventMutation, schema::StreamName};

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
    pub fn new(mutation: mutation::AddEventMutation, version: u64) -> Self {
        return Event {
            stream: mutation.stream.to_string(),
            key: mutation.key.to_string(),
            event: mutation.event.to_string(),
            version,
            timestamp: 0, // will be filled on insert
            attributes: mutation.attributes.into_iter().map(Into::into).collect(),
        };
    }

    pub fn set_timestamp(&mut self) -> Result<(), String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?;
        self.timestamp = now.as_millis();
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    pub name: String,
    pub value: String,
}

impl From<mutation::Attribute> for Attribute {
    fn from(a: mutation::Attribute) -> Self {
        Attribute {
            name: a.name,
            value: a.value,
        }
    }
}

#[derive(Debug)]
pub enum DBError {
    AddError(String),
    ReadError(String),
}

impl fmt::Display for DBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DBError::AddError(msg) => write!(f, "Add Error: {}", msg),
            DBError::ReadError(msg) => write!(f, "Read Error: {}", msg),
        }
    }
}

#[derive(Debug)]
pub struct Streams(pub HashMap<(String, String), Arc<RwLock<Vec<Event>>>>);

#[derive(Debug)]
pub struct DB {
    pub streams: Arc<RwLock<Streams>>,
}

impl DB {
    pub fn new() -> Self {
        return DB {
            streams: Arc::new(RwLock::new(crate::db::Streams(HashMap::new()))),
        };
    }

    pub fn add(&self, event: Event) -> Result<(), DBError> {
        let k = (event.stream.clone(), event.key.clone());

        // Get clone of event stream
        let stream = {
            let streams = self
                .streams
                .read()
                .map_err(|_| DBError::AddError("failed to read streams".to_string()))?;
            streams
                .0
                .get(&k)
                .ok_or(DBError::AddError("stream not found".to_string()))?
                .clone()
        };

        let latest_version = self.get_latest_version(event.stream.clone(), event.key.clone())?;
        if latest_version + 1 != event.version {
            return Err(DBError::AddError("version is not latest".to_string()));
        }

        stream
            .write()
            .map_err(|e| DBError::AddError(format!("failed to add event: {}", e.to_string())))?
            .push(event);
        Ok(())
    }

    pub fn get_latest(&self, stream_name: String, key: String) -> Result<Option<Event>, DBError> {
        let streams = self.streams.read().map_err(|e| {
            DBError::ReadError(format!("failed to read streams: {}", e.to_string()))
        })?;
        match streams.0.get(&(stream_name, key)) {
            Some(events_lock) => {
                let events = events_lock.read().map_err(|e| {
                    DBError::ReadError(format!("failed to read event stream: {}", e.to_string()))
                })?;
                Ok(events.last().cloned())
            }
            None => Ok(None),
        }
    }

    pub fn get_latest_version(&self, stream_name: String, key: String) -> Result<u64, DBError> {
        return Ok(self.get_latest(stream_name, key)?.map_or(0, |e| e.version));
    }
}
