use core::fmt;
use std::collections::HashMap;

use std::default;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::ast::schema::Schema;
use crate::ast::{mutation, schema};
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
    MigrateError(String),
}

impl fmt::Display for DBError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DBError::AddError(msg) => write!(f, "Add Error: {}", msg),
            DBError::ReadError(msg) => write!(f, "Read Error: {}", msg),
            DBError::MigrateError(msg) => write!(f, "Migrate Error: {}", msg),
        }
    }
}

#[derive(Debug)]
pub struct Streams(pub HashMap<(String, String), Arc<RwLock<Vec<Event>>>>);

#[derive(Debug)]
pub struct DB {
    pub streams: Arc<RwLock<Streams>>,
    pub schema: Arc<RwLock<Schema>>,
}

impl DB {
    pub fn new(base_schema: Option<Schema>) -> Self {
        let schema = base_schema.unwrap_or(Default::default());

        return DB {
            streams: Arc::new(RwLock::new(Streams(HashMap::new()))),
            schema: Arc::new(RwLock::new(schema)),
        };
    }

    // TODO! make migration less naive?
    // migration currently completely overwrites previous
    pub fn migrate(&self, schema: Schema) -> Result<(), DBError> {
        let mut current_schema = self
            .schema
            .write()
            .map_err(|e| DBError::MigrateError(e.to_string()))?;
        current_schema.streams = schema.streams;
        current_schema.events = schema.events;
        current_schema.attributes = schema.attributes;
        Ok(())
    }

    pub fn get_schema(&self) -> Result<Schema, DBError> {
        let schema = self.schema.read().map_err(|e| {
            DBError::ReadError(format!("failed to read streams: {}", e.to_string()))
        })?;

        dbg!(&schema);

        return Ok(schema.clone());
    }

    pub fn add(&self, mut event: Event) -> Result<(), DBError> {
        let k = (event.stream.clone(), event.key.clone());
        let stream = {
            let mut streams = self
                .streams
                .write()
                .map_err(|_| DBError::AddError("failed to read streams".to_string()))?;
            let stream = streams.0.entry(k).or_insert(Arc::new(RwLock::new(vec![])));
            stream.clone()
        };

        let latest_version = self.get_latest_version(event.stream.clone(), event.key.clone())?;
        if latest_version + 1 != event.version {
            return Err(DBError::AddError("version is not latest".to_string()));
        }

        event.set_timestamp().map_err(|e| {
            DBError::AddError(format!("failed to set timestamp: {}", e.to_string()))
        })?;

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

    pub fn get_events(&self, stream_name: String, key: String) -> Result<Vec<Event>, DBError> {
        let streams = self.streams.read().map_err(|e| {
            DBError::ReadError(format!("failed to read streams: {}", e.to_string()))
        })?;
        match streams.0.get(&(stream_name, key)) {
            Some(events_lock) => {
                let events = events_lock.read().map_err(|e| {
                    DBError::ReadError(format!("failed to read event stream: {}", e.to_string()))
                })?;
                Ok(events.clone())
            }
            None => return Err(DBError::ReadError(format!("failed to read events"))),
        }
    }

    pub fn get_latest_version(&self, stream_name: String, key: String) -> Result<u64, DBError> {
        return Ok(self.get_latest(stream_name, key)?.map_or(0, |e| e.version));
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_db() {
        let schema = schema::Schema {
            streams: HashMap::from([(
                schema::StreamName("account".to_string()),
                schema::Stream {
                    name: schema::StreamName("account".to_string()),
                    key: "account-id".to_string(),
                },
            )]),
            events: HashMap::from([(
                (
                    schema::StreamName("account".to_string()),
                    schema::EventName("AccountCreated".to_string()),
                ),
                schema::Event {
                    name: schema::EventName("AccountCreated".to_string()),
                    stream_name: schema::StreamName("account".to_string()),
                },
            )]),
            attributes: HashMap::from([(
                (
                    schema::StreamName("account".to_string()),
                    schema::EventName("AccountCreated".to_string()),
                    schema::AttributeName("owner-name".to_string()),
                ),
                schema::Attribute {
                    name: schema::AttributeName("owner-name".to_string()),
                    event_name: schema::EventName("AccountCreated".to_string()),
                    stream_name: schema::StreamName("account".to_string()),
                    required: true,
                    attribute_type: "string".to_string(),
                },
            )]),
        };

        let db = DB::new(Some(schema));

        let stream_name = "account".to_string();
        let event_name = "AccountCreated".to_string();
        let key = "123".to_string();

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

        let event = Event {
            stream: stream_name.clone(),
            key: key.clone(),
            event: event_name.clone(),
            attributes: vec![Attribute {
                name: "owner-name".to_string(),
                value: "axel".to_string(),
            }],
            version: 1,
            timestamp: now.as_millis(),
        };

        match db.add(event.clone()) {
            Ok(_) => (),
            Err(e) => panic!("failed to add event: {}", e),
        }

        let got = match db.get_latest(stream_name.clone(), key.clone()) {
            Ok(e) => e,
            Err(e) => panic!("failed to add event: {}", e),
        };

        if got.is_none() {
            panic!("get_latest returned None")
        }

        assert_eq!(event, got.unwrap());

        let got_latest_verion = match db.get_latest_version(stream_name, key) {
            Ok(e) => e,
            Err(e) => panic!("failed to add event: {}", e),
        };

        if got_latest_verion != event.version {
            panic!(
                "got latest version {}, expected {}",
                got_latest_verion, event.version
            )
        }
    }

    #[test]
    fn test_db_migrate() {
        let schema = schema::Schema {
            streams: HashMap::from([(
                schema::StreamName("account".to_string()),
                schema::Stream {
                    name: schema::StreamName("account".to_string()),
                    key: "account-id".to_string(),
                },
            )]),
            events: HashMap::from([(
                (
                    schema::StreamName("account".to_string()),
                    schema::EventName("AccountCreated".to_string()),
                ),
                schema::Event {
                    name: schema::EventName("AccountCreated".to_string()),
                    stream_name: schema::StreamName("account".to_string()),
                },
            )]),
            attributes: HashMap::from([(
                (
                    schema::StreamName("account".to_string()),
                    schema::EventName("AccountCreated".to_string()),
                    schema::AttributeName("owner-name".to_string()),
                ),
                schema::Attribute {
                    name: schema::AttributeName("owner-name".to_string()),
                    event_name: schema::EventName("AccountCreated".to_string()),
                    stream_name: schema::StreamName("account".to_string()),
                    required: true,
                    attribute_type: "string".to_string(),
                },
            )]),
        };

        let db = DB::new(None);

        match db.migrate(schema) {
            Ok(_) => (),
            Err(e) => panic!("failed to migrate: {}", e),
        }

        let stream_name = "account".to_string();
        let event_name = "AccountCreated".to_string();
        let key = "123".to_string();

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

        let event = Event {
            stream: stream_name.clone(),
            key: key.clone(),
            event: event_name.clone(),
            attributes: vec![Attribute {
                name: "owner-name".to_string(),
                value: "axel".to_string(),
            }],
            version: 1,
            timestamp: now.as_millis(),
        };

        match db.add(event.clone()) {
            Ok(_) => (),
            Err(e) => panic!("failed to add event: {}", e),
        }
    }

    #[test]
    fn test_ensure_serial_version() {
        let schema = schema::Schema {
            streams: HashMap::from([(
                schema::StreamName("account".to_string()),
                schema::Stream {
                    name: schema::StreamName("account".to_string()),
                    key: "account-id".to_string(),
                },
            )]),
            events: HashMap::from([(
                (
                    schema::StreamName("account".to_string()),
                    schema::EventName("AccountCreated".to_string()),
                ),
                schema::Event {
                    name: schema::EventName("AccountCreated".to_string()),
                    stream_name: schema::StreamName("account".to_string()),
                },
            )]),
            attributes: HashMap::from([(
                (
                    schema::StreamName("account".to_string()),
                    schema::EventName("AccountCreated".to_string()),
                    schema::AttributeName("owner-name".to_string()),
                ),
                schema::Attribute {
                    name: schema::AttributeName("owner-name".to_string()),
                    event_name: schema::EventName("AccountCreated".to_string()),
                    stream_name: schema::StreamName("account".to_string()),
                    required: true,
                    attribute_type: "string".to_string(),
                },
            )]),
        };

        let db = DB::new(None);

        match db.migrate(schema) {
            Ok(_) => (),
            Err(e) => panic!("failed to migrate: {}", e),
        }

        let stream_name = "account".to_string();
        let event_name = "AccountCreated".to_string();
        let key = "123".to_string();

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

        let event = Event {
            stream: stream_name.clone(),
            key: key.clone(),
            event: event_name.clone(),
            attributes: vec![Attribute {
                name: "owner-name".to_string(),
                value: "axel".to_string(),
            }],
            version: 1,
            timestamp: now.as_millis(),
        };

        match db.add(event.clone()) {
            Ok(_) => (),
            Err(e) => panic!("failed to add event: {}", e),
        }

        match db.add(event.clone()) {
            Ok(_) => panic!("expected failure to add event due to version incorrect"),
            Err(e) => println!("failed to add event: {}", e),
        }
    }
}
