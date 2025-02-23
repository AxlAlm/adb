use crate::ast::schema::Schema;
use crate::event::Event;

use core::fmt;
use std::collections::HashMap;

use std::sync::{Arc, RwLock};

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
        return Ok(schema.clone());
    }

    pub fn add_event(&self, event: Event) -> Result<(), DBError> {
        let k = (event.stream.clone(), event.key.clone());
        let stream = {
            let mut streams = self
                .streams
                .write()
                .map_err(|_| DBError::AddError("failed to read streams".to_string()))?;
            let stream = streams.0.entry(k).or_insert(Arc::new(RwLock::new(vec![])));
            stream.clone()
        };

        let last_version = stream
            .read()
            .map_err(|_| DBError::AddError("failed to read streams".to_string()))?
            .last()
            .map_or(0, |e| e.version);

        if event.version != last_version + 1 {
            return Err(DBError::AddError(
                "failed to add event as verion is not serial".to_string(),
            ));
        }

        stream
            .write()
            .map_err(|e| DBError::AddError(format!("failed to add event: {}", e.to_string())))?
            .push(event);
        Ok(())
    }

    pub fn get_events(
        &self,
        stream_name: String,
        key: String,
    ) -> Result<Option<Vec<Event>>, DBError> {
        let streams = self.streams.read().map_err(|e| {
            DBError::ReadError(format!("failed to read streams: {}", e.to_string()))
        })?;

        match streams.0.get(&(stream_name, key)) {
            Some(events_lock) => {
                let events = events_lock.read().map_err(|e| {
                    DBError::ReadError(format!("failed to read event stream: {}", e.to_string()))
                })?;
                Ok(Some(events.clone()))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::ast::schema;
    use crate::event::{Attribute, Event};
    use std::time::{SystemTime, UNIX_EPOCH};

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

        match db.add_event(event.clone()) {
            Ok(_) => (),
            Err(e) => panic!("failed to add event: {}", e),
        }

        let got = match db.get_events(stream_name.clone(), key.clone()) {
            Ok(e) => e.unwrap(),
            Err(e) => panic!("failed to add event: {}", e),
        };

        if got.len() != 1 {
            panic!("event stream is empty. expected one event")
        }

        assert_eq!(event, got[0]);
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

        match db.add_event(event.clone()) {
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

        match db.add_event(event.clone()) {
            Ok(_) => (),
            Err(e) => panic!("failed to add event: {}", e),
        }

        match db.add_event(event.clone()) {
            Ok(_) => panic!("expected failure to add event due to version incorrect"),
            Err(e) => println!("failed to add event: {}", e),
        }
    }
}
