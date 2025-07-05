use crate::event::Event;
use crate::planner;

use std::error::Error;

use std::collections::{HashMap, HashSet};
use std::fmt;

use std::sync::{Arc, RwLock};

pub struct DBError {
    message: String,
}

impl DBError {
    fn new(message: &str) -> Self {
        DBError {
            message: message.to_string(),
        }
    }
}

impl fmt::Display for DBError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl fmt::Debug for DBError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DBError: {}", self.message)
    }
}

impl Error for DBError {}

// (stream, key) : []Events
#[derive(Debug)]
pub struct Streams(pub HashMap<(String, String), Arc<RwLock<Vec<Event>>>>);

#[derive(Debug, Clone, PartialEq)]
pub struct AttributeDetails {
    pub required: bool,
    pub attribute_type: String,
}

#[derive(Debug, PartialEq, Default, Clone)]
struct Schema {
    // stream
    pub streams: HashSet<String>,
    // stream, event
    pub events: HashSet<(String, String)>,
    // stream, event, attribute -> data_type
    pub attributes: HashMap<(String, String, String), String>,
}

#[derive(Debug)]
pub struct DB {
    pub streams: Arc<RwLock<Streams>>,
    pub schema: Arc<RwLock<Schema>>,
}

impl DB {
    pub fn new() -> Self {
        return DB {
            streams: Arc::new(RwLock::new(Streams(HashMap::new()))),
            schema: Arc::new(RwLock::new(Default::default())),
        };
    }

    pub fn exec(&self, plan: &planner::ExecutionPlan) -> Result<(), DBError> {
        for op in plan.operations.iter() {
            match op {
                planner::Operation::CreateStream { name } => {
                    self.create_stream(&name)?;
                }
                planner::Operation::CreateEvent {
                    stream_name: stream,
                    name,
                } => {
                    self.create_event(stream, name)?;
                }
                planner::Operation::CreateAttribute {
                    name,
                    event_name: event,
                    stream_name: stream,
                    data_type,
                } => {
                    self.create_attribute(stream, event, name, data_type)?;
                }
                planner::Operation::CheckStreamExists { name } => {
                    self.check_stream_exists(name)?;
                }

                _ => return Err(DBError::new("unsupported operation")),
            }
        }

        Ok(())
    }

    fn check_stream_exists(&self, stream_name: &str) -> Result<(), DBError> {
        self.schema
            .read()
            .map_err(|e| DBError::new(&format!("failed to read schema: {}", e.to_string())))?
            .streams
            .contains(stream_name);
        Ok(())
    }

    fn check_event_exists(&self, stream_name: &str, event_name: &str) -> Result<(), DBError> {
        self.schema
            .read()
            .map_err(|e| DBError::new(&format!("failed to read schema: {}", e.to_string())))?
            .events
            .contains(&(stream_name.to_string(), event_name.to_string()));
        Ok(())
    }

    fn create_stream(&self, name: &str) -> Result<(), DBError> {
        self.schema
            .write()
            .map_err(|e| {
                DBError::new(&format!(
                    "failed to aquire write access for schema: {}",
                    e.to_string()
                ))
            })?
            .streams
            .insert(name.to_string());

        return Ok(());
    }

    pub fn create_event(&self, stream_name: &str, event_name: &str) -> Result<(), DBError> {
        self.schema
            .write()
            .map_err(|e| {
                DBError::new(&format!(
                    "failed to aquire write access for schema: {}",
                    e.to_string()
                ))
            })?
            .events
            .insert((stream_name.to_string(), event_name.to_string()));
        return Ok(());
    }

    pub fn create_attribute(
        &self,
        stream_name: &str,
        event_name: &str,
        attribute_name: &str,
        data_type: &str,
    ) -> Result<(), DBError> {
        self.schema
            .write()
            .map_err(|e| DBError::new(&format!("failed to read schema: {}", e.to_string())))?
            .attributes
            .insert(
                (
                    stream_name.to_string(),
                    event_name.to_string(),
                    attribute_name.to_string(),
                ),
                data_type.to_string(),
            );
        return Ok(());
    }

    pub fn add_event(&self, event: Event) -> Result<(), DBError> {
        let streams = self
            .streams
            .read()
            .map_err(|_| DBError::new(&"failed to read streams".to_string()))?;

        let stream_arc = streams
            .0
            .get(&(event.stream.clone(), event.key.clone()))
            .ok_or(DBError::new(&"stream not found".to_string()))?;

        let mut stream = stream_arc
            .write()
            .map_err(|_| DBError::new(&"failed to write to stream".to_string()))?;

        if event.version != stream.last().map_or(0, |e| e.version) + 1 {
            return Err(DBError::new(
                &"failed to add event as version is not serial".to_string(),
            ));
        }

        stream.push(event);
        Ok(())
    }

    pub fn get_events(
        &self,
        stream_name: String,
        key: String,
    ) -> Result<Option<Vec<Event>>, DBError> {
        let streams = self
            .streams
            .read()
            .map_err(|e| DBError::new(&format!("failed to read streams: {}", e.to_string())))?;

        match streams.0.get(&(stream_name, key)) {
            Some(events_lock) => {
                let events = events_lock.read().map_err(|e| {
                    DBError::new(&format!("failed to read event stream: {}", e.to_string()))
                })?;
                Ok(Some(events.clone()))
            }
            None => Ok(None),
        }
    }
}
