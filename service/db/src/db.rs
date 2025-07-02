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
        let k = (event.stream.clone(), event.key.clone());
        let stream = {
            let streams = self
                .streams
                .read()
                .map_err(|_| DBError::new(&"failed to read streams".to_string()))?
                .0
                .get_mut(&k)
                .ok_or(DBError::new(&"failed to read streams".to_string()))?
                .write()
                .map_err(|_| DBError::new(&"failed to read streams".to_string()))?;

            if event.version != streams.last().map_or(0, |e| e.version) + 1 {
                return Err(DBError::new(
                    &"failed to add event as verion is not serial".to_string(),
                ));
            }
        };

        let last_version = stream
            .read()
            .map_err(|_| DBError::new(&"failed to read streams".to_string()))?
            .last()
            .map_or(0, |e| e.version);

        if event.version != last_version + 1 {}

        stream
            .write()
            .map_err(|e| DBError::new(&format!("failed to add event: {}", e.to_string())))?
            .push(event);
        Ok(())
    }

    // pub fn get_events(
    //     &self,
    //     stream_name: String,
    //     key: String,
    // ) -> Result<Option<Vec<Event>>, DBError> {
    //     let streams = self
    //         .streams
    //         .read()
    //         .map_err(|e| DBError::new(&format!("failed to read streams: {}", e.to_string())))?;

    //     match streams.0.get(&(stream_name, key)) {
    //         Some(events_lock) => {
    //             let events = events_lock.read().map_err(|e| {
    //                 DBError::new(&format!("failed to read event stream: {}", e.to_string()))
    //             })?;
    //             Ok(Some(events.clone()))
    //         }
    //         None => Ok(None),
    //     }
    // }
}

// #[cfg(test)]
// mod tests {

//     use super::*;
//     use crate::ast::schema::{Attribute, Event, Schema};
//     use crate::event;
//     use std::time::{SystemTime, UNIX_EPOCH};

//     #[test]
//     fn test_db() {
//         let db = DB::new(None);

//         let stream_name = "account".to_string();
//         let event_name = "AccountCreated".to_string();
//         let key = "123".to_string();

//         let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

//         let event = event::Event {
//             stream: stream_name.clone(),
//             key: key.clone(),
//             event: event_name.clone(),
//             attributes: vec![event::Attribute {
//                 name: "owner-name".to_string(),
//                 value: "axel".to_string(),
//             }],
//             version: 1,
//             timestamp: now.as_millis(),
//         };

//         match db.add_event(event.clone()) {
//             Ok(_) => (),
//             Err(e) => panic!("failed to add event: {}", e),
//         }

//         let got = match db.get_events(stream_name.clone(), key.clone()) {
//             Ok(e) => e.unwrap(),
//             Err(e) => panic!("failed to add event: {}", e),
//         };

//         if got.len() != 1 {
//             panic!("event stream is empty. expected one event")
//         }

//         assert_eq!(event, got[0]);
//     }

//     // #[test]
//     // fn test_db() {
//     //     let schema = Schema {
//     //         streams: HashMap::from([(
//     //             "account".to_string(),
//     //             Stream {
//     //                 name: "account".to_string(),
//     //                 key: "account-id".to_string(),
//     //             },
//     //         )]),
//     //         events: HashMap::from([(
//     //             ("account".to_string(), "AccountCreated".to_string()),
//     //             Event {
//     //                 name: "AccountCreated".to_string(),
//     //                 stream_name: "account".to_string(),
//     //             },
//     //         )]),
//     //         attributes: HashMap::from([(
//     //             (
//     //                 "account".to_string(),
//     //                 "AccountCreated".to_string(),
//     //                 "owner-name".to_string(),
//     //             ),
//     //             Attribute {
//     //                 name: "owner-name".to_string(),
//     //                 event_name: "AccountCreated".to_string(),
//     //                 stream_name: "account".to_string(),
//     //                 required: true,
//     //                 attribute_type: "string".to_string(),
//     //             },
//     //         )]),
//     //     };

//     //     let db = DB::new(Some(schema));

//     //     let stream_name = "account".to_string();
//     //     let event_name = "AccountCreated".to_string();
//     //     let key = "123".to_string();

//     //     let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

//     //     let event = event::Event {
//     //         stream: stream_name.clone(),
//     //         key: key.clone(),
//     //         event: event_name.clone(),
//     //         attributes: vec![event::Attribute {
//     //             name: "owner-name".to_string(),
//     //             value: "axel".to_string(),
//     //         }],
//     //         version: 1,
//     //         timestamp: now.as_millis(),
//     //     };

//     //     match db.add_event(event.clone()) {
//     //         Ok(_) => (),
//     //         Err(e) => panic!("failed to add event: {}", e),
//     //     }

//     //     let got = match db.get_events(stream_name.clone(), key.clone()) {
//     //         Ok(e) => e.unwrap(),
//     //         Err(e) => panic!("failed to add event: {}", e),
//     //     };

//     //     if got.len() != 1 {
//     //         panic!("event stream is empty. expected one event")
//     //     }

//     //     assert_eq!(event, got[0]);
//     // }

//     // #[test]
//     // fn test_ensure_serial() {
//     //     let schema = Schema {
//     //         streams: HashMap::from([(
//     //             "account".to_string(),
//     //             Stream {
//     //                 name: "account".to_string(),
//     //                 key: "account-id".to_string(),
//     //             },
//     //         )]),
//     //         events: HashMap::from([(
//     //             ("account".to_string(), "AccountCreated".to_string()),
//     //             Event {
//     //                 name: "AccountCreated".to_string(),
//     //                 stream_name: "account".to_string(),
//     //             },
//     //         )]),
//     //         attributes: HashMap::from([(
//     //             (
//     //                 "account".to_string(),
//     //                 "AccountCreated".to_string(),
//     //                 "owner-name".to_string(),
//     //             ),
//     //             Attribute {
//     //                 name: "owner-name".to_string(),
//     //                 event_name: "AccountCreated".to_string(),
//     //                 stream_name: "account".to_string(),
//     //                 required: true,
//     //                 attribute_type: "string".to_string(),
//     //             },
//     //         )]),
//     //     };

//     //     let db = DB::new(Some(schema));

//     //     let stream_name = "account".to_string();
//     //     let event_name = "AccountCreated".to_string();
//     //     let key = "123".to_string();

//     //     let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

//     //     let event = event::Event {
//     //         stream: stream_name.clone(),
//     //         key: key.clone(),
//     //         event: event_name.clone(),
//     //         attributes: vec![event::Attribute {
//     //             name: "owner-name".to_string(),
//     //             value: "axel".to_string(),
//     //         }],
//     //         version: 1,
//     //         timestamp: now.as_millis(),
//     //     };

//     //     match db.add_event(event.clone()) {
//     //         Ok(_) => (),
//     //         Err(e) => panic!("failed to add event: {}", e),
//     //     }

//     //     match db.add_event(event.clone()) {
//     //         Ok(_) => panic!("expected failure to add event due to version incorrect"),
//     //         Err(e) => println!("failed to add event: {}", e),
//     //     }
//     // }

//     // #[test]
//     // fn test_fail_to_create_event_for_non_existing_stream() {
//     //     let schema = Schema {
//     //         streams: HashMap::from([(
//     //             "account".to_string(),
//     //             Stream {
//     //                 name: "account".to_string(),
//     //                 key: "account-id".to_string(),
//     //             },
//     //         )]),
//     //         events: HashMap::from([(
//     //             ("account".to_string(), "AccountCreated".to_string()),
//     //             Event {
//     //                 name: "AccountCreated".to_string(),
//     //                 stream_name: "account".to_string(),
//     //             },
//     //         )]),
//     //         attributes: HashMap::from([(
//     //             (
//     //                 "account".to_string(),
//     //                 "AccountCreated".to_string(),
//     //                 "owner-name".to_string(),
//     //             ),
//     //             Attribute {
//     //                 name: "owner-name".to_string(),
//     //                 event_name: "AccountCreated".to_string(),
//     //                 stream_name: "account".to_string(),
//     //                 required: true,
//     //                 attribute_type: "string".to_string(),
//     //             },
//     //         )]),
//     //     };

//     //     let db = DB::new(Some(schema));

//     //     let event = schema::Event {
//     //         name: "AccountCreated".to_string(),
//     //         stream_name: "DOES NOT EXIST".to_string(),
//     //     };

//     //     match db.create_event(event.clone()) {
//     //         Ok(_) => panic!("expect to fail to create event"),
//     //         Err(e) => println!("successfully failed: {}", e),
//     //     }
//     // }

//     // #[test]
//     // fn test_fail_to_create_attribute_for_non_existing_stream_event() {
//     //     let schema = Schema {
//     //         streams: HashMap::from([(
//     //             "account".to_string(),
//     //             Stream {
//     //                 name: "account".to_string(),
//     //                 key: "account-id".to_string(),
//     //             },
//     //         )]),
//     //         events: HashMap::from([(
//     //             ("account".to_string(), "AccountCreated".to_string()),
//     //             Event {
//     //                 name: "AccountCreated".to_string(),
//     //                 stream_name: "account".to_string(),
//     //             },
//     //         )]),
//     //         attributes: HashMap::from([(
//     //             (
//     //                 "account".to_string(),
//     //                 "AccountCreated".to_string(),
//     //                 "owner-name".to_string(),
//     //             ),
//     //             Attribute {
//     //                 name: "owner-name".to_string(),
//     //                 event_name: "AccountCreated".to_string(),
//     //                 stream_name: "account".to_string(),
//     //                 required: true,
//     //                 attribute_type: "string".to_string(),
//     //             },
//     //         )]),
//     //     };

//     //     let db = DB::new(Some(schema));

//     //     let event = schema::Attribute {
//     //         name: "AccountCreated".to_string(),
//     //         stream_name: "DOES NOT EXIST".to_string(),
//     //         event_name: "DOES NOT EXIST".to_string(),
//     //         required: true,
//     //         attribute_type: "string".to_string(),
//     //     };

//     //     match db.create_attribute(event.clone()) {
//     //         Ok(_) => panic!("expect to fail to create attribute"),
//     //         Err(e) => println!("successfully failed: {}", e),
//     //     }
//     // }
// }
