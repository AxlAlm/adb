use std::collections::HashMap;

use crate::ast::add::AddEvent;

#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    pub name: String,
    pub event_name: String,
    pub stream_name: String,
    pub required: bool,
    pub attribute_type: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub name: String,
    pub stream_name: String,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Stream {
    pub name: String,
    pub key: String,
}

#[derive(Debug, PartialEq, Default, Clone)]
pub struct Schema {
    pub streams: HashMap<String, Stream>,
    pub events: HashMap<(String, String), Event>,
    pub attributes: HashMap<(String, String, String), Attribute>,
}

impl Schema {
    pub fn validate_add_operation(&self, add_event: &AddEvent) -> Result<(), String> {
        let stream_name = add_event.stream.to_string();
        let event_name = add_event.event.to_string();

        if !self.streams.contains_key(&stream_name) {
            return Err(format!("Stream '{}' not found", add_event.stream));
        }

        if !self
            .events
            .contains_key(&(stream_name.clone(), event_name.clone()))
        {
            return Err(format!(
                "Event '{}' not found in stream '{}'",
                add_event.event, add_event.stream
            ));
        }

        for a in add_event.attributes.iter() {
            if !self.attributes.contains_key(&(
                stream_name.clone(),
                event_name.clone(),
                a.name.to_string(),
            )) {
                return Err(format!(
                    "Attribute '{}' not found in event '{}' of stream '{}'",
                    a.name, add_event.event, add_event.stream
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ast::add;

    #[test]
    fn test_validate_mutation_valid() {
        let schema = Schema {
            streams: HashMap::from([(
                "account".to_string(),
                Stream {
                    name: "account".to_string(),
                    key: "account-id".to_string(),
                },
            )]),
            events: HashMap::from([(
                ("account".to_string(), "AccountCreated".to_string()),
                Event {
                    name: "AccountCreated".to_string(),
                    stream_name: "account".to_string(),
                },
            )]),
            attributes: HashMap::from([(
                (
                    "account".to_string(),
                    "AccountCreated".to_string(),
                    "owner-name".to_string(),
                ),
                Attribute {
                    name: "owner-name".to_string(),
                    event_name: "AccountCreated".to_string(),
                    stream_name: "account".to_string(),
                    required: true,
                    attribute_type: "string".to_string(),
                },
            )]),
        };

        let add_op = add::AddEvent {
            stream: "account".to_string(),
            key: "123".to_string(),
            event: "AccountCreated".to_string(),
            attributes: vec![add::AddEventAttribute {
                name: "owner-name".to_string(),
                value: "axel".to_string(),
            }],
        };

        match schema.validate_add_operation(&add_op) {
            Ok(_) => println!("success"),
            Err(e) => panic!("expected success. Got error {}", e),
        }
    }

    #[test]
    fn test_validate_mutation_invalid_stream() {
        let schema = Schema {
            streams: HashMap::from([(
                "account".to_string(),
                Stream {
                    name: "account".to_string(),
                    key: "account-id".to_string(),
                },
            )]),
            events: HashMap::from([(
                ("account".to_string(), "AccountCreated".to_string()),
                Event {
                    name: "AccountCreated".to_string(),
                    stream_name: "account".to_string(),
                },
            )]),
            attributes: HashMap::from([(
                (
                    "account".to_string(),
                    "AccountCreated".to_string(),
                    "owner-name".to_string(),
                ),
                Attribute {
                    name: "owner-name".to_string(),
                    event_name: "AccountCreated".to_string(),
                    stream_name: "account".to_string(),
                    required: true,
                    attribute_type: "string".to_string(),
                },
            )]),
        };

        let add_op = add::AddEvent {
            stream: "NON_EXISTENT_STREAM".to_string(),
            key: "123".to_string(),
            event: "AccountCreated".to_string(),
            attributes: vec![add::AddEventAttribute {
                name: "owner-name".to_string(),
                value: "axel".to_string(),
            }],
        };

        match schema.validate_add_operation(&add_op) {
            Ok(_) => panic!("expected error"),
            Err(e) => println!("success. Got error {}", e),
        }
    }

    #[test]
    fn test_validate_mutation_invalid_event() {
        let schema = Schema {
            streams: HashMap::from([(
                "account".to_string(),
                Stream {
                    name: "account".to_string(),
                    key: "account-id".to_string(),
                },
            )]),
            events: HashMap::from([(
                ("account".to_string(), "AccountCreated".to_string()),
                Event {
                    name: "AccountCreated".to_string(),
                    stream_name: "account".to_string(),
                },
            )]),
            attributes: HashMap::from([(
                (
                    "account".to_string(),
                    "AccountCreated".to_string(),
                    "owner-name".to_string(),
                ),
                Attribute {
                    name: "owner-name".to_string(),
                    event_name: "AccountCreated".to_string(),
                    stream_name: "account".to_string(),
                    required: true,
                    attribute_type: "string".to_string(),
                },
            )]),
        };

        let add_op = add::AddEvent {
            stream: "account".to_string(),
            key: "123".to_string(),
            event: "NON_EXISTENT_EVENT".to_string(),
            attributes: vec![add::AddEventAttribute {
                name: "owner-name".to_string(),
                value: "axel".to_string(),
            }],
        };

        match schema.validate_add_operation(&add_op) {
            Ok(_) => panic!("expected error"),
            Err(e) => println!("success. Got error {}", e),
        }
    }

    #[test]
    fn test_validate_mutation_invalid_attribute() {
        let schema = Schema {
            streams: HashMap::from([(
                "account".to_string(),
                Stream {
                    name: "account".to_string(),
                    key: "account-id".to_string(),
                },
            )]),
            events: HashMap::from([(
                ("account".to_string(), "AccountCreated".to_string()),
                Event {
                    name: "AccountCreated".to_string(),
                    stream_name: "account".to_string(),
                },
            )]),
            attributes: HashMap::from([(
                (
                    "account".to_string(),
                    "AccountCreated".to_string(),
                    "owner-name".to_string(),
                ),
                Attribute {
                    name: "owner-name".to_string(),
                    event_name: "AccountCreated".to_string(),
                    stream_name: "account".to_string(),
                    required: true,
                    attribute_type: "string".to_string(),
                },
            )]),
        };

        let add_op = add::AddEvent {
            stream: "account".to_string(),
            key: "123".to_string(),
            event: "AccountCreated".to_string(),
            attributes: vec![add::AddEventAttribute {
                name: "NON_EXISTENT_ATTRIBUTE".to_string(),
                value: "axel".to_string(),
            }],
        };

        match schema.validate_add_operation(&add_op) {
            Ok(_) => panic!("expected error"),
            Err(e) => println!("success. Got error {}", e),
        }
    }
}
