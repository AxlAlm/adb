use std::{collections::HashMap, fmt};

use super::mutation;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct StreamName(pub String);

impl StreamName {
    pub fn new(s: impl Into<String>) -> Self {
        StreamName(s.into())
    }
}
impl fmt::Display for StreamName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct EventName(pub String);

impl EventName {
    pub fn new(s: impl Into<String>) -> Self {
        EventName(s.into())
    }
}

impl fmt::Display for EventName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialOrd for EventName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for EventName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct AttributeName(pub String);

impl AttributeName {
    pub fn new(s: impl Into<String>) -> Self {
        AttributeName(s.into())
    }
}

impl fmt::Display for AttributeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialOrd for AttributeName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for AttributeName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    pub name: AttributeName,
    pub event_name: EventName,
    pub stream_name: StreamName,
    pub required: bool,
    pub attribute_type: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub name: EventName,
    pub stream_name: StreamName,
}

#[derive(Debug, PartialEq)]
pub struct Stream {
    pub name: StreamName,
    pub key: String,
}

#[derive(Debug, PartialEq)]
pub struct Schema {
    pub streams: HashMap<StreamName, Stream>,
    pub events: HashMap<(StreamName, EventName), Event>,
    pub attributes: HashMap<(StreamName, EventName, AttributeName), Attribute>,
}

impl Schema {
    pub fn is_mutation_possible(
        &self,
        mutation: &mutation::AddEventMutation,
    ) -> Result<(), String> {
        let stream_name = &StreamName::new(mutation.stream.to_string());
        let event_name = &EventName::new(mutation.event.to_string());

        if !self.streams.contains_key(stream_name) {
            return Err(format!("Stream '{}' not found", mutation.stream));
        }

        if !self
            .events
            .contains_key(&(stream_name.clone(), event_name.clone()))
        {
            return Err(format!(
                "Event '{}' not found in stream '{}'",
                mutation.event, mutation.stream
            ));
        }

        for a in mutation.attributes.iter() {
            if !self.attributes.contains_key(&(
                stream_name.clone(),
                event_name.clone(),
                AttributeName::new(a.name.to_string()),
            )) {
                return Err(format!(
                    "Attribute '{}' not found in event '{}' of stream '{}'",
                    a.name, mutation.event, mutation.stream
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_mutation_possible_valid() {
        let schema = Schema {
            streams: HashMap::from([(
                StreamName("account".to_string()),
                Stream {
                    name: StreamName("account".to_string()),
                    key: "account-id".to_string(),
                },
            )]),
            events: HashMap::from([(
                (
                    StreamName("account".to_string()),
                    EventName("AccountCreated".to_string()),
                ),
                Event {
                    name: EventName("AccountCreated".to_string()),
                    stream_name: StreamName("account".to_string()),
                },
            )]),
            attributes: HashMap::from([(
                (
                    StreamName("account".to_string()),
                    EventName("AccountCreated".to_string()),
                    AttributeName("owner-name".to_string()),
                ),
                Attribute {
                    name: AttributeName("owner-name".to_string()),
                    event_name: EventName("AccountCreated".to_string()),
                    stream_name: StreamName("account".to_string()),
                    required: true,
                    attribute_type: "string".to_string(),
                },
            )]),
        };

        let mutation = mutation::AddEventMutation {
            stream: "account".to_string(),
            key: "123".to_string(),
            event: "AccountCreated".to_string(),
            attributes: vec![mutation::Attribute {
                name: "owner-name".to_string(),
                value: "axel".to_string(),
            }],
        };

        match schema.is_mutation_possible(&mutation) {
            Ok(_) => println!("success"),
            Err(e) => panic!("expected success. Got error {}", e),
        }
    }

    #[test]
    fn test_is_mutation_possible_invalid_stream() {
        let schema = Schema {
            streams: HashMap::from([(
                StreamName("account".to_string()),
                Stream {
                    name: StreamName("account".to_string()),
                    key: "account-id".to_string(),
                },
            )]),
            events: HashMap::from([(
                (
                    StreamName("account".to_string()),
                    EventName("AccountCreated".to_string()),
                ),
                Event {
                    name: EventName("AccountCreated".to_string()),
                    stream_name: StreamName("account".to_string()),
                },
            )]),
            attributes: HashMap::from([(
                (
                    StreamName("account".to_string()),
                    EventName("AccountCreated".to_string()),
                    AttributeName("owner-name".to_string()),
                ),
                Attribute {
                    name: AttributeName("owner-name".to_string()),
                    event_name: EventName("AccountCreated".to_string()),
                    stream_name: StreamName("account".to_string()),
                    required: true,
                    attribute_type: "string".to_string(),
                },
            )]),
        };

        let mutation = mutation::AddEventMutation {
            stream: "NON_EXISTENT_STREAM".to_string(),
            key: "123".to_string(),
            event: "AccountCreated".to_string(),
            attributes: vec![mutation::Attribute {
                name: "owner-name".to_string(),
                value: "axel".to_string(),
            }],
        };

        match schema.is_mutation_possible(&mutation) {
            Ok(_) => panic!("expected error"),
            Err(e) => println!("success. Got error {}", e),
        }
    }

    #[test]
    fn test_is_mutation_possible_invalid_event() {
        let schema = Schema {
            streams: HashMap::from([(
                StreamName("account".to_string()),
                Stream {
                    name: StreamName("account".to_string()),
                    key: "account-id".to_string(),
                },
            )]),
            events: HashMap::from([(
                (
                    StreamName("account".to_string()),
                    EventName("AccountCreated".to_string()),
                ),
                Event {
                    name: EventName("AccountCreated".to_string()),
                    stream_name: StreamName("account".to_string()),
                },
            )]),
            attributes: HashMap::from([(
                (
                    StreamName("account".to_string()),
                    EventName("AccountCreated".to_string()),
                    AttributeName("owner-name".to_string()),
                ),
                Attribute {
                    name: AttributeName("owner-name".to_string()),
                    event_name: EventName("AccountCreated".to_string()),
                    stream_name: StreamName("account".to_string()),
                    required: true,
                    attribute_type: "string".to_string(),
                },
            )]),
        };

        let mutation = mutation::AddEventMutation {
            stream: "account".to_string(),
            key: "123".to_string(),
            event: "NON_EXISTENT_EVENT".to_string(),
            attributes: vec![mutation::Attribute {
                name: "owner-name".to_string(),
                value: "axel".to_string(),
            }],
        };

        match schema.is_mutation_possible(&mutation) {
            Ok(_) => panic!("expected error"),
            Err(e) => println!("success. Got error {}", e),
        }
    }

    #[test]
    fn test_is_mutation_possible_invalid_attribute() {
        let schema = Schema {
            streams: HashMap::from([(
                StreamName("account".to_string()),
                Stream {
                    name: StreamName("account".to_string()),
                    key: "account-id".to_string(),
                },
            )]),
            events: HashMap::from([(
                (
                    StreamName("account".to_string()),
                    EventName("AccountCreated".to_string()),
                ),
                Event {
                    name: EventName("AccountCreated".to_string()),
                    stream_name: StreamName("account".to_string()),
                },
            )]),
            attributes: HashMap::from([(
                (
                    StreamName("account".to_string()),
                    EventName("AccountCreated".to_string()),
                    AttributeName("owner-name".to_string()),
                ),
                Attribute {
                    name: AttributeName("owner-name".to_string()),
                    event_name: EventName("AccountCreated".to_string()),
                    stream_name: StreamName("account".to_string()),
                    required: true,
                    attribute_type: "string".to_string(),
                },
            )]),
        };

        let mutation = mutation::AddEventMutation {
            stream: "account".to_string(),
            key: "123".to_string(),
            event: "AccountCreated".to_string(),
            attributes: vec![mutation::Attribute {
                name: "NON_EXISTENT_ATTRIBUTE".to_string(),
                value: "axel".to_string(),
            }],
        };

        match schema.is_mutation_possible(&mutation) {
            Ok(_) => panic!("expected error"),
            Err(e) => println!("success. Got error {}", e),
        }
    }
}
