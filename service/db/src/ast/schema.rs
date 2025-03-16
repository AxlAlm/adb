use core::fmt;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamID(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventID(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AttributeID(pub String);

impl fmt::Display for StreamID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for EventID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for AttributeID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventIndex {
    stream: StreamID,
    event: EventID,
}

impl EventIndex {
    pub fn new(stream: StreamID, event: EventID) -> Self {
        return EventIndex { event, stream };
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AttributeIndex {
    stream: StreamID,
    event: EventID,
    attribute: AttributeID,
}

impl AttributeIndex {
    pub fn new(stream: StreamID, event: EventID, attribute: AttributeID) -> Self {
        return AttributeIndex {
            event,
            stream,
            attribute,
        };
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    pub name: AttributeID,
    pub event: EventID,
    pub stream: StreamID,
    pub required: bool,
    pub attribute_type: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub name: EventID,
    pub stream: StreamID,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Stream {
    pub name: StreamID,
    pub aggregate_id: String,
}

#[derive(Debug, PartialEq, Default, Clone)]
pub struct Schema {
    pub streams: HashMap<StreamID, Stream>,
    pub events: HashMap<EventIndex, Event>,
    pub attributes: HashMap<AttributeIndex, Attribute>,
}

impl Schema {
    pub fn stream_exists(&self, k: &StreamID) -> bool {
        self.streams.contains_key(k)
    }
    pub fn event_exists(&self, k: &EventIndex) -> bool {
        self.events.contains_key(k)
    }
    pub fn attribute_exits(&self, k: &AttributeIndex) -> bool {
        self.attributes.contains_key(k)
    }
}
