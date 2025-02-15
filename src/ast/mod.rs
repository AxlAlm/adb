use std::{collections::HashMap, fmt};

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
