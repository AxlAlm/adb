use std::collections::HashMap;

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
    pub fn stream_exists(&self, k: &str) -> bool {
        self.streams.contains_key(k)
    }
    pub fn event_exists(&self, k: &(String, String)) -> bool {
        self.events.contains_key(k)
    }
    pub fn attribute_exits(&self, k: &(String, String, String)) -> bool {
        self.attributes.contains_key(k)
    }
}
