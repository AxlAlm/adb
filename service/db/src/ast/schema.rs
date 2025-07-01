use std::collections::{HashMap, HashSet};

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

// #[derive(Debug, PartialEq, Clone)]
// pub struct Stream {
//     pub name: String,
//     pub key: String,
// }

#[derive(Debug, PartialEq, Default, Clone)]
pub struct Schema {
    pub streams: HashSet<String>,
    pub events: HashMap<(String, String), Event>,
    pub attributes: HashMap<(String, String, String), Attribute>,
}
