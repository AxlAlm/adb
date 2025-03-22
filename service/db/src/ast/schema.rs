use std::collections::HashMap;

// #[derive(Debug, Clone, PartialEq, Eq, Hash)]
// pub struct StreamID(pub String);

// #[derive(Debug, Clone, PartialEq, Eq, Hash)]
// pub struct EventID(pub String);

// #[derive(Debug, Clone, PartialEq, Eq, Hash)]
// pub struct AttributeID(pub String);

// impl fmt::Display for StreamID {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "{}", self.0)
//     }
// }

// impl fmt::Display for EventID {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "{}", self.0)
//     }
// }

// impl fmt::Display for AttributeID {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "{}", self.0)
//     }
// }

// #[derive(Debug, Clone, PartialEq, Eq, Hash)]
// pub struct EventIndex {
//     stream: String,
//     event: String,
// }

// impl EventIndex {
//     pub fn new(stream: StreamID, event: EventID) -> Self {
//         return EventIndex { event, stream };
//     }
// }

// #[derive(Debug, Clone, PartialEq, Eq, Hash)]
// pub struct AttributeIndex {
//     stream: String,
//     event: String,
//     attribute: String,
// }

// impl AttributeIndex {
//     pub fn new(stream: String, event: String, attribute: String) -> Self {
//         return AttributeIndex {
//             event,
//             stream,
//             attribute,
//         };
//     }
// }

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
