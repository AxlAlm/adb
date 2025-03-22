use core::fmt;
use std::{collections::HashMap, u64};

use crate::event::Attribute;

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

// pub enum Command {
//     Add,
//     Create,
//     Find,
//     Show,
//     List,
// }

// pub enum Entity {
//     Stream,
//     Event,
//     Attribute,
//     Value,
// }

// pub enum Operations {
//     Sum,
//     Last,
//     Add,
// }

pub struct Create {
    entity: EntityNode,
}

pub enum EntityNode {
    Stream {
        name: StreamID,
        aggregate_id: String,
    },
    Event {
        name: EventID,
        stream: StreamID,
    },
    Attribute {
        name: AttributeID,
        event: EventID,
        stream: StreamID,
        required: bool,
        attribute_type: String,
    },
}

pub struct Add {
    event: Event,
}

pub struct Event {
    name: EventID,
}

// pub enum AstNode {
//     Find {
//         line_number: u32,
//         char_start_number: u32,
//         attributes: Option<Box<Attribute>>,
//     },

//     Float {
//         node_type: String,
//         line_number: u32,
//         char_start_number: u32,
//         left: Option<Box<AstNode>>,
//         right: Option<Box<AstNode>>,
//     },
//     String {
//         node_type: String,
//         line_number: u32,
//         char_start_number: u32,
//         left: Option<Box<AstNode>>,
//         right: Option<Box<AstNode>>,
//     },
//     Boolean {
//         node_type: String,
//         line_number: u32,
//         char_start_number: u32,
//         left: Option<Box<AstNode>>,
//         right: Option<Box<AstNode>>,
//     },
// }

// // pub enum AstNode {
// //     Int {
// //         node_type: String,
// //         value: i64,
// //         line_number: u32,
// //         char_start_number: u32,
// //         left: Option<Box<AstNode>>,
// //         right: Option<Box<AstNode>>,
// //     },
// //     Float {
// //         node_type: String,
// //         line_number: u32,
// //         char_start_number: u32,
// //         left: Option<Box<AstNode>>,
// //         right: Option<Box<AstNode>>,
// //     },
// //     String {
// //         node_type: String,
// //         line_number: u32,
// //         char_start_number: u32,
// //         left: Option<Box<AstNode>>,
// //         right: Option<Box<AstNode>>,
// //     },
// //     Boolean {
// //         node_type: String,
// //         line_number: u32,
// //         char_start_number: u32,
// //         left: Option<Box<AstNode>>,
// //         right: Option<Box<AstNode>>,
// //     },
// // }
