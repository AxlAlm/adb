impl Event {
    pub fn new(
        stream: String,
        key: String,
        event: String,
        version: u64,
        timestamp: u128,
        attributes: Vec<Attribute>,
    ) -> Self {
        return Event {
            stream,
            key,
            event,
            version,
            timestamp,
            attributes,
        };
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub stream: String,
    pub key: String,
    pub event: String,
    pub version: u64,
    pub timestamp: u128,
    pub attributes: Vec<Attribute>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attribute {
    pub name: String,
    pub value: String,
}
