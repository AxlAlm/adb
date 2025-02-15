#[derive(Debug, Clone, PartialEq)]
pub struct AddEventMutation {
    pub stream: String,
    pub key: String,
    pub event: String,
    pub attributes: Vec<Attribute>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    pub name: String,
    pub value: String,
}
