use super::general::Operation;
use crate::db::{DBError, DB};
use crate::event::{Attribute, Event};
use core::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

const FIELDS_OPENER: &str = "(";
const FIELDS_CLOSER: &str = ")";
const STREAM_INDICATOR: &str = "->";

#[derive(Debug)]
pub enum AddError {
    AddError(String),
    ValidationError(String),
    ParseError(String),
}

impl From<DBError> for AddError {
    fn from(error: DBError) -> Self {
        AddError::AddError(error.to_string())
    }
}

impl fmt::Display for AddError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AddError::AddError(msg) => write!(f, "Add Error: {}", msg),
            AddError::ParseError(msg) => write!(f, "Parse Error: {}", msg),
            AddError::ValidationError(msg) => write!(f, "Validation Error: {}", msg),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AddEvent {
    pub stream: String,
    pub key: String,
    pub event: String,
    pub attributes: Vec<AddEventAttribute>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AddEventAttribute {
    pub name: String,
    pub value: String,
}

impl From<AddEventAttribute> for Attribute {
    fn from(a: AddEventAttribute) -> Self {
        Attribute {
            name: a.name,
            value: a.value,
        }
    }
}

pub fn add(op: Operation, db: &DB) -> Result<String, AddError> {
    let add_ops = parse(&op.body)?;
    let schema = db.get_schema()?;

    if !schema.stream_exists(&add_ops.stream) {
        return Err(AddError::ValidationError(format!(
            "stream '{}' not found",
            add_ops.stream
        )));
    }

    if !schema.event_exists(&(add_ops.stream.clone(), add_ops.event.clone())) {
        return Err(AddError::ValidationError(format!(
            "event '{:?}' not found",
            (add_ops.stream, add_ops.event)
        )));
    }
    let missing_attributes: Vec<(String, String, String)> = add_ops
        .attributes
        .iter()
        .filter(|a| {
            !schema.attribute_exits(&(
                add_ops.stream.clone(),
                add_ops.event.clone(),
                a.name.clone(),
            ))
        })
        .map(|a| {
            (
                add_ops.stream.clone(),
                add_ops.event.clone(),
                a.name.clone(),
            )
        })
        .collect();

    if !missing_attributes.is_empty() {
        return Err(AddError::ValidationError(format!(
            "attributes '{:?}' not found",
            missing_attributes
        )));
    }

    let latest_version = db
        .get_events(add_ops.stream.to_string(), add_ops.key.to_string())?
        .unwrap_or_default()
        .last()
        .map_or(0, |event| event.version);

    let new_version = latest_version + 1;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| AddError::AddError(format!("unable to create timestamp: {}", e)))?
        .as_millis();

    let attributes = add_ops.attributes.into_iter().map(Into::into).collect();
    let event = Event::new(
        add_ops.stream,
        add_ops.key,
        add_ops.event,
        new_version,
        timestamp,
        attributes,
    );
    db.add_event(event.clone())?;

    Ok(format!("added '{:#?}'", event))
}

//example input: AccountCreated(...)TO account:123;
fn parse(input: &str) -> Result<AddEvent, AddError> {
    let j = match input.find(FIELDS_OPENER) {
        Some(index) => index,
        None => return Err(AddError::ParseError(format!("missing feilds"))),
    };
    let event = input[..j].trim().to_string();

    // extract stream and key
    //AccountCreated(...)->account:123; -> account, 123
    let splits: Vec<&str> = input.split(STREAM_INDICATOR).collect();
    if splits.len() != 2 {
        return Err(AddError::ParseError(format!("missing stream indicator")));
    }
    let (stream, key) = match splits[1].trim().split_once(":") {
        Some((x, y)) => (x.trim().to_string(), y.trim().to_string()),
        _ => return Err(AddError::ParseError(format!("missing stream:key pair"))),
    };

    // extract attributes
    //AccountCreated(owner-name=axel ...)->account:123; -> owner-name=axel ...
    let i = match input.find(FIELDS_OPENER) {
        Some(index) => index + 1,
        None => {
            return Err(AddError::ParseError(format!(
                "missing/invalid fields clause"
            )))
        }
    };
    let j = match input.find(FIELDS_CLOSER) {
        Some(index) => index,
        None => {
            return Err(AddError::ParseError(format!(
                "missing/invalid fields clause"
            )))
        }
    };
    let values: Vec<&str> = input[i..j].split(",").collect();

    let mut attributes = vec![];
    for v in values {
        let (name, val) = match v.split_once("=") {
            Some(x) => x,
            _ => return Err(AddError::ParseError(format!("unable to parse field value"))),
        };

        attributes.push(AddEventAttribute {
            name: name.trim().to_string(),
            value: val.trim().to_string().replace('"', ""),
        });
    }

    return Ok(AddEvent {
        stream,
        key,
        event,
        attributes,
    });
}

#[cfg(test)]
mod tests_add_parse {

    use crate::operation::general::OperationType;

    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_parse_add() {
        let op = Operation {
            op_type: OperationType::Add,
            body: String::from(
                r#"AccountCreated(owner-name="axel", created_at="2025-01-02 14:00:00")->account:123"#,
            ),
        };

        let expected = AddEvent {
            stream: "account".to_string(),
            key: "123".to_string(),
            event: "AccountCreated".to_string(),
            attributes: vec![
                AddEventAttribute {
                    name: "owner-name".to_string(),
                    value: "axel".to_string(),
                },
                AddEventAttribute {
                    name: "created_at".to_string(),
                    value: "2025-01-02 14:00:00".to_string(),
                },
            ],
        };

        let mutations = match parse(&op.body) {
            Ok(x) => x,
            Err(_) => panic!("Got error expected none!"),
        };

        assert_eq!(expected, mutations);
    }

    #[test]
    fn test_parse_add_fail() {
        // missing "->", correct should be:
        //  "AccountCreated(owner-name="axel", created_at="2025-01-02 14:00:00")->account:123"
        let op = Operation {
            op_type: OperationType::Add,
            body: String::from(
                r#"AccountCreated(owner-name="axel", created_at="2025-01-02 14:00:00")account:123"#,
            ),
        };

        match parse(&op.body) {
            Ok(_) => panic!("expect to fail parsing"),
            Err(_) => println!("got expected error!"),
        };
    }

    #[test]
    fn test_parse_add_fail_case2() {
        // missing "(", correct should be:
        //  "AccountCreated(owner-name="axel", created_at="2025-01-02 14:00:00")->account:123"
        let op = Operation {
            op_type: OperationType::Add,
            body: String::from(
                r#"AccountCreatedowner-name="axel", created_at="2025-01-02 14:00:00")account:123"#,
            ),
        };

        match parse(&op.body) {
            Ok(_) => panic!("expect to fail parsing"),
            Err(_) => println!("got expected error!"),
        };
    }
}

#[cfg(test)]
mod add_tests {
    use crate::{
        ast::schema::{Attribute, Event, Schema, Stream},
        operation::general::OperationType,
    };

    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_add_valid() {
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
            attributes: HashMap::from([
                (
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
                ),
                (
                    (
                        "account".to_string(),
                        "AccountCreated".to_string(),
                        "created_at".to_string(),
                    ),
                    Attribute {
                        name: "create_at".to_string(),
                        event_name: "AccountCreated".to_string(),
                        stream_name: "account".to_string(),
                        required: true,
                        attribute_type: "string".to_string(),
                    },
                ),
            ]),
        };

        let op = Operation {
            op_type: OperationType::Add,
            body: String::from(
                r#"AccountCreated(owner-name="axel", created_at="2025-01-02 14:00:00")->account:123"#,
            ),
        };

        let db = DB::new(Some(schema));

        match add(op, &db) {
            Ok(_) => println!("Success"),
            Err(e) => panic!("Failed. Got error {}", e),
        }
    }

    #[test]
    fn test_add_stream_attribute_does_not_exist() {
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

        let op = Operation {
            op_type: OperationType::Add,
            body: String::from(
                r#"AccountCreated(owner-name="axel", created_at="2025-01-02 14:00:00")->account:123"#,
            ),
        };

        let db = DB::new(Some(schema));

        match add(op, &db) {
            Ok(_) => panic!("expected error"),
            Err(e) => println!("success. Got error {}", e),
        }
    }
}
