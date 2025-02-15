use std::collections::HashMap;

const BLOCK_SEPERATOR: &str = ";";
const FIELDS_OPENER: &str = "(";
// const FIELDS_CLOSER: &str = ")";
const COMMENT_OPENER: &str = "//";

#[derive(Debug)]
pub enum SchemaParserError {
    InvalidBLock,
    InvalidValue,
    StreamNotFound,
    EventNotFound,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    pub event: String,
    pub name: String,
    pub required: bool,
    pub attribute_type: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub stream: String,
    pub name: String,
    pub attributes: Vec<Attribute>,
}

#[derive(Debug, PartialEq)]
pub struct Stream {
    pub name: String,
    pub key: String,
    pub events: Vec<Event>,
}

#[derive(Debug, PartialEq)]
pub struct Schema {
    pub streams: Vec<Stream>,
}

// stream(...)  -> stream
fn extract_block_type(input: &str) -> Result<String, SchemaParserError> {
    let splits: Vec<&str> = input.split("(").collect();
    if splits.len() != 2 {
        return Err(SchemaParserError::InvalidBLock);
    }

    let block_type = splits[0].to_lowercase();
    return Ok(block_type);
}

// stream(hello, hello-id)  -> hello, hello-id
fn extract_fields(input: &str) -> Result<Vec<&str>, SchemaParserError> {
    let i = match input.find(FIELDS_OPENER) {
        Some(index) => index + 1,
        None => return Err(SchemaParserError::InvalidBLock),
    };
    let j = input.len() - 1;
    let values = input[i..j].split(",").collect();
    return Ok(values);
}

// test, test-id -> stream {name: test, key:test-id}
fn create_stream(values: Vec<&str>) -> Result<Stream, SchemaParserError> {
    if values.len() != 2 {
        return Err(SchemaParserError::InvalidBLock);
    }

    let name = parse_value(values[0])?;
    let key = parse_value(values[1])?;

    Ok(Stream {
        name,
        events: vec![],
        key,
    })
}

// test-stream, test -> event {stream: test-stream, name: test}
fn create_event(values: Vec<&str>) -> Result<Event, SchemaParserError> {
    if values.len() != 2 {
        return Err(SchemaParserError::InvalidBLock);
    }

    let stream = parse_value(values[0])?;
    let name = parse_value(values[1])?;

    Ok(Event {
        stream,
        name,
        attributes: vec![],
    })
}

// test-event, test, true, string ->
// attribute {event: event-name, name: test, required: true, attribute_type: str}
fn create_attribute(values: Vec<&str>) -> Result<Attribute, SchemaParserError> {
    if values.len() != 4 {
        return Err(SchemaParserError::InvalidBLock);
    }

    let event = parse_value(values[0])?;
    let name = parse_value(values[1])?;
    let required_value = parse_value(values[2])?;
    let required = match required_value {
        s if s == "true" => true,
        s if s == "false" => false,
        _ => return Err(SchemaParserError::InvalidValue),
    };
    let attribute_type = parse_value(values[3])?;

    Ok(Attribute {
        event,
        name,
        required,
        attribute_type,
    })
}

fn parse_value(input: &str) -> Result<String, SchemaParserError> {
    if input.trim().is_empty() {
        return Err(SchemaParserError::InvalidValue);
    }
    return Ok(input.to_string());
}

pub fn parse_schema(input: &str) -> Result<Schema, SchemaParserError> {
    let mut streams_map: HashMap<String, Stream> = HashMap::new();
    let mut events_map: HashMap<String, Event> = HashMap::new();

    for line in input.lines() {
        // to solve cases where we have trailing comments;
        //     stream(x,y); // trailing comment
        let block = match line.trim().splitn(2, BLOCK_SEPERATOR).next() {
            Some(x) => x,
            _ => return Err(SchemaParserError::InvalidBLock),
        };

        let block = String::from_iter(block.chars().filter(|x| !x.is_whitespace()));

        if block.is_empty() {
            continue;
        }

        if block.starts_with(COMMENT_OPENER) {
            continue;
        }

        let block_type = extract_block_type(&block)?;
        let values = extract_fields(&block)?;

        if block_type == "stream" {
            let stream = create_stream(values)?;
            streams_map.insert(stream.name.clone(), stream);
        } else if block_type == "event" {
            let event = create_event(values)?;
            events_map.insert(event.name.clone(), event);
        } else if block_type == "attribute" {
            let attribute = create_attribute(values)?;
            let event = match events_map.get_mut(&attribute.event) {
                Some(x) => x,
                _ => return Err(SchemaParserError::EventNotFound),
            };
            event.attributes.push(attribute);
        }
    }

    for event in events_map.values() {
        let stream = match streams_map.get_mut(&event.stream) {
            Some(x) => x,
            _ => return Err(SchemaParserError::StreamNotFound),
        };
        stream.events.push(event.clone());
    }

    let streams: Vec<Stream> = streams_map.into_values().collect();
    Ok(Schema { streams })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn sort_schema_events(mut schema: Schema) -> Schema {
        for stream in &mut schema.streams {
            stream.events.sort_by(|a, b| a.name.cmp(&b.name));
            for event in &mut stream.events {
                event.attributes.sort_by(|a, b| a.name.cmp(&b.name));
            }
        }
        schema
    }

    #[test]
    fn test_basic_schema_parse() {
        let schema = String::from(
            r#"
        stream(account, account-id);
        event(account, AccountCreated);
        attribute(AccountCreated, owner-name, true, string);
        attribute(AccountCreated, country, true, string);
        event(account, AccountClosed);
        attribute(AccountClosed, closed_at, true, time);
                "#,
        );

        let expected = Schema {
            streams: vec![Stream {
                name: "account".to_string(),
                key: "account-id".to_string(),
                events: vec![
                    Event {
                        stream: "account".to_string(),
                        name: "AccountCreated".to_string(),
                        attributes: vec![
                            Attribute {
                                event: "AccountCreated".to_string(),
                                name: "owner-name".to_string(),
                                required: true,
                                attribute_type: "string".to_string(),
                            },
                            Attribute {
                                event: "AccountCreated".to_string(),
                                name: "country".to_string(),
                                required: true,
                                attribute_type: "string".to_string(),
                            },
                        ],
                    },
                    Event {
                        stream: "account".to_string(),
                        name: "AccountClosed".to_string(),
                        attributes: vec![Attribute {
                            event: "AccountClosed".to_string(),
                            name: "closed_at".to_string(),
                            required: true,
                            attribute_type: "time".to_string(),
                        }],
                    },
                ],
            }],
        };

        let result = match parse_schema(&schema) {
            Ok(value) => value,
            Err(error) => {
                println!("Error occurred: {:?}", error);
                panic!("Test failed due to error: {:?}", error);
            }
        };
        assert_eq!(sort_schema_events(result), sort_schema_events(expected));
    }

    #[test]
    fn test_handle_comments() {
        let schema = String::from(
            r#"
        // THIS IS SOME COMMENT
        stream(account, account-id);
        // THIS IS ANOTHER COMMENT
        // AND IT IS A MULTI BLOCK COMMENT
        event(account, AccountCreated); // COMMENT AFTER
        attribute(AccountCreated, owner-name, true, string);
  
                "#,
        );

        let expected = Schema {
            streams: vec![Stream {
                name: "account".to_string(),
                key: "account-id".to_string(),
                events: vec![Event {
                    stream: "account".to_string(),
                    name: "AccountCreated".to_string(),
                    attributes: vec![Attribute {
                        event: "AccountCreated".to_string(),
                        name: "owner-name".to_string(),
                        required: true,
                        attribute_type: "string".to_string(),
                    }],
                }],
            }],
        };

        let result = match parse_schema(&schema) {
            Ok(value) => value,
            Err(error) => {
                println!("Error occurred: {:?}", error);
                panic!("Test failed due to error: {:?}", error);
            }
        };

        assert_eq!(sort_schema_events(result), sort_schema_events(expected));
    }

    #[test]
    fn test_handle_stream_missing() {
        let schema = String::from(
            r#"
        stream(account, account-id);
        event(DOES_NOT_EXIST, AccountCreated);
                "#,
        );

        match parse_schema(&schema) {
            Ok(_) => panic!("expected schema parsing to fail"),
            Err(_e @ SchemaParserError::StreamNotFound) => println!("success"),
            Err(_) => panic!("incorrect error type returned"),
        };
    }

    #[test]
    fn test_handle_event_missing() {
        let schema = String::from(
            r#"
        stream(account, account-id);
        event(account, AccountCreated);
        attribute(DOES_NOT_EXIST, owner-name, true, string);
                "#,
        );

        match parse_schema(&schema) {
            Ok(_) => panic!("expected schema parsing to fail"),
            Err(_e @ SchemaParserError::EventNotFound) => println!("success"),
            Err(_) => panic!("incorrect error type returned"),
        };
    }
}
