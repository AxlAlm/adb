use std::collections::HashMap;

const BLOCK_SEPERATOR: &str = ";";
const FIELDS_OPENER: &str = "(";
const FIELDS_CLOSER: &str = ")";
const COMMENT_OPENER: &str = "//";

#[derive(Debug)]
pub enum ParseError {
    InvalidBLock,
    InvalidValue,
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
fn extract_block_type(input: &str) -> Result<String, ParseError> {
    let splits: Vec<&str> = input.split("(").collect();
    if splits.len() != 2 {
        return Err(ParseError::InvalidBLock);
    }

    let block_type = splits[0].to_lowercase();
    return Ok(block_type);
}

// stream(hello, hello-id)  -> hello, hello-id
fn extract_fields(input: &str) -> Result<Vec<&str>, ParseError> {
    let i = match input.find(FIELDS_OPENER) {
        Some(index) => index + 1,
        None => return Err(ParseError::InvalidBLock),
    };
    let j = input.len() - 1;
    let values = input[i..j].split(",").collect();
    return Ok(values);
}

// test, test-id -> stream {name: test, key:test-id}
fn create_stream(values: Vec<&str>) -> Result<Stream, ParseError> {
    if values.len() != 2 {
        return Err(ParseError::InvalidBLock);
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
fn create_event(values: Vec<&str>) -> Result<Event, ParseError> {
    if values.len() != 2 {
        return Err(ParseError::InvalidBLock);
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
fn create_attribute(values: Vec<&str>) -> Result<Attribute, ParseError> {
    if values.len() != 4 {
        return Err(ParseError::InvalidBLock);
    }

    let event = parse_value(values[0])?;
    let name = parse_value(values[1])?;
    let required_value = parse_value(values[2])?;
    let required = match required_value {
        s if s == "true" => true,
        s if s == "false" => false,
        _ => return Err(ParseError::InvalidValue),
    };
    let attribute_type = parse_value(values[3])?;

    Ok(Attribute {
        event,
        name,
        required,
        attribute_type,
    })
}

fn parse_value(input: &str) -> Result<String, ParseError> {
    if input.trim().is_empty() {
        return Err(ParseError::InvalidValue);
    }
    return Ok(input.to_string());
}

pub fn parse_schema(input: &str) -> Result<Schema, ParseError> {
    let mut streams_map: HashMap<String, Stream> = HashMap::new();
    let mut events_map: HashMap<String, Event> = HashMap::new();

    for line in input.lines() {
        // to solve cases where we have trailing comments;
        //     stream(x,y); // trailing comment
        let block = match line.trim().splitn(2, BLOCK_SEPERATOR).next() {
            Some(x) => x,
            _ => return Err(ParseError::InvalidBLock),
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
            let event = events_map.get_mut(&attribute.event).unwrap();
            event.attributes.push(attribute);
        }
    }

    for event in events_map.values() {
        dbg!(&streams_map);
        let stream = streams_map.get_mut(&event.stream).unwrap();
        stream.events.push(event.clone());
    }

    let streams: Vec<Stream> = streams_map.into_values().collect();
    dbg!(&streams);
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

    // #[test]
    // fn test_basic_schema_parse() {
    //     let schema = String::from(
    //         r#"{:streams
    //                 [Account
    //                     :events [
    //                         [AccountCreated :fields [account-id owner-name balance]]
    //                         [MoneyDeposited :fields [account-id amount]]
    //                         [MoneyWithdrawn :fields [account-id amount]]
    //                     ]
    //                     :key account-id]
    //             }"#,
    //     );

    //     let result = parse_schema(&schema).unwrap();

    //     // Verify stream
    //     assert_eq!(result.streams.len(), 1);
    //     let stream = &result.streams[0];
    //     assert_eq!(stream.name, "Account");
    //     assert_eq!(stream.key, "account-id");

    //     // Verify events
    //     assert_eq!(stream.events.len(), 3);

    //     // Check AccountCreated event
    //     let account_created = &stream.events[0];
    //     assert_eq!(account_created.name, "AccountCreated");
    //     assert_eq!(account_created.fields.len(), 3);
    //     assert_eq!(account_created.fields[0].name, "account-id");
    //     assert_eq!(account_created.fields[1].name, "owner-name");
    //     assert_eq!(account_created.fields[2].name, "balance");

    //     // Check MoneyDeposited event
    //     let money_deposited = &stream.events[1];
    //     assert_eq!(money_deposited.name, "MoneyDeposited");
    //     assert_eq!(money_deposited.fields.len(), 2);
    //     assert_eq!(money_deposited.fields[0].name, "account-id");
    //     assert_eq!(money_deposited.fields[1].name, "amount");

    //     // Check MoneyWithdrawn event
    //     let money_withdrawn = &stream.events[2];
    //     assert_eq!(money_withdrawn.name, "MoneyWithdrawn");
    //     assert_eq!(money_withdrawn.fields.len(), 2);
    //     assert_eq!(money_withdrawn.fields[0].name, "account-id");
    //     assert_eq!(money_withdrawn.fields[1].name, "amount");
    // }

    // #[test]
    // fn test_invalid_schema() {
    //     // Missing :events keyword
    //     let schema =
    //         String::from(r#"[:streams [Account [AccountCreated :fields [id]] :key account-id]]"#);
    //     let result = parse_schema(&schema);
    //     assert!(matches!(result, Err(ParseError::ExpectedToken(":events"))));

    //     // Missing :fields keyword
    //     let schema =
    //         String::from(r#"[:streams [Account :events [[AccountCreated [id]]] :key account-id]]"#);
    //     let result = parse_schema(&schema);
    //     assert!(matches!(result, Err(ParseError::ExpectedToken(":fields"))));

    //     // Missing :key keyword
    //     let schema = String::from(
    //         r#"[:streams [Account :events [[AccountCreated :fields [id]]] account-id]]"#,
    //     );
    //     let result = parse_schema(&schema);
    //     assert!(matches!(result, Err(ParseError::ExpectedToken(":key"))));
    // }

    // #[test]
    // fn test_empty_schema() {
    //     let schema = String::from("[:streams]");
    //     let result = parse_schema(&schema).unwrap();
    //     assert_eq!(result.streams.len(), 0);
    // }

    // #[test]
    // fn test_multiple_streams() {
    //     let schema = String::from(
    //         r#"[:streams
    //         [Account :events [[AccountCreated :fields [id]]] :key id]
    //         [User :events [[UserCreated :fields [id name]]] :key id]
    //     ]"#,
    //     );

    //     let result = parse_schema(&schema).unwrap();
    //     assert_eq!(result.streams.len(), 2);
    //     assert_eq!(result.streams[0].name, "Account");
    //     assert_eq!(result.streams[1].name, "User");
    // }

    // #[test]
    // fn test_comment() {
    //     let schema = String::from(
    //         r#"[:streams
    //         // some comment 1
    //         [Account :events [[AccountCreated :fields [id]]] :key id]
    //         // some comment 2
    //         [User :events [[UserCreated :fields [id name]]] :key id]
    //     ]"#,
    //     );

    //     let result = parse_schema(&schema).unwrap();
    //     assert_eq!(result.streams.len(), 2);
    //     assert_eq!(result.streams[0].name, "Account");
    //     assert_eq!(result.streams[1].name, "User");
    // }
}
