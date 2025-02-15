use std::collections::HashMap;

use crate::ast::schema::{Attribute, AttributeName, Event, EventName, Schema, Stream, StreamName};

const BLOCK_SEPERATOR: &str = ";";
const FIELDS_OPENER: &str = "(";
// const FIELDS_CLOSER: &str = ")";
const COMMENT_OPENER: &str = "//";

#[allow(dead_code)]
#[derive(Debug)]
pub enum SchemaParserError {
    InvalidBlock(String),
    CreateStreamError(String),
    CreateEventError(String),
    CreateAttributeError(String),
}

// stream(...)  -> stream
fn extract_block_type(input: &str) -> Result<String, SchemaParserError> {
    let splits: Vec<&str> = input.split("(").collect();
    if splits.len() != 2 {
        return Err(SchemaParserError::InvalidBlock(format!(
            "unable to extract type from '{}'",
            input
        )));
    }

    let block_type = splits[0].to_lowercase();
    return Ok(block_type);
}

// stream(hello, hello-id)  -> hello, hello-id
fn extract_fields(input: &str) -> Result<Vec<&str>, SchemaParserError> {
    let i = match input.find(FIELDS_OPENER) {
        Some(index) => index + 1,
        None => {
            return Err(SchemaParserError::InvalidBlock(format!(
                "unable to extract fields from '{}'",
                input
            )))
        }
    };
    let j = input.len() - 1;
    let values = input[i..j].split(",").collect();
    return Ok(values);
}

// test, test-id -> stream {name: test, key:test-id}
fn create_stream(values: Vec<&str>) -> Result<Stream, SchemaParserError> {
    if values.len() != 2 {
        return Err(SchemaParserError::CreateStreamError(format!(
            "unable to create stream from '{:?}'",
            values
        )));
    }

    let name = values[0].trim();
    if name.is_empty() {
        return Err(SchemaParserError::CreateStreamError(
            "stream name is empty".to_string(),
        ));
    }

    let key = values[1].trim().to_string();
    if key.is_empty() {
        return Err(SchemaParserError::CreateStreamError(
            "stream name is empty".to_string(),
        ));
    }

    Ok(Stream {
        name: StreamName::new(name),
        key,
    })
}

// test-stream, test -> event {stream: test-stream, name: test}
fn create_event(values: Vec<&str>) -> Result<Event, SchemaParserError> {
    if values.len() != 2 {
        return Err(SchemaParserError::CreateEventError(format!(
            "unable to create event from '{:?}'",
            values
        )));
    }

    let stream_name = values[0].trim();
    if stream_name.is_empty() {
        return Err(SchemaParserError::CreateEventError(
            "stream name is empty".to_string(),
        ));
    }

    let name = values[1].trim();
    if name.is_empty() {
        return Err(SchemaParserError::CreateEventError(
            "event name is empty".to_string(),
        ));
    }

    Ok(Event {
        name: EventName::new(name),
        stream_name: StreamName::new(stream_name),
    })
}

// test-event, test, true, string ->
// attribute {event: event-name, name: test, required: true, attribute_type: str}
fn create_attribute(values: Vec<&str>) -> Result<Attribute, SchemaParserError> {
    if values.len() != 5 {
        return Err(SchemaParserError::CreateAttributeError(format!(
            "attribute is missing fields. Unable to create attribute from '{:?}'",
            values
        )));
    }

    let stream_name = values[0].trim();
    if stream_name.is_empty() {
        return Err(SchemaParserError::CreateAttributeError(
            "stream name is empty".to_string(),
        ));
    }

    let event_name = values[1].trim();
    if event_name.is_empty() {
        return Err(SchemaParserError::CreateAttributeError(
            "event name is empty".to_string(),
        ));
    }

    let name = values[2].trim();
    if name.is_empty() {
        return Err(SchemaParserError::CreateAttributeError(
            "name is empty".to_string(),
        ));
    }

    let required_value = values[3].trim();
    if required_value.is_empty() {
        return Err(SchemaParserError::CreateAttributeError(
            "required is empty".to_string(),
        ));
    }

    let required = match required_value {
        s if s == "true" => true,
        s if s == "false" => false,
        _ => {
            return Err(SchemaParserError::CreateAttributeError(format!(
                "failed to create attribute. required field value is not true or false. value={}",
                required_value
            )))
        }
    };

    let attribute_type = values[4].trim().to_string();
    if attribute_type.is_empty() {
        return Err(SchemaParserError::CreateAttributeError(
            "type is empty".to_string(),
        ));
    }

    Ok(Attribute {
        stream_name: StreamName::new(stream_name),
        event_name: EventName::new(event_name),
        name: AttributeName::new(name),
        required,
        attribute_type,
    })
}

// fn parse_value(input: &str, field: String) -> Result<String, SchemaParserError> {
//     if input.trim().is_empty() {
//         return Err(SchemaParserError::InvalidField(format!(
//             "value for field {} is empty",
//             field
//         )));
//     }
//     return Ok(input.to_string());
// }

pub fn parse(input: &str) -> Result<Schema, SchemaParserError> {
    let mut streams: HashMap<StreamName, Stream> = HashMap::new();
    let mut events: HashMap<(StreamName, EventName), Event> = HashMap::new();
    let mut attributes: HashMap<(StreamName, EventName, AttributeName), Attribute> = HashMap::new();

    for line in input.lines() {
        // to solve cases where we have trailing comments;
        //     stream(x,y); // trailing comment
        let block = match line.trim().splitn(2, BLOCK_SEPERATOR).next() {
            Some(x) => x,
            _ => {
                return Err(SchemaParserError::InvalidBlock(format!(
                    "block missing ';'. Block: {}",
                    line
                )))
            }
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

        dbg!(&block_type, &values);

        if block_type == "stream" {
            let stream = create_stream(values)?;
            streams.insert(stream.name.clone(), stream);
        } else if block_type == "event" {
            let event = create_event(values)?;
            if streams.get(&event.stream_name).is_none() {
                return Err(SchemaParserError::CreateEventError(format!(
                    "stream {} does not exist",
                    event.stream_name.clone(),
                )));
            }
            events.insert(
                (event.stream_name.clone(), event.name.clone()),
                event.clone(),
            );
        } else if block_type == "attribute" {
            let attribute = create_attribute(values)?;

            if events
                .get(&(attribute.stream_name.clone(), attribute.event_name.clone()))
                .is_none()
            {
                return Err(SchemaParserError::CreateAttributeError(format!(
                    "stream and event ({}, {})  does not exist",
                    attribute.stream_name, attribute.event_name,
                )));
            }

            attributes.insert(
                (
                    attribute.stream_name.clone(),
                    attribute.event_name.clone(),
                    attribute.name.clone(),
                ),
                attribute.clone(),
            );
        }
    }

    Ok(Schema {
        streams,
        events,
        attributes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_basic_schema_parse() {
        let schema = String::from(
            r#"
        stream(account, account-id);
        event(account, AccountCreated);
        attribute(account, AccountCreated, owner-name, true, string);
        attribute(account, AccountCreated, country, true, string);
        event(account, AccountClosed);
        attribute(account, AccountClosed, closed_at, true, time);
                "#,
        );

        let expected = Schema {
            streams: HashMap::from([(
                StreamName("account".to_string()),
                Stream {
                    name: StreamName("account".to_string()),
                    key: "account-id".to_string(),
                },
            )]),
            events: HashMap::from([
                (
                    (
                        StreamName("account".to_string()),
                        EventName("AccountCreated".to_string()),
                    ),
                    Event {
                        name: EventName("AccountCreated".to_string()),
                        stream_name: StreamName("account".to_string()),
                    },
                ),
                (
                    (
                        StreamName("account".to_string()),
                        EventName("AccountClosed".to_string()),
                    ),
                    Event {
                        name: EventName("AccountClosed".to_string()),
                        stream_name: StreamName("account".to_string()),
                    },
                ),
            ]),
            attributes: HashMap::from([
                (
                    (
                        StreamName("account".to_string()),
                        EventName("AccountCreated".to_string()),
                        AttributeName("owner-name".to_string()),
                    ),
                    Attribute {
                        name: AttributeName("owner-name".to_string()),
                        event_name: EventName("AccountCreated".to_string()),
                        stream_name: StreamName("account".to_string()),
                        required: true,
                        attribute_type: "string".to_string(),
                    },
                ),
                (
                    (
                        StreamName("account".to_string()),
                        EventName("AccountCreated".to_string()),
                        AttributeName("country".to_string()),
                    ),
                    Attribute {
                        name: AttributeName("country".to_string()),
                        event_name: EventName("AccountCreated".to_string()),
                        stream_name: StreamName("account".to_string()),
                        required: true,
                        attribute_type: "string".to_string(),
                    },
                ),
                (
                    (
                        StreamName("account".to_string()),
                        EventName("AccountClosed".to_string()),
                        AttributeName("closed_at".to_string()),
                    ),
                    Attribute {
                        name: AttributeName("closed_at".to_string()),
                        event_name: EventName("AccountClosed".to_string()),
                        stream_name: StreamName("account".to_string()),
                        required: true,
                        attribute_type: "time".to_string(),
                    },
                ),
            ]),
        };
        let result = match parse(&schema) {
            Ok(value) => value,
            Err(error) => {
                println!("Error occurred: {:?}", error);
                panic!("Test failed due to error: {:?}", error);
            }
        };

        assert_eq!(expected, result)
    }

    // #[test]
    // fn test_duplicates() {
    //     let schema = String::from(
    //         r#"
    //     stream(account, account-id);
    //     event(account, AccountCreated);
    //     event(account, AccountCreated);
    //     event(account, AccountCreated);
    //     attribute(AccountCreated, owner-name, true, string);
    //     attribute(AccountCreated, country, true, string);
    //     attribute(AccountCreated, country, true, string);
    //     attribute(AccountCreated, country, true, string);
    //             "#,
    //     );

    //     let expected = Schema {
    //         streams: vec![Stream {
    //             name: "account".to_string(),
    //             key: "account-id".to_string(),
    //             events: vec![Event {
    //                 stream: "account".to_string(),
    //                 name: "AccountCreated".to_string(),
    //                 attributes: vec![
    //                     Attribute {
    //                         event: "AccountCreated".to_string(),
    //                         name: "owner-name".to_string(),
    //                         required: true,
    //                         attribute_type: "string".to_string(),
    //                     },
    //                     Attribute {
    //                         event: "AccountCreated".to_string(),
    //                         name: "country".to_string(),
    //                         required: true,
    //                         attribute_type: "string".to_string(),
    //                     },
    //                 ],
    //             }],
    //         }],
    //     };

    //     let result = match parse_schema(&schema) {
    //         Ok(value) => value,
    //         Err(error) => {
    //             println!("Error occurred: {:?}", error);
    //             panic!("Test failed due to error: {:?}", error);
    //         }
    //     };
    //     assert_eq!(sort_schema_events(result), sort_schema_events(expected));
    // }

    #[test]
    fn test_handle_comments() {
        let schema = String::from(
            r#"
        // THIS IS SOME COMMENT
        stream(account, account-id);
        // THIS IS ANOTHER COMMENT
        // AND IT IS A MULTI BLOCK COMMENT
        event(account, AccountCreated); // COMMENT AFTER
        attribute(account, AccountCreated, owner-name, true, string);
  
                "#,
        );

        let expected = Schema {
            streams: HashMap::from([(
                StreamName("account".to_string()),
                Stream {
                    name: StreamName("account".to_string()),
                    key: "account-id".to_string(),
                },
            )]),
            events: HashMap::from([(
                (
                    StreamName("account".to_string()),
                    EventName("AccountCreated".to_string()),
                ),
                Event {
                    name: EventName("AccountCreated".to_string()),
                    stream_name: StreamName("account".to_string()),
                },
            )]),
            attributes: HashMap::from([(
                (
                    StreamName("account".to_string()),
                    EventName("AccountCreated".to_string()),
                    AttributeName("owner-name".to_string()),
                ),
                Attribute {
                    name: AttributeName("owner-name".to_string()),
                    event_name: EventName("AccountCreated".to_string()),
                    stream_name: StreamName("account".to_string()),
                    required: true,
                    attribute_type: "string".to_string(),
                },
            )]),
        };

        let result = match parse(&schema) {
            Ok(value) => value,
            Err(error) => {
                println!("Error occurred: {:?}", error);
                panic!("Test failed due to error: {:?}", error);
            }
        };

        assert_eq!(result, expected);
    }

    #[test]
    fn test_handle_stream_missing() {
        let schema = String::from(
            r#"
        stream(account, account-id);
        event(DOES_NOT_EXIST, AccountCreated);
                "#,
        );

        match parse(&schema) {
            Ok(_) => panic!("expected schema parsing to fail"),
            Err(_e @ SchemaParserError::CreateEventError(_)) => println!("success"),
            Err(_) => panic!("incorrect error type returned"),
        };
    }

    #[test]
    fn test_handle_event_missing() {
        let schema = String::from(
            r#"
        stream(account, account-id);
        event(account, AccountCreated);
        attribute(account, DOES_NOT_EXIST, owner-name, true, string);
                "#,
        );

        match parse(&schema) {
            Ok(_) => panic!("expected schema parsing to fail"),
            Err(_e @ SchemaParserError::CreateAttributeError(_)) => println!("success"),
            Err(_) => panic!("incorrect error type returned"),
        };
    }
}
