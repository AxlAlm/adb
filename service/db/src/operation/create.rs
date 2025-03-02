use crate::ast::schema::{Attribute, Event, Stream};
use crate::db::{self, DBError};

const BLOCK_SEPERATOR: &str = ";";
const FIELDS_OPENER: &str = "(";
// const FIELDS_CLOSER: &str = ")";
const COMMENT_OPENER: &str = "//";

#[allow(dead_code)]
#[derive(Debug)]
pub enum CreateError {
    ParseError(String),
    CreateError(String),
    CreateStreamError(String),
    CreateEventError(String),
    CreateAttributeError(String),
}

impl From<DBError> for CreateError {
    fn from(error: DBError) -> Self {
        CreateError::CreateError(error.to_string())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CreateOperation {
    CreateStream(Stream),
    CreateEvent(Event),
    CreateAttribute(Attribute),
}

pub fn create(input: &str, db: &db::DB) -> Result<(), CreateError> {
    let op = parse(input)?;
    match op {
        CreateOperation::CreateStream(op) => db.create_stream(op)?,
        CreateOperation::CreateEvent(op) => db.create_event(op)?,
        CreateOperation::CreateAttribute(op) => db.create_attribute(op)?,
    };
    Ok(())
}

// stream(...)  -> stream
fn extract_block_type(input: &str) -> Result<String, CreateError> {
    let splits: Vec<&str> = input.split("(").collect();
    if splits.len() != 2 {
        return Err(CreateError::ParseError(format!(
            "unable to extract type from '{}'",
            input
        )));
    }

    let block_type = splits[0].to_lowercase();
    return Ok(block_type);
}

// stream(hello, hello-id)  -> hello, hello-id
fn extract_fields(input: &str) -> Result<Vec<&str>, CreateError> {
    let i = match input.find(FIELDS_OPENER) {
        Some(index) => index + 1,
        None => {
            return Err(CreateError::ParseError(format!(
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
fn create_stream(values: Vec<&str>) -> Result<Stream, CreateError> {
    if values.len() != 2 {
        return Err(CreateError::CreateStreamError(format!(
            "unable to create stream from '{:?}'",
            values
        )));
    }

    let name = values[0].trim();
    if name.is_empty() {
        return Err(CreateError::CreateStreamError(
            "stream name is empty".to_string(),
        ));
    }

    let key = values[1].trim().to_string();
    if key.is_empty() {
        return Err(CreateError::CreateStreamError(
            "stream name is empty".to_string(),
        ));
    }

    Ok(Stream {
        name: name.to_string(),
        key,
    })
}

// test-stream, test -> event {stream: test-stream, name: test}
fn create_event(values: Vec<&str>) -> Result<Event, CreateError> {
    if values.len() != 2 {
        return Err(CreateError::CreateEventError(format!(
            "unable to create event from '{:?}'",
            values
        )));
    }

    let stream_name = values[0].trim();
    if stream_name.is_empty() {
        return Err(CreateError::CreateEventError(
            "stream name is empty".to_string(),
        ));
    }

    let name = values[1].trim();
    if name.is_empty() {
        return Err(CreateError::CreateEventError(
            "event name is empty".to_string(),
        ));
    }

    Ok(Event {
        name: name.to_string(),
        stream_name: stream_name.to_string(),
    })
}

// test-event, test, true, string ->
// attribute {event: event-name, name: test, required: true, attribute_type: str}
fn create_attribute(values: Vec<&str>) -> Result<Attribute, CreateError> {
    if values.len() != 5 {
        return Err(CreateError::CreateAttributeError(format!(
            "attribute is missing fields. Unable to create attribute from '{:?}'",
            values
        )));
    }

    let stream_name = values[0].trim();
    if stream_name.is_empty() {
        return Err(CreateError::CreateAttributeError(
            "stream name is empty".to_string(),
        ));
    }

    let event_name = values[1].trim();
    if event_name.is_empty() {
        return Err(CreateError::CreateAttributeError(
            "event name is empty".to_string(),
        ));
    }

    let name = values[2].trim();
    if name.is_empty() {
        return Err(CreateError::CreateAttributeError(
            "name is empty".to_string(),
        ));
    }

    let required_value = values[3].trim();
    if required_value.is_empty() {
        return Err(CreateError::CreateAttributeError(
            "required is empty".to_string(),
        ));
    }

    let required = match required_value {
        s if s == "true" => true,
        s if s == "false" => false,
        _ => {
            return Err(CreateError::CreateAttributeError(format!(
                "failed to create attribute. required field value is not true or false. value={}",
                required_value
            )))
        }
    };

    let attribute_type = values[4].trim().to_string();
    if attribute_type.is_empty() {
        return Err(CreateError::CreateAttributeError(
            "type is empty".to_string(),
        ));
    }

    Ok(Attribute {
        stream_name: stream_name.to_string(),
        event_name: event_name.to_string(),
        name: name.to_string(),
        required,
        attribute_type,
    })
}

fn parse(input: &str) -> Result<CreateOperation, CreateError> {
    let op_wo_comments = input
        .trim()
        .splitn(2, BLOCK_SEPERATOR)
        .next()
        .ok_or_else(|| CreateError::ParseError("empty operation".to_string()))?;

    if op_wo_comments.is_empty() {
        return Err(CreateError::ParseError("empty operation".to_string()));
    }

    if !op_wo_comments.to_lowercase().starts_with("create") {
        return Err(CreateError::ParseError(
            "not a create operation".to_string(),
        ));
    }
    let op_type_end_idx = op_wo_comments
        .char_indices()
        .take(6) // "create" has 6 characters
        .last()
        .map(|(idx, _)| idx + 1)
        .ok_or_else(|| CreateError::ParseError("empty operation".to_string()))?;

    let body = op_wo_comments[op_type_end_idx..].trim_start();
    let body = String::from_iter(body.chars().filter(|x| !x.is_whitespace()));

    let type_name = extract_block_type(&body)?;
    let values = extract_fields(&body)?;
    match type_name.as_str() {
        "stream" => Ok(CreateOperation::CreateStream(create_stream(values)?)),
        "event" => Ok(CreateOperation::CreateEvent(create_event(values)?)),
        "attribute" => Ok(CreateOperation::CreateAttribute(create_attribute(values)?)),
        _ => Err(CreateError::ParseError(format!(
            "unsupported type {}",
            type_name
        ))),
    }
}

fn starts_with_create(input: &str) -> bool {
    let trimmed = input.trim_start();
    trimmed.to_lowercase().starts_with("create")
}

#[cfg(test)]
mod parse_tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_parse_create_stream() {
        let input = String::from("create stream(account, account-id);");

        let result = match parse(&input) {
            Ok(value) => value,
            Err(error) => {
                println!("Error occurred: {:?}", error);
                panic!("Test failed due to error: {:?}", error);
            }
        };

        let expected = CreateOperation::CreateStream(Stream {
            name: "account".to_string(),
            key: "account-id".to_string(),
        });

        assert_eq!(expected, result)
    }

    #[test]
    fn test_parse_create_event() {
        let input = String::from("create event(account, AccountCreated);");

        let result = match parse(&input) {
            Ok(value) => value,
            Err(error) => {
                println!("Error occurred: {:?}", error);
                panic!("Test failed due to error: {:?}", error);
            }
        };

        let expected = CreateOperation::CreateEvent(Event {
            name: "AccountCreated".to_string(),
            stream_name: "account".to_string(),
        });

        assert_eq!(expected, result)
    }

    #[test]
    fn test_parse_create_attribute() {
        let input =
            String::from("create attribute(account, AccountCreated, owner-name, true, string);");

        let result = match parse(&input) {
            Ok(value) => value,
            Err(error) => {
                println!("Error occurred: {:?}", error);
                panic!("Test failed due to error: {:?}", error);
            }
        };

        let expected = CreateOperation::CreateAttribute(Attribute {
            name: "owner-name".to_string(),
            event_name: "AccountCreated".to_string(),
            stream_name: "account".to_string(),
            required: true,
            attribute_type: "string".to_string(),
        });

        assert_eq!(expected, result)
    }

    #[test]
    fn test_parse_create_many_spaces() {
        let input = String::from("  create    stream  (  account,     account-id  )   ;");

        let result = match parse(&input) {
            Ok(value) => value,
            Err(error) => {
                println!("Error occurred: {:?}", error);
                panic!("Test failed due to error: {:?}", error);
            }
        };

        let expected = CreateOperation::CreateStream(Stream {
            name: "account".to_string(),
            key: "account-id".to_string(),
        });

        assert_eq!(expected, result)
    }

    #[test]
    fn test_parse_create_with_comment() {
        let input = String::from("create stream(account, account-id); // this is a comment");

        let result = match parse(&input) {
            Ok(value) => value,
            Err(error) => {
                println!("Error occurred: {:?}", error);
                panic!("Test failed due to error: {:?}", error);
            }
        };

        let expected = CreateOperation::CreateStream(Stream {
            name: "account".to_string(),
            key: "account-id".to_string(),
        });

        assert_eq!(expected, result)
    }

    // #[test]
    // fn test_parse_create_multi_() {
    //     let input = String::from("create stream(account, account-id); // this is a comment");

    //     let result = match parse(&input) {
    //         Ok(value) => value,
    //         Err(error) => {
    //             println!("Error occurred: {:?}", error);
    //             panic!("Test failed due to error: {:?}", error);
    //         }
    //     };

    //     let expected = CreateOperation::CreateStream(Stream {
    //         name: "account".to_string(),
    //         key: "account-id".to_string(),
    //     });

    //     assert_eq!(expected, result)
    // }
}
