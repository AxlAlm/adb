use crate::dbs;

const BLOCK_SEPERATOR: &str = ";";
const FIELDS_OPENER: &str = "(";
// const FIELDS_CLOSER: &str = ")";
const COMMENT_OPENER: &str = "//";

#[allow(dead_code)]
#[derive(Debug)]
pub enum CreateError {
    ParseError(String),
    CreateStreamError(String),
    CreateEventError(String),
    CreateAttributeError(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CreateOperation {
    CreateStream(CreateStream),
    CreateEvent(CreateEvent),
    CreateAttribute(CreateAttribute),
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateStream {
    pub name: String,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateEvent {
    pub name: String,
    pub stream_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateAttribute {
    pub name: String,
    pub event_name: String,
    pub stream_name: String,
    pub required: bool,
    pub attribute_type: String,
}

pub fn create(input: &str, db: &db::DB) -> Result<(), CreateError> {
    let create_operation = parse(input)?;
    // db.migrate(schema).map_err(|e| e.to_string())?;
    return Ok(());
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
fn create_stream(values: Vec<&str>) -> Result<CreateStream, CreateError> {
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

    Ok(CreateStream {
        name: name.to_string(),
        key,
    })
}

// test-stream, test -> event {stream: test-stream, name: test}
fn create_event(values: Vec<&str>) -> Result<CreateEvent, CreateError> {
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

    Ok(CreateEvent {
        name: name.to_string(),
        stream_name: stream_name.to_string(),
    })
}

// test-event, test, true, string ->
// attribute {event: event-name, name: test, required: true, attribute_type: str}
fn create_attribute(values: Vec<&str>) -> Result<CreateAttribute, CreateError> {
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

    Ok(CreateAttribute {
        stream_name: stream_name.to_string(),
        event_name: event_name.to_string(),
        name: name.to_string(),
        required,
        attribute_type,
    })
}

fn parse(input: &str) -> Result<CreateOperation, CreateError> {
    let block = input
        .trim()
        .splitn(2, BLOCK_SEPERATOR)
        .next()
        .ok_or_else(|| CreateError::ParseError("empty operation".to_string()))?;

    let block = String::from_iter(block.chars().filter(|x| !x.is_whitespace()));

    if block.is_empty() {
        return Err(CreateError::ParseError("empty operation".to_string()));
    }

    // create stream(...) -> stream()
    let clause = block
        .splitn(2, " ")
        .next()
        .ok_or_else(|| CreateError::ParseError("invalid operation".to_string()))?;

    let type_name = extract_block_type(&clause)?;
    let values = extract_fields(&clause)?;
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

        let expected = CreateOperation::CreateStream(CreateStream {
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

        let expected = CreateOperation::CreateEvent(CreateEvent {
            name: "AccountCreated".to_string(),
            stream_name: "account".to_string(),
        });

        assert_eq!(expected, result)
    }

    #[test]
    fn test_parse_create_attribute() {
        let input = String::from("attribute(account, AccountCreated, owner-name, true, string);");

        let result = match parse(&input) {
            Ok(value) => value,
            Err(error) => {
                println!("Error occurred: {:?}", error);
                panic!("Test failed due to error: {:?}", error);
            }
        };

        let expected = CreateOperation::CreateAttribute(CreateAttribute {
            name: "owner-name".to_string(),
            event_name: "AccountCreated".to_string(),
            stream_name: "account".to_string(),
            required: true,
            attribute_type: "string".to_string(),
        });

        assert_eq!(expected, result)
    }
}
