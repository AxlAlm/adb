use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Attribute {
    pub event: String,
    pub name: String,
    pub required: bool,
    pub attribute_type: String,
}

#[derive(Debug)]
pub struct Event {
    pub stream: String,
    pub name: String,
    pub attributes: Vec<Attribute>,
}

#[derive(Debug)]
pub struct Stream {
    pub name: String,
    pub key: String,
    pub events: Vec<Event>,
}

#[derive(Debug)]
pub struct Schema {
    pub streams: Vec<Stream>,
}

#[derive(Debug)]
pub enum ParseError {
    InvalidBLock,
    UnknownBlockType,
}

const BLOCK_SEPERATOR: &str = ";";
const FIELDS_OPENER: &str = "(";
const FIELDS_CLOSER: &str = ")";

// stream(...)  -> stream
fn extract_block_type(input: &str) -> Result<String, ParseError> {
    // if !input.contains(pat)
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

fn create_stream(values: Vec<&str>) -> Result<Stream, ParseError> {
    if values.len() != 2 {
        return Err(ParseError::InvalidBLock);
    }

    Ok(Stream {
        name: values[0].to_lowercase(),
        events: vec![],
        key: values[1].to_lowercase(),
    })
}

// event(<stream, name>, <event name>);
fn create_event(values: Vec<&str>) -> Result<Event, ParseError> {
    if values.len() != 2 {
        return Err(ParseError::InvalidBLock);
    }

    Ok(Attribute {
        stream: values[0].to_lowercase(),
        name: values[1].to_lowercase(),
        fields: vec![],
    })
}

// event(<stream, name>, <event name>);
fn create_attribute(values: Vec<&str>) -> Result<Event, ParseError> {
    if values.len() != 2 {
        return Err(ParseError::InvalidBLock);
    }

    Ok(Event {
        stream: values[0].to_lowercase(),
        name: values[1].to_lowercase(),
        fields: vec![],
    })
}

pub fn parse_schema(input: &str) -> Result<Schema, ParseError> {
    let cleaned = String::from_iter(input.chars().filter(|x| !x.is_whitespace()));

    let mut streams_map: HashMap<String, Stream> = HashMap::new();

    for block in cleaned.split(BLOCK_SEPERATOR) {
        if block.is_empty() {
            continue;
        }

        let block_type = extract_block_type(block)?;
        let values = extract_fields(block)?;

        dbg!("----");
        dbg!(&block, &block_type, &values);

        if block_type == "stream" {
            let stream = create_stream(values)?;
            dbg!(&stream);
            streams_map.insert(stream.name.clone(), stream);
        } else if block_type == "event" {
            let event = create_event(values)?;
            dbg!(&event);
        } else if block_type == "attribute" {
            let attribute = create_attribute(values)?;
            dbg!(&attributes);
        }

        // match block_type {
        //     "stream" => {
        //         dbg!("ok");
        //     }
        //     _ => Err(ParseError::UnknownBlockType),
        // }
    }

    let streams: Vec<Stream> = streams_map.into_values().collect();
    dbg!(&streams);
    Ok(Schema { streams })
}

// stream(<stream name>, <key name>);
// event(<stream, name>, <event name>);
// attribute(<event name>, <attribute name>, <required>, <type>);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_schema_parse() {
        let schema = String::from(
            r#"
        stream(account, account-id);
        event(AccountCreated, account);
        attribute(AccountCreated, owner-name, true, string);
                "#,
        );

        let result = parse_schema(&schema).unwrap();

        // Verify stream
        assert_eq!(result.streams.len(), 1);
        let stream = &result.streams[0];
        assert_eq!(stream.name, "Account");
        assert_eq!(stream.key, "account-id");

        // Verify events
        assert_eq!(stream.events.len(), 3);

        // Check AccountCreated event
        let account_created = &stream.events[0];
        assert_eq!(account_created.name, "AccountCreated");
        assert_eq!(account_created.fields.len(), 3);
        assert_eq!(account_created.fields[0].name, "account-id");
        assert_eq!(account_created.fields[1].name, "owner-name");
        assert_eq!(account_created.fields[2].name, "balance");

        // Check MoneyDeposited event
        let money_deposited = &stream.events[1];
        assert_eq!(money_deposited.name, "MoneyDeposited");
        assert_eq!(money_deposited.fields.len(), 2);
        assert_eq!(money_deposited.fields[0].name, "account-id");
        assert_eq!(money_deposited.fields[1].name, "amount");

        // Check MoneyWithdrawn event
        let money_withdrawn = &stream.events[2];
        assert_eq!(money_withdrawn.name, "MoneyWithdrawn");
        assert_eq!(money_withdrawn.fields.len(), 2);
        assert_eq!(money_withdrawn.fields[0].name, "account-id");
        assert_eq!(money_withdrawn.fields[1].name, "amount");
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
