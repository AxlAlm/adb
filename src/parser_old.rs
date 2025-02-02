use std::collections::HashMap;

// const TOKEN_KEYWORD: &str = "KEYWORD";
// const TOKEN_IDENTIFIER: &str = "IDENTIFIER";
// const BLOCK_IDENTIFIER: &str = "BLOCK_IDENTIFIER";
// const TOKEN_ATTRIBUTE: &str = "ATTRIBUTE";
// const TOKEN_FIELD_NAME: &str = "FIELD_NAME";
// const STREAM_IDENTIFIER: &str = "STREAM";

// const EVENTS: &str = "EVENTS";

// const EVENT_IDENTIFIER: &str = "STREAM";

// Optional: If you want to be more specific about event identifiers being different
// pub const TOKEN_EVENT_IDENTIFIER: &str = "EVENT_IDENTIFIER";

// #[derive(Debug, PartialEq)]
// pub enum TokenType {
//     Keyword,
//     Identifier,
//     Attribute,
//     FieldName,
//     EventIdentifier,
// }

// // Or if you prefer string constants:
// fn get_token_type(token: &str) -> &'static str {
//     match token {
//         ":streams" | ":events" | ":fields" | ":key" => TOKEN_KEYWORD,
//         "Account" => TOKEN_IDENTIFIER,
//         "AccountCreated" | "MoneyDeposited" | "MoneyWithdrawn" => TOKEN_EVENT_IDENTIFIER,
//         "account-id" | "owner-name" | "balance" | "amount" => TOKEN_FIELD_NAME,
//         _ => "UNKNOWN",
//     }
// }

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub field_type: String, // We'll assume all fields are strings for now
}

#[derive(Debug)]
pub struct EventType {
    pub name: String,
    pub fields: Vec<Field>,
    pub constraints: Vec<String>,
}

#[derive(Debug)]
pub struct Stream {
    pub name: String,
    pub events: Vec<EventType>,
    pub key: String,
}

#[derive(Debug)]
pub struct Schema {
    pub streams: Vec<Stream>,
}

#[derive(Debug)]
pub enum ParseError {
    UnexpectedChar(char),
    UnexpectedEof,
    InvalidToken,
    ExpectedToken(&'static str),
    ExpectedIdentifier(String), // What we expected
    InvalidIdentifier(String),  // What we got
}

// pub fn parse_schema(input: &str) -> Result<Schema, ParseError> {
//     // if previous is [ and next is is_alphanumeric then we know we expect a name
//     // if current is : then we kn
//     //
//     let mut streams_map: HashMap<String, Stream> = HashMap::new();
//     let mut event_type_map: HashMap<String, EventType> = HashMap::new();

//     // token values
//     let mut current_keyword_name = "".to_string();
//     let mut current_stream_name = "".to_string();
//     let mut current_event_name = "".to_string();
//     let mut current_attribute_name = "".to_string();

//     // token buffer
//     let mut current_buffer: Vec<char> = vec![];

//     // token type
//     let mut current_semantic_token = "";

//     //  EXAMPLE
//     //
//     // {:streams                                    ; KEYWORD
//     //   [Account                                   ; IDENTIFIER
//     //     :events [                                ; ATTRIBUTE
//     //       [AccountCreated :fields [account-id]]   ; IDENTIFIER ATTRIBUTE [FIELD_NAMES]
//     //     ]
//     //     :key account-id                          ; ATTRIBUTE FIELD_NAME
//     //   ]
//     // }

//     for c in input.chars() {
//         if c.is_whitespace() {
//             continue;
//         }

//         if c == '{' {
//             dbg!("WHAT");
//             current_semantic_token = "STREAMS";
//             // current_token_type = TOKEN_IDENTIFIER;
//             continue;
//         }

//         if c == '[' || c == ':' {
//             if c == '[' && current_semantic_token == "STREAMS" {
//                 dbg!("moving to stream", &current_buffer);
//                 current_semantic_token = "STREAM";
//             } else if c == ':' && current_semantic_token == "STREAM" {
//                 dbg!(
//                     "moving to stream attributes events",
//                     &current_buffer,
//                     &current_stream_name
//                 );
//                 current_semantic_token = "STREAM_ATTRIBUTE_EVENTS";
//             } else if c == '[' && current_semantic_token == "STREAM_ATTRIBUTE_EVENTS" {
//                 dbg!("moving to event", &current_buffer, &current_stream_name);
//                 current_semantic_token = "EVENT";
//             } else if c == ':' && current_semantic_token == "EVENT" {
//                 dbg!(
//                     "moving to event attributes",
//                     &current_buffer,
//                     &current_stream_name
//                 );
//                 current_semantic_token = "EVENT_ATTRIBUTE_FEILDS";
//             }

//             dbg!("current semantic token", &current_semantic_token,);
//             if current_semantic_token == "STREAMS" {
//                 current_keyword_name = String::from_iter(current_buffer.iter());
//                 current_buffer = vec![];
//             } else if current_semantic_token == "STREAM" {
//                 dbg!("stream name done", &current_buffer);
//                 current_stream_name = String::from_iter(current_buffer.iter());
//                 streams_map.insert(
//                     current_stream_name.to_string(),
//                     Stream {
//                         name: current_stream_name.to_string(),
//                         events: vec![],
//                         key: "".to_string(),
//                     },
//                 );
//                 current_buffer = vec![];
//             } else if current_semantic_token == "EVENT" {
//                 current_event_name = String::from_iter(current_buffer.iter());
//                 event_type_map.insert(
//                     current_event_name.to_string(),
//                     EventType {
//                         name: current_event_name.to_string(),
//                         fields: vec![],
//                         constraints: vec![],
//                     },
//                 );

//                 current_buffer = vec![];
//             } else if current_semantic_token == "EVENT_ATTRIBUTE_FEILDS" {
//                 current_attribute_name = String::from_iter(current_buffer.iter());
//                 let event_type = event_type_map
//                     .get_mut(&current_event_name.to_string())
//                     .unwrap();
//                 event_type.fields.push(Field {
//                     name: current_attribute_name.to_string(),
//                     field_type: "string".to_string(),
//                 });

//                 current_buffer = vec![];
//             }
//         }

//         if c == ']' {
//             if current_semantic_token == "EVENT_ATTRIBUTE_FEILDS" {
//                 current_semantic_token = "EVENT";
//             }

//             if current_semantic_token == "EVENT" {
//                 let stream = streams_map
//                     .get_mut(&current_stream_name.to_string())
//                     .unwrap();
//                 let event_type = event_type_map
//                     .remove(&current_event_name.to_string())
//                     .unwrap();
//                 stream.events.push(event_type);

//                 current_semantic_token = "STREAM_ATTRIBUTE_EVENTS";
//             }

//             if current_semantic_token == "STREAM_ATTRIBUTE_EVENTS" {
//                 current_semantic_token = "STREAM";
//             }

//             if current_semantic_token == "STREAM" {}
//         }

//         // if we are not on
//         if c.is_alphanumeric() || c == '-' || c == '_' {
//             current_buffer.push(c);
//         }
//     }

//     let streams: Vec<Stream> = streams_map.into_values().collect();
//     dbg!(&streams);
//     Ok(Schema { streams })
// }

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
