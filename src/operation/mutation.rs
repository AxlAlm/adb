use crate::ast::mutation::AddEventMutation;
use crate::ast::schema::Schema;

pub fn mutate(mutation: AddEventMutation, schema: &Schema) -> Result<String, String> {
    schema.validate_mutation(&mutation)?;
    Ok("".to_string())
}

#[cfg(test)]
mod tests {

    use crate::ast::mutation;
    use crate::ast::schema;

    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_mutate_valid() {
        let schema = Schema {
            streams: HashMap::from([(
                schema::StreamName("account".to_string()),
                schema::Stream {
                    name: schema::StreamName("account".to_string()),
                    key: "account-id".to_string(),
                },
            )]),
            events: HashMap::from([(
                (
                    schema::StreamName("account".to_string()),
                    schema::EventName("AccountCreated".to_string()),
                ),
                schema::Event {
                    name: schema::EventName("AccountCreated".to_string()),
                    stream_name: schema::StreamName("account".to_string()),
                },
            )]),
            attributes: HashMap::from([(
                (
                    schema::StreamName("account".to_string()),
                    schema::EventName("AccountCreated".to_string()),
                    schema::AttributeName("owner-name".to_string()),
                ),
                schema::Attribute {
                    name: schema::AttributeName("owner-name".to_string()),
                    event_name: schema::EventName("AccountCreated".to_string()),
                    stream_name: schema::StreamName("account".to_string()),
                    required: true,
                    attribute_type: "string".to_string(),
                },
            )]),
        };

        let mutation = mutation::AddEventMutation {
            stream: "account".to_string(),
            key: "123".to_string(),
            event: "AccountCreated".to_string(),
            attributes: vec![mutation::Attribute {
                name: "owner-name".to_string(),
                value: "axel".to_string(),
            }],
        };

        match mutate(mutation, &schema) {
            Ok(_) => println!("Success"),
            Err(e) => panic!("Failed. Got error {}", e),
        }
    }

    #[test]
    fn test_mutate_stream_invalid() {
        let schema = Schema {
            streams: HashMap::from([(
                schema::StreamName("account".to_string()),
                schema::Stream {
                    name: schema::StreamName("account".to_string()),
                    key: "account-id".to_string(),
                },
            )]),
            events: HashMap::from([(
                (
                    schema::StreamName("account".to_string()),
                    schema::EventName("AccountCreated".to_string()),
                ),
                schema::Event {
                    name: schema::EventName("AccountCreated".to_string()),
                    stream_name: schema::StreamName("account".to_string()),
                },
            )]),
            attributes: HashMap::from([(
                (
                    schema::StreamName("account".to_string()),
                    schema::EventName("AccountCreated".to_string()),
                    schema::AttributeName("owner-name".to_string()),
                ),
                schema::Attribute {
                    name: schema::AttributeName("owner-name".to_string()),
                    event_name: schema::EventName("AccountCreated".to_string()),
                    stream_name: schema::StreamName("account".to_string()),
                    required: true,
                    attribute_type: "string".to_string(),
                },
            )]),
        };

        let mutation = mutation::AddEventMutation {
            stream: "NON_EXISTENT_STREAM".to_string(),
            key: "123".to_string(),
            event: "AccountCreated".to_string(),
            attributes: vec![mutation::Attribute {
                name: "owner-name".to_string(),
                value: "axel".to_string(),
            }],
        };

        match mutate(mutation, &schema) {
            Ok(_) => panic!("expected error"),
            Err(e) => println!("success. Got error {}", e),
        }
    }
}
