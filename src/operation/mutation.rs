use crate::db::DB;
use crate::event::Event;
use crate::parser;

// TODO! FIX ERROR HANDLING HERE
pub fn mutate(input: &str, db: &DB) -> Result<(), String> {
    let mutations = match parser::mutation::parse(input) {
        Ok(x) => x,
        Err(_) => panic!("failed to parse mutation"),
    };

    for mutation in mutations {
        db.get_schema()
            .map_err(|e| e.to_string())?
            .validate_mutation(&mutation)?;

        let latest_version = db
            .get_events(mutation.stream.to_string(), mutation.key.to_string())
            .map_err(|e| e.to_string())?
            .unwrap_or_default()
            .last()
            .map_or(0, |event| event.version);

        let new_version = latest_version + 1;
        let event = Event::new(mutation, new_version)?;
        db.add_event(event).map_err(|e| e.to_string())?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use crate::ast::schema;

    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_mutate_valid() {
        let schema = schema::Schema {
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

        let input = r#"
        ADD AccountCreated(owner-name="axel") TO account:123
    "#;

        let db = DB::new(Some(schema));

        match mutate(input, &db) {
            Ok(_) => println!("Success"),
            Err(e) => panic!("Failed. Got error {}", e),
        }
    }

    #[test]
    fn test_mutate_stream_invalid() {
        let schema = schema::Schema {
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

        let input = r#"
        ADD AccountCreated(owner-name="axel") TO NON_EXISTENT_STREAM:123
    "#;

        let db = DB::new(Some(schema));

        match mutate(input, &db) {
            Ok(_) => panic!("expected error"),
            Err(e) => println!("success. Got error {}", e),
        }
    }
}
