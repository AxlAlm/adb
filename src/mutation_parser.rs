#[derive(Debug)]
pub enum MutationParserError {
    InvalidMutation,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AddEventMutation {
    pub stream: String,
    pub key: String,
    pub event: String,
    pub attributes: Vec<Attribute>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    pub name: String,
    pub value: String,
}

pub fn parse_mutation(input: &str) -> Result<AddEventMutation, MutationParserError> {
    Ok(AddEventMutation {
        stream: "".to_string(),
        key: "".to_string(),
        event: "".to_string(),
        attributes: vec![],
    })
}

#[cfg(test)]
mod test {

    use super::*;
    use pretty_assertions::assert_eq;

    fn sort_attributes(mut mutation: AddEventMutation) -> AddEventMutation {
        mutation.attributes.sort_by(|a, b| a.name.cmp(&b.name));
        return mutation;
    }

    #[test]
    fn test_parse_mutation() {
        let input = String::from(
            r#"
            ADD AccountCreated(123, owner-name=axel, create_at=2025-01-02 14:00:00) TO account
            "#,
        );

        let expected = AddEventMutation {
            stream: "account".to_string(),
            key: "123".to_string(),
            event: "AccountCreated".to_string(),
            attributes: vec![Attribute {
                name: "owner-name".to_string(),
                value: "axel".to_string(),
            }],
        };

        let mutation = match parse_mutation(&input) {
            Ok(x) => x,
            Err(_) => panic!("Got error expected none!"),
        };

        assert_eq!(sort_attributes(expected), sort_attributes(mutation))
    }
}
