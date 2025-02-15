const BLOCK_SEPERATOR: &str = ";";
const FIELDS_OPENER: &str = "(";
const FIELDS_CLOSER: &str = ")";
const COMMENT_OPENER: &str = "//";

const ADD_OP_OPENER: &str = "ADD";
const STREAM_INDICATOR: &str = "TO";

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

fn parse_mutation_line(input: &str) -> Result<AddEventMutation, MutationParserError> {
    // extract event
    // ADD AccountCreated(...) TO account; -> AccountCreated
    let i = match input.find(ADD_OP_OPENER) {
        Some(index) => index + 3,
        None => return Err(MutationParserError::InvalidMutation),
    };
    let j = match input.find(FIELDS_OPENER) {
        Some(index) => index,
        None => return Err(MutationParserError::InvalidMutation),
    };
    let event = input[i..j].trim().to_string();

    // extract stream and key
    // ADD AccountCreated(...) TO account:123; -> account, 123
    let splits: Vec<&str> = input.split(STREAM_INDICATOR).collect();
    if splits.len() != 2 {
        return Err(MutationParserError::InvalidMutation);
    }
    let (stream, key) = match splits[1].trim().split_once(":") {
        Some((x, y)) => (x.trim().to_string(), y.trim().to_string()),
        _ => return Err(MutationParserError::InvalidMutation),
    };

    // extract attributes
    // ADD AccountCreated(owner-name=axel ...) TO account; -> owner-name=axel ...
    let i = match input.find(FIELDS_OPENER) {
        Some(index) => index + 1,
        None => return Err(MutationParserError::InvalidMutation),
    };
    let j = match input.find(FIELDS_CLOSER) {
        Some(index) => index,
        None => return Err(MutationParserError::InvalidMutation),
    };
    let values: Vec<&str> = input[i..j].split(",").collect();

    let mut attributes = vec![];
    for v in values {
        let (name, val) = match v.split_once("=") {
            Some(x) => x,
            _ => return Err(MutationParserError::InvalidMutation),
        };

        attributes.push(Attribute {
            name: name.trim().to_string(),
            value: val.trim().to_string().replace('"', ""),
        });
    }

    return Ok(AddEventMutation {
        stream,
        key,
        event,
        attributes,
    });
}

pub fn parse_mutation(input: &str) -> Result<Vec<AddEventMutation>, MutationParserError> {
    let mut mutations = vec![];
    for line in input.lines() {
        // to solve cases where we have trailing comments;
        //     stream(x,y); // trailing comment
        let block = match line.trim().splitn(2, BLOCK_SEPERATOR).next() {
            Some(x) => x,
            _ => return Err(MutationParserError::InvalidMutation),
        };

        if block.is_empty() {
            continue;
        }

        if block.starts_with(COMMENT_OPENER) {
            continue;
        }

        if !block.starts_with(ADD_OP_OPENER) {
            continue;
        }

        let mutation = parse_mutation_line(&block)?;
        mutations.push(mutation);
    }

    Ok(mutations)
}

#[cfg(test)]
mod test {

    use super::*;
    use pretty_assertions::assert_eq;

    fn sort_attributes(mut mutations: Vec<AddEventMutation>) -> Vec<AddEventMutation> {
        mutations.sort_by(|a, b| a.stream.cmp(&b.stream));
        for mutation in &mut mutations {
            mutation.attributes.sort_by(|a, b| a.name.cmp(&b.name));
        }

        return mutations;
    }

    #[test]
    fn test_parse_mutation() {
        let input = String::from(
            r#"
            ADD AccountCreated(owner-name="axel", created_at="2025-01-02 14:00:00") TO account:123;
            "#,
        );

        let expected = vec![AddEventMutation {
            stream: "account".to_string(),
            key: "123".to_string(),
            event: "AccountCreated".to_string(),
            attributes: vec![
                Attribute {
                    name: "owner-name".to_string(),
                    value: "axel".to_string(),
                },
                Attribute {
                    name: "created_at".to_string(),
                    value: "2025-01-02 14:00:00".to_string(),
                },
            ],
        }];

        let mutations = match parse_mutation(&input) {
            Ok(x) => x,
            Err(_) => panic!("Got error expected none!"),
        };

        assert_eq!(sort_attributes(expected), sort_attributes(mutations))
    }

    #[test]
    fn test_parse_mutation_with_comments() {
        let input = String::from(
            r#"
            // COMMENT
            ADD AccountCreated(owner-name="axel", created_at="2025-01-02 14:00:00") TO account:123; // TRAILING
                                                                                                    // COMMENT
            "#,
        );

        let expected = vec![AddEventMutation {
            stream: "account".to_string(),
            key: "123".to_string(),
            event: "AccountCreated".to_string(),
            attributes: vec![
                Attribute {
                    name: "owner-name".to_string(),
                    value: "axel".to_string(),
                },
                Attribute {
                    name: "created_at".to_string(),
                    value: "2025-01-02 14:00:00".to_string(),
                },
            ],
        }];

        let mutations = match parse_mutation(&input) {
            Ok(x) => x,
            Err(_) => panic!("Got error expected none!"),
        };

        assert_eq!(sort_attributes(expected), sort_attributes(mutations))
    }
}
