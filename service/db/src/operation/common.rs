use core::fmt;

#[derive(Debug, PartialEq)]
pub struct Operation {
    pub op_type: OperationType,
    pub body: String,
}

#[derive(Debug, PartialEq)]
pub enum OperationType {
    Add,
    Create,
}

pub enum CommonError {
    ParseError(String),
}

impl fmt::Display for CommonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommonError::ParseError(msg) => write!(f, "Parse Error: {}", msg),
        }
    }
}

const ADD_OPERATION_TYPE: &str = "add";
const CREATE_OPERATION_TYPE: &str = "create";

fn parse(input: &str) -> Result<Operation, CommonError> {
    let op_trimmed = input
        .trim()
        .splitn(2, ";")
        .next()
        .ok_or_else(|| CommonError::ParseError("empty operation".to_string()))?;

    if op_trimmed.is_empty() {
        return Err(CommonError::ParseError("empty operation".to_string()));
    }

    let op_trimmed = trim_comment(&op_trimmed);

    dbg!(&op_trimmed);

    let mut op_parts = op_trimmed.splitn(2, " ");
    let op_type = op_parts
        .next()
        .ok_or_else(|| CommonError::ParseError("missing operation type".to_string()))?
        .trim()
        .to_lowercase();

    let body: String = op_parts
        .next()
        .ok_or_else(|| CommonError::ParseError("missing operation type".to_string()))?
        .chars()
        .filter(|c| {
            c.is_alphanumeric()
                || *c == '-'
                || *c == ','
                || *c == ';'
                || *c == '('
                || *c == ')'
                || *c == ':'
                || *c == '"'
                || *c == '='
        })
        .collect();

    if body.is_empty() {
        return Err(CommonError::ParseError("missing body".to_string()));
    }

    match op_type.as_str() {
        ADD_OPERATION_TYPE => Ok(Operation {
            op_type: OperationType::Add,
            body,
        }),
        CREATE_OPERATION_TYPE => Ok(Operation {
            op_type: OperationType::Create,
            body,
        }),
        _ => Err(CommonError::ParseError(format!(
            "'{}' is not a supported operation",
            op_type
        ))),
    }
}

fn trim_comment(input: &str) -> String {
    let mut filtered = "".to_string();
    for line in input.lines() {
        if !line.trim().starts_with("//") {
            filtered += line
        }
    }

    return filtered.trim().to_string();
}

#[cfg(test)]
mod tests_op_common {

    use super::*;

    #[test]
    fn test_parse_add() {
        let input = r#"
        ADD AccountCreated(owner-name="axel") TO account:123;
    "#;

        let want = Operation {
            op_type: OperationType::Add,
            body: String::from(r#"AccountCreated(owner-name="axel")TOaccount:123"#),
        };

        let got = match parse(input) {
            Ok(o) => o,
            Err(e) => panic!("failed to parse input: {}", e),
        };

        assert_eq!(want, got)
    }

    #[test]
    fn test_parse_create() {
        let input = String::from("create event(account, AccountCreated);");

        let want = Operation {
            op_type: OperationType::Create,
            body: String::from("event(account,AccountCreated)"),
        };

        let got = match parse(&input) {
            Ok(o) => o,
            Err(e) => panic!("failed to parse input: {}", e),
        };

        assert_eq!(want, got)
    }

    #[test]
    fn test_parse_handle_whitespace() {
        let input = String::from("     create     event(account ,  AccountCreated )  ;");

        let want = Operation {
            op_type: OperationType::Create,
            body: String::from("event(account,AccountCreated)"),
        };

        let got = match parse(&input) {
            Ok(o) => o,
            Err(e) => panic!("failed to parse input: {}", e),
        };

        assert_eq!(want, got)
    }

    #[test]
    fn test_parse_create_handle_comment() {
        let input = String::from("create event(account, AccountCreated); // this is a comment");

        let want = Operation {
            op_type: OperationType::Create,
            body: String::from("event(account,AccountCreated)"),
        };

        let got = match parse(&input) {
            Ok(o) => o,
            Err(e) => panic!("failed to parse input: {}", e),
        };

        assert_eq!(want, got)
    }

    #[test]
    fn test_parse_create_multiline_w_comments() {
        let input = String::from(
            r#"
            // some comment
            create 
                event(
                        account,
                        AccountCreated
                );"#,
        );

        let want = Operation {
            op_type: OperationType::Create,
            body: String::from("event(account,AccountCreated)"),
        };

        let got = match parse(&input) {
            Ok(o) => o,
            Err(e) => panic!("failed to parse input: {}", e),
        };

        assert_eq!(want, got)
    }

    #[test]
    fn test_parse_unsupported_operation_type() {
        let input = String::from("NOTSUPPORTED event(account, AccountCreated);");
        match parse(&input) {
            Ok(_) => panic!("expected failure"),
            Err(_) => eprintln!("successfully failed"),
        };
    }

    #[test]
    fn test_parse_empty_command() {
        let input = String::from("");
        match parse(&input) {
            Ok(_) => panic!("expected failure"),
            Err(_) => eprintln!("successfully failed"),
        };
    }

    #[test]
    fn test_parse_empty_body() {
        let input = String::from("create ;");
        match parse(&input) {
            Ok(_) => panic!("expected failure"),
            Err(_) => eprintln!("successfully failed"),
        };
    }
}
