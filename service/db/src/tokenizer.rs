use std::char;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    LiteralStr(String),
    LiteralInt(i64),
    LiteralFloat(f64),
    Identifier(String),
    CommandIdentifer(String),
    Auxiliary(String),
    EOF,              // ;
    AttributeBinding, // =
    Seperator,        // ,
    GroupStart,       // (
    GroupEnd,         // )
}

#[derive(Debug, Clone, PartialEq)]
pub struct TokenizerError {
    message: String,
    line_position: usize,
    char_position: usize,
}

impl TokenizerError {
    fn new(message: &str, line_position: usize, char_position: usize) -> Self {
        return TokenizerError {
            message: message.to_string(),
            line_position,
            char_position,
        };
    }
}

pub struct Tokens<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
    current_line_idx: usize,
    current_char_idx: usize,
    current_token_idx: usize,
}

fn is_group_start(c: &char) -> bool {
    return c == &'(';
}

fn is_group_end(c: &char) -> bool {
    return c == &')';
}

fn is_eof(c: &char) -> bool {
    return c == &';';
}

fn is_seperator(c: &char) -> bool {
    return c == &',';
}

fn is_attribute_binder(c: &char) -> bool {
    return c == &'=';
}

fn is_supported_identifier_literal_char(c: &char) -> bool {
    return c.is_alphanumeric() || c == &'_' || c == &'-' || c == &'"' || c == &'.';
}

impl<'a> Tokens<'a> {
    fn new(input: &'a str) -> Self {
        return Self {
            chars: input.chars().peekable(),
            current_line_idx: 0,
            current_char_idx: 0,
            current_token_idx: 0,
        };
    }

    fn next(&mut self) -> Result<Token, TokenizerError> {
        let is_first_token = self.current_char_idx == 0;
        let mut buffer: Vec<char> = vec![];

        loop {
            let c = self.chars.next().ok_or_else(|| TokenizerError {
                message: "unexpected end of input".to_string(),
                line_position: self.current_line_idx,
                char_position: self.current_char_idx,
            })?;

            self.current_char_idx += 1;
            if c.is_whitespace() {
                if c == '\n' {
                    self.current_line_idx += 1
                }
                continue;
            }

            if is_eof(&c) {
                break;
            }

            if is_seperator(&c) {
                return Ok(Token::Seperator);
            }

            if is_attribute_binder(&c) {
                return Ok(Token::AttributeBinding);
            }

            if is_group_start(&c) {
                return Ok(Token::GroupStart);
            }

            if is_group_end(&c) {
                return Ok(Token::GroupEnd);
            }

            if !is_supported_identifier_literal_char(&c) {
                return Err(TokenizerError::new(
                    &format!("found unsupported character {}", c),
                    self.current_line_idx,
                    self.current_char_idx,
                ));
            }

            buffer.push(c);

            // if next char is a contiuation on current token we continue
            // otherwise we will create and return a token and reset buffer
            let next_c = self.chars.peek().ok_or_else(|| TokenizerError {
                message: "unexpected end of input".to_string(),
                line_position: self.current_line_idx,
                char_position: self.current_char_idx,
            })?;
            if is_supported_identifier_literal_char(&next_c) {
                continue;
            }

            let buffer_string: String = buffer.iter().collect();

            if is_first_token {
                return Ok(Token::CommandIdentifer(buffer_string));
            } else if buffer_string == "on" || buffer_string == "to" {
                return Ok(Token::Auxiliary(buffer_string));
            } else if buffer[0].is_numeric() {
                if buffer_string.contains(".") {
                    let parsed_value = buffer_string.parse::<f64>().map_err(|_| {
                        TokenizerError::new(
                            "failed to parse float",
                            self.current_line_idx,
                            self.current_char_idx,
                        )
                    })?;
                    return Ok(Token::LiteralFloat(parsed_value));
                }

                let parsed_value = buffer_string.parse::<i64>().map_err(|_| {
                    TokenizerError::new(
                        "failed to parse int",
                        self.current_line_idx,
                        self.current_char_idx,
                    )
                })?;
                return Ok(Token::LiteralInt(parsed_value));
            } else if buffer[0] == '"' {
                return Ok(Token::LiteralStr(buffer_string.replace('"', "")));
            } else {
                return Ok(Token::Identifier(buffer_string));
            }
        }

        return Ok(Token::EOF);
    }
}

fn tokenize<'a>(input: &'a str) -> Tokens<'a> {
    return Tokens::new(input);
}

#[cfg(test)]
mod tokenizer_test {
    use super::{tokenize, Token};

    #[test]
    fn test_fail_with_missing_eof() {
        let test_cases = vec![("missing semicolon", "show schema", 1)];

        for (test_name, input, expected_token_count) in test_cases {
            let mut tokens = tokenize(input);

            // Consume the expected number of tokens
            for _ in 0..expected_token_count {
                tokens.next().unwrap();
            }

            // Next token should cause an error
            match tokens.next() {
                Ok(_) => panic!("Expected error was not raised in test case: {}", test_name),
                Err(e) => eprintln!(
                    "Expected error was raised in test case {}: {:?}",
                    test_name, e
                ),
            }
        }
    }

    #[test]
    fn test_show() {
        let test_cases = vec![
            (
                "normal",
                "show schema;",
                vec![
                    Token::CommandIdentifer("show".to_string()),
                    Token::Identifier("schema".to_string()),
                    Token::EOF,
                ],
            ),
            (
                "with spaces",
                "show     schema;",
                vec![
                    Token::CommandIdentifer("show".to_string()),
                    Token::Identifier("schema".to_string()),
                    Token::EOF,
                ],
            ),
            (
                "with newline",
                "show
             schema;",
                vec![
                    Token::CommandIdentifer("show".to_string()),
                    Token::Identifier("schema".to_string()),
                    Token::EOF,
                ],
            ),
        ];

        for (test_name, input, expected_tokens) in test_cases {
            let mut tokens = tokenize(input);

            for expected_token in expected_tokens {
                assert_eq!(
                    tokens.next().unwrap(),
                    expected_token,
                    "Failed in test case: {}",
                    test_name
                );
            }
        }
    }

    #[test]
    fn test_create_stream() {
        let test_cases = vec![
            (
                "normal",
                "create stream account;",
                vec![
                    Token::CommandIdentifer("create".to_string()),
                    Token::Identifier("stream".to_string()),
                    Token::Identifier("account".to_string()),
                    Token::EOF,
                ],
            ),
            (
                "with spaces",
                "create    stream   account;",
                vec![
                    Token::CommandIdentifer("create".to_string()),
                    Token::Identifier("stream".to_string()),
                    Token::Identifier("account".to_string()),
                    Token::EOF,
                ],
            ),
            (
                "with newline",
                "create    
             stream   
            
             account;",
                vec![
                    Token::CommandIdentifer("create".to_string()),
                    Token::Identifier("stream".to_string()),
                    Token::Identifier("account".to_string()),
                    Token::EOF,
                ],
            ),
        ];

        for (test_name, input, expected_tokens) in test_cases {
            let mut tokens = tokenize(input);

            for expected_token in expected_tokens {
                assert_eq!(
                    tokens.next().unwrap(),
                    expected_token,
                    "Failed in test case: {}",
                    test_name
                );
            }

            // No need to check for error since we expect to consume all tokens
        }
    }

    #[test]
    fn create_event() {
        let test_cases = vec![
            (
                "create event statement",
                "
                create event AccountCreated(
                    owner string
                ) on account;",
                vec![
                    Token::CommandIdentifer("create".to_string()),
                    Token::Identifier("event".to_string()),
                    Token::Identifier("AccountCreated".to_string()),
                    Token::GroupStart,
                    Token::Identifier("owner".to_string()),
                    Token::Identifier("string".to_string()),
                    Token::GroupEnd,
                    Token::Auxiliary("on".to_string()),
                    Token::Identifier("account".to_string()),
                    Token::EOF,
                ],
            ),
            (
                "create event multiple attributes",
                "
                create event AccountCreated(
                    owner string,
                    ammount int 
                ) on account;",
                vec![
                    Token::CommandIdentifer("create".to_string()),
                    Token::Identifier("event".to_string()),
                    Token::Identifier("AccountCreated".to_string()),
                    Token::GroupStart,
                    Token::Identifier("owner".to_string()),
                    Token::Identifier("string".to_string()),
                    Token::Seperator,
                    Token::Identifier("ammount".to_string()),
                    Token::Identifier("int".to_string()),
                    Token::GroupEnd,
                    Token::Auxiliary("on".to_string()),
                    Token::Identifier("account".to_string()),
                    Token::EOF,
                ],
            ),
        ];

        for (test_name, input, expected_tokens) in test_cases {
            let mut tokens = tokenize(input);

            for expected_token in expected_tokens {
                assert_eq!(
                    tokens.next().unwrap(),
                    expected_token,
                    "Failed in test case: {}",
                    test_name
                );
            }
        }
    }

    #[test]
    fn add_event() {
        let test_cases = vec![
            (
                "add event to account",
                r#"add AccountCreated(user_id="123", inital_amount=100.59, currency="SEK") to account(id="123");"#,
                vec![
                    Token::CommandIdentifer("add".to_string()),
                    Token::Identifier("AccountCreated".to_string()),
                    Token::GroupStart,
                    Token::Identifier("user_id".to_string()),
                    Token::AttributeBinding,
                    Token::LiteralStr("123".to_string()),
                    Token::Seperator,
                    Token::Identifier("inital_amount".to_string()),
                    Token::AttributeBinding,
                    Token::LiteralFloat(100.59),
                    Token::Seperator,
                    Token::Identifier("currency".to_string()),
                    Token::AttributeBinding,
                    Token::LiteralStr("SEK".to_string()),
                    Token::GroupEnd,
                    Token::Auxiliary("to".to_string()),
                    Token::Identifier("account".to_string()),
                    Token::GroupStart,
                    Token::Identifier("id".to_string()),
                    Token::AttributeBinding,
                    Token::LiteralStr("123".to_string()),
                    Token::GroupEnd,
                    Token::EOF,
                ],
            ),
            (
                "add event to accounti (int amount)",
                r#"add AccountCreated(inital_amount=100) to account(id="123");"#,
                vec![
                    Token::CommandIdentifer("add".to_string()),
                    Token::Identifier("AccountCreated".to_string()),
                    Token::GroupStart,
                    Token::Identifier("inital_amount".to_string()),
                    Token::AttributeBinding,
                    Token::LiteralInt(100),
                    Token::GroupEnd,
                    Token::Auxiliary("to".to_string()),
                    Token::Identifier("account".to_string()),
                    Token::GroupStart,
                    Token::Identifier("id".to_string()),
                    Token::AttributeBinding,
                    Token::LiteralStr("123".to_string()),
                    Token::GroupEnd,
                    Token::EOF,
                ],
            ),
            (
                "add event to account handles spaces and newlines",
                r#"add AccountCreated(
                        inital_amount   =100
                        ) 
                to account(id="123");"#,
                vec![
                    Token::CommandIdentifer("add".to_string()),
                    Token::Identifier("AccountCreated".to_string()),
                    Token::GroupStart,
                    Token::Identifier("inital_amount".to_string()),
                    Token::AttributeBinding,
                    Token::LiteralInt(100),
                    Token::GroupEnd,
                    Token::Auxiliary("to".to_string()),
                    Token::Identifier("account".to_string()),
                    Token::GroupStart,
                    Token::Identifier("id".to_string()),
                    Token::AttributeBinding,
                    Token::LiteralStr("123".to_string()),
                    Token::GroupEnd,
                    Token::EOF,
                ],
            ),
        ];

        for (test_name, input, expected_tokens) in test_cases {
            let mut tokens = tokenize(input);

            for expected_token in expected_tokens {
                assert_eq!(
                    tokens.next().unwrap(),
                    expected_token,
                    "Failed in test case: {}",
                    test_name
                );
            }

            assert_eq!(
                tokens.next().is_err(),
                true,
                "Should error after all tokens in test: {}",
                test_name
            );
        }
    }
}
