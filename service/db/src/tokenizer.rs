use std::char;

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    Literal,
    Identifier,
    CommandIdentifer,
    Auxiliary,
    // EOF, // no needed yet, but when we have transactions?
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

#[derive(Debug, Clone, PartialEq)]
pub enum TokenValue {
    Int(i64),
    Float(f64),
    String(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub value: TokenValue,
}

pub struct Tokens {
    string: String,
    current_line_idx: usize,
    current_char_idx: usize,
}

impl Tokens {
    fn new(string: String) -> Self {
        return Self {
            string,
            current_line_idx: 0,
            current_char_idx: 0,
        };
    }

    fn next(&mut self) -> Result<Token, TokenizerError> {
        let is_first_token = self.current_char_idx == 0;
        let mut buffer: Vec<char> = vec![];

        for c in self.string[self.current_char_idx..].chars() {
            self.current_char_idx += 1;

            if c.is_whitespace()
                || c == ';'
                || c == '('
                || c == ')'
                || c == ','
                || c == '='
                || c == ':'
            {
                if c == '\n' {
                    self.current_line_idx += 1
                }

                if buffer.len() != 0 {
                    break;
                }
                continue;
            }

            buffer.push(c);
        }

        let first_char = match buffer.first() {
            Some(c) => c.clone(),
            None => {
                return Err(TokenizerError {
                    message: "failed to parse token".to_string(),
                    line_position: self.current_line_idx,
                    char_position: self.current_char_idx,
                })
            }
        };

        let buffer_string = buffer.iter().collect();

        let kind: TokenKind;
        let value: TokenValue;

        if buffer_string == "on" || buffer_string == "to" {
            kind = TokenKind::Auxiliary;
            value = TokenValue::String(buffer_string);
        } else if is_first_token {
            kind = TokenKind::CommandIdentifer;
            value = TokenValue::String(buffer_string);
        } else if first_char.is_numeric() && buffer_string.contains(".") {
            let parsed_value = buffer_string.parse::<f64>().map_err(|_| {
                TokenizerError::new(
                    "failed to parse float",
                    self.current_line_idx,
                    self.current_char_idx,
                )
            })?;
            kind = TokenKind::Literal;
            value = TokenValue::Float(parsed_value);
        } else if first_char.is_numeric() {
            let parsed_value = buffer_string.parse::<i64>().map_err(|_| {
                TokenizerError::new(
                    "failed to parse int",
                    self.current_line_idx,
                    self.current_char_idx,
                )
            })?;
            kind = TokenKind::Literal;
            value = TokenValue::Int(parsed_value);
        } else if first_char != '"' {
            kind = TokenKind::Identifier;
            value = TokenValue::String(buffer_string);
        } else if first_char == '"' {
            kind = TokenKind::Literal;
            value = TokenValue::String(buffer_string.replace('"', ""));
        } else {
            return Err(TokenizerError::new(
                &format!("unexpected token. Unable to tokenize {}", buffer_string),
                self.current_line_idx,
                self.current_char_idx,
            ));
        }

        return Ok(Token { kind, value });
    }
}

fn tokenize(input: String) -> Tokens {
    return Tokens::new(input);
}

#[cfg(test)]
mod tokenizer_test_show {
    use super::{tokenize, Token, TokenKind, TokenValue};

    #[test]
    fn test_tokenizer_show() {
        let mut tokens = tokenize(String::from("show schema"));

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::CommandIdentifer,
                value: TokenValue::String("show".to_string()),
            },
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Identifier,
                value: TokenValue::String("schema".to_string()),
            }
        );
    }

    #[test]
    fn test_tokenizer_show_with_spaces() {
        let mut tokens = tokenize(String::from("show     schema"));

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::CommandIdentifer,
                value: TokenValue::String("show".to_string()),
            },
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Identifier,
                value: TokenValue::String("schema".to_string()),
            }
        );
    }

    #[test]
    fn test_tokenizer_show_with_newlines() {
        let mut tokens = tokenize(String::from(
            "show     


                schema",
        ));

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::CommandIdentifer,
                value: TokenValue::String("show".to_string()),
            },
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Identifier,
                value: TokenValue::String("schema".to_string()),
            }
        );
    }
}

#[cfg(test)]
mod tokenizer_test_create {

    use super::{tokenize, Token, TokenKind, TokenValue};

    #[test]
    fn test_tokenize_create_stream() {
        let mut tokens = tokenize(String::from("create stream account;"));

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::CommandIdentifer,
                value: TokenValue::String("create".to_string()),
            },
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Identifier,
                value: TokenValue::String("stream".to_string()),
            }
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Identifier,
                value: TokenValue::String("account".to_string()),
            }
        );

        assert_eq!(tokens.next().is_err(), true);
    }

    #[test]
    fn test_tokenize_create_event() {
        let mut tokens = tokenize(String::from(
            "
            create event AccountCreated(
                owner string
            ) on account;",
        ));

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::CommandIdentifer,
                value: TokenValue::String("create".to_string()),
            },
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Identifier,
                value: TokenValue::String("event".to_string()),
            }
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Identifier,
                value: TokenValue::String("AccountCreated".to_string()),
            }
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Identifier,
                value: TokenValue::String("owner".to_string()),
            }
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Identifier,
                value: TokenValue::String("string".to_string()),
            }
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Auxiliary,
                value: TokenValue::String("on".to_string()),
            }
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Identifier,
                value: TokenValue::String("account".to_string()),
            }
        );

        assert_eq!(tokens.next().is_err(), true);
    }
}

#[cfg(test)]
mod tokenizer_test_add {
    use super::{tokenize, Token, TokenKind, TokenValue};

    #[test]
    fn test_tokenize_add_event() {
        let mut tokens = tokenize(String::from(
            r#"add AccountCreated(user_id="123", inital_amount=100.59, currency="SEK") to account(id="123");"#,
        ));

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::CommandIdentifer,
                value: TokenValue::String("add".to_string()),
            },
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Identifier,
                value: TokenValue::String("AccountCreated".to_string()),
            },
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Identifier,
                value: TokenValue::String("user_id".to_string()),
            },
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Literal,
                value: TokenValue::String("123".to_string()),
            },
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Identifier,
                value: TokenValue::String("inital_amount".to_string()),
            },
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Literal,
                value: TokenValue::Float(100.59),
            },
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Identifier,
                value: TokenValue::String("currency".to_string()),
            },
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Literal,
                value: TokenValue::String("SEK".to_string()),
            },
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Auxiliary,
                value: TokenValue::String("to".to_string()),
            }
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Identifier,
                value: TokenValue::String("account".to_string()),
            },
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Identifier,
                value: TokenValue::String("id".to_string()),
            },
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Literal,
                value: TokenValue::String("123".to_string()),
            },
        );

        assert_eq!(tokens.next().is_err(), true);
    }

    #[test]
    fn test_tokenize_add_event_invalid() {
        let mut tokens = tokenize(String::from(
            r#"add AccountCreated(user_id, currency="SEK") to account(id="123");"#,
        ));

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::CommandIdentifer,
                value: TokenValue::String("add".to_string()),
            },
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Identifier,
                value: TokenValue::String("AccountCreated".to_string()),
            },
        );

        assert_eq!(
            tokens.next().unwrap(),
            Token {
                kind: TokenKind::Identifier,
                value: TokenValue::String("user_id".to_string()),
            },
        );

        match tokens.next() {
            Ok(t) => panic!("Expected error but got token: {:?}", t),
            Err(_) => eprint!("failed successfully!"),
        }
    }
}
