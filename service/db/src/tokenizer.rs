use std::{char, error::Error, fmt};

pub fn tokenize<'a>(input: &'a str) -> Tokens<'a> {
    return Tokens::new(input);
}

pub struct Tokens<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
    current_line_idx: usize,
    current_char_idx: usize,
    peeked_token: Option<Result<Token, TokenizerError>>,
}

impl<'a> Tokens<'a> {
    fn new(input: &'a str) -> Self {
        return Self {
            chars: input.chars().peekable(),
            current_line_idx: 0,
            current_char_idx: 0,
            peeked_token: None,
        };
    }

    fn next_internal(&mut self) -> Result<Token, TokenizerError> {
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

            if is_accessor(&c) {
                let next_c = self.chars.peek().ok_or_else(|| TokenizerError {
                    message: "unexpected end of input".to_string(),
                    line_position: self.current_line_idx,
                    char_position: self.current_char_idx,
                })?;

                if !next_c.is_numeric() {
                    return Ok(Token::Accessor);
                }
            }

            if is_group_start(&c) {
                return Ok(Token::GroupStart);
            }

            if is_group_end(&c) {
                return Ok(Token::GroupEnd);
            }

            if is_operator(&c) {
                // we peek on next char to see if operator is a two character operator
                // e.g. >=
                let next_c = self.chars.peek().ok_or_else(|| TokenizerError {
                    message: "unexpected end of input".to_string(),
                    line_position: self.current_line_idx,
                    char_position: self.current_char_idx,
                })?;

                // if next char is not an operator and char is "="
                // we know its and Assign token
                if !is_operator(&next_c) && is_assign(&c) && buffer.len() == 0 {
                    return Ok(Token::Assign);
                }
            }

            // Check if we should continue building the current token or finalize it
            let should_end_token = match self.chars.peek() {
                Some(&next_c) => {
                    if next_c.is_whitespace()
                        || is_seperator(&next_c)
                        || is_group_start(&next_c)
                        || is_group_end(&next_c)
                        || (is_accessor(&next_c) && !c.is_numeric())
                    {
                        true
                    } else if is_operator(&c) && is_operator(&next_c) {
                        // Special case for multi-character operators like <=, >=, ==
                        // Here we want to continue if both current and next are operators
                        // This handles cases like <= where both < and = are operators
                        false
                    } else if is_accessor(&c) && next_c.is_alphabetic() {
                        true
                    } else if is_assign(&next_c) {
                        // If next char is an operator and current isn't part of a multi-char operator
                        true
                    } else if is_eof(&next_c) {
                        true
                    } else {
                        false
                    }
                }
                None => true, // End of input means end of token
            };

            buffer.push(c);
            if !should_end_token {
                continue;
            }

            let buffer_string: String = buffer.iter().collect();

            if let Some(operator) = Operator::from_str(&buffer_string) {
                return Ok(Token::Operator(operator));
            }

            if let Some(keyword) = Keyword::from_str(&buffer_string) {
                return Ok(Token::Keyword(keyword));
            }

            if let Some(function) = Function::from_str(&buffer_string) {
                return Ok(Token::Function(function));
            }

            if buffer_string == "on" {
                return Ok(Token::AuxiliaryOn);
            }
            if buffer_string == "to" {
                return Ok(Token::AuxiliaryTo);
            }

            if buffer[0].is_numeric() {
                let is_float = buffer_string.contains(".");
                return parse_numeric(
                    &buffer_string,
                    is_float,
                    self.current_line_idx,
                    self.current_char_idx,
                );
            }

            if buffer[0] == '"' {
                return Ok(Token::LiteralStr(buffer_string.replace('"', "")));
            }

            return Ok(Token::Identifier(buffer_string));
        }

        return Ok(Token::EOF);
    }

    pub fn peek(&mut self) -> Result<Token, TokenizerError> {
        match &self.peeked_token {
            Some(t) => t.clone(),
            None => {
                let token = self.next_internal();
                self.peeked_token = Some(token.clone());
                return token;
            }
        }
    }

    pub fn next(&mut self) -> Result<Token, TokenizerError> {
        if let Some(t) = self.peeked_token.take() {
            return t;
        }

        self.next_internal()
    }
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

impl fmt::Display for TokenizerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at line {} and positions {}",
            self.message, self.line_position, self.char_position
        )
    }
}

impl Error for TokenizerError {}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    LiteralStr(String),
    LiteralInt(i64),
    LiteralFloat(f64),
    LiteralBool(bool),
    Identifier(String),
    Accessor,
    EOF,        // ;
    Seperator,  // ,
    GroupStart, // (
    GroupEnd,   // )
    Assign,

    AuxiliaryOn,
    AuxiliaryTo,

    Keyword(Keyword),
    Function(Function),
    Operator(Operator),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Keyword {
    // Commands
    Show,
    Create,
    Add,
    Find,

    // Other
    Limit,
    Where,
}

impl Keyword {
    fn from_str(input: &str) -> Option<Self> {
        match input.to_lowercase().as_str() {
            "show" => Some(Keyword::Show),
            "create" => Some(Keyword::Create),
            "add" => Some(Keyword::Add),
            "find" => Some(Keyword::Find),
            "limit" => Some(Keyword::Limit),
            "where" => Some(Keyword::Where),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Function {
    Sum,
    Max,
    Min,
    Avg,
    Count,
}

impl Function {
    fn from_str(input: &str) -> Option<Self> {
        match input.to_lowercase().as_str() {
            "sum" => Some(Function::Sum),
            "max" => Some(Function::Max),
            "min" => Some(Function::Min),
            "avg" => Some(Function::Avg),
            "count" => Some(Function::Count),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    Add,
    Multiply,
    Subtract,
    Divide,
    Modulus,
    Equal,
    NotEqual,
    Less,
    Greater,
    GreaterOrEqual,
    LessOrEqual,
}

impl Operator {
    fn from_str(input: &str) -> Option<Self> {
        match input {
            "+" => Some(Operator::Add),
            "*" => Some(Operator::Multiply),
            "-" => Some(Operator::Subtract),
            "/" => Some(Operator::Divide),
            "==" => Some(Operator::Equal),
            "<" => Some(Operator::Less),
            ">" => Some(Operator::Greater),
            ">=" => Some(Operator::GreaterOrEqual),
            "<=" => Some(Operator::LessOrEqual),
            "%" => Some(Operator::Modulus),
            _ => None,
        }
    }
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

fn is_accessor(c: &char) -> bool {
    return c == &'.';
}

fn is_assign(c: &char) -> bool {
    return c == &'=';
}

fn is_operator(c: &char) -> bool {
    return c == &'=' || c == &'+' || c == &'-' || c == &'*' || c == &'<' || c == &'>';
}

fn is_supported_identifier_literal_char(c: &char) -> bool {
    return c.is_alphanumeric() || c == &'_' || c == &'-' || c == &'"' || c == &'.';
}

// Helper function to avoid code duplication for numeric parsing
fn parse_numeric(
    s: &str,
    is_float: bool,
    line_idx: usize,
    char_idx: usize,
) -> Result<Token, TokenizerError> {
    if is_float {
        let parsed_value = s
            .parse::<f64>()
            .map_err(|_| TokenizerError::new("failed to parse float", line_idx, char_idx))?;
        Ok(Token::LiteralFloat(parsed_value))
    } else {
        let parsed_value = s
            .parse::<i64>()
            .map_err(|_| TokenizerError::new("failed to parse int", line_idx, char_idx))?;
        Ok(Token::LiteralInt(parsed_value))
    }
}

#[cfg(test)]
mod tokenizer_test {
    use super::{tokenize, Function, Keyword, Operator, Token};

    #[test]
    fn test_fail_with_missing_eof() {
        let test_cases = vec![("missing semicolon", "show schema", 2)];

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
                    Token::Keyword(Keyword::Show),
                    Token::Identifier("schema".to_string()),
                    Token::EOF,
                ],
            ),
            (
                "with spaces",
                "show     schema;",
                vec![
                    Token::Keyword(Keyword::Show),
                    Token::Identifier("schema".to_string()),
                    Token::EOF,
                ],
            ),
            (
                "with newline",
                "show
             schema;",
                vec![
                    Token::Keyword(Keyword::Show),
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
                    Token::Keyword(Keyword::Create),
                    Token::Identifier("stream".to_string()),
                    Token::Identifier("account".to_string()),
                    Token::EOF,
                ],
            ),
            (
                "with spaces",
                "create    stream   account;",
                vec![
                    Token::Keyword(Keyword::Create),
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
                    Token::Keyword(Keyword::Create),
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
                    Token::Keyword(Keyword::Create),
                    Token::Identifier("event".to_string()),
                    Token::Identifier("AccountCreated".to_string()),
                    Token::GroupStart,
                    Token::Identifier("owner".to_string()),
                    Token::Identifier("string".to_string()),
                    Token::GroupEnd,
                    Token::AuxiliaryOn,
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
                    Token::Keyword(Keyword::Create),
                    Token::Identifier("event".to_string()),
                    Token::Identifier("AccountCreated".to_string()),
                    Token::GroupStart,
                    Token::Identifier("owner".to_string()),
                    Token::Identifier("string".to_string()),
                    Token::Seperator,
                    Token::Identifier("ammount".to_string()),
                    Token::Identifier("int".to_string()),
                    Token::GroupEnd,
                    Token::AuxiliaryOn,
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
                    Token::Keyword(Keyword::Add),
                    Token::Identifier("AccountCreated".to_string()),
                    Token::GroupStart,
                    Token::Identifier("user_id".to_string()),
                    Token::Assign,
                    Token::LiteralStr("123".to_string()),
                    Token::Seperator,
                    Token::Identifier("inital_amount".to_string()),
                    Token::Assign,
                    Token::LiteralFloat(100.59),
                    Token::Seperator,
                    Token::Identifier("currency".to_string()),
                    Token::Assign,
                    Token::LiteralStr("SEK".to_string()),
                    Token::GroupEnd,
                    Token::AuxiliaryTo,
                    Token::Identifier("account".to_string()),
                    Token::GroupStart,
                    Token::Identifier("id".to_string()),
                    Token::Assign,
                    Token::LiteralStr("123".to_string()),
                    Token::GroupEnd,
                    Token::EOF,
                ],
            ),
            (
                "add event to accounti (int amount)",
                r#"add AccountCreated(inital_amount=100) to account(id="123");"#,
                vec![
                    Token::Keyword(Keyword::Add),
                    Token::Identifier("AccountCreated".to_string()),
                    Token::GroupStart,
                    Token::Identifier("inital_amount".to_string()),
                    Token::Assign,
                    Token::LiteralInt(100),
                    Token::GroupEnd,
                    Token::AuxiliaryTo,
                    Token::Identifier("account".to_string()),
                    Token::GroupStart,
                    Token::Identifier("id".to_string()),
                    Token::Assign,
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
                    Token::Keyword(Keyword::Add),
                    Token::Identifier("AccountCreated".to_string()),
                    Token::GroupStart,
                    Token::Identifier("inital_amount".to_string()),
                    Token::Assign,
                    Token::LiteralInt(100),
                    Token::GroupEnd,
                    Token::AuxiliaryTo,
                    Token::Identifier("account".to_string()),
                    Token::GroupStart,
                    Token::Identifier("id".to_string()),
                    Token::Assign,
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

    #[test]
    fn test_find() {
        let test_cases = vec![
            (
                "simple find",
                // finds user ids for 10 accounts
                "
                find 
                     account.user_id
                limit 
                    10;
                    ",
                vec![
                    Token::Keyword(Keyword::Find),
                    Token::Identifier("account".to_string()),
                    Token::Accessor,
                    Token::Identifier("user_id".to_string()),
                    Token::Keyword(Keyword::Limit),
                    Token::LiteralInt(10),
                    Token::EOF,
                ],
            ),
            (
                "with aggregation",
                // finds user ids for 10 accounts
                "
                find sum(account.amount);
                ",
                vec![
                    Token::Keyword(Keyword::Find),
                    Token::Function(Function::Sum),
                    Token::GroupStart,
                    Token::Identifier("account".to_string()),
                    Token::Accessor,
                    Token::Identifier("amount".to_string()),
                    Token::GroupEnd,
                    Token::EOF,
                ],
            ),
            (
                "with more complex aggregation",
                // sums amount from various aggregates
                "
                find 
                     sum(account.amount)
                     + sum(loan.amount) 
                     - sum(savings.amount);
                ",
                vec![
                    Token::Keyword(Keyword::Find),
                    // sum(account.amount)
                    Token::Function(Function::Sum),
                    Token::GroupStart,
                    Token::Identifier("account".to_string()),
                    Token::Accessor,
                    Token::Identifier("amount".to_string()),
                    Token::GroupEnd,
                    // +
                    Token::Operator(Operator::Add),
                    // sum(loan.amount)
                    Token::Function(Function::Sum),
                    Token::GroupStart,
                    Token::Identifier("loan".to_string()),
                    Token::Accessor,
                    Token::Identifier("amount".to_string()),
                    Token::GroupEnd,
                    // -
                    Token::Operator(Operator::Subtract),
                    // sum(loan.amount)
                    Token::Function(Function::Sum),
                    Token::GroupStart,
                    Token::Identifier("savings".to_string()),
                    Token::Accessor,
                    Token::Identifier("amount".to_string()),
                    Token::GroupEnd,
                    Token::EOF,
                ],
            ),
            (
                "where clause",
                // get the user_id for accounts where sum amount is more than 100
                r#"
                find 
                    account.user_id
                where
                    sum(account.amount) > 100;
                    ;
                "#,
                vec![
                    Token::Keyword(Keyword::Find),
                    Token::Identifier("account".to_string()),
                    Token::Accessor,
                    Token::Identifier("user_id".to_string()),
                    Token::Keyword(Keyword::Where),
                    // sum(account.amount)
                    Token::Function(Function::Sum),
                    Token::GroupStart,
                    Token::Identifier("account".to_string()),
                    Token::Accessor,
                    Token::Identifier("amount".to_string()),
                    Token::GroupEnd,
                    Token::Operator(Operator::Greater),
                    Token::LiteralInt(100),
                    Token::EOF,
                ],
            ),
            (
                "more filters",
                // get the user_id for accounts where sum amount is more than 100
                r#"
                find 
                    account.user_id
                where
                    sum(account.amount) > 100,
                    account.created_at <= "2024-01-02",
                    account.type == "savings";
                "#,
                vec![
                    Token::Keyword(Keyword::Find),
                    Token::Identifier("account".to_string()),
                    Token::Accessor,
                    Token::Identifier("user_id".to_string()),
                    Token::Keyword(Keyword::Where),
                    // sum(account.amount) > 100,
                    Token::Function(Function::Sum),
                    Token::GroupStart,
                    Token::Identifier("account".to_string()),
                    Token::Accessor,
                    Token::Identifier("amount".to_string()),
                    Token::GroupEnd,
                    Token::Operator(Operator::Greater),
                    Token::LiteralInt(100),
                    Token::Seperator,
                    // account.created_at < "2024-01-02";
                    Token::Identifier("account".to_string()),
                    Token::Accessor,
                    Token::Identifier("created_at".to_string()),
                    Token::Operator(Operator::LessOrEqual),
                    Token::LiteralStr("2024-01-02".to_string()),
                    Token::Seperator,
                    //account.type = "savings";
                    Token::Identifier("account".to_string()),
                    Token::Accessor,
                    Token::Identifier("type".to_string()),
                    Token::Operator(Operator::Equal),
                    Token::LiteralStr("savings".to_string()),
                    Token::EOF,
                ],
            ),
            (
                "more gte",
                // get the user_id for accounts where sum amount is more than 100
                r#"
                find 
                    account.user_id
                where
                    sum(account.amount) >= 100;
                "#,
                vec![
                    Token::Keyword(Keyword::Find),
                    Token::Identifier("account".to_string()),
                    Token::Accessor,
                    Token::Identifier("user_id".to_string()),
                    Token::Keyword(Keyword::Where),
                    // sum(account.amount) > 100,
                    Token::Function(Function::Sum),
                    Token::GroupStart,
                    Token::Identifier("account".to_string()),
                    Token::Accessor,
                    Token::Identifier("amount".to_string()),
                    Token::GroupEnd,
                    Token::Operator(Operator::GreaterOrEqual),
                    Token::LiteralInt(100),
                    Token::EOF,
                ],
            ),
            (
                "multiple selector",
                r#"
                find 
                    "test",
                    account.user_id,
                    sum(account.amount);
                "#,
                vec![
                    Token::Keyword(Keyword::Find),
                    Token::LiteralStr("test".to_string()),
                    Token::Seperator,
                    Token::Identifier("account".to_string()),
                    Token::Accessor,
                    Token::Identifier("user_id".to_string()),
                    Token::Seperator,
                    // sum(account.amount)
                    Token::Function(Function::Sum),
                    Token::GroupStart,
                    Token::Identifier("account".to_string()),
                    Token::Accessor,
                    Token::Identifier("amount".to_string()),
                    Token::GroupEnd,
                    Token::EOF,
                ],
            ),
            (
                "join",
                r#"
                find 
                    "test"
                where
                    account.user_id == user.id;
                "#,
                vec![
                    Token::Keyword(Keyword::Find),
                    Token::LiteralStr("test".to_string()),
                    Token::Keyword(Keyword::Where),
                    Token::Identifier("account".to_string()),
                    Token::Accessor,
                    Token::Identifier("user_id".to_string()),
                    Token::Operator(Operator::Equal),
                    Token::Identifier("user".to_string()),
                    Token::Accessor,
                    Token::Identifier("id".to_string()),
                    Token::EOF,
                ],
            ),
        ];

        for (test_name, input, expected_tokens) in test_cases {
            let mut tokens = tokenize(input);
            for expected_token in expected_tokens {
                let t = tokens.next().unwrap();
                assert_eq!(t, expected_token, "Failed in test case: {}", test_name);
            }
        }
    }
}
