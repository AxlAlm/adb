use std::{error::Error, fmt};

use crate::ast::ast;
use crate::tokenizer::{tokenize, Function, Keyword, Operator, Token, TokenizerError, Tokens};

macro_rules! match_extract {
    // for extracting the value of the enums. e.g. Token::Operator(op) => op
    ($tokens:expr, $token_enum:ident::$variant:ident($pattern:pat) => $result:expr) => {{
        let token = $tokens.next()?;
        match token {
            $token_enum::$variant($pattern) => $result,
            token => {
                return Err(ParserError::new(&format!(
                    "Expected {}::{} token but got {:?}",
                    stringify!($token_enum),
                    stringify!($variant),
                    token
                )))
            }
        }
    }};

    // For simple variants without values like Token::Accessor
    ($tokens:expr, $token_enum:path) => {{
        let token = $tokens.next()?;
        match token {
            $token_enum => (),
            token => {
                return Err(ParserError::new(&format!(
                    "Expected {} token but got {:?}",
                    stringify!($token_enum),
                    token
                )))
            }
        }
    }};
}

fn parse(input: &str) -> Result<ast::Transaction, ParserError> {
    let mut tokens = tokenize(input);

    let mut commands = vec![];
    let token = tokens.next()?;
    match token {
        Token::Keyword(Keyword::Show) => {
            let cmd = parse_show(&mut tokens)?;
            commands.push(cmd);
        }
        Token::Keyword(Keyword::Find) => {
            let cmd = parse_find(&mut tokens)?;
            commands.push(cmd);
        }
        _ => {
            return Err(ParserError::new(&format!(
                "got unexpected token '{:?}'",
                token
            )))
        }
    };

    return Ok(ast::Transaction { commands });
}

fn parse_find(tokens: &mut Tokens<'_>) -> Result<ast::Command, ParserError> {
    let projections = parse_projection_clauses(tokens)?;

    let token = tokens.next()?;
    let mut predicates = vec![];
    let mut limit = None;
    match token {
        Token::Keyword(Keyword::Where) => {
            predicates = parse_predicates(tokens)?;
        }
        Token::Keyword(Keyword::Limit) => {
            limit = match_extract!(tokens, Token::LiteralInt(n) => Some(ast::Limit(n)));
        }
        _ => {
            return Err(ParserError::new(&format!(
                "got unexpected token '{:?}'",
                token
            )))
        }
    }

    let cmd = ast::Command::Find {
        projections,
        predicates,
        limit,
    };
    Ok(cmd)
}

fn parse_predicates(tokens: &mut Tokens<'_>) -> Result<Vec<ast::Predicate>, ParserError> {
    let mut predicates = vec![];
    loop {
        let peeked_token = tokens.peek()?;
        if peeked_token == Token::EOF {
            break;
        }

        let left = parse_expression(tokens)?;
        let token_operator = match_extract!(tokens, Token::Operator(op) => op);
        let operator = map_operator_to_binary_operator(&token_operator);
        let right = parse_expression(tokens)?;

        let predicate = ast::Predicate::BinaryOperation(ast::BinaryOperation {
            left: Box::new(left),
            operator,
            right: Box::new(right),
        });

        predicates.push(predicate);
    }

    Ok(predicates)
}

fn parse_projection_clauses(
    tokens: &mut Tokens<'_>,
) -> Result<Vec<ast::ProjectionClause>, ParserError> {
    let mut projections = vec![];
    loop {
        let peeked_token = tokens.peek()?;
        if matches!(
            peeked_token,
            Token::Keyword(Keyword::Where) | Token::Keyword(Keyword::Limit)
        ) {
            break;
        }

        projections.push(ast::ProjectionClause {
            alias: "".to_string(),
            projection: parse_expression(tokens)?,
        });
    }

    Ok(projections)
}

// account.user_id
// sum(account.amount)
// sum(account.amount) + (100 + 100))
fn parse_expression(tokens: &mut Tokens<'_>) -> Result<ast::Expression, ParserError> {
    let token = tokens.next()?;
    let expression = match token {
        Token::Identifier(stream) => {
            match_extract!(tokens, Token::Accessor);
            let attribute = match_extract!(tokens, Token::Identifier(v) => v);
            ast::Expression::Attribute { stream, attribute }
        }
        Token::Function(Function::Sum) => {
            match_extract!(tokens, Token::GroupStart);
            let expression = parse_expression(tokens)?;
            match_extract!(tokens, Token::GroupEnd);
            ast::Expression::Aggregate {
                function: ast::AggregateFunction::Sum,
                argument: Box::new(expression),
            }
        }
        Token::LiteralStr(str) => ast::Expression::Literal(ast::Literal(ast::Value::String(str))),
        Token::LiteralInt(int) => ast::Expression::Literal(ast::Literal(ast::Value::Int(int))),
        _ => return Err(ParserError::new(&format!("unexpected token: {:?}`", token))),
    };

    if matches!(
        tokens.peek()?,
        Token::Operator(Operator::Add) | Token::Operator(Operator::Subtract)
    ) {
        tokens.next()?;
        return Ok(ast::Expression::BinaryOperation(ast::BinaryOperation {
            left: Box::new(expression),
            operator: ast::BinaryOperator::Add,
            right: Box::new(parse_expression(tokens)?),
        }));
    }

    Ok(expression)
}

fn parse_show(tokens: &mut Tokens<'_>) -> Result<ast::Command, ParserError> {
    let entity = parse_entity(tokens)?;
    let cmd = ast::Command::Show { entity };
    Ok(cmd)
}

fn parse_entity(tokens: &mut Tokens<'_>) -> Result<ast::Entity, ParserError> {
    // let entity_name = match_extract!(tokens, Token::Identifier(entity_name) => entity_name)
    let token = tokens.next()?;
    match token {
        Token::Identifier(name) => match name.as_str() {
            "schema" => Ok(ast::Entity::Schema),
            _ => Err(ParserError::new(&format!(
                "unsupported entity type '{}'",
                name
            ))),
        },
        _ => Err(ParserError::new(&format!(
            "expected Identifer got {:?}`",
            token,
        ))),
    }
}

#[derive(Debug)]
struct ParserError {
    message: String,
}

impl ParserError {
    fn new(message: &str) -> Self {
        ParserError {
            message: message.to_string(),
        }
    }
}

impl fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<TokenizerError> for ParserError {
    fn from(e: TokenizerError) -> ParserError {
        return ParserError::new(&e.to_string());
    }
}

impl Error for ParserError {}

pub fn map_operator_to_binary_operator(operator: &Operator) -> ast::BinaryOperator {
    match operator {
        Operator::Add => ast::BinaryOperator::Add,
        Operator::Subtract => ast::BinaryOperator::Subtract,
        Operator::Multiply => ast::BinaryOperator::Multiply,
        Operator::Divide => ast::BinaryOperator::Divide,
        Operator::Modulus => ast::BinaryOperator::Modulus,
        Operator::Equal => ast::BinaryOperator::Equal,
        Operator::NotEqual => ast::BinaryOperator::NotEqual,
        Operator::Less => ast::BinaryOperator::LessThan,
        Operator::Greater => ast::BinaryOperator::GreaterThan,
        Operator::LessOrEqual => ast::BinaryOperator::LessEqual,
        Operator::GreaterOrEqual => ast::BinaryOperator::GreaterEqual,
    }
}

#[cfg(test)]
mod parser_test {

    use crate::ast::ast;

    use super::parse;

    #[test]
    fn test_parse_show() {
        let ast = match parse("show schema;") {
            Ok(a) => a,
            Err(_) => panic!("failed to parse"),
        };
        let expected = ast::Transaction {
            commands: vec![ast::Command::Show {
                entity: ast::Entity::Schema,
            }],
        };

        assert_eq!(expected, ast)
    }

    #[test]
    fn test_parse_find() {
        let test_cases = vec![
            (
                "simple find with limit",
                "
            find 
                 account.user_id
            limit 
                10;
            ",
                ast::Transaction {
                    commands: vec![ast::Command::Find {
                        projections: vec![ast::ProjectionClause {
                            alias: "".to_string(),
                            projection: ast::Expression::Attribute {
                                stream: "account".to_string(),
                                attribute: "user_id".to_string(),
                            },
                        }],
                        predicates: vec![],
                        limit: Some(ast::Limit(10)),
                    }],
                },
            ),
            (
                "find with where and aggregate",
                r#"
            find 
                 sum(account.amount)
            where 
                account.user_id == "123";
            "#,
                ast::Transaction {
                    commands: vec![ast::Command::Find {
                        projections: vec![ast::ProjectionClause {
                            alias: "".to_string(),
                            projection: ast::Expression::Aggregate {
                                function: ast::AggregateFunction::Sum,
                                argument: Box::new(ast::Expression::Attribute {
                                    stream: "account".to_string(),
                                    attribute: "amount".to_string(),
                                }),
                            },
                        }],
                        predicates: vec![ast::Predicate::BinaryOperation(ast::BinaryOperation {
                            left: Box::new(ast::Expression::Attribute {
                                stream: "account".to_string(),
                                attribute: "user_id".to_string(),
                            }),
                            operator: ast::BinaryOperator::Equal,
                            right: Box::new(ast::Expression::Literal(ast::Literal(
                                ast::Value::String("123".to_string()),
                            ))),
                        })],
                        limit: None,
                    }],
                },
            ),
            (
                "nested projection",
                r#"
            find 
                 sum(account.amount) + sum(savings.loan) + 100
            limit
                10

            "#,
                ast::Transaction {
                    commands: vec![ast::Command::Find {
                        projections: vec![ast::ProjectionClause {
                            alias: "".to_string(),
                            projection: ast::Expression::BinaryOperation(ast::BinaryOperation {
                                left: Box::new(ast::Expression::Aggregate {
                                    function: ast::AggregateFunction::Sum,
                                    argument: Box::new(ast::Expression::Attribute {
                                        stream: "account".to_string(),
                                        attribute: "amount".to_string(),
                                    }),
                                }),
                                operator: ast::BinaryOperator::Add,
                                right: Box::new(ast::Expression::BinaryOperation(
                                    ast::BinaryOperation {
                                        left: Box::new(ast::Expression::Aggregate {
                                            function: ast::AggregateFunction::Sum,
                                            argument: Box::new(ast::Expression::Attribute {
                                                stream: "savings".to_string(),
                                                attribute: "loan".to_string(),
                                            }),
                                        }),
                                        operator: ast::BinaryOperator::Add,
                                        right: Box::new(ast::Expression::Literal(ast::Literal(
                                            ast::Value::Int(100),
                                        ))),
                                    },
                                )),
                            }),
                        }],
                        predicates: vec![],
                        limit: Some(ast::Limit(10)),
                    }],
                },
            ),
        ];

        for (name, input, expected) in test_cases {
            let ast = match parse(input) {
                Ok(a) => a,
                Err(e) => panic!("test cases '{}' failed parsing: {}", name, e),
            };

            assert_eq!(expected, ast)
        }
    }
}
