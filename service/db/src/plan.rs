use std::{error::Error, fmt};

use crate::ast::ast;

pub fn plan(transaction: &ast::Transaction) -> Result<ExecutionPlan, PlanError> {
    for cmd in transaction.commands.iter() {
        dbg!(cmd);
    }

    let plan = ExecutionPlan { operations: vec![] };
    Ok(plan)
}

#[derive(Debug, PartialEq, Eq)]
pub struct ExecutionPlan {
    pub operations: Vec<Operation>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Operation {
    ConflictCheckStream {
        name: String,
    },
    CreateStream {
        name: String,
    },
    CreateEvent {
        name: String,
        stream: String,
    },
    CreateAttribute {
        name: String,
        event: String,
        stream: String,
        data_type: DataType,
    },
    UpdateSchemaStream {
        name: String,
    },
    UpdateSchemaEvent {
        name: String,
        stream: String,
    },
    UpdateSchemaAttribute {
        name: String,
        event: String,
        stream: String,
        data_type: DataType,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum DataType {
    String,
    Int,
    Float,
    Bool,
}

#[derive(Debug)]
pub struct PlanError {
    message: String,
}

impl PlanError {
    fn new(msg: &str) -> Self {
        return PlanError {
            message: msg.to_string(),
        };
    }
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for PlanError {}

#[cfg(test)]
mod plan_test {
    use super::*;
    use crate::ast::ast;

    #[test]
    fn test_plan_create() {
        let test_cases = vec![
            (
                "plan create stream",
                ast::Transaction {
                    commands: vec![ast::Command::Create {
                        entity: ast::Entity::Stream("account".to_string()),
                    }],
                },
                ExecutionPlan {
                    operations: vec![
                        Operation::ConflictCheckStream {
                            name: "account".to_string(),
                        },
                        Operation::CreateStream {
                            name: "account".to_string(),
                        },
                        Operation::UpdateSchemaStream {
                            name: "account".to_string(),
                        },
                    ],
                },
            ),
            (
                "create event",
                ast::Transaction {
                    commands: vec![ast::Command::Create {
                        entity: ast::Entity::Event {
                            name: "AccountCreated".to_string(),
                            stream: "account".to_string(),
                            attributes: vec![
                                ast::AttributeDefinition {
                                    name: "owner".to_string(),
                                    data_type: "string".to_string(),
                                },
                                ast::AttributeDefinition {
                                    name: "amount".to_string(),
                                    data_type: "int".to_string(),
                                },
                            ],
                        },
                    }],
                },
                ExecutionPlan { operations: vec![] },
            ),
        ];

        for (name, trx, expected) in test_cases {
            let got = match plan(&trx) {
                Ok(a) => a,
                Err(e) => panic!("test cases '{}' failed planning: {}", name, e),
            };
            assert_eq!(expected, got)
        }
    }
}
