use std::{error::Error, fmt};

use crate::{ast::ast, event};

pub fn plan(transaction: &ast::Transaction) -> Result<ExecutionPlan, PlanError> {
    dbg!("OKEFOKEOKEKFOEKF");

    let mut operations = vec![];
    for cmd in transaction.commands.iter() {
        match cmd {
            ast::Command::Create { entity } => match entity {
                ast::Entity::Stream(name) => {
                    operations.push(Operation::CheckStreamExists {
                        name: name.to_string(),
                    });
                    operations.push(Operation::CreateStream { name: name.clone() })
                }
                ast::Entity::Event {
                    name,
                    stream_name,
                    attributes,
                } => {
                    operations.push(Operation::CheckStreamExists {
                        name: stream_name.to_string(),
                    });

                    operations.push(Operation::CheckEventExists {
                        name: name.to_string(),
                    });

                    operations.push(Operation::CreateEvent {
                        name: name.to_string(),
                        stream_name: stream_name.to_string(),
                    });

                    operations.extend(attributes.iter().map(|a| Operation::CreateAttribute {
                        name: a.name.clone(),
                        event_name: name.clone(),
                        stream_name: stream_name.clone(),
                        data_type: a.data_type.clone(),
                    }));
                }
                _ => return Err(PlanError::new("unreconizable entity")),
            },
            _ => return Err(PlanError::new("cannot handle that command")),
        }
    }

    let plan = ExecutionPlan { operations };

    dbg!(&plan);

    Ok(plan)
}

#[derive(Debug, PartialEq, Eq)]
pub struct ExecutionPlan {
    pub operations: Vec<Operation>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Operation {
    CheckStreamExists {
        name: String,
    },
    CheckEventExists {
        name: String,
    },
    CreateStream {
        name: String,
    },
    CreateEvent {
        name: String,
        stream_name: String,
    },
    CreateAttribute {
        name: String,
        event_name: String,
        stream_name: String,
        data_type: String,
    },

    AddEvent {
        event: event::Event,
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

    // #[test]
    // fn test_plan_create() {
    //     let test_cases = vec![
    //         (
    //             "plan create stream",
    //             ast::Transaction {
    //                 commands: vec![ast::Command::Create {
    //                     entity: ast::Entity::Stream("account".to_string()),
    //                 }],
    //             },
    //             ExecutionPlan {
    //                 operations: vec![
    //                     Operation::ConflictCheckStream {
    //                         name: "account".to_string(),
    //                     },
    //                     Operation::CreateStream {
    //                         name: "account".to_string(),
    //                     },
    //                     Operation::UpdateSchemaStream {
    //                         name: "account".to_string(),
    //                     },
    //                 ],
    //             },
    //         ),
    //         (
    //             "create event",
    //             ast::Transaction {
    //                 commands: vec![ast::Command::Create {
    //                     entity: ast::Entity::Event {
    //                         name: "AccountCreated".to_string(),
    //                         stream: "account".to_string(),
    //                         attributes: vec![
    //                             ast::AttributeDefinition {
    //                                 name: "owner".to_string(),
    //                                 data_type: "string".to_string(),
    //                             },
    //                             ast::AttributeDefinition {
    //                                 name: "amount".to_string(),
    //                                 data_type: "int".to_string(),
    //                             },
    //                         ],
    //                     },
    //                 }],
    //             },
    //             ExecutionPlan { operations: vec![] },
    //         ),
    //     ];

    //     for (name, trx, expected) in test_cases {
    //         let got = match plan(&trx) {
    //             Ok(a) => a,
    //             Err(e) => panic!("test cases '{}' failed planning: {}", name, e),
    //         };
    //         assert_eq!(expected, got)
    //     }
    // }
}
