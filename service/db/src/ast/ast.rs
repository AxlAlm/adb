#[derive(Debug, PartialEq)]
pub struct Transaction {
    pub commands: Vec<Command>,
}

#[derive(Debug, PartialEq)]
pub enum Command {
    Show {
        entity: Entity,
    },

    Create {
        entity: Entity,
    },

    Add {
        event: Event,
        stream: String,
        stream_id: String,
    },

    Find {
        projections: Vec<Projection>,
        predicates: Vec<Predicate>,
        limit: Option<Limit>,
    },
}

#[derive(Debug, PartialEq)]
pub enum Entity {
    Schema,
    Stream(String),
    Event {
        name: String,
        stream: String,
        attributes: Vec<AttributeDefinition>,
    },
}

#[derive(Debug, PartialEq)]
pub struct AttributeDefinition {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, PartialEq)]
pub struct Event {
    pub name: String,
    pub values: Vec<AttributeValue>,
}

#[derive(Debug, PartialEq)]
pub struct AttributeValue {
    pub name: String,
    pub value: Value,
}

#[derive(Debug, PartialEq)]
pub enum Value {
    Bool(bool),
    String(String),
    Int(i64),
    Float(f64),
}

#[derive(Debug, PartialEq)]
pub struct Projection {
    pub alias: String,
    pub projection: Expression,
}

#[derive(Debug, PartialEq)]
pub enum Predicate {
    BinaryOperation {
        left: Expression,
        operator: BinaryOperator,
        right: Expression,
    },
}

#[derive(Debug, PartialEq)]
pub struct Limit(pub i64);

#[derive(Debug, PartialEq)]
pub enum Function {
    Sum,
    Min,
    Max,
    Avg,
    Count,
}

#[derive(Debug, PartialEq)]
pub enum Expression {
    Literal(Value),
    Aggregate {
        function: Function,
        argument: Box<Expression>,
    },
    Attribute {
        stream: String,
        attribute: String,
    },
    UnaryOperation {
        operator: UnaryOperator,
        operand: Box<Expression>,
    },
    BinaryOperation {
        left: Box<Expression>,
        operator: BinaryOperator,
        right: Box<Expression>,
    },
}

#[derive(Debug, PartialEq)]
pub enum UnaryOperator {
    Negate,
}

#[derive(Debug, PartialEq)]
pub enum BinaryOperator {
    // Arithmetic operators
    Add,      // +
    Subtract, // -
    Multiply, // *
    Divide,   // /
    Modulus,  // %

    // Logical operators
    And, // AND
    Or,  // OR

    // Comparison operators
    Equal,        // =
    NotEqual,     // !=
    LessThan,     // <
    GreaterThan,  // >
    LessEqual,    // <=
    GreaterEqual, // >=

    // Set operators
    In,    // IN
    NotIn, // NOT IN
}
