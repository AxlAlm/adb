#[derive(Debug, PartialEq)]
pub struct Transaction {
    pub commands: Vec<Command>,
}

#[derive(Debug, PartialEq)]
pub enum Command {
    // show command
    Show {
        entity: Entity,
    },

    // examle:
    //      create stream(account)
    //      create event(AccountCreated) on account
    //      create attribute(name=owner, type=string, required=true) on account:AccountCreated
    Create {
        entity: Entity,
    },

    // add command
    // examle:
    //      add AccountCreated(owner="axel") to account:123;
    Add {
        event: Event,
        stream: String,
        stream_id: String,
    },

    // find command
    Find {
        // examples:
        // find
        //      "axel" // literal
        //      account.id // attribute (is the last attribute value)
        //      sum(amount) - sum(loan) // aggregate
        // ...
        projections: Vec<ProjectionClause>,
        predicates: Vec<Predicate>,
        limit: Option<Limit>,
    },
}

#[derive(Debug, PartialEq)]
pub enum Entity {
    Schema,
    Stream(String),
    Event { name: String, stream: String },
}

#[derive(Debug, PartialEq)]
pub struct Event {
    name: String,
    values: Vec<AttributeValue>,
}

#[derive(Debug, PartialEq)]
pub struct AttributeValue {
    name: String,
    value: Value,
}

#[derive(Debug, PartialEq)]
pub struct ProjectionClause {
    pub alias: String,
    pub projection: Expression,
}

// where
#[derive(Debug, PartialEq)]
pub enum Predicate {
    // examples:
    // acccount.owner-name != "gunnar"
    // sum(account.ammount) < 100
    // _ OR _ ( _ AND _)
    BinaryOperation(BinaryOperation),
}

#[derive(Debug, PartialEq)]
pub struct Limit(pub i64);

#[derive(Debug, PartialEq)]
pub enum AggregateFunction {
    Sum,
    Min,
    Max,
    Avg,
    Count,
}

#[derive(Debug, PartialEq)]
pub enum Value {
    Bool(bool),
    String(String),
    Int(i64),
    Float(f64),
    Ignore,
}

// e.g. "axel", 1, true
#[derive(Debug, PartialEq)]
pub struct Literal(pub Value);

#[derive(Debug, PartialEq)]
pub enum UnaryOperator {
    Negate, // -x
}

#[derive(Debug, PartialEq)]
pub struct BinaryOperation {
    pub left: Box<Expression>,
    pub operator: BinaryOperator,
    pub right: Box<Expression>,
}

#[derive(Debug, PartialEq)]
pub enum Expression {
    Literal(Literal),
    Aggregate {
        function: AggregateFunction,
        argument: Box<Expression>,
    },
    Expression(Box<Expression>),
    Attribute {
        stream: String,
        attribute: String,
    },
    UnaryOperation {
        operator: UnaryOperator,
        operand: Box<Expression>,
    },
    BinaryOperation(BinaryOperation),
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
