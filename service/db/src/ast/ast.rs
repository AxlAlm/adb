// SHOW COMNAND
pub struct Show {}

// CREATE COMMAND
pub struct Create {
    entity: EntityNode,
}

pub enum EntityNode {
    Stream {
        name: String,
        aggregate_id: String,
    },
    Event {
        name: String,
        stream: String,
    },
    Attribute {
        name: String,
        event: String,
        stream: String,
        required: bool,
        attribute_type: String,
    },
}

// ADD COMMAND
pub struct Add {
    event: Event,
    stream: String,
    aggregate: String,
}

pub struct Event {
    name: String,
    values: Vec<AttributeValue>,
}

pub struct AttributeValue {
    name: String,
    value: Value,
}

// FIND COMMAND
//
// // find the address and name of accounts
// find
//     ?name,
//     ?address,
//     ?account_id,
//     sum(?account.amount) as balance
// where
//     ?user user.address ?address,
//     ?user user.name ?name,
//     ?user user.id ?user_id,
//     ?account. account.user_id ?user_id,
//     ?account. account.amount? amount,
//     _ account.id ?account_id;

pub struct Find {
    // examples:
    // find
    //      "axel" // literal
    //      ?name // attribute (is the last attribute value)
    //      sum(?amount) - sum(?loan) // aggregate
    // ...
    projections: Vec<ProjectionClause>,
    predicates: Vec<Predicate>,
}

// example:
//  sum(?amount) as balance
//  "axel" as owner
pub struct ProjectionClause {
    alias: String,
    projection: Projection,
}

pub enum Projection {
    Variable(Variable),
    Literal(Literal),
    Aggregate(Aggregate),
}

pub enum Predicate {
    // examples:
    // ?acccount.owner-name != "gunnar"
    // sum(?account.ammount) < 100
    // _ OR _ ( _ AND _)
    Filter(BinaryOperator),

    // example:
    // ?account account.owner-name "gunnar"
    Relation {
        aggregate: Variable,
        attribute: Attribute,
        value: Expression,
    },
}

pub struct Attribute {
    aggregate: String,
    name: String,
}

// GENERAL
pub struct Aggregate {
    function: AggregateFunction,
    argument: Box<Expression>,
}

pub enum AggregateFunction {
    Sum,
    Min,
    Max,
    Avg,
    Count,
}

pub enum Value {
    Bool(bool),
    String(String),
    Int(i64),
    Float(f64),
    Ignore,
}

// e.g. "axel", 1, true
pub struct Literal {
    pub value: Value,
}
// e.g.  ?variable // change to Variable?
pub struct Variable {
    pub name: String,
}

pub enum UnaryOperator {
    // Arithmetic
    Negate, // -x
}

pub struct BinaryOperation {
    left: Box<Expression>,
    operator: BinaryOperator,
    right: Box<Expression>,
}

pub enum Expression {
    Literal(Literal),
    Variable(Variable),
    UnaryOperation {
        operator: UnaryOperator,
        operand: Box<Expression>,
    },
    BinaryOperation(BinaryOperator),
}

pub enum BinaryOperator {
    // Arithmetic operators
    Add,      // +
    Subtract, // -
    Multiply, // *
    Divide,   // /
    // Modulus,  // %

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
