use crate::db;
use crate::parser;

// TODO! fix error handling
pub fn migrate(input: &str, db: &db::DB) -> Result<(), String> {
    let schema = match parser::schema::parse(input) {
        Ok(x) => x,
        Err(_) => panic!("failed to parse schema"),
    };
    db.migrate(schema).map_err(|e| e.to_string())?;
    return Ok(());
}
