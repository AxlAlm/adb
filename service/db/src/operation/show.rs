use core::fmt;

use super::general::Operation;
use crate::db::{DBError, DB};

#[derive(Debug)]
pub enum ShowError {
    ShowError(String),
}

impl From<DBError> for ShowError {
    fn from(error: DBError) -> Self {
        ShowError::ShowError(error.to_string())
    }
}

impl fmt::Display for ShowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShowError::ShowError(msg) => write!(f, "Show Error: {}", msg),
        }
    }
}

pub fn show(op: Operation, db: &DB) -> Result<String, ShowError> {
    if op.body.trim().to_lowercase() != "schema" {
        return Err(ShowError::ShowError(format!(
            "'{}' does not exist",
            op.body
        )));
    }

    let schema = db.get_schema()?;
    return Ok(format!("{:#?}", schema));
}
