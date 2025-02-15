use crate::parser::mutation::AddEventMutation;
use crate::parser::schema::Schema;

// #[derive(Debug)]
// pub enum SchemaParserError {
//     InvalidBLock,
//     InvalidValue,
//     StreamNotFound,
//     EventNotFound,
// }
//
#[derive(Debug)]
pub enum MutationError {
    MutationInvalid(String),
    MutationFailed,
}

// is mutation possible?
fn validate_mutation(mutation: AddEventMutation, schema: Schema) -> Result<bool, MutationError> {
    // let mut stream_exists = false;
    // for stream in schema.streams {
    //     if stream.name == mutation.stream {
    //         stream_exists = true;
    //     }
    // }

    // if !stream_exists {
    //     return Err(MutationError::MutationInvalid(format!(
    //         "Stream '{}' does not exist in schema",
    //         mutation.stream
    //     )));
    // }

    // let mut stream_exists = false;
    // for stream in schema.streams {
    //     if stream.name == mutation.stream {
    //         stream_exists = true;
    //     }
    // }

    // if !stream_exists {
    //     return Err(MutationError::MutationInvalid(format!(
    //         "Stream '{}' does not exist in schema",
    //         mutation.stream
    //     )));
    // }

    Ok(true)
}

pub fn mutate(mutation: AddEventMutation, schema: Schema) -> Result<String, MutationError> {
    println!("mutate done");
    Ok("".to_string())
}
