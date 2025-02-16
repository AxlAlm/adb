use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use db::{Streams, DB};

mod ast;
mod db;
mod operation;
mod parser;

fn main() {
    let schema = r#"
stream(accounts, account-id);
event(accounts, AccountCreated);
attribute(accounts, AccountCreated, owner-name, true, string);
    "#;

    let schema = match parser::schema::parse(&schema) {
        Ok(x) => x,
        Err(_) => panic!("failed to parse schema"),
    };

    let mutation = r#"
        ADD AccountCreated(owner-name="axel") TO accounts:123
    "#;

    let mutations = match parser::mutation::parse(&mutation) {
        Ok(x) => x,
        Err(_) => panic!("failed to parse mutation"),
    };

    dbg!(&schema);
    dbg!(&mutations);

    // #[derive(Debug)]
    // pub struct Streams(HashMap<(String, String), Arc<RwLock<Vec<Event>>>>);

    // #[derive(Debug)]
    // pub struct DB {
    //     pub streams: Arc<RwLock<Streams>>,
    // }

    let db = DB {
        streams: Arc::new(RwLock::new(Streams(HashMap::new()))),
    };

    for mutation in mutations {
        match operation::mutation::mutate(mutation, &schema, &db) {
            Ok(_) => println!("mutation done"),
            Err(e) => panic!("failed to mutate. {}", e),
        }
    }
}
