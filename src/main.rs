mod ast;
mod db;
mod operation;
mod parser;

fn main() {
    let db = db::DB::new(None);

    let input_migration = r#"
stream(accounts, account-id);
event(accounts, AccountCreated);
attribute(accounts, AccountCreated, owner-name, true, string);
    "#;

    match operation::migration::migrate(&input_migration, &db) {
        Ok(_) => println!("migration done"),
        Err(_) => panic!("failed to parse schema"),
    };

    let mutatation_input = r#"
        ADD AccountCreated(owner-name="axel") TO accounts:123
    "#;

    for _ in 0..5 {
        match operation::mutation::mutate(mutatation_input, &db) {
            Ok(_) => println!("mutation done"),
            Err(e) => panic!("failed to mutate. {}", e),
        }
    }

    let events = db
        .get_events("accounts".to_string(), "123".to_string())
        .unwrap();
    dbg!(events);
}
