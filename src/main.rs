mod mutation_parser;
mod schema_parser;

fn main() {
    let schema = r#"
stream(accounts, account-id);
event(accounts, AccountCreated);
attribute(AccountCreated, owner-name, true, string);
    "#;

    let schema = match schema_parser::parse_schema(&schema) {
        Ok(x) => x,
        Err(_) => panic!("failed to parse schema"),
    };

    let mutation = r#"
        ADD AccountCreated(owner-name="axel") TO accounts:123
    "#;

    let mutations = match mutation_parser::parse_mutation(&mutation) {
        Ok(x) => x,
        Err(_) => panic!("failed to parse mutation"),
    };

    dbg!(&schema);
    dbg!(&mutations);
}
