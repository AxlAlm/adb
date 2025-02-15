mod ast;
mod operation;
mod parser;

fn main() {
    let schema = r#"
stream(accounts, account-id);
event(accounts, AccountCreated);
attribute(AccountCreated, owner-name, true, string);
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

    // operation::mutation::mutate();
}
