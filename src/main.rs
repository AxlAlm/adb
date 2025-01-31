// mod generator;
mod parser;

// use crate::generator::CodeGenerator;

// Example usage
fn main() {
    // let schema = r#"
    // [:schema
    //   [event :fields [id type version timestamp stream]]
    //   [AccountCreated :fields [account-id owner-name balance]]
    //   [MoneyDeposited :fields [account-id amount]
    //                   :constraints [(> amount 0)]]
    //   [MoneyWithdrawn :fields [account-id amount]
    //                   :constraints [(> amount 0)]]
    // ]
    // [:streams
    //   [account
    //     :events [AccountCreated MoneyDeposited MoneyWithdrawn]
    //     :key account-id
    //     :ordering version]
    // ]
    // "#;

    // let generator = CodeGenerator::new("./src/adb".to_string());
    // generator.generate(schema)?;

    // Ok(())
}
