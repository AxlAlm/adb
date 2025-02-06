// mod generator;
mod mutation_parser;
mod schema_parser;

// use crate::generator::CodeGenerator;

// Example usage
fn main() {
    let schema = r#"
    "#;

    let schema = match schema_parser::parse_schema(&schema) {
        Ok(x) => x,
        Err(_) => panic!("OH NO"),
    };

    // let generator = CodeGenerator::new("./src/adb".to_string());
    // generator.generate(schema)?;

    // Ok(())
}
