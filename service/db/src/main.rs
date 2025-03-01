mod ast;
mod client;
mod db;
mod event;
mod operation;
use std::sync::Arc;

// fn main() {
//     connect();

//     //     let db = db::DB::new(None);

//     //     let input_migration = r#"
//     // stream(accounts, account-id);
//     // event(accounts, AccountCreated);
//     // attribute(accounts, AccountCreated, owner-name, true, string);
//     //     "#;

//     //     match operation::migration::migrate(&input_migration, &db) {
//     //         Ok(_) => println!("migration done"),
//     //         Err(_) => panic!("failed to parse schema"),
//     //     };

//     //     let mutatation_input = r#"
//     //         ADD AccountCreated(owner-name="axel") TO accounts:123
//     //     "#;

//     //     for _ in 0..5 {
//     //         match operation::mutation::mutate(mutatation_input, &db) {
//     //             Ok(_) => println!("mutation done"),
//     //             Err(e) => panic!("failed to mutate. {}", e),
//     //         }
//     //     }

//     //     let events = db
//     //         .get_events("accounts".to_string(), "123".to_string())
//     //         .unwrap();
//     //     dbg!(events);
// }
//
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(db::DB::new(None));

    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("Server listening on port 8080");

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New connection from: {}", addr);

        let db = db.clone();
        tokio::spawn(async move { handle_connection(socket, db).await });
    }
}

async fn handle_connection(mut socket: TcpStream, db: Arc<db::DB>) {
    loop {
        let mut buffer = [0; 1024];
        match socket.read(&mut buffer).await {
            Ok(n) if n == 0 => {
                println!("Connection closed by client");
                return;
            }
            Ok(n) => {
                let msg = String::from_utf8_lossy(&buffer[..n]);
                println!("Received: {}", msg);

                let msg = match operation::add::add(&msg, &db) {
                    Ok(_) => "mutation done".to_string(),
                    Err(e) => format!("failed to mutate. {}", e),
                };

                let return_msg = msg.to_string().into_bytes();
                if let Err(e) = socket.write(&return_msg).await {
                    eprintln!("Failed to send response: {}", e);
                    return;
                }
            }
            Err(e) => {
                eprintln!("Failed to read from connection: {}", e);
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::schema;
    use crate::db::DB;
    use crate::operation::add::add;

    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use tokio::time::Duration;

    use std::collections::HashMap;

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_concurrent_write_to_different_keys() {
        let schema = schema::Schema {
            streams: HashMap::from([(
                "account".to_string(),
                schema::Stream {
                    name: "account".to_string(),
                    key: "account-id".to_string(),
                },
            )]),
            events: HashMap::from([(
                ("account".to_string(), "AccountCreated".to_string()),
                schema::Event {
                    name: "AccountCreated".to_string(),
                    stream_name: "account".to_string(),
                },
            )]),
            attributes: HashMap::from([(
                (
                    "account".to_string(),
                    "AccountCreated".to_string(),
                    "owner-name".to_string(),
                ),
                schema::Attribute {
                    name: "owner-name".to_string(),
                    event_name: "AccountCreated".to_string(),
                    stream_name: "account".to_string(),
                    required: true,
                    attribute_type: "string".to_string(),
                },
            )]),
        };

        let db = Arc::new(DB::new(Some(schema)));
        // let read_counter = Arc::new(AtomicU32::new(0));
        // let write_counter = Arc::new(AtomicU32::new(0));
        let failed_write_counter = Arc::new(AtomicU32::new(0));

        let mut set = tokio::task::JoinSet::new();

        let input = r#"
        ADD AccountCreated(owner-name="axel") TO account:123
    "#;

        // Spawn writer tasks
        for _writer_id in 0..1 {
            let db = db.clone();
            let failed_write_counter = failed_write_counter.clone();
            let input = input.to_string();

            set.spawn(async move {
                let end_time = tokio::time::Instant::now() + Duration::from_secs(2);

                while tokio::time::Instant::now() < end_time {
                    match add(&input, &db) {
                        Ok(()) => {}
                        Err(_e) => {
                            failed_write_counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
        }

        let input = r#"
        ADD AccountCreated(owner-name="axel") TO account:1234
    "#;

        // Spawn writer tasks
        for _writer_id in 0..1 {
            let db = db.clone();
            let failed_write_counter = failed_write_counter.clone();
            let input = input.to_string();

            set.spawn(async move {
                let end_time = tokio::time::Instant::now() + Duration::from_secs(2);

                while tokio::time::Instant::now() < end_time {
                    match add(&input, &db) {
                        Ok(()) => {}
                        Err(_e) => {
                            failed_write_counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
        }

        let input = r#"
        ADD AccountCreated(owner-name="axel") TO account:12345
    "#;

        // Spawn writer tasks
        for _writer_id in 0..1 {
            let db = db.clone();
            let failed_write_counter = failed_write_counter.clone();
            let input = input.to_string();

            set.spawn(async move {
                let end_time = tokio::time::Instant::now() + Duration::from_secs(2);

                while tokio::time::Instant::now() < end_time {
                    match add(&input, &db) {
                        Ok(()) => {}
                        Err(_e) => {
                            failed_write_counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
        }

        // Wait for all tasks to complete
        while let Some(res) = set.join_next().await {
            res.unwrap();
        }

        let value = failed_write_counter.load(Ordering::Relaxed);
        if value > 0 {
            panic!("expected no write failures got {}", value)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_concurrent_write_to_same_key() {
        let schema = schema::Schema {
            streams: HashMap::from([(
                "account".to_string(),
                schema::Stream {
                    name: "account".to_string(),
                    key: "account-id".to_string(),
                },
            )]),
            events: HashMap::from([(
                ("account".to_string(), "AccountCreated".to_string()),
                schema::Event {
                    name: "AccountCreated".to_string(),
                    stream_name: "account".to_string(),
                },
            )]),
            attributes: HashMap::from([(
                (
                    "account".to_string(),
                    "AccountCreated".to_string(),
                    "owner-name".to_string(),
                ),
                schema::Attribute {
                    name: "owner-name".to_string(),
                    event_name: "AccountCreated".to_string(),
                    stream_name: "account".to_string(),
                    required: true,
                    attribute_type: "string".to_string(),
                },
            )]),
        };

        let db = Arc::new(DB::new(Some(schema)));
        let failed_write_counter = Arc::new(AtomicU32::new(0));

        let mut set = tokio::task::JoinSet::new();

        let input = r#"
        ADD AccountCreated(owner-name="axel") TO account:123
    "#;

        // Spawn writer tasks
        for _writer_id in 0..5 {
            let db = db.clone();
            let failed_write_counter = failed_write_counter.clone();
            let input = input.to_string();

            set.spawn(async move {
                let end_time = tokio::time::Instant::now() + Duration::from_secs(2);

                while tokio::time::Instant::now() < end_time {
                    match add(&input, &db) {
                        Ok(()) => {}
                        Err(_e) => {
                            failed_write_counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
        }

        // Wait for all tasks to complete
        while let Some(res) = set.join_next().await {
            res.unwrap();
        }

        let value = failed_write_counter.load(Ordering::Relaxed);
        if value == 0 {
            panic!("expected write failures, got none")
        }
    }
}
