mod ast;
mod db;
mod event;
mod operation;
mod tokenizer;
use std::sync::Arc;

use operation::add::add;
use operation::create::create;
use operation::general::{parse_operation, OperationType};

use operation::show::show;
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
                let return_msg = exec(&msg, db.clone()).await;
                match return_msg {
                    Err(e) => {
                        if let Err(e) = socket.write(&e.to_string().into_bytes()).await {
                            eprintln!("failed to write message: {}", e);
                            return;
                        }
                    }
                    Ok(m) => {
                        let _ = socket.write(&m.into_bytes()).await;
                        continue;
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to read from connection: {}", e);
                return;
            }
        }
    }
}

async fn exec(msg: &str, db: Arc<db::DB>) -> Result<String, String> {
    let op = parse_operation(msg).map_err(|e| e.to_string())?;
    match op.op_type {
        OperationType::Add => {
            return Ok(add(op, &db).map_err(|e| e.to_string())?);
        }
        OperationType::Create => {
            return Ok(create(op, &db).map_err(|e| e.to_string())?);
        }
        OperationType::Show => {
            return Ok(show(op, &db).map_err(|e| e.to_string())?);
        }
        OperationType::Find => {
            return Ok(show(op, &db).map_err(|e| e.to_string())?);
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ast::schema;
    use crate::db::DB;

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
        ADD AccountCreated(owner-name="axel") -> account:123;
    "#;

        // Spawn writer tasks
        for _writer_id in 0..1 {
            let db = db.clone();
            let failed_write_counter = failed_write_counter.clone();
            let input = input.to_string();

            set.spawn(async move {
                let end_time = tokio::time::Instant::now() + Duration::from_secs(2);

                while tokio::time::Instant::now() < end_time {
                    match exec(&input, db.clone()).await {
                        Ok(_) => {}
                        Err(_e) => {
                            failed_write_counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
        }

        let input = r#"
        ADD AccountCreated(owner-name="axel") -> account:1234;
    "#;

        // Spawn writer tasks
        for _writer_id in 0..1 {
            let db = db.clone();
            let failed_write_counter = failed_write_counter.clone();
            let input = input.to_string();
            // let db = db.clone();

            set.spawn(async move {
                let end_time = tokio::time::Instant::now() + Duration::from_secs(2);

                while tokio::time::Instant::now() < end_time {
                    match exec(&input, db.clone()).await {
                        Ok(_) => {}
                        Err(_e) => {
                            failed_write_counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
        }

        let input = r#"
        ADD AccountCreated(owner-name="axel") -> account:12345;
    "#;

        // Spawn writer tasks
        for _writer_id in 0..1 {
            let db = db.clone();
            let failed_write_counter = failed_write_counter.clone();
            let input = input.to_string();

            set.spawn(async move {
                let end_time = tokio::time::Instant::now() + Duration::from_secs(2);

                while tokio::time::Instant::now() < end_time {
                    match exec(&input, db.clone()).await {
                        Ok(_) => {}
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
        ADD AccountCreated(owner-name="axel") -> account:123;
    "#;

        // Spawn writer tasks
        for _writer_id in 0..5 {
            let db = db.clone();
            let failed_write_counter = failed_write_counter.clone();
            let input = input.to_string();

            set.spawn(async move {
                let end_time = tokio::time::Instant::now() + Duration::from_secs(2);

                while tokio::time::Instant::now() < end_time {
                    match exec(&input, db.clone()).await {
                        Ok(_) => {}
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
