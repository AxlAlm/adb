mod ast;
mod db;
mod event;
mod parser;
mod planner;
mod tokenizer;
use std::sync::Arc;

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
    let trx = parser::parse(msg).map_err(|e| format!("failed to parse: {}", e))?;
    let plan = planner::plan(&trx).map_err(|e| format!("failed to plan: {}", e))?;

    db.exec(&plan)
        .map_err(|e| format!("failed to execute plan: {}", e))?;

    dbg!(&trx, &plan);

    return Ok("all ok".to_string());
}

#[cfg(test)]
mod e2e_test {
    use super::*;
    use crate::db::DB;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_create_stream() {
        let db = Arc::new(DB::new(None));
        let cmd = "create stream account;";

        match exec(&cmd, db.clone()).await {
            Ok(_) => eprintln!("created stream succeefully"),
            Err(e) => panic!("failed to create stream: {}", e),
        }

        // running it again should work. No conflict as stream already exists
        match exec(&cmd, db.clone()).await {
            Ok(_) => eprintln!("created stream succeefully"),
            Err(e) => panic!("failed to create stream: {}", e),
        }
    }

    #[tokio::test]
    async fn test_create_event() {
        let db = Arc::new(DB::new(None));

        // lets first create a stream
        let cmd = "create stream account;";
        match exec(&cmd, db.clone()).await {
            Ok(_) => eprintln!("created stream succeefully"),
            Err(e) => panic!("failed to create stream: {}", e),
        }

        let cmd = "create event AccountCreated(
                    owner string,
                    amount int 
                ) on account;";
        match exec(&cmd, db.clone()).await {
            Ok(_) => eprintln!("created stream succeefully"),
            Err(e) => panic!("failed to create event: {}", e),
        }

        // running it again should not work. As events has attribute that can change
        // we should conflict when trying to create a new
        match exec(&cmd, db.clone()).await {
            Ok(_) => eprintln!("created stream succeefully"),
            Err(e) => panic!("failed to create event: {}", e),
        }
    }
}

#[cfg(test)]
mod e2e_concurrency_test {
    use super::*;

    use crate::db::DB;

    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use tokio::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_concurrent_write_to_different_keys() {
        let db = Arc::new(DB::new(None));

        // DO SOME SCHEMA STUFF

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
        let db = Arc::new(DB::new(None));

        // DO SOME SCHEMA STUFF
        //
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
