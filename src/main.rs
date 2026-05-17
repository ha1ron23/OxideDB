mod errors;

use errors::OxideError;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::fs::{OpenOptions, File};
use std::io::{Write, BufReader, BufRead};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, warn, error, instrument};

type Db = Arc<Mutex<BTreeMap<String, String>>>;

const WAL_FILE: &str = "appendonly.wal";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let addr = "127.0.0.1:6379";
    let listener = TcpListener::bind(addr).await?;
    info!(address = %addr, "OxideDB server successfully initialized");

    let db: Db = Arc::new(Mutex::new(BTreeMap::new()));
    if let Err(e) = recover_from_wal(&db) {
        warn!(error = %e, "WAL recovery skipped or failed");
    }

    loop {
        let (mut socket, peer_addr) = listener.accept().await?;
        let db = db.clone();

        info!(client = %peer_addr, "New inbound connection established");

        tokio::spawn(async move {
            let mut buf = [0; 4096];
            let mut request_buffer = String::new();
            loop {
                let n = match socket.read(&mut buf).await {
                    Ok(0) => {
                        info!(client = %peer_addr, "Client closed connection");
                        return;
                    }
                    Ok(n) => n,
                    Err(e) => {
                        error!(client = %peer_addr, error = %e, "Socket read error occurred");
                        return;
                    }
                };

                let chunk = String::from_utf8_lossy(&buf[..n]);
                request_buffer.push_str(&chunk);

                if !request_buffer.contains('\n') {
                    continue;
                }

                let lines: Vec<&str> = request_buffer.lines().map(|l| l.trim()).filter(|l| !l.is_empty()).collect();
                if lines.is_empty() {
                    continue;
                }

            
                match parse_and_process(&lines, &db) {
                    Ok(response) => {
                        if socket.write_all(response.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                    Err(oxide_error) => {
                        warn!(client = %peer_addr, error = ?oxide_error, "Failed to process client request");
                        let err_resp = oxide_error.to_resp();
                        if socket.write_all(err_resp.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                }

                request_buffer.clear();
            }
        });
    }
}

fn parse_and_process(lines: &[&str], db: &Db) -> Result<String, OxideError> {
    if !lines[0].starts_with('*') {
        return Err(OxideError::ProtocolError { expected: '*', found: lines[0].chars().next().unwrap_or(' ') });
    }

    let count: usize = lines[0][1..].parse().map_err(|_| OxideError::InvalidArrayLength)?;
    let expected_lines = 1 + (count * 2);

    if lines.len() < expected_lines {
        return Ok(String::new());
    }

    let mut args = Vec::with_capacity(count);
    let mut i = 1;
    while i < lines.len() && args.len() < count {
        if lines[i].starts_with('$') && i + 1 < lines.len() {
            args.push(lines[i + 1]);
        }
        i += 2;
    }

    if args.len() != count {
        return Err(OxideError::ProtocolDesync);
    }

    process_command(&args, db)
}

#[instrument(skip(db))]
fn process_command(args: &[&str], db: &Db) -> Result<String, OxideError> {
    if args.is_empty() {
        return Err(OxideError::UnknownCommand { cmd: "".to_string() });
    }
    
    let mut db_guard = db.lock().map_err(|_| OxideError::PoisonedLock)?;
    let command = args[0].to_uppercase();

    match command.as_str() {
        "SET" => {
            if args.len() != 3 {
                return Err(OxideError::WrongArgsCount { cmd: "SET".to_string() });
            }
            append_to_wal(args[1], args[2])?;
            db_guard.insert(args[1].to_string(), args[2].to_string());
            info!(key = %args[1], "Successfully executed SET command");
            Ok("+OK\r\n".to_string())
        }
        "GET" => {
            if args.len() != 2 {
                return Err(OxideError::WrongArgsCount { cmd: "GET".to_string() });
            }
            match db_guard.get(args[1]) {
                Some(value) => {
                    info!(key = %args[1], "Successfully executed GET command (Hit)");
                    Ok(format!("${}\r\n{}\r\n", value.len(), value))
                }
                None => {
                    info!(key = %args[1], "Successfully executed GET command (Miss)");
                    Ok("$-1\r\n".to_string())
                }
            }
        }
        _ => Err(OxideError::UnknownCommand { cmd: command }),
    }
}

fn append_to_wal(key: &str, value: &str) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(WAL_FILE)?;
    writeln!(file, "{}={}", key, value)?;
    Ok(())
}

fn recover_from_wal(db: &Db) -> std::io::Result<()> {
    let file = match File::open(WAL_FILE) {
        Ok(f) => f,
        Err(_) => return Ok(()),
    };

    let reader = BufReader::new(file);
    let mut db_guard = db.lock().map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Poisoned lock"))?;
    let mut count = 0;

    for line in reader.lines() {
        let line = line?;
        if let Some((key, value)) = line.split_once('=') {
            db_guard.insert(key.to_string(), value.to_string());
            count += 1;
        }
    }

    if count > 0 {
        info!(records = count, "Successfully recovered state from WAL storage");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_command_set_and_get() {
        let db: Db = Arc::new(Mutex::new(BTreeMap::new()));
        let set_args = vec!["SET", "test_key", "test_value"];
        let set_res = process_command(&set_args, &db).unwrap();
        assert_eq!(set_res, "+OK\r\n");

        let get_args = vec!["GET", "test_key"];
        let get_res = process_command(&get_args, &db).unwrap();
        assert_eq!(get_res, "$10\r\ntest_value\r\n");
    }
}
