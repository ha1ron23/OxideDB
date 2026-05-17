mod errors;

use errors::OxideError;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::fs::{OpenOptions, File};
use std::io::{Write, BufReader, BufRead};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, warn, error};
use std::env;

type Db = Arc<Mutex<BTreeMap<String, String>>>;
const WAL_FILE: &str = "appendonly.wal";
const DEFAULT_ADDR: &str = "127.0.0.1:6379";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let addr = env::var("OXIDE_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());
    let listener = TcpListener::bind(&addr).await?;
    info!(address = %addr, "OxideDB started");

    let db: Db = Arc::new(Mutex::new(BTreeMap::new()));
    if let Err(e) = recover_from_wal(&db) {
        warn!(error = %e, "WAL recovery failed");
    }

    loop {
        let (mut socket, peer) = listener.accept().await?;
        let db = db.clone();
        tokio::spawn(async move {
            let mut buffer = Vec::new();
            loop {
                let mut chunk = [0; 4096];
                let n = match socket.read(&mut chunk).await {
                    Ok(0) => {
                        info!(client = %peer, "connection closed");
                        return;
                    }
                    Ok(n) => n,
                    Err(e) => {
                        error!(client = %peer, error = %e, "read error");
                        return;
                    }
                };
                buffer.extend_from_slice(&chunk[..n]);

                loop {
                    match parse_resp_command(&buffer) {
                        Ok(Some((cmd, used))) => {
                            buffer.drain(0..used);
                            let response = process_command(&cmd, &db);
                            if let Err(e) = socket.write_all(response.as_bytes()).await {
                                error!(client = %peer, error = %e, "write error");
                                return;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            warn!(client = %peer, error = %e, "parse error");
                            let _ = socket.write_all(e.to_resp().as_bytes()).await;
                            buffer.clear();
                            break;
                        }
                    }
                }
            }
        });
    }
}

fn parse_resp_command(buf: &[u8]) -> Result<Option<(Vec<String>, usize)>, OxideError> {
    if buf.is_empty() {
        return Ok(None);
    }
    if buf[0] != b'*' {
        return Err(OxideError::ProtocolError { expected: '*', found: buf[0] as char });
    }
    let mut pos = 1;
    let array_len = read_int(buf, &mut pos)?;
    if array_len < 0 {
        return Err(OxideError::InvalidArrayLength);
    }
    if array_len == 0 {
        return Ok(Some((vec![], pos)));
    }
    let mut args = Vec::with_capacity(array_len as usize);
    for _ in 0..array_len {
        if pos >= buf.len() {
            return Ok(None);
        }
        if buf[pos] != b'$' {
            return Err(OxideError::ProtocolError { expected: '$', found: buf[pos] as char });
        }
        pos += 1;
        let bulk_len = read_int(buf, &mut pos)?;
        if bulk_len < 0 {
            args.push(String::new());
            continue;
        }
        let bulk_start = pos;
        let bulk_end = pos + bulk_len as usize;
        if bulk_end + 2 > buf.len() {
            return Ok(None);
        }
        let bulk_str = String::from_utf8(buf[bulk_start..bulk_end].to_vec())
            .map_err(|_| OxideError::InvalidBulkString)?;
        args.push(bulk_str);
        pos = bulk_end + 2;
    }
    Ok(Some((args, pos)))
}

fn read_int(buf: &[u8], pos: &mut usize) -> Result<isize, OxideError> {
    let start = *pos;
    while *pos < buf.len() && buf[*pos] != b'\r' {
        *pos += 1;
    }
    if *pos + 1 >= buf.len() || buf[*pos] != b'\r' || buf[*pos + 1] != b'\n' {
        return Err(OxideError::IncompleteRequest);
    }
    let num_str = std::str::from_utf8(&buf[start..*pos])
        .map_err(|_| OxideError::InvalidInteger)?;
    let num = num_str.parse().map_err(|_| OxideError::InvalidInteger)?;
    *pos += 2;
    Ok(num)
}

fn process_command(args: &[String], db: &Db) -> String {
    if args.is_empty() {
        return OxideError::EmptyCommand.to_resp();
    }
    let cmd = args[0].to_uppercase();
    match cmd.as_str() {
        "SET" => {
            if args.len() != 3 {
                return OxideError::WrongArgsCount { cmd: "SET".to_string() }.to_resp();
            }
            if let Err(e) = append_to_wal(&args[1], &args[2]) {
                error!(error = %e, "WAL write failed");
                return OxideError::WalWriteError(e).to_resp();
            }
            let mut db = db.lock().unwrap();
            db.insert(args[1].clone(), args[2].clone());
            "+OK\r\n".to_string()
        }
        "GET" => {
            if args.len() != 2 {
                return OxideError::WrongArgsCount { cmd: "GET".to_string() }.to_resp();
            }
            let db = db.lock().unwrap();
            match db.get(&args[1]) {
                Some(v) => format!("${}\r\n{}\r\n", v.len(), v),
                None => "$-1\r\n".to_string(),
            }
        }
        "DEL" => {
            if args.len() != 2 {
                return OxideError::WrongArgsCount { cmd: "DEL".to_string() }.to_resp();
            }
            let mut db = db.lock().unwrap();
            let existed = db.remove(&args[1]).is_some();
            format!(":{}\r\n", if existed { 1 } else { 0 })
        }
        _ => OxideError::UnknownCommand { cmd }.to_resp(),
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
    let mut db = db.lock().unwrap();
    let mut count = 0;
    for line in reader.lines() {
        let line = line?;
        if let Some((k, v)) = line.split_once('=') {
            db.insert(k.to_string(), v.to_string());
            count += 1;
        }
    }
    if count > 0 {
        info!(records = count, "WAL recovered");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get_del() {
        let db = Arc::new(Mutex::new(BTreeMap::new()));
        let set = vec!["SET".to_string(), "x".to_string(), "42".to_string()];
        assert_eq!(process_command(&set, &db), "+OK\r\n");
        let get = vec!["GET".to_string(), "x".to_string()];
        assert_eq!(process_command(&get, &db), "$2\r\n42\r\n");
        let del = vec!["DEL".to_string(), "x".to_string()];
        assert_eq!(process_command(&del, &db), ":1\r\n");
        let get2 = vec!["GET".to_string(), "x".to_string()];
        assert_eq!(process_command(&get2, &db), "$-1\r\n");
    }

    #[test]
    fn test_resp_parser() {
        let data = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
        let (cmd, used) = parse_resp_command(data).unwrap().unwrap();
        assert_eq!(used, data.len());
        assert_eq!(cmd, vec!["GET", "foo"]);
    }

    #[test]
    fn test_resp_multiple_commands() {
        let data = b"*2\r\n$3\r\nGET\r\n$3\r\na\r\n*2\r\n$3\r\nGET\r\n$3\r\nb\r\n";
        let (cmd1, used1) = parse_resp_command(data).unwrap().unwrap();
        assert_eq!(cmd1, vec!["GET", "a"]);
        let (cmd2, used2) = parse_resp_command(&data[used1..]).unwrap().unwrap();
        assert_eq!(cmd2, vec!["GET", "b"]);
        assert_eq!(used1 + used2, data.len());
    }
}
