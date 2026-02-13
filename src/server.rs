// server.rs

use crate::db::{Database, Value, ValueType, load_rdb, save_rdb};
use crate::queue::CommandQueue;
use crate::resp::{RespEncoder, RespParser, RespValue};
use std::collections::HashMap;
use std::io;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

const MAX_BUFFER_SIZE: usize = 512 * 1024 * 1024; // 512MB 防 DoS

pub async fn run_server(addr: &str) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await?;

    type Cmd = (RespValue, oneshot::Sender<RespValue>);

    let (tx_cmd, mut rx_cmd) = mpsc::channel::<Cmd>(1024);

    println!("Mini-Redis listening on {}", addr);
    let mut db = Database::new();
    load_rdb(&mut db, "dump.rdb").ok();
    loop {
        let (mut socket, peer_addr) = listener.accept().await?;
        println!("New connection from {}", peer_addr);

        let tx_cmd_clone = tx_cmd.clone();
        tokio::spawn(async move {
            let mut parser = RespParser::new();
            loop {
                if let Some(command) = parser.read_value(&mut socket).await.unwrap() {
                    println!("Received command: {:?}", command);
                    let (resp_tx, mut resp_rx) = oneshot::channel();
                    tx_cmd_clone.send((command, resp_tx)).await.unwrap();
                    if let Ok(response) = resp_rx.await {
                        let resp_bytes = RespEncoder::encode_resp(&response);
                        socket.write_all(&resp_bytes).await.unwrap();
                        socket.flush().await.unwrap();
                    }
                } else {
                    print!("Client disconnected\n");
                    return;
                }
            }
        });
        loop {
            if rx_cmd.capacity() == 0 {
                break;
            }
            if let Some((command, resp_tx)) = rx_cmd.recv().await {
                let response = execute_command(&command, &mut db);
                resp_tx.send(response).unwrap();
            } else {
                break;
            }
        }
    }
}

async fn handle_client(mut socket: TcpStream, db: Arc<Mutex<Database>>) -> io::Result<()> {
    let mut parser = RespParser::new();
    print!("Handling new client\n");
    loop {
        // 你的神级函数：内部自动处理流式读 + 解析
        if let Some(command) = parser.read_value(&mut socket).await? {
            println!("Received command: {:?}", command);
            let response = {
                let mut db_guard = db.lock().unwrap(); // 锁在这里
                execute_command(&command, &mut *db_guard)
            }; // guard 离开作用域，锁自动释放
            let resp_bytes = RespEncoder::encode_resp(&response);

            socket.write_all(&resp_bytes).await?;
            socket.flush().await?;
        } else {
            print!("Client disconnected\n");
            return Ok(()); // 连接关闭
        }
    }
}

fn execute_command(command: &RespValue, _db: &mut Database) -> RespValue {
    // 命令必须是 Array
    let array = match command {
        RespValue::Array(Some(arr)) => arr,
        _ => return RespValue::Error("ERR invalid command".to_string()),
    };

    // 命令名是第一个元素（Bulk String 或 Simple String）
    let cmd_name = match array.first() {
        Some(RespValue::BulkString(Some(bytes))) => {
            String::from_utf8_lossy(bytes).to_ascii_uppercase()
        }
        Some(RespValue::SimpleString(s)) => s.to_ascii_uppercase(),
        _ => return RespValue::Error("ERR invalid command name".to_string()),
    };

    match cmd_name.as_str() {
        "PING" => {
            // PING 或 PING message
            if array.len() == 1 {
                RespValue::SimpleString("PONG".to_string())
            } else if array.len() == 2 {
                // 返回第二个参数
                match &array[1] {
                    RespValue::BulkString(Some(data)) => RespValue::BulkString(Some(data.clone())),
                    RespValue::SimpleString(s) => {
                        RespValue::BulkString(Some(s.as_bytes().to_vec()))
                    }
                    _ => RespValue::Error("ERR wrong arguments for PING".to_string()),
                }
            } else {
                RespValue::Error("ERR wrong number of arguments for PING".to_string())
            }
        }

        "ECHO" => {
            if array.len() == 2 {
                match &array[1] {
                    RespValue::BulkString(Some(data)) => RespValue::BulkString(Some(data.clone())),
                    RespValue::SimpleString(s) => {
                        RespValue::BulkString(Some(s.as_bytes().to_vec()))
                    }
                    _ => RespValue::Error("ERR invalid argument for ECHO".to_string()),
                }
            } else {
                RespValue::Error("ERR wrong number of arguments for ECHO".to_string())
            }
        }

        "SET" => {
            if array.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for SET".to_string());
            }

            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };

            let value = match &array[2] {
                RespValue::BulkString(Some(v)) => v.clone(),
                _ => return RespValue::Error("ERR invalid value".to_string()),
            };

            let mut curr_db = _db.get_current();
            curr_db.insert(
                key,
                Value {
                    data: ValueType::String(value),
                    expire_at: None,
                },
            );

            RespValue::SimpleString("OK".to_string())
        }

        "GET" => {
            if array.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for GET".to_string());
            }

            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };

            let curr_db = _db.get_current();
            match curr_db.get(&key) {
                Some(v) => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO)
                        .as_secs();

                    if let Some(expire) = v.expire_at {
                        if now >= expire {
                            curr_db.remove(&key); // 惰性删除
                            return RespValue::BulkString(None);
                        }
                    }

                    match &v.data {
                        ValueType::String(res) => RespValue::BulkString(Some(res.clone())),
                        _ => RespValue::Error("wrong data type for this order".to_string()),
                    }
                }
                None => RespValue::BulkString(None),
            }
        }

        "DEL" => {
            if array.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments for DEL".to_string());
            }
            let curr_db = _db.get_current();
            let mut removed = 0;
            for item in &array[1..] {
                let key = match item {
                    RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                    RespValue::SimpleString(s) => s.clone(),
                    _ => continue,
                };
                removed = if curr_db.remove(&key).is_some() {
                    removed + 1
                } else {
                    removed + 0
                };
            }
            RespValue::Integer(removed as i64)
        }

        "HSET" => {
            if array.len() != 4 {
                return RespValue::Error("ERR wrong number of arguments for HSET".to_string());
            }
            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let field = match &array[2] {
                RespValue::BulkString(Some(v)) => String::from_utf8_lossy(v).to_string(),
                _ => return RespValue::Error("ERR invalid value".to_string()),
            };
            let value = match &array[3] {
                RespValue::BulkString(Some(v)) => v.clone(),
                _ => return RespValue::Error("ERR invalid value".to_string()),
            };
            let curr_db = _db.get_current();
            match curr_db.get_mut(&key) {
                Some(v) => match &mut v.data {
                    ValueType::Hash(hValue) => {
                        hValue.insert(field.clone(), value.clone());
                    }
                    _ => {
                        let mut hashValue = HashMap::new();
                        hashValue.insert(field, value);
                        curr_db.insert(
                            key,
                            Value {
                                data: ValueType::Hash(hashValue),
                                expire_at: None,
                            },
                        );
                    }
                },
                None => {
                    let mut hashValue = HashMap::new();
                    hashValue.insert(field, value);
                    curr_db.insert(
                        key,
                        Value {
                            data: ValueType::Hash(hashValue),
                            expire_at: None,
                        },
                    );
                }
            }
            RespValue::SimpleString("OK".to_string())
        }

        "HGET" => {
            if array.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for HGET".to_string());
            }
            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let field = match &array[2] {
                RespValue::BulkString(Some(v)) => String::from_utf8_lossy(v).to_string(),
                _ => return RespValue::Error("ERR invalid value".to_string()),
            };
            let curr_db = _db.get_current();
            match curr_db.get(&key) {
                Some(v) => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO)
                        .as_secs();

                    if let Some(expire) = v.expire_at {
                        if now >= expire {
                            curr_db.remove(&key); // 惰性删除
                            return RespValue::BulkString(None);
                        }
                    }
                    match &v.data {
                        ValueType::Hash(hashData) => match hashData.get(&field) {
                            Some(hValue) => RespValue::BulkString(Some(hValue.clone())),
                            None => RespValue::BulkString(None),
                        },
                        _ => RespValue::Error("wrong data type for this order".to_string()),
                    }
                }
                None => RespValue::BulkString(None),
            }
        }

        "HGETALL" => {
            if array.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for HGETALL".to_string());
            }
            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let curr_db = _db.get_current();
            match curr_db.get(&key) {
                Some(v) => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO)
                        .as_secs();

                    if let Some(expire) = v.expire_at {
                        if now >= expire {
                            curr_db.remove(&key); // 惰性删除
                            return RespValue::BulkString(None);
                        }
                    }
                    match &v.data {
                        ValueType::Hash(hashData) => {
                            println!("HGETALL LENGTH: {}", hashData.len());
                            let mut resArray: Vec<RespValue> =
                                Vec::with_capacity(hashData.len() * 2);
                            for (key, value) in hashData {
                                resArray.push(RespValue::BulkString(Some(
                                    key.clone().as_bytes().to_vec(),
                                )));
                                resArray.push(RespValue::BulkString(Some(value.clone())));
                            }
                            println!("HGETALL DATA");
                            println!("{:?}", resArray);
                            RespValue::Array(Some(resArray.clone()))
                        }
                        _ => RespValue::Error("wrong data type for this order".to_string()),
                    }
                }
                None => RespValue::BulkString(None),
            }
        }

        "HDEL" => {
            if array.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments for HDEL".to_string());
            }
            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let curr_db = _db.get_current();
            match curr_db.get_mut(&key) {
                Some(v) => {
                    for item in &array[2..] {
                        let field = match &item {
                            RespValue::BulkString(Some(v)) => {
                                String::from_utf8_lossy(v).to_string()
                            }
                            _ => return RespValue::Error("ERR invalid value".to_string()),
                        };
                        match &mut v.data {
                            ValueType::Hash(hValue) => {
                                hValue.remove(&field);
                            }
                            _ => continue,
                        }
                    }
                    RespValue::SimpleString("OK".to_string())
                }
                None => RespValue::SimpleString("OK".to_string()),
            }
        }

        "SAVE" => {
            save_rdb(_db, "dump.rdb").ok();
            RespValue::SimpleString("OK".to_string())
        }
        _ => RespValue::Error(format!("ERR unknown command '{}'", cmd_name)),
    }
}
