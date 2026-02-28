// server.rs

use crate::db::{Database, Value, ValueType, load_rdb, save_rdb};
use crate::queue::CommandQueue;
use crate::resp::{self, RespEncoder, RespParser, RespValue};
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
    tokio::spawn(async move {
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
    });
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
            if array.len() < 3 || array.len() > 6 {
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
            match array.len() {
                3 => {
                    //only key and value
                    curr_db.insert(
                        key,
                        Value {
                            data: ValueType::String(value),
                            expire_at: None,
                        },
                    );

                    RespValue::SimpleString("OK".to_string())
                }
                4 => match &array[3] {
                    RespValue::BulkString(Some(opt)) => {
                        let opt_str = String::from_utf8_lossy(opt).to_ascii_uppercase();
                        if opt_str == "NX" {
                            curr_db.entry(key).or_insert(Value {
                                data: ValueType::String(value),
                                expire_at: None,
                            });
                            RespValue::SimpleString("OK".to_string())
                        } else if opt_str == "XX" {
                            if let Some(existing) = curr_db.get_mut(&key) {
                                existing.data = ValueType::String(value);
                                existing.expire_at = None;
                                RespValue::SimpleString("OK".to_string())
                            } else {
                                RespValue::SimpleString("OK".to_string())
                            }
                        } else {
                            RespValue::Error("ERR unknown option".to_string())
                        }
                    }
                    _ => RespValue::Error("ERR invalid option".to_string()),
                },
                5 => {
                    let opt = match &array[3] {
                        RespValue::BulkString(Some(opt)) => {
                            String::from_utf8_lossy(opt).to_ascii_uppercase()
                        }
                        _ => return RespValue::Error("ERR invalid option".to_string()),
                    };
                    let expire_time = match &array[4] {
                        RespValue::BulkString(Some(v)) => {
                            String::from_utf8_lossy(v).parse::<u64>().unwrap_or(0)
                        }
                        _ => return RespValue::Error("ERR invalid expire time".to_string()),
                    };
                    if opt == "EX" {
                        let expire_at = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or(Duration::ZERO)
                            .as_secs()
                            + expire_time;
                        curr_db.insert(
                            key,
                            Value {
                                data: ValueType::String(value),
                                expire_at: Some(expire_at),
                            },
                        );
                        RespValue::SimpleString("OK".to_string())
                    } else if opt == "PX" {
                        let expire_at = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or(Duration::ZERO)
                            .as_secs()
                            + expire_time / 1000;
                        curr_db.insert(
                            key,
                            Value {
                                data: ValueType::String(value),
                                expire_at: Some(expire_at),
                            },
                        );
                        RespValue::SimpleString("OK".to_string())
                    } else {
                        RespValue::Error("ERR unknown option".to_string())
                    }
                }
                6 => {
                    let opt1 = match &array[3] {
                        RespValue::BulkString(Some(opt)) => {
                            String::from_utf8_lossy(opt).to_ascii_uppercase()
                        }
                        _ => return RespValue::Error("ERR invalid option".to_string()),
                    };
                    let expire_time = match &array[4] {
                        RespValue::BulkString(Some(v)) => {
                            String::from_utf8_lossy(v).parse::<u64>().unwrap_or(0)
                        }
                        _ => return RespValue::Error("ERR invalid expire time".to_string()),
                    };
                    let opt2 = match &array[5] {
                        RespValue::BulkString(Some(opt)) => {
                            String::from_utf8_lossy(opt).to_ascii_uppercase()
                        }
                        _ => return RespValue::Error("ERR invalid option".to_string()),
                    };
                    if (opt1 == "NX" && opt2 == "EX") || (opt1 == "EX" && opt2 == "NX") {
                        let expire_at = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or(Duration::ZERO)
                            .as_secs()
                            + expire_time;
                        curr_db.entry(key).or_insert(Value {
                            data: ValueType::String(value),
                            expire_at: Some(expire_at),
                        });
                        RespValue::SimpleString("OK".to_string())
                    } else if (opt1 == "XX" && opt2 == "EX") || (opt1 == "EX" && opt2 == "XX") {
                        if let Some(existing) = curr_db.get_mut(&key) {
                            existing.data = ValueType::String(value);
                            existing.expire_at = Some(
                                SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or(Duration::ZERO)
                                    .as_secs()
                                    + expire_time,
                            );
                            RespValue::SimpleString("OK".to_string())
                        } else {
                            RespValue::SimpleString("OK".to_string())
                        }
                    } else if (opt1 == "NX" && opt2 == "PX") || (opt1 == "PX" && opt2 == "NX") {
                        let expire_at = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or(Duration::ZERO)
                            .as_secs()
                            + expire_time / 1000;
                        curr_db.entry(key).or_insert(Value {
                            data: ValueType::String(value),
                            expire_at: Some(expire_at),
                        });
                        RespValue::SimpleString("OK".to_string())
                    } else if (opt1 == "XX" && opt2 == "PX") || (opt1 == "PX" && opt2 == "XX") {
                        if let Some(existing) = curr_db.get_mut(&key) {
                            existing.data = ValueType::String(value);
                            existing.expire_at = Some(
                                SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or(Duration::ZERO)
                                    .as_secs()
                                    + expire_time / 1000,
                            );
                            RespValue::SimpleString("OK".to_string())
                        } else {
                            RespValue::SimpleString("OK".to_string())
                        }
                    } else {
                        RespValue::Error("ERR unknown option combination".to_string())
                    }
                }
                _ => RespValue::Error("ERR wrong number of arguments for SET".to_string()),
            }
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

        "EXPIRE" => {
            if array.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for EXPIRE".to_string());
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
            curr_db.get_mut(key.as_str()).map_or_else(
                || RespValue::Integer(0),
                |v| {
                    let expire_at = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO)
                        .as_secs()
                        + String::from_utf8_lossy(&value).parse::<u64>().unwrap_or(0);
                    v.expire_at = Some(expire_at);
                    RespValue::Integer(1)
                },
            )
        }

        "TTL" => {
            if array.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for TTL".to_string());
            }

            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let mut curr_db = _db.get_current();
            match curr_db.get(&key) {
                Some(v) => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO)
                        .as_secs();
                    match v.expire_at {
                        Some(expire) if expire > now => {
                            resp::RespValue::Integer((expire - now) as i64)
                        }
                        Some(_) => {
                            curr_db.remove(&key); // 惰性删除
                            resp::RespValue::Integer(-2) // key不存在
                        }
                        None => resp::RespValue::Integer(-1), // key存在但没有过期时间
                    }
                }
                None => resp::RespValue::Integer(-2), // key不存在
            }
        }

        "INCR" => {
            if array.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for INCR".to_string());
            }
            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let mut curr_db = _db.get_current();
            match curr_db.get_mut(&key) {
                Some(v) => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO)
                        .as_secs();

                    if let Some(expire) = v.expire_at {
                        if now >= expire {
                            curr_db.remove(&key); // 惰性删除
                            curr_db.insert(
                                key,
                                Value {
                                    data: ValueType::String(b"1".to_vec()),
                                    expire_at: None,
                                },
                            );
                            return RespValue::Integer(1);
                        }
                    }

                    match &mut v.data {
                        ValueType::String(s) => {
                            let num = String::from_utf8_lossy(s).parse::<i64>();
                            match num {
                                Ok(n) => {
                                    let new_val = n + 1;
                                    *s = new_val.to_string().as_bytes().to_vec();
                                    RespValue::Integer(new_val)
                                }
                                Err(_) => {
                                    RespValue::Error("ERR value is not an integer".to_string())
                                }
                            }
                        }
                        _ => RespValue::Error("ERR value is not an integer".to_string()),
                    }
                }
                None => {
                    curr_db.insert(
                        key,
                        Value {
                            data: ValueType::String(b"1".to_vec()),
                            expire_at: None,
                        },
                    );
                    RespValue::Integer(1)
                }
            }
        }

        "DECR" => {
            if array.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for DECR".to_string());
            }
            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let mut curr_db = _db.get_current();
            match curr_db.get_mut(&key) {
                Some(v) => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO)
                        .as_secs();

                    if let Some(expire) = v.expire_at {
                        if now >= expire {
                            curr_db.remove(&key); // 惰性删除
                            curr_db.insert(
                                key,
                                Value {
                                    data: ValueType::String(b"-1".to_vec()),
                                    expire_at: None,
                                },
                            );
                            return RespValue::Integer(-1);
                        }
                    }

                    match &mut v.data {
                        ValueType::String(s) => {
                            let num = String::from_utf8_lossy(s).parse::<i64>();
                            match num {
                                Ok(n) => {
                                    let new_val = n - 1;
                                    *s = new_val.to_string().as_bytes().to_vec();
                                    RespValue::Integer(new_val)
                                }
                                Err(_) => {
                                    RespValue::Error("ERR value is not an integer".to_string())
                                }
                            }
                        }
                        _ => RespValue::Error("ERR value is not an integer".to_string()),
                    }
                }
                None => {
                    curr_db.insert(
                        key,
                        Value {
                            data: ValueType::String(b"-1".to_vec()),
                            expire_at: None,
                        },
                    );
                    RespValue::Integer(-1)
                }
            }
        }

        "INCRBY" => {
            if array.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for INCRBY".to_string());
            }
            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let increment = match &array[2] {
                RespValue::BulkString(Some(v)) => match String::from_utf8_lossy(v).parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => {
                        return RespValue::Error("ERR increment is not an integer".to_string());
                    }
                },
                _ => return RespValue::Error("ERR invalid increment".to_string()),
            };
            let mut curr_db = _db.get_current();
            match curr_db.get_mut(&key) {
                Some(v) => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO)
                        .as_secs();

                    if let Some(expire) = v.expire_at {
                        if now >= expire {
                            curr_db.remove(&key); // 惰性删除
                            curr_db.insert(
                                key,
                                Value {
                                    data: ValueType::String(
                                        increment.to_string().as_bytes().to_vec(),
                                    ),
                                    expire_at: None,
                                },
                            );
                            return RespValue::Integer(increment);
                        }
                    }

                    match &mut v.data {
                        ValueType::String(s) => {
                            let num = String::from_utf8_lossy(s).parse::<i64>();
                            match num {
                                Ok(n) => {
                                    let new_val = n + increment;
                                    *s = new_val.to_string().as_bytes().to_vec();
                                    RespValue::Integer(new_val)
                                }
                                Err(_) => {
                                    RespValue::Error("ERR value is not an integer".to_string())
                                }
                            }
                        }
                        _ => RespValue::Error("ERR value is not an integer".to_string()),
                    }
                }
                None => {
                    curr_db.insert(
                        key,
                        Value {
                            data: ValueType::String(increment.to_string().as_bytes().to_vec()),
                            expire_at: None,
                        },
                    );
                    RespValue::Integer(increment)
                }
            }
        }

        "DECRBY" => {
            if array.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for DECRBY".to_string());
            }
            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let decrement = match &array[2] {
                RespValue::BulkString(Some(v)) => match String::from_utf8_lossy(v).parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => {
                        return RespValue::Error("ERR decrement is not an integer".to_string());
                    }
                },
                _ => return RespValue::Error("ERR invalid decrement".to_string()),
            };
            let mut curr_db = _db.get_current();
            match curr_db.get_mut(&key) {
                Some(v) => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO)
                        .as_secs();

                    if let Some(expire) = v.expire_at {
                        if now >= expire {
                            curr_db.remove(&key); // 惰性删除
                            curr_db.insert(
                                key,
                                Value {
                                    data: ValueType::String(
                                        decrement.to_string().as_bytes().to_vec(),
                                    ),
                                    expire_at: None,
                                },
                            );
                            return RespValue::Integer(decrement);
                        }
                    }

                    match &mut v.data {
                        ValueType::String(s) => {
                            let num = String::from_utf8_lossy(s).parse::<i64>();
                            match num {
                                Ok(n) => {
                                    let new_val = n - decrement;
                                    *s = new_val.to_string().as_bytes().to_vec();
                                    RespValue::Integer(new_val)
                                }
                                Err(_) => {
                                    RespValue::Error("ERR value is not an integer".to_string())
                                }
                            }
                        }
                        _ => RespValue::Error("ERR value is not an integer".to_string()),
                    }
                }
                None => {
                    curr_db.insert(
                        key,
                        Value {
                            data: ValueType::String(decrement.to_string().as_bytes().to_vec()),
                            expire_at: None,
                        },
                    );
                    RespValue::Integer(decrement)
                }
            }
        }

        "EXISTS" => {
            if array.len() < 2 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let current_db = _db.get_current();
            let mut count = 0;
            for item in &array[1..] {
                let key = match item {
                    RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                    RespValue::SimpleString(s) => s.clone(),
                    _ => continue,
                };
                if current_db.contains_key(&key) {
                    count += 1;
                }
            }
            RespValue::Integer(count)
        }

        "TYPE" => {
            if array.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for TYPE".to_string());
            }
            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let current_db = _db.get_current();
            current_db.get(&key).map_or_else(
                || RespValue::Error("ERR no such key".to_string()),
                |v| match &v.data {
                    ValueType::String(_) => RespValue::SimpleString("string".to_string()),
                    ValueType::Hash(_) => RespValue::SimpleString("hash".to_string()),
                },
            )
        }

        "DBSIZE" => {
            let current_db = _db.get_current();
            RespValue::Integer(current_db.len() as i64)
        }

        "FLUSHDB" => {
            let mut current_db = _db.get_current();
            current_db.clear();
            RespValue::SimpleString("OK".to_string())
        }

        "RENAME" => {
            if(array.len() != 3) {
                return RespValue::Error("ERR wrong number of arguments for RENAME".to_string());
            }
            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let new_key = match &array[2] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid new key".to_string()),
            };
            let mut current_db = _db.get_current();
            if !current_db.contains_key(&key) {
                return RespValue::Error("ERR no such key".to_string());
            }
            if current_db.contains_key(&new_key) {
                return RespValue::Error("ERR new key already exists".to_string());
            }
            if let Some(value) = current_db.remove(&key) {
                current_db.insert(new_key, value);
            }
            RespValue::SimpleString("OK".to_string())
        }

        "KEYS" => {
            if array.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for KEYS".to_string());
            }
            let pattern = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid pattern".to_string()),
            };
            let current_db = _db.get_current();
            let mut matching_keys = Vec::new();
            for key in current_db.keys() {
                if pattern == "*" || key.contains(&pattern.replace("*", "")) {
                    matching_keys.push(RespValue::BulkString(Some(key.as_bytes().to_vec())));
                }
            }
            RespValue::Array(Some(matching_keys))
        }

        "SELECT" => {
            if(array.len() != 2) {
                return RespValue::Error("ERR wrong number of arguments for SELECT".to_string());
            }
            let index = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid index".to_string()),
            };
            let index: usize = match index.parse() {
                Ok(i) => i,
                Err(_) => return RespValue::Error("ERR invalid index".to_string()),
            };
            if !_db.select_db(index) {
                return RespValue::Error("ERR invalid index".to_string());
            }
            RespValue::SimpleString("OK".to_string())
        }
        _ => RespValue::Error(format!("ERR unknown command '{}'", cmd_name)),
    }
}
