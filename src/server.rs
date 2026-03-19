// server.rs

use crate::db::{Database, Value, ValueType, load_rdb, save_rdb};
use crate::pubsub;
use crate::pubsub::PubSub;
use crate::queue::CommandQueue;
use crate::resp::{self, RespEncoder, RespParser, RespValue};
use core::error;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_stream::StreamMap;
use tokio_stream::wrappers::BroadcastStream;

const MAX_BUFFER_SIZE: usize = 512 * 1024 * 1024; // 512MB 防 DoS

pub async fn run_server(addr: &str) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await?;

    type Cmd = (RespValue, oneshot::Sender<RespValue>);

    let (tx_cmd, mut rx_cmd) = mpsc::channel::<Cmd>(1024);
    let pubsub = Arc::new(Mutex::new(PubSub::new()));
    println!("Mini-Redis listening on {}", addr);
    let mut db = Database::new();
    load_rdb(&mut db, "dump.rdb").ok();
    let clone_pubsub = Arc::clone(&pubsub);
    tokio::spawn(async move {
        loop {
            if rx_cmd.capacity() == 0 {
                break;
            }
            if let Some((command, resp_tx)) = rx_cmd.recv().await {
                let response =
                    execute_command(&command, &mut db, &mut clone_pubsub.lock().unwrap());
                resp_tx.send(response).unwrap();
            } else {
                break;
            }
        }
    });
    loop {
        let (mut socket, peer_addr) = listener.accept().await?;
        let tx_clone = tx_cmd.clone();
        let pubsub_clone = Arc::clone(&pubsub);
        println!("New connection from {}", peer_addr);
        tokio::spawn(async move {
            let mut parse = RespParser::new();
            let mut stream_map = StreamMap::new();
            loop {
                if !stream_map.is_empty() {
                    tokio::select! {
                        result = parse.read_value(&mut socket)=> {
                            match result {
                                 Ok(Some(resp_value))=> {
                                match &resp_value {
                                RespValue::Array(Some(arr)) => {
                                    let cmd_name = match arr.first() {
                                        Some(RespValue::BulkString(Some(bytes))) => {
                                            String::from_utf8_lossy(bytes).to_ascii_uppercase()
                                        }
                                        Some(RespValue::SimpleString(s)) => s.to_ascii_uppercase(),
                                        _ => {
                                            let error_response = RespValue::Error("ERR invalid command name".to_string());
                                            let resp_bytes = RespEncoder::encode_resp(&error_response);
                                            socket.write_all(&resp_bytes).await.unwrap();
                                            socket.flush().await.unwrap();
                                            continue;
                                        },
                                    };
                                    if(cmd_name == "SUBSCRIBE"){
                                        if arr.len() < 2 {
                                            let error_response = RespValue::Error("ERR wrong number of arguments for SUBSCRIBE".to_string());
                                            let resp_bytes = RespEncoder::encode_resp(&error_response);
                                            socket.write_all(&resp_bytes).await.unwrap();
                                            socket.flush().await.unwrap();
                                            continue;
                                        }
                                        for item in &arr[1..]{
                                            let channel = match item {
                                                RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
                                                RespValue::SimpleString(s) => s.clone(),
                                                _ => {
                                                    let error_response = RespValue::Error("ERR invalid channel name".to_string());
                                                    let resp_bytes = RespEncoder::encode_resp(&error_response);
                                                    socket.write_all(&resp_bytes).await.unwrap();
                                                    socket.flush().await.unwrap();
                                                    continue;
                                                },
                                            };
                                            let receiver = {
                                                let mut pubsub = pubsub_clone.lock().unwrap();
                                                pubsub.subscribe(&channel)
                                            };
                                            stream_map.insert(channel.clone(), BroadcastStream::new(receiver));
                                            let response = RespValue::Array(Some(vec![
                                                RespValue::BulkString(Some(b"subscribe".to_vec())),
                                                RespValue::BulkString(Some(channel.as_bytes().to_vec())),
                                                RespValue::Integer(stream_map.len() as i64),
                                            ]));
                                            let resp_bytes = RespEncoder::encode_resp(&response);
                                            socket.write_all(&resp_bytes).await.unwrap();
                                            socket.flush().await.unwrap();
                                        }

                                    }
                                    else if(cmd_name == "UNSUBSCRIBE") {
                                        if arr.len() < 2 {
                                            for item in stream_map.keys() {
                                                let mut pubsub = pubsub_clone.lock().unwrap();
                                                pubsub.unsubscribe(item);
                                            }
                                            stream_map.clear();
                                            let response = RespValue::Array(Some(vec![
                                                RespValue::BulkString(Some(b"unsubscribe".to_vec())),
                                                RespValue::BulkString(None),
                                                RespValue::Integer(0),
                                            ]));
                                            let resp_bytes = RespEncoder::encode_resp(&response);
                                            socket.write_all(&resp_bytes).await.unwrap();
                                            socket.flush().await.unwrap();
                                            continue;
                                        }
                                        for item in &arr[1..]{
                                            let channel = match item {
                                                RespValue::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
                                                RespValue::SimpleString(s) => s.clone(),
                                                _ => {
                                                    let error_response = RespValue::Error("ERR invalid channel name".to_string());
                                                    let resp_bytes = RespEncoder::encode_resp(&error_response);
                                                    socket.write_all(&resp_bytes).await.unwrap();
                                                    socket.flush().await.unwrap();
                                                    continue;
                                                },
                                            };
                                            {
                                                let mut pubsub = pubsub_clone.lock().unwrap();
                                                pubsub.unsubscribe(&channel);
                                            }
                                            stream_map.remove(&channel);
                                            let response = RespValue::Array(Some(vec![
                                                RespValue::BulkString(Some(b"unsubscribe".to_vec())),
                                                RespValue::BulkString(Some(channel.as_bytes().to_vec())),
                                                RespValue::Integer(stream_map.len() as i64),
                                            ]));
                                            let resp_bytes = RespEncoder::encode_resp(&response);
                                            socket.write_all(&resp_bytes).await.unwrap();
                                            socket.flush().await.unwrap();
                                        }

                                    }
                                    else if cmd_name == "PING" || cmd_name == "ECHO" {
                                        let (resp_tx, resp_rx) = oneshot::channel();
                                        tx_clone.send((resp_value.clone(), resp_tx)).await.unwrap();
                                        if let Ok(response) = resp_rx.await {
                                            let resp_bytes = RespEncoder::encode_resp(&response);
                                            socket.write_all(&resp_bytes).await.unwrap();
                                            socket.flush().await.unwrap();
                                        }
                                    }
                                    else {
                                        // subscribe mode can't execute other commands, only subscribe and unsubscribe
                                        let error_response = RespValue::Error("ERR only SUBSCRIBE and UNSUBSCRIBE commands are allowed in subscribe mode".to_string());
                                        let resp_bytes = RespEncoder::encode_resp(&error_response);
                                        socket.write_all(&resp_bytes).await.unwrap();
                                        socket.flush().await.unwrap();
                                    }
                                }
                                _ => {
                                    let error_response = RespValue::Error("ERR invalid command format".to_string());
                                    let resp_bytes = RespEncoder::encode_resp(&error_response);
                                    socket.write_all(&resp_bytes).await.unwrap();
                                    socket.flush().await.unwrap();
                                }
                            }}
                            Ok(None) => {
                                    // 客户端断开连接
                                    println!("Client {} disconnected (subscribe mode)", peer_addr);
                                    // 清理所有订阅
                                    let channels: Vec<String> = stream_map.keys().cloned().collect();
                                    let mut pubsub = pubsub_clone.lock().unwrap();
                                    for channel in &channels {
                                        pubsub.unsubscribe(channel);
                                    }
                                    return;  // 退出整个 Task
                                }
                                Err(e) => {
                                    eprintln!("Parse error from {} in subscribe mode: {}", peer_addr, e);
                                    // 清理所有订阅
                                    let channels: Vec<String> = stream_map.keys().cloned().collect();
                                    let mut pubsub = pubsub_clone.lock().unwrap();
                                    for channel in &channels {
                                        pubsub.unsubscribe(channel);
                                    }
                                    return;  // 退出整个 Task
                                }
                            }
                        }

                        Some((channel, msg)) = stream_map.next() => {
                            let message = match msg {
                                Ok(m) => m,
                                Err(_) => continue, // 订阅者已断开，继续监听其他消息
                            };
                            let response = RespValue::Array(Some(vec![
                                RespValue::BulkString(Some(b"message".to_vec())),
                                RespValue::BulkString(Some(channel.as_bytes().to_vec())),
                                RespValue::BulkString(Some(message.as_bytes().to_vec())),
                            ]));
                            let resp_bytes = RespEncoder::encode_resp(&response);
                            socket.write_all(&resp_bytes).await.unwrap();
                            socket.flush().await.unwrap();
                        }
                         else => {
                            break;
                         }
                    }
                } else {
                    if let Ok(Some(resp_value)) = parse.read_value(&mut socket).await {
                        match &resp_value {
                            RespValue::Array(Some(arr)) => {
                                let cmd_name = match arr.first() {
                                    Some(RespValue::BulkString(Some(bytes))) => {
                                        String::from_utf8_lossy(bytes).to_ascii_uppercase()
                                    }
                                    Some(RespValue::SimpleString(s)) => s.to_ascii_uppercase(),
                                    _ => {
                                        let error_response = RespValue::Error(
                                            "ERR invalid command name".to_string(),
                                        );
                                        let resp_bytes = RespEncoder::encode_resp(&error_response);
                                        socket.write_all(&resp_bytes).await.unwrap();
                                        socket.flush().await.unwrap();
                                        continue;
                                    }
                                };
                                if cmd_name == "SUBSCRIBE" {
                                    if arr.len() < 2 {
                                        let error_response = RespValue::Error(
                                            "ERR wrong number of arguments for SUBSCRIBE"
                                                .to_string(),
                                        );
                                        let resp_bytes = RespEncoder::encode_resp(&error_response);
                                        socket.write_all(&resp_bytes).await.unwrap();
                                        socket.flush().await.unwrap();
                                        continue;
                                    }
                                    for item in &arr[1..] {
                                        let channel = match item {
                                            RespValue::BulkString(Some(bytes)) => {
                                                String::from_utf8_lossy(bytes).to_string()
                                            }
                                            RespValue::SimpleString(s) => s.clone(),
                                            _ => {
                                                let error_response = RespValue::Error(
                                                    "ERR invalid channel name".to_string(),
                                                );
                                                let resp_bytes =
                                                    RespEncoder::encode_resp(&error_response);
                                                socket.write_all(&resp_bytes).await.unwrap();
                                                socket.flush().await.unwrap();
                                                continue;
                                            }
                                        };
                                        let receiver = {
                                            let mut pubsub = pubsub_clone.lock().unwrap();
                                            pubsub.subscribe(&channel)
                                        };
                                        stream_map.insert(
                                            channel.clone(),
                                            BroadcastStream::new(receiver),
                                        );
                                        let response = RespValue::Array(Some(vec![
                                            RespValue::BulkString(Some(b"subscribe".to_vec())),
                                            RespValue::BulkString(Some(
                                                channel.as_bytes().to_vec(),
                                            )),
                                            RespValue::Integer(stream_map.len() as i64),
                                        ]));
                                        let resp_bytes = RespEncoder::encode_resp(&response);
                                        socket.write_all(&resp_bytes).await.unwrap();
                                        socket.flush().await.unwrap();
                                    }
                                } else if cmd_name == "UNSUBSCRIBE" {
                                    // normal mode only return 0 when command is unsubscribe
                                    let response = RespValue::Array(Some(vec![
                                        RespValue::BulkString(Some(b"unsubscribe".to_vec())),
                                        RespValue::BulkString(None),
                                        RespValue::Integer(0),
                                    ]));
                                    let resp_bytes = RespEncoder::encode_resp(&response);
                                    socket.write_all(&resp_bytes).await.unwrap();
                                    socket.flush().await.unwrap();
                                } else {
                                    let (resp_tx, resp_rx) = oneshot::channel();
                                    tx_clone.send((resp_value.clone(), resp_tx)).await.unwrap();
                                    if let Ok(response) = resp_rx.await {
                                        let resp_bytes = RespEncoder::encode_resp(&response);
                                        socket.write_all(&resp_bytes).await.unwrap();
                                        socket.flush().await.unwrap();
                                    }
                                }
                            }
                            _ => {
                                let error_response =
                                    RespValue::Error("ERR invalid command format".to_string());
                                let resp_bytes = RespEncoder::encode_resp(&error_response);
                                socket.write_all(&resp_bytes).await.unwrap();
                                socket.flush().await.unwrap();
                            }
                        }
                    } else {
                        println!("Client {} disconnected", peer_addr);
                        break;
                    }
                }
            }
        });
    }
}

fn execute_command(command: &RespValue, _db: &mut Database, pubsub: &mut PubSub) -> RespValue {
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
                    ValueType::List(_) => RespValue::SimpleString("list".to_string()),
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
            if (array.len() != 3) {
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
            if (array.len() != 2) {
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

        "APPEND" => {
            if (array.len() != 3) {
                return RespValue::Error("ERR wrong number of arguments for APPEND".to_string());
            }
            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let append_value = match &array[2] {
                RespValue::BulkString(Some(v)) => v.clone(),
                _ => return RespValue::Error("ERR invalid value".to_string()),
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
                                    data: ValueType::String(append_value.clone()),
                                    expire_at: None,
                                },
                            );
                            return RespValue::Integer(append_value.len() as i64);
                        }
                    }

                    match &mut v.data {
                        ValueType::String(s) => {
                            s.extend_from_slice(&append_value);
                            RespValue::Integer(s.len() as i64)
                        }
                        _ => RespValue::Error("ERR value is not a string".to_string()),
                    }
                }
                None => {
                    curr_db.insert(
                        key,
                        Value {
                            data: ValueType::String(append_value.clone()),
                            expire_at: None,
                        },
                    );
                    RespValue::Integer(append_value.len() as i64)
                }
            }
        }

        "STRLEN" => {
            if (array.len() != 2) {
                return RespValue::Error("ERR wrong number of arguments for STRLEN".to_string());
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
                            return RespValue::Integer(0);
                        }
                    }

                    match &v.data {
                        ValueType::String(s) => RespValue::Integer(s.len() as i64),
                        _ => RespValue::Error("ERR value is not a string".to_string()),
                    }
                }
                None => RespValue::Integer(0),
            }
        }

        "HEXISTS" => {
            if array.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for HEXISTS".to_string());
            }
            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let field = match &array[2] {
                RespValue::BulkString(Some(v)) => String::from_utf8_lossy(v).to_string(),
                _ => return RespValue::Error("ERR invalid field".to_string()),
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
                            return RespValue::Integer(0);
                        }
                    }

                    match &v.data {
                        ValueType::Hash(hashData) => {
                            if hashData.contains_key(&field) {
                                RespValue::Integer(1)
                            } else {
                                RespValue::Integer(0)
                            }
                        }
                        _ => RespValue::Error("ERR value is not a hash".to_string()),
                    }
                }
                None => RespValue::Integer(0),
            }
        }

        "HLEN" => {
            if array.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for HLEN".to_string());
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
                            return RespValue::Integer(0);
                        }
                    }

                    match &v.data {
                        ValueType::Hash(hashData) => RespValue::Integer(hashData.len() as i64),
                        _ => RespValue::Error("ERR value is not a hash".to_string()),
                    }
                }
                None => RespValue::Integer(0),
            }
        }

        "HKEYS" => {
            if array.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for HKEYS".to_string());
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
                            return RespValue::Array(Some(vec![]));
                        }
                    }

                    match &v.data {
                        ValueType::Hash(hashData) => {
                            let keys: Vec<RespValue> = hashData
                                .keys()
                                .map(|k| RespValue::BulkString(Some(k.as_bytes().to_vec())))
                                .collect();
                            RespValue::Array(Some(keys))
                        }
                        _ => RespValue::Error("ERR value is not a hash".to_string()),
                    }
                }
                None => RespValue::Array(Some(vec![])),
            }
        }

        "HVALS" => {
            if array.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for HVALS".to_string());
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
                            return RespValue::Array(Some(vec![]));
                        }
                    }

                    match &v.data {
                        ValueType::Hash(hashData) => {
                            let values: Vec<RespValue> = hashData
                                .values()
                                .map(|v| RespValue::BulkString(Some(v.clone())))
                                .collect();
                            RespValue::Array(Some(values))
                        }
                        _ => RespValue::Error("ERR value is not a hash".to_string()),
                    }
                }
                None => RespValue::Array(Some(vec![])),
            }
        }

        "LPUSH" | "RPUSH" => {
            if array.len() < 3 {
                return RespValue::Error(format!("ERR wrong number of arguments for {}", cmd_name));
            }
            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let mut curr_db = _db.get_current();
            let list = match curr_db.get_mut(&key) {
                Some(v) => match &mut v.data {
                    ValueType::List(l) => l,
                    ValueType::String(_) | ValueType::Hash(_) => {
                        return RespValue::Error("ERR value is not a list".to_string());
                    }
                },
                None => {
                    curr_db.insert(
                        key.clone(),
                        Value {
                            data: ValueType::List(VecDeque::new()),
                            expire_at: None,
                        },
                    );
                    match curr_db.get_mut(&key) {
                        Some(v) => match &mut v.data {
                            ValueType::List(l) => l,
                            _ => unreachable!(),
                        },
                        None => unreachable!(),
                    }
                }
            };
            for item in &array[2..] {
                let value = match item {
                    RespValue::BulkString(Some(v)) => v.clone(),
                    _ => return RespValue::Error("ERR invalid value".to_string()),
                };
                if cmd_name == "LPUSH" {
                    list.push_front(value);
                } else {
                    list.push_back(value);
                }
            }
            RespValue::Integer(list.len() as i64)
        }

        "LPOP" | "RPOP" => {
            if array.len() != 2 {
                return RespValue::Error(format!("ERR wrong number of arguments for {}", cmd_name));
            }
            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let mut curr_db = _db.get_current();
            match curr_db.get_mut(&key) {
                Some(v) => match &mut v.data {
                    ValueType::List(l) => {
                        let result = if cmd_name == "LPOP" {
                            l.pop_front()
                        } else {
                            l.pop_back()
                        };
                        match result {
                            Some(item) => RespValue::BulkString(Some(item)),
                            None => RespValue::SimpleString("None".to_string()),
                        }
                    }
                    ValueType::String(_) | ValueType::Hash(_) => {
                        return RespValue::Error("ERR value is not a list".to_string());
                    }
                },
                None => RespValue::SimpleString("None".to_string()),
            }
        }

        "LLEN" => {
            if array.len() != 2 {
                return RespValue::Error("ERR wrong number of arguments for LLEN".to_string());
            }
            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let curr_db = _db.get_current();
            match curr_db.get(&key) {
                Some(v) => match &v.data {
                    ValueType::List(l) => RespValue::Integer(l.len() as i64),
                    ValueType::String(_) | ValueType::Hash(_) => {
                        return RespValue::Error("ERR value is not a list".to_string());
                    }
                },
                None => RespValue::Integer(0),
            }
        }

        "LRANGE" => {
            if array.len() != 4 {
                return RespValue::Error("ERR wrong number of arguments for LRANGE".to_string());
            }
            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let start = match &array[2] {
                RespValue::BulkString(Some(v)) => match String::from_utf8_lossy(v).parse::<isize>()
                {
                    Ok(n) => n,
                    Err(_) => return RespValue::Error("ERR invalid start index".to_string()),
                },
                _ => return RespValue::Error("ERR invalid start index".to_string()),
            };
            let stop = match &array[3] {
                RespValue::BulkString(Some(v)) => match String::from_utf8_lossy(v).parse::<isize>()
                {
                    Ok(n) => n,
                    Err(_) => return RespValue::Error("ERR invalid stop index".to_string()),
                },
                _ => return RespValue::Error("ERR invalid stop index".to_string()),
            };
            let curr_db = _db.get_current();
            match curr_db.get(&key) {
                Some(v) => match &v.data {
                    ValueType::List(l) => {
                        let len = l.len() as isize;
                        let start_idx = if start < 0 { len + start } else { start };
                        let stop_idx = if stop < 0 { len + stop } else { stop };
                        if start_idx >= len || stop_idx < 0 || start_idx > stop_idx {
                            return RespValue::Array(Some(vec![]));
                        }
                        let result: Vec<RespValue> = l
                            .iter()
                            .skip(start_idx as usize)
                            .take((stop_idx - start_idx + 1) as usize)
                            .map(|item| RespValue::BulkString(Some(item.clone())))
                            .collect();
                        RespValue::Array(Some(result))
                    }
                    ValueType::String(_) | ValueType::Hash(_) => {
                        return RespValue::Error("ERR value is not a list".to_string());
                    }
                },
                None => RespValue::Array(Some(vec![])),
            }
        }

        "LINDEX" => {
            if array.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for LINDEX".to_string());
            }
            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let index = match &array[2] {
                RespValue::BulkString(Some(v)) => match String::from_utf8_lossy(v).parse::<isize>()
                {
                    Ok(n) => n,
                    Err(_) => return RespValue::Error("ERR invalid index".to_string()),
                },
                _ => return RespValue::Error("ERR invalid index".to_string()),
            };
            let curr_db = _db.get_current();
            match curr_db.get(&key) {
                Some(v) => match &v.data {
                    ValueType::List(l) => {
                        let len = l.len() as isize;
                        let idx = if index < 0 { len + index } else { index };
                        if idx < 0 || idx >= len {
                            return RespValue::SimpleString("NONE".to_string());
                        }
                        match l.get(idx as usize) {
                            Some(item) => RespValue::BulkString(Some(item.clone())),
                            None => RespValue::SimpleString("NONE".to_string()),
                        }
                    }
                    ValueType::String(_) | ValueType::Hash(_) => {
                        return RespValue::Error("ERR value is not a list".to_string());
                    }
                },
                None => RespValue::SimpleString("NONE".to_string()),
            }
        }

        "LSET" => {
            if array.len() != 4 {
                return RespValue::Error("ERR wrong number of arguments for LSET".to_string());
            }
            let key = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid key".to_string()),
            };
            let index = match &array[2] {
                RespValue::BulkString(Some(v)) => match String::from_utf8_lossy(v).parse::<isize>()
                {
                    Ok(n) => n,
                    Err(_) => return RespValue::Error("ERR invalid index".to_string()),
                },
                _ => return RespValue::Error("ERR invalid index".to_string()),
            };
            let new_value = match &array[3] {
                RespValue::BulkString(Some(v)) => v.clone(),
                _ => return RespValue::Error("ERR invalid value".to_string()),
            };
            let mut curr_db = _db.get_current();
            match curr_db.get_mut(&key) {
                Some(v) => match &mut v.data {
                    ValueType::List(l) => {
                        let len = l.len() as isize;
                        let idx = if index < 0 { len + index } else { index };
                        if idx < 0 || idx >= len {
                            return RespValue::Error("ERR index out of range".to_string());
                        }
                        if let Some(item) = l.get_mut(idx as usize) {
                            *item = new_value;
                            RespValue::SimpleString("OK".to_string())
                        } else {
                            RespValue::Error("ERR index out of range".to_string())
                        }
                    }
                    ValueType::String(_) | ValueType::Hash(_) => {
                        return RespValue::Error("ERR value is not a list".to_string());
                    }
                },
                None => RespValue::Error("ERR no such key".to_string()),
            }
        }

        "PUBLISH" => {
            if array.len() != 3 {
                return RespValue::Error("ERR wrong number of arguments for PUBLISH".to_string());
            }
            let topic = match &array[1] {
                RespValue::BulkString(Some(bs)) => String::from_utf8_lossy(bs).to_string(),
                RespValue::SimpleString(s) => s.clone(),
                _ => return RespValue::Error("ERR invalid topic".to_string()),
            };
            let message = match &array[2] {
                RespValue::BulkString(Some(v)) => String::from_utf8_lossy(v).to_string(),
                _ => return RespValue::Error("ERR invalid message".to_string()),
            };
            let subscribers = pubsub.publish(&topic, &message);
            RespValue::Integer(subscribers as i64)
        }
        _ => RespValue::Error(format!("ERR unknown command '{}'", cmd_name)),
    }
}
