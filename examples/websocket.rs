// =============================================================
// 简易 WebSocket 服务器 - 学习练习
// =============================================================
//
// 运行：cargo run --example websocket
//
// 测试：浏览器 F12 控制台输入
//   const ws = new WebSocket("ws://localhost:9000");
//   ws.onopen = () => console.log("Connected!");
//   ws.onmessage = (e) => console.log("Received:", e.data);
//   ws.send("Hello");
//
// Step 1: HTTP 握手（复用 http_server 的解析逻辑）
// Step 2: WebSocket 帧解码（二进制协议解析）
// Step 3: WebSocket 帧编码（服务器发送）
// Step 4: 全双工通信（异步读写分离）
// =============================================================

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::{Buf, BufMut, BytesMut};
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::time::Instant;
use std::time::Duration;
use tokio::time::interval;
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const WS_MAGIC: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

// ==================== 数据结构 ====================

struct HttpRequest {
    method: String,
    path: String,
    version: String,
    headers: HashMap<String, String>,
}

enum ParseState {
    RequestLine,
    Headers,
}

/// WebSocket 帧的 opcode 类型
#[derive(Debug, Clone, PartialEq)]
enum Opcode {
    Text,   // 0x1
    Continuation, // 0x0
    Binary, // 0x2
    Close,  // 0x8
    Ping,   // 0x9
    Pong,   // 0xA
}

/// 解析后的 WebSocket 帧
#[derive(Debug)]
struct WsFrame {
    fin: bool,
    opcode: Opcode,
    payload: Vec<u8>,
}

// ==================== 入口 ====================

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("0.0.0.0:9000")
        .await
        .expect("Failed to bind to address");

    loop {
        let (socket, addr) = listener
            .accept()
            .await
            .expect("Failed to accept connection");
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket).await {
                eprintln!("Connection error: {}", e);
            }
        });
    }
}

// ==================== Step 1: HTTP 握手 ====================

async fn handle_connection(mut stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = BytesMut::with_capacity(4096);
    // 1. 读取 HTTP 升级请求（复用你的 HTTP 解析逻辑）
    let request = loop {
        if let Some(req) = parse_http_request(&mut buffer) {
            break req;
        }
        if stream.read_buf(&mut buffer).await? == 0 {
            return Err("Connection closed during handshake".into());
        }
    };
    // 2. 验证是否是 WebSocket 升级请求
    //    TODO: 检查 request.headers 中的 "upgrade" 是否为 "websocket"
    //    TODO: 如果不是，返回 400 Bad Request
    if (request.headers.get("upgrade").map(|v| v.to_lowercase()) != Some("websocket".to_string())) {
        let response = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
        stream.write_all(response.as_bytes()).await?;
        return Err("Not a WebSocket upgrade request".into());
    }
    // 3. 获取 Sec-WebSocket-Key 并计算 Sec-WebSocket-Accept
    let client_key = match request.headers.get("sec-websocket-key") {
        Some(key) => key,
        None => {
            let response = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
            stream.write_all(response.as_bytes()).await?;
            return Err("Missing Sec-WebSocket-Key".into());
        }
    };
    let accept_key = compute_accept_key(client_key);

    // 4. 发送 101 Switching Protocols 响应
    //    TODO: 构建并发送 HTTP 101 响应
    //    格式:
    //    HTTP/1.1 101 Switching Protocols\r\n
    //    Upgrade: websocket\r\n
    //    Connection: Upgrade\r\n
    //    Sec-WebSocket-Accept: {accept_key}\r\n
    //    \r\n

    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {}\r\n\
         \r\n",
        accept_key
    );
    stream.write_all(response.as_bytes()).await?;

    // 5. 进入 WebSocket 帧通信
    handle_websocket(&mut stream, &mut buffer).await
}

/// Sec-WebSocket-Accept = Base64(SHA1(client_key + WS_MAGIC))
fn compute_accept_key(client_key: &str) -> String {
    // TODO: 实现 SHA1 + Base64 计算
    // 提示：
    let mut hasher = Sha1::new();
    hasher.update(client_key.trim().as_bytes());
    hasher.update(WS_MAGIC.as_bytes());
    let hash = hasher.finalize();
    BASE64.encode(&hash)
}

// ==================== Step 2: WebSocket 帧解码 ====================

async fn handle_websocket(
    stream: &mut TcpStream,
    buffer: &mut BytesMut,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut fullPayload: Vec<u8> = Vec::new();
    let mut frameList: Vec<WsFrame> = Vec::new();
    // 心跳相关
    let mut ping_interval = interval(Duration::from_secs(30));  // 每 30 秒发一次 Ping
    let mut last_pong = Instant::now();                          // 上次收到 Pong 的时间
    let pong_timeout = Duration::from_secs(60);                  // 60 秒没收到 Pong 则断开
    loop {
        // 尝试从 buffer 中解析一帧
        if let Some(frame) = decode_frame(buffer) {
            if(!frame.fin) {
                match frame.opcode {
                    Opcode::Binary | Opcode::Text | Opcode::Continuation => {
                        frameList.push(frame);
                        continue;
                    }
                    _=> {
                        // 错误的续帧，直接关闭连接
                        let response_frame = encode_frame(Opcode::Close, &[]);
                        stream.write_all(&response_frame).await?;
                        break;
                    }
                }
            }
            match frame.opcode {
                Opcode::Text => {
                    // TODO: 回显消息（调用 encode_frame 编码后写入 stream）
                    let response_frame = encode_frame(Opcode::Text, &frame.payload);
                    stream.write_all(&response_frame).await?;
                }
                Opcode::Binary => {
                    let response_frame = encode_frame(Opcode::Binary, &frame.payload);
                    stream.write_all(&response_frame).await?;
                }
                Opcode::Ping => {
                    let response_frame = encode_frame(Opcode::Pong, &frame.payload);
                    stream.write_all(&response_frame).await?;
                }
                Opcode::Pong => {
                    last_pong = Instant::now();
                }
                Opcode::Continuation => {
                    if frameList.is_empty(){
                        // 错误的续帧，直接关闭连接
                        let response_frame = encode_frame(Opcode::Close, &[]);
                        stream.write_all(&response_frame).await?;
                        break;
                    }
                    for frame in frameList.iter() {
                        fullPayload.extend_from_slice(&frame.payload);
                    }
                    fullPayload.extend_from_slice(&frame.payload);
                    let response_frame = encode_frame(frameList[0].opcode.clone(), &fullPayload);
                    stream.write_all(&response_frame).await?;
                    fullPayload.clear();
                    frameList.clear();
                }
                Opcode::Close => {
                    let response_frame = encode_frame(Opcode::Close, &frame.payload);
                    stream.write_all(&response_frame).await?;

                    break;
                }
            }
            continue;
        }

        tokio:: select! {
            result = stream.read_buf(buffer)=>{
                if result? == 0 {
                    // 连接关闭
                    break;
                }                
            }
            _=ping_interval.tick()=>{
                if last_pong.elapsed() > pong_timeout {
                    // 没有及时收到 Pong，关闭连接
                    let response_frame = encode_frame(Opcode::Close, b"Ping timeout");
                    stream.write_all(&response_frame).await?;
                    break;
                }
                // 发送 Ping 帧
                let ping_frame = encode_frame(Opcode::Ping, b"heartbeat");
                stream.write_all(&ping_frame).await?;
            }
        }
    }
    Ok(())
}

/// 试探性解析一个 WebSocket 帧
/// 数据够则返回 Some(WsFrame) 并消费 buffer，不够则返回 None
fn decode_frame(buffer: &mut BytesMut) -> Option<WsFrame> {
    // 至少需要 2 字节
    if buffer.len() < 2 {
        return None;
    }

    // ---- 第一个字节：FIN(1bit) + RSV(3bit) + opcode(4bit) ----
    let fin = buffer[0] & 0x80 != 0;
    let opcode_num = buffer[0] & 0x0F;

    // ---- 第二个字节：MASK(1bit) + payload_len(7bit) ----
    let masked = buffer[1] & 0x80 != 0;
    let payload_len_7 = buffer[1] & 0x7F;

    // ---- 计算实际 payload 长度 ----
    // TODO: 根据 payload_len_7 的值决定：
    //   ≤125:   payload_len = payload_len_7
    //   126:    payload_len = buffer[2..4] 解析为 u16
    //   127:    payload_len = buffer[2..10] 解析为
    // 注意：如果 masked，payload_len 的位置会向后移动 4 字节（mask key）
    let mut payload_len = 0; // TODO: 实现长度解析逻辑
    if payload_len_7 <= 125 {
        // payload_len = payload_len_7
        payload_len = payload_len_7 as usize;
    } else if payload_len_7 == 126 {
        // payload_len = buffer[2..4] 解析为 u16
        if buffer.len() < 4 {
            return None; // 数据不完整
        }
        payload_len = u16::from_be_bytes([buffer[2], buffer[3]]) as usize;
    } else {
        // payload_len = buffer[2..10] 解析为 u64
        if buffer.len() < 10 {
            return None; // 数据不完整
        }
        payload_len = u64::from_be_bytes([
            buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7], buffer[8], buffer[9],
        ]) as usize;
    }

    // ---- 计算总头部长度 ----
    let mut header_len = 2; // 基础头部长度
    if payload_len_7 == 126 {
        header_len += 2; // 16-bit 扩展长度
    } else if payload_len_7 == 127 {
        header_len += 8; // 64-bit 扩展长度
    }
    if masked {
        header_len += 4; // mask key
    }
    // ---- 检查 buffer 中数据是否完整 ----
    if buffer.len() < header_len + payload_len {
        return None;
    }
    // ---- 数据完整，开始消费 ----
    // 读取 mask_key（在 advance 之前读，因为 advance 会丢弃头部）
    let mask_key = if masked {
        [
            buffer[header_len - 4],
            buffer[header_len - 3],
            buffer[header_len - 2],
            buffer[header_len - 1],
        ]
    } else {
        [0u8; 4]
    };

    // 跳过头部
    buffer.advance(header_len);

    // 取出 payload 并转为 Vec<u8>
    let mut payload = buffer.split_to(payload_len).to_vec();

    // unmask
    if masked {
        for i in 0..payload.len() {
            payload[i] ^= mask_key[i % 4];
        }
    }

    // ---- 构造 WsFrame 返回 ----
    // ...existing code...
    Some(WsFrame {
        fin,
        opcode: match opcode_num {
            0x1 => Opcode::Text,
            0x2 => Opcode::Binary,
            0x8 => Opcode::Close,
            0x9 => Opcode::Ping,
            0xA => Opcode::Pong,
            0x0 => Opcode::Continuation,
            _ => return None, // 不支持的 opcode
        },
        payload,
    })
}

// ==================== Step 3: WebSocket 帧编码 ====================

/// 编码一个 WebSocket 帧（服务器→客户端，不需要 mask）
fn encode_frame(opcode: Opcode, payload: &[u8]) -> BytesMut {
    let mut buf = BytesMut::new();
    match opcode {
        Opcode::Text => buf.put_u8(0x81),   // FIN=1, opcode=0x1
        Opcode::Binary => buf.put_u8(0x82), // FIN=1, opcode=0x2
        Opcode::Close => buf.put_u8(0x88),  // FIN=1, opcode=0x8
        Opcode::Ping => buf.put_u8(0x89),   // FIN=1, opcode=0x9
        Opcode::Pong => buf.put_u8(0x8A),   // FIN=1, opcode=0xA
        Opcode::Continuation => buf.put_u8(0x00), // FIN=0, opcode=0x0
    }
    let payload_len = payload.len();
    if payload_len <= 125 {
        buf.put_u8(payload_len as u8);
    } else if payload_len <= 65535 {
        buf.put_u8(126);
        buf.put_u16(payload_len as u16);
    } else {
        buf.put_u8(127);
        buf.put_u64(payload_len as u64);
    }

    buf.put_slice(payload);
    buf
}

// ==================== HTTP 解析（复用你的代码） ====================

fn read_line(cursor: &mut Cursor<&[u8]>) -> Option<Vec<u8>> {
    let position = cursor.position() as usize;
    let data = *cursor.get_ref();
    for i in position..data.len().saturating_sub(1) {
        if data[i] == b'\r' && data[i + 1] == b'\n' {
            cursor.set_position((i + 2) as u64);
            return Some(data[position..i].to_vec());
        }
    }
    None
}

fn parse_http_request(buffer: &mut BytesMut) -> Option<HttpRequest> {
    let mut cursor = Cursor::new(&buffer[..]);
    let mut state = ParseState::RequestLine;
    let mut res = HttpRequest {
        method: String::new(),
        path: String::new(),
        version: String::new(),
        headers: HashMap::new(),
    };
    loop {
        match state {
            ParseState::RequestLine => {
                let line = read_line(&mut cursor)?;
                let mut parts = line.split(|&b| b == b' ');
                res.method = String::from_utf8(parts.next()?.to_vec()).ok()?;
                res.path = String::from_utf8(parts.next()?.to_vec()).ok()?;
                res.version = String::from_utf8(parts.next()?.to_vec()).ok()?;
                state = ParseState::Headers;
            }
            ParseState::Headers => {
                let line = read_line(&mut cursor)?;
                if line.is_empty() {
                    // 头部结束，WebSocket 握手不需要 body
                    let consumed = cursor.position() as usize;
                    drop(cursor);
                    buffer.advance(consumed);
                    return Some(res);
                }
                let mut parts = line.splitn(2, |&b| b == b':');
                let key = String::from_utf8(parts.next()?.to_vec()).ok()?;
                let value = String::from_utf8(parts.next()?.to_vec()).ok()?;
                res.headers
                    .insert(key.trim().to_lowercase(), value.trim().to_string());
            }
        }
    }
}
