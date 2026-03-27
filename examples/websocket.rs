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
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const WS_MAGIC: &str = "258EAFA5-E914-47DA-95CA-5AB0DC85B11B";

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
    println!("WebSocket server listening on ws://0.0.0.0:9000");

    loop {
        let (socket, addr) = listener
            .accept()
            .await
            .expect("Failed to accept connection");
        println!("Connection from {}", addr);
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
    loop {
        // 尝试从 buffer 中解析一帧
        if let Some(frame) = decode_frame(buffer) {
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
                    println!("Received Ping");
                    let response_frame = encode_frame(Opcode::Pong, &frame.payload);
                    stream.write_all(&response_frame).await?;
                }
                Opcode::Pong => {
                    println!("Received Pong");
                    // 通常不需要处理
                }
                Opcode::Close => {
                    println!("Received Close");
                    let response_frame = encode_frame(Opcode::Close, &frame.payload);
                    stream.write_all(&response_frame).await?;
                    
                    break;
                }
            }
            continue;
        }

        // buffer 中数据不够，继续从 socket 读取
        if stream.read_buf(buffer).await? == 0 {
            println!("Connection closed");
            break;
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
    // TODO: let fin = ...
    // TODO: let opcode_num = ...

    // ---- 第二个字节：MASK(1bit) + payload_len(7bit) ----
    // TODO: let masked = ...
    // TODO: let payload_len_7 = ...

    // ---- 计算实际 payload 长度 ----
    // TODO: 根据 payload_len_7 的值决定：
    //   0~125:  长度就是 payload_len_7
    //   126:    接下来 2 字节是长度（u16 大端序）
    //   127:    接下来 8 字节是长度（u64 大端序）

    // ---- 计算总头部长度 ----
    // TODO: header_len = 2 + 扩展长度字节数 + (masked ? 4 : 0)

    // ---- 检查 buffer 中数据是否完整 ----
    // TODO: if buffer.len() < header_len + payload_len { return None; }

    // ---- 数据完整，开始消费 ----
    // TODO: buffer.advance() 跳过头部
    // TODO: 读取 mask_key（如果 masked）
    // TODO: buffer.split_to(payload_len) 取出 payload
    // TODO: 如果 masked，对 payload 做 XOR 解码：payload[i] ^= mask_key[i % 4]

    // ---- 构造 WsFrame 返回 ----
    // TODO: 根据 opcode_num 构造 Opcode 枚举
    // TODO: return Some(WsFrame { fin, opcode, payload })

    todo!()
}

// ==================== Step 3: WebSocket 帧编码 ====================

/// 编码一个 WebSocket 帧（服务器→客户端，不需要 mask）
fn encode_frame(opcode: Opcode, payload: &[u8]) -> BytesMut {
    let mut buf = BytesMut::new();

    // ---- 第一个字节：FIN=1 + opcode ----
    // TODO: let opcode_num = match opcode { ... };
    // TODO: buf.put_u8(0x80 | opcode_num);

    // ---- 第二个字节：MASK=0 + payload length ----
    // TODO: 根据 payload.len() 决定：
    //   ≤125:   buf.put_u8(len as u8)
    //   ≤65535: buf.put_u8(126) + buf.put_u16(len)
    //   >65535: buf.put_u8(127) + buf.put_u64(len)

    // ---- payload ----
    // TODO: buf.put_slice(payload);

    todo!()
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
