// =============================================================
// 简易 HTTP/1.1 服务器 - 学习练习
// =============================================================
//
// 目标：从零手写一个支持基本功能的 HTTP/1.1 服务器
// 运行：cargo run --example http_server
//
// 学习路线（按步骤逐步实现）：
//
// Step 1: TCP 监听与连接接受
// Step 2: 解析 HTTP 请求（请求行 + 头部 + Body）
// Step 3: 构建 HTTP 响应并发送
// Step 4: 路由分发
// Step 5: 静态文件服务
// Step 6: 持久连接 (Keep-Alive)
// Step 7: (可选) 用 tokio 改造为异步版本
// =============================================================

use bytes::{Buf, BytesMut};
use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::net::{TcpListener, TcpStream};

struct HttpRequest {
    method: String,
    path: String,
    version: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

struct HttpResponse {
    status_code: u16,
    status_text: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

enum parse_state {
    RequestLine,
    Headers,
    Body,
}

fn main() {
    let listen = TcpListener::bind("0.0.0.0:8080").expect("Failed to bind to address");
    println!("HTTP server listening on http://0.0.0.0:8080");
    let (socket, addr) = listen.accept().expect("Failed to accept connection");
    println!("Accepted connection from {}", addr);
}

fn read_request(stream: &mut TcpStream) -> Option<HttpRequest> {
    loop {}
}

fn read_line(cursor: &mut Cursor<&[u8]>) -> Option<&[u8]> {
    let position = cursor.position() as usize;
    let data = cursor.get_ref();
    for i in postion..data.len() - 1 {
        if data[i] == b'\r' && data[i + 1] == b'\n' {
            cursor.set_position((i + 2) as u64);
            return Some(&data[position..i]);
        }
    }
    None
}

fn parse_request(buffer: &[u8]) -> Option<HttpRequest> {
    let mut cursor = Cursor::new(buffer);
    let mut state = parse_state::RequestLine;
    let mut res = HttpRequest {
        method: String::new(),
        path: String::new(),
        version: String::new(),
        headers: HashMap::new(),
        body: Vec::new(),
    };
    loop {
        match state {
            parse_state::RequestLine => {
                if let Some(request) = read_line(&mut cursor) {
                    let mut space_split = request.split(|&b| b == b' ');
                    res.method = String::from_utf8(space_split.next()?.to_vec()).ok()?;
                    res.path = String::from_utf8(space_split.next()?.to_vec()).ok()?;
                    res.version = String::from_utf8(space_split.next()?.to_vec()).ok()?;
                    state = parse_state::Headers;
                } else {
                    continue;
                }
            }
            parse_state::Headers => {
                loop {
                    if let Some(key_value) = read_line(&mut cursor){
                        if key_value.is_empty() {
                            state = parse_state::Body;
                            break;
                        }
                        let mut colon_split = key_value.split(|&b| b == b':');
                        let key = String::from_utf8(colon_split.next()?.to_vec()).ok()?;
                        let value = String::from_utf8(colon_split.next()?.to_vec()).ok()?;
                        res.headers.insert(key.trim().to_string(), value.trim().to_string());
                    }
                    else {
                        continue;
                    }
                }
            }
            parse_state::Body => {}
            _ => {
                return None;
            }
        }
    }
}
