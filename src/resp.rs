use bytes::{Buf, BytesMut};
use std::{
    i64,
    io::{self, Cursor, Read},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// RESP 数据类型
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// 简单字符串: +OK\r\n
    SimpleString(String),
    /// 错误: -Error message\r\n
    Error(String),
    /// 整数: :1000\r\n
    Integer(i64),
    /// 批量字符串: $6\r\nfoobar\r\n
    BulkString(Option<Vec<u8>>),
    /// 数组: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    Array(Option<Vec<RespValue>>),
}

#[derive(Debug)]
enum ParserState {
    Idle,
    ParsingArray {
        remaining: usize,
        elements: Vec<RespValue>,
    },
    // 进阶可加：ParsingBulkPayload { remaining_bytes: usize, payload: Vec<u8> }
    // 用于超大 Bulk String 分块读取
}

/// RESP 协议解析器
pub struct RespParser {
    buffer: BytesMut,
    state: ParserState,
}

impl RespParser {
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(4096),
            state: ParserState::Idle,
        }
    }

    /// 从流中读取并解析 RESP 值
    pub async fn read_value<T>(&mut self, stream: &mut T) -> io::Result<Option<RespValue>>
    where
        T: AsyncReadExt + Unpin,
    {
        loop {
            println!("=== Loop iteration start ===");

            if let Some(value) = self.parse_buffer()? {
                println!("Parsed RESP value: {:?}", value);
                return Ok(Some(value));
            }
            if matches!(self.state, ParserState::ParsingArray { .. }) && !self.buffer.is_empty() {
                continue; // 继续读取数据以完成当前解析
            }
            println!("About to read from stream...");
            println!("Before stream.read_buf() call");
            let read_result = stream.read_buf(&mut self.buffer).await;
            println!("After stream.read_buf() call, result: {:?}", read_result);
            
            match read_result {
                Ok(0) => {
                    println!("Client disconnected");
                    return Ok(None);
                }
                Ok(n) => {
                    println!("Read {} bytes from stream", n);
                    // 继续 loop
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    println!("WouldBlock error, continuing...");
                    // 没数据，继续等
                    continue; // 或什么都不做
                }
                Err(e) => {
                    println!("Stream read error: {:?}", e);
                    return Err(e);
                }
            }
        }
    }

    /// 尝试从缓冲区解析一个完整的 RESP 值
    fn parse_buffer(&mut self) -> io::Result<Option<RespValue>> {
        println!("Parsing buffer with length: {}", self.buffer.len());
        println!("Current parser state: {:?}", self.state);
        // 克隆缓冲区数据以避免借用冲突
        let buffer_data = self.buffer.clone();
        let mut cursor = Cursor::new(&buffer_data[..]);

        let result = if matches!(self.state, ParserState::Idle) {
            println!("Parser is idle, trying to parse new value");
            match self.try_parse_value(&mut cursor) {
                Ok(value) => {
                    let pos = cursor.position() as usize;
                    self.buffer.advance(pos);
                    Ok(Some(value))
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // try_parse_value可能已经设置了ParsingArray状态
                    let pos = cursor.position() as usize;
                    self.buffer.advance(pos);
                    println!(
                        "Advanced buffer by {} bytes, new state: {:?}",
                        pos, self.state
                    );
                    Ok(None)
                }
                Err(e) => Err(e),
            }
        } else if matches!(self.state, ParserState::ParsingArray { .. }) {
            println!("Continuing to parse array with remaining elements");
            println!(
                "Available buffer data: {:?}",
                String::from_utf8_lossy(&buffer_data)
            );
            println!("Buffer hex: {:?}", buffer_data);
            match self.try_parse_value(&mut cursor) {
                Ok(value) => {
                    println!("Successfully parsed array element: {:?}", value);
                    let pos = cursor.position() as usize;
                    self.buffer.advance(pos);

                    // 手动处理数组状态
                    if let ParserState::ParsingArray {
                        remaining,
                        elements,
                    } = &mut self.state
                    {
                        elements.push(value);
                        *remaining -= 1;
                        println!("Added element to array, remaining: {}", remaining);
                        if *remaining == 0 {
                            let completed = std::mem::take(elements);
                            self.state = ParserState::Idle;
                            println!("Array parsing completed: {:?}", completed);
                            Ok(Some(RespValue::Array(Some(completed))))
                        } else {
                            Ok(None)
                        }
                    } else {
                        unreachable!()
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    println!(
                        "UnexpectedEof in array element parsing at cursor position: {}",
                        cursor.position()
                    );
                    Ok(None)
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    println!("WouldBlock in array element parsing");
                    Ok(None)
                }
                Err(e) => {
                    println!(
                        "Error in array parsing: {:?} at cursor position: {}",
                        e,
                        cursor.position()
                    );
                    Err(e)
                }
            }
        } else {
            unreachable!("Unknown parser state")
        };
        result
    }

    /// 尝试解析单个值（用于避免借用冲突）
    fn try_parse_value(&mut self, cursor: &mut Cursor<&[u8]>) -> io::Result<RespValue> {
        let mut type_byte = [0u8; 1];
        std::io::Read::read_exact(cursor, &mut type_byte)?;

        match type_byte[0] {
            b'+' => Ok(RespValue::SimpleString(Self::parse_simple_string(cursor)?)),
            b'-' => Ok(RespValue::Error(Self::parse_error(cursor)?)),
            b':' => Ok(RespValue::Integer(Self::parse_integer(cursor)?)),
            b'$' => Ok(RespValue::BulkString(Self::parse_bulk_string(cursor)?)),
            b'*' => {
                println!("Starting to parse array");
                match self.parse_array(cursor) {
                    Ok(array_value) => Ok(array_value),
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        // 数组解析已设置状态，需要保存cursor位置
                        Err(e)
                    }
                    Err(e) => Err(e),
                }
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown type: {}", type_byte[0] as char),
            )),
        }
    }

    /// 解析简单字符串: +OK\r\n
    fn parse_simple_string(cursor: &mut Cursor<&[u8]>) -> io::Result<String> {
        let line = Self::read_line(cursor)?; // 用 ? 传播错误，简洁又安全

        Ok(String::from_utf8_lossy(&line).into_owned())
    }

    /// 解析错误: -Error message\r\n
    fn parse_error(cursor: &mut Cursor<&[u8]>) -> io::Result<String> {
        let line = Self::read_line(cursor)?;
        Ok(String::from_utf8_lossy(&line).into_owned())
    }

    /// 解析整数: :1000\r\n
    fn parse_integer(cursor: &mut Cursor<&[u8]>) -> io::Result<i64> {
        let mut result: i64 = 0;
        let mut i = 0;
        let line = Self::read_line(cursor)?;
        if line.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "empty integer"));
        }
        let mut minus = false;
        if line[i] == b'-' {
            minus = true;
            i = i + 1;
        }
        if line[i] == b'+' {
            i = i + 1;
        }
        while i < line.len() && line[i] >= b'0' && line[i] <= b'9' {
            let byte = line[i];
            let digit = (byte - b'0') as i64;
            if !minus && result > (i64::MAX - digit) / 10 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "integer overflow",
                ));
            } else if minus && result > -1 * ((i64::MIN + digit) / 10) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "integer underflow",
                ));
            } else {
                result = result * 10 + digit;
                i = i + 1;
            }
        }
        if i < line.len() {
            Err(io::Error::new(io::ErrorKind::InvalidData, "invalid data"))
        } else {
            Ok(if minus { -result } else { result })
        }
    }

    /// 解析批量字符串: $6\r\nfoobar\r\n 或 $-1\r\n (null)
    fn parse_bulk_string(cursor: &mut Cursor<&[u8]>) -> io::Result<Option<Vec<u8>>> {
        let length: i64 = Self::parse_integer(cursor)?;
        let position = cursor.position();

        if length == -1 {
            return Ok(None);
        } else if length < -1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "negative bulk string length except -1",
            ));
        } else if length > 512 * 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bulk string too large",
            ));
        }

        let length = length as usize;

        if length == 0 {
            let data = cursor.get_ref();
            if !(data[position as usize] == b'\r' && data[position as usize + 1] == b'\n') {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "InvalidData"));
            }
            cursor.set_position(position + 2);
            return Ok(Some(vec![]));
        } else {
            let data = cursor.get_ref();
            if data.len() < position as usize + length + 2 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "need more data",
                ));
            } else if !(data[position as usize + length] == b'\r'
                && data[position as usize + length + 1] == b'\n')
            {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "InvalidData"));
            } else {
                let result = data[position as usize..(position as usize + length)].to_vec();
                cursor.set_position(position + length as u64 + 2);
                return Ok(Some(result));
            }
        }
    }

    /// 解析数组: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    fn parse_array(&mut self, cursor: &mut Cursor<&[u8]>) -> io::Result<RespValue> {
        let length_64: i64 = Self::parse_integer(cursor)?;
        if length_64 < -1 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "InvalidData"));
        }
        if length_64 == -1 {
            return Ok(RespValue::Array(None));
        }
        if length_64 == 0 {
            return Ok(RespValue::Array(Some(vec![])));
        }
        let length = usize::try_from(length_64).unwrap();
        self.state = ParserState::ParsingArray {
            remaining: length,
            elements: Vec::with_capacity(length),
        };
        println!("Array parsing started, expecting {} elements", length);
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "array elements pending",
        ))
    }

    /// 读取一行直到 \r\n
    fn read_line(cursor: &mut Cursor<&[u8]>) -> io::Result<Vec<u8>> {
        let start = cursor.position() as usize;
        let data = cursor.get_ref();
        let length = data.len();
        for i in start..length {
            if i + 1 < length && data[i] == b'\r' && data[i + 1] == b'\n' {
                let result = data[start..i].to_vec();
                cursor.set_position((i + 2) as u64);
                return Ok(result);
            }
        }
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "not found \\r and \\n",
        ))
    }
}

/// RESP 协议编码器
pub struct RespEncoder;

impl RespEncoder {
    /// 将 RESP 值编码并写入流
    pub async fn write_value<T>(stream: &mut T, value: &RespValue) -> io::Result<()>
    where
        T: AsyncWriteExt + Unpin,
    {
        let bytes = Self::encode_resp(value);
        stream.write_all(&bytes).await?;
        stream.flush().await?;
        Ok(())
    }

    /// 将 RESP 值编码为字节
    pub fn encode_resp(value: &RespValue) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128); // 预分配，避免频繁 realloc

        match value {
            RespValue::SimpleString(s) => {
                buf.extend_from_slice(b"+");
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }

            RespValue::Error(s) => {
                buf.extend_from_slice(b"-");
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }

            RespValue::Integer(i) => {
                buf.extend_from_slice(b":");
                buf.extend_from_slice(i.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }

            RespValue::BulkString(None) => {
                buf.extend_from_slice(b"$-1\r\n");
            }

            RespValue::BulkString(Some(data)) => {
                buf.extend_from_slice(b"$");
                buf.extend_from_slice(data.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }

            RespValue::Array(None) => {
                buf.extend_from_slice(b"*-1\r\n");
            }

            RespValue::Array(Some(elements)) => {
                buf.extend_from_slice(b"*");
                buf.extend_from_slice(elements.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");

                for elem in elements {
                    buf.extend_from_slice(&Self::encode_resp(elem));
                }
            }
        }

        buf
    }
}

#[cfg(test)]
mod tests {
    // 暂时注释掉测试代码
}
