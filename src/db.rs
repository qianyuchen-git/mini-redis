use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::iter::Filter;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq)]
pub enum ValueType {
    String(Vec<u8>),
    Hash(HashMap<String, Vec<u8>>),
}
#[derive(Debug, Clone)]
pub struct Value {
    pub data: ValueType,
    pub expire_at: Option<u64>,
}
#[derive(Debug, Clone)]
pub struct Database {
    dbs: Vec<HashMap<String, Value>>,
    current_db: usize,
}

impl Database {
    pub fn new() -> Self {
        let mut dbs: Vec<HashMap<String, Value>> = vec![];
        for i in 0..16 {
            dbs.push(HashMap::new());
        }
        Database { dbs, current_db: 0 }
    }

    pub fn get_current(&mut self) -> &mut HashMap<String, Value> {
        &mut self.dbs[self.current_db]
    }

    pub fn select_db(&mut self, index: usize) -> bool {
        if index < self.dbs.len() {
            self.current_db = index;
            true
        } else {
            false
        }
    }
}

const RDB_MAGIC: &[u8] = b"MINIRDB001\n";
const RDB_END: &[u8] = b"END\n";

pub fn save_rdb(db: &mut Database, path: &str) -> io::Result<()> {
    let mut file = BufWriter::new(File::create(path)?);

    // 头部
    file.write_all(RDB_MAGIC)?;
    let db_count = db.dbs.len() as u32;
    file.write_all(&db_count.to_le_bytes())?;

    // 每个 db
    for (index, hashmap) in db.dbs.iter().enumerate() {
        file.write_all(&(index as u32).to_le_bytes())?;

        let key_count = hashmap.len() as u32;
        file.write_all(&key_count.to_le_bytes())?;

        for (key, value) in hashmap {
            // key
            let key_bytes = key.as_bytes();
            file.write_all(&(key_bytes.len() as u32).to_le_bytes())?;
            file.write_all(key_bytes)?;

            // 类型 + 过期
            let type_byte = match &value.data {
                ValueType::String(_) => 0u8,
                ValueType::Hash(_) => 1u8,
            };
            file.write_all(&[type_byte])?;
            if let Some(expire) = value.expire_at {
                file.write_all(&expire.to_le_bytes());
            } else {
                file.write_all(&(0u64).to_le_bytes())?;
            }

            // 值内容
            match &value.data {
                ValueType::String(data) => {
                    file.write_all(&(data.len() as u32).to_le_bytes())?;
                    file.write_all(data)?;
                }
                ValueType::Hash(hash) => {
                    let field_count = hash.len() as u32;
                    file.write_all(&field_count.to_le_bytes())?;
                    for (f, v) in hash {
                        let f_bytes = f.as_bytes();
                        file.write_all(&(f_bytes.len() as u32).to_le_bytes())?;
                        file.write_all(f_bytes)?;

                        file.write_all(&(v.len() as u32).to_le_bytes())?;
                        file.write_all(v)?;
                    }
                }
            }
        }
    }

    // 尾部
    file.write_all(RDB_END)?;
    file.flush()?;
    Ok(())
}

pub fn load_rdb(db: &mut Database, path: &str) -> io::Result<()> {
    if !Path::new(path).exists() {
        println!("No RDB file found, starting with empty DB");
        return Ok(());
    }

    let mut file = BufReader::new(File::open(path)?);
    let mut buf = vec![0u8; RDB_MAGIC.len()];
    file.read_exact(&mut buf)?;
    if buf != RDB_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid RDB magic",
        ));
    }

    let mut db_count_bytes = [0u8; 4];
    file.read_exact(&mut db_count_bytes)?;
    let db_count = u32::from_le_bytes(db_count_bytes) as usize;
    db.dbs = vec![HashMap::new(); db_count];

    for _ in 0..db_count {
        let mut index_bytes = [0u8; 4];
        file.read_exact(&mut index_bytes)?;
        let index = u32::from_le_bytes(index_bytes) as usize;
        let mut key_count_bytes = [0u8; 4];
        file.read_exact(&mut key_count_bytes)?;
        let key_count = u32::from_le_bytes(key_count_bytes) as usize;

        for _ in 0..key_count {
            // key
            let mut key_len_bytes = [0u8; 4];
            file.read_exact(&mut key_len_bytes)?;
            let key_len = u32::from_le_bytes(key_len_bytes) as usize;
            let mut key_bytes = vec![0u8; key_len];
            file.read_exact(&mut key_bytes)?;
            let key = String::from_utf8_lossy(&key_bytes).to_string();
            println!("KEY VALUE = {0}", key);
            // 类型 + 过期
            let mut type_byte = [0u8; 1];
            file.read_exact(&mut type_byte)?;
            let typ = type_byte[0];

            let mut expire_bytes = [0u8; 8];
            file.read_exact(&mut expire_bytes)?;
            let expire = u64::from_le_bytes(expire_bytes);

            // 值
            let value = match typ {
                0 => {
                    // String
                    let mut len_bytes = [0u8; 4];
                    file.read_exact(&mut len_bytes)?;
                    let len = u32::from_le_bytes(len_bytes) as usize;
                    let mut data = vec![0u8; len];
                    file.read_exact(&mut data)?;
                    println!(
                        "value data = {0}",
                        String::from_utf8_lossy(&data).to_string()
                    );
                    ValueType::String(data)
                }
                1 => {
                    // Hash
                    let mut field_count_bytes = [0u8; 4];
                    file.read_exact(&mut field_count_bytes)?;
                    let field_count = u32::from_le_bytes(field_count_bytes) as usize;

                    let mut hash = std::collections::HashMap::new();
                    for _ in 0..field_count {
                        // field
                        let mut f_len_bytes = [0u8; 4];
                        file.read_exact(&mut f_len_bytes)?;
                        let f_len = u32::from_le_bytes(f_len_bytes) as usize;
                        let mut f_bytes = vec![0u8; f_len];
                        file.read_exact(&mut f_bytes)?;
                        let field = String::from_utf8_lossy(&f_bytes).to_string();

                        // value
                        let mut v_len_bytes = [0u8; 4];
                        file.read_exact(&mut v_len_bytes)?;
                        let v_len = u32::from_le_bytes(v_len_bytes) as usize;
                        let mut v_bytes = vec![0u8; v_len];
                        file.read_exact(&mut v_bytes)?;

                        hash.insert(field, v_bytes);
                    }
                    ValueType::Hash(hash)
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Unknown value type",
                    ));
                }
            };
            db.dbs[index].insert(
                key,
                Value {
                    data: value,
                    expire_at: (if expire == 0u64 { None } else { Some(expire) }),
                },
            );
        }
    }

    // 检查尾部
    let mut end_buf = [0u8; 4];
    file.read_exact(&mut end_buf)?;
    if end_buf != *RDB_END {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid RDB end marker",
        ));
    }

    println!("RDB loaded successfully");
    Ok(())
}
