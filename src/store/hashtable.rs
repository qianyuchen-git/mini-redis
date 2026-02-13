use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

/// 哈希表的初始容量
const INITIAL_CAPACITY: usize = 16;

/// 负载因子阈值,超过此值触发扩容
const LOAD_FACTOR_THRESHOLD: f64 = 0.75;

/// 哈希表条目
pub struct Entry<K, V> {
    pub key: K,
    pub value: V,
    /// 链表法解决冲突
    pub next: Option<Box<Entry<K, V>>>,
}

impl<K, V> Entry<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Entry {
            key,
            value,
            next: None,
        }
    }
}

/// 渐进式哈希表 (Redis Dict)
/// 支持渐进式 rehash,避免阻塞
pub struct HashTable<K, V> {
    /// 主哈希表
    table: Vec<Option<Box<Entry<K, V>>>>,
    /// rehash时的新哈希表
    rehash_table: Option<Vec<Option<Box<Entry<K, V>>>>>,
    /// 当前元素数量
    size: usize,
    /// 主表容量
    capacity: usize,
    /// rehash进度: -1表示未在rehash, >= 0表示当前rehash到的bucket索引
    rehash_idx: isize,
}

impl<K: Hash + Eq, V> HashTable<K, V> {
    /// 创建新的哈希表
    pub fn new() -> Self {
        let mut table = Vec::with_capacity(INITIAL_CAPACITY);
        for _ in 0..INITIAL_CAPACITY {
            table.push(None);
        }
        HashTable {
            table,
            rehash_table: None,
            size: 0,
            capacity: INITIAL_CAPACITY,
            rehash_idx: -1,
        }
    }

    /// 创建指定容量的哈希表
    pub fn with_capacity(capacity: usize) -> Self {
        let cap = capacity.next_power_of_two();
        let mut table = Vec::with_capacity(cap);
        for _ in 0..cap {
            table.push(None);
        }
        HashTable {
            table,
            rehash_table: None,
            size: 0,
            capacity: cap,
            rehash_idx: -1,
        }
    }

    /// 获取元素数量
    pub fn len(&self) -> usize {
        self.size
    }

    /// 判断是否为空
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// 计算哈希值
    fn hash(&self, key: &K) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// 获取bucket索引
    fn get_bucket_index(&self, hash: u64, capacity: usize) -> usize {
        (hash as usize) & (capacity - 1)
    }

    /// 判断是否正在rehash
    fn is_rehashing(&self) -> bool {
        self.rehash_idx >= 0
    }

    /// 检查是否需要扩容
    fn should_expand(&self) -> bool {
        // TODO: 实现扩容判断逻辑
        // 负载因子 = size / capacity
        todo!("实现扩容判断")
    }

    /// 开始rehash过程
    fn start_rehash(&mut self) {
        // TODO: 实现rehash初始化
        // 1. 创建新表,容量为原来的2倍
        // 2. 设置rehash_idx = 0
        todo!("实现rehash初始化")
    }

    /// 执行一步渐进式rehash
    /// 每次操作时迁移n个bucket
    fn rehash_step(&mut self, n: usize) {
        // TODO: 实现渐进式rehash
        // 1. 迁移n个非空bucket的数据到新表
        // 2. 更新rehash_idx
        // 3. 如果完成,切换表并清理
        todo!("实现渐进式rehash")
    }

    /// 插入或更新键值对
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        // TODO: 实现插入逻辑
        // 1. 如果正在rehash,执行一步rehash
        // 2. 检查是否需要扩容
        // 3. 计算hash和bucket索引
        // 4. 查找key,存在则更新,不存在则插入
        // 5. 如果正在rehash,需要同时查两个表
        todo!("实现插入逻辑")
    }

    /// 查找键对应的值
    pub fn get(&self, key: &K) -> Option<&V> {
        // TODO: 实现查找逻辑
        // 1. 在主表中查找
        // 2. 如果正在rehash且未找到,在新表中查找
        todo!("实现查找逻辑")
    }

    /// 可变查找
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        // TODO: 实现可变查找
        todo!("实现可变查找")
    }

    /// 删除键值对
    pub fn remove(&mut self, key: &K) -> Option<V> {
        // TODO: 实现删除逻辑
        // 1. 如果正在rehash,执行一步rehash
        // 2. 在主表中查找并删除
        // 3. 如果正在rehash且未找到,在新表中查找删除
        todo!("实现删除逻辑")
    }

    /// 检查键是否存在
    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    /// 清空哈希表
    pub fn clear(&mut self) {
        self.table.clear();
        for _ in 0..INITIAL_CAPACITY {
            self.table.push(None);
        }
        self.rehash_table = None;
        self.size = 0;
        self.capacity = INITIAL_CAPACITY;
        self.rehash_idx = -1;
    }

    /// 获取所有键
    pub fn keys(&self) -> Vec<&K> {
        // TODO: 实现获取所有键
        todo!("实现获取所有键")
    }

    /// 获取所有值
    pub fn values(&self) -> Vec<&V> {
        // TODO: 实现获取所有值
        todo!("实现获取所有值")
    }
}

impl<K: Hash + Eq, V> Default for HashTable<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_hashtable() {
        let table: HashTable<String, i32> = HashTable::new();
        assert_eq!(table.len(), 0);
        assert!(table.is_empty());
    }

    #[test]
    #[ignore]
    fn test_insert() {
        let mut table = HashTable::new();
        assert_eq!(table.insert("key1".to_string(), 100), None);
        assert_eq!(table.insert("key1".to_string(), 200), Some(100));
        assert_eq!(table.len(), 1);
    }

    #[test]
    #[ignore]
    fn test_get() {
        let mut table = HashTable::new();
        table.insert("key1".to_string(), 100);
        assert_eq!(table.get(&"key1".to_string()), Some(&100));
        assert_eq!(table.get(&"key2".to_string()), None);
    }

    #[test]
    #[ignore]
    fn test_remove() {
        let mut table = HashTable::new();
        table.insert("key1".to_string(), 100);
        assert_eq!(table.remove(&"key1".to_string()), Some(100));
        assert_eq!(table.len(), 0);
    }
}
