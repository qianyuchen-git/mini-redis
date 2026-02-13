/// ZipList 编码类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    /// 整数编码
    Int16,
    Int32,
    Int64,
    /// 字节数组编码
    Bytes,
}

/// ZipList 条目
#[derive(Debug)]
pub struct ZipListEntry {
    /// 前一个条目的长度(用于反向遍历)
    pub prev_len: usize,
    /// 编码类型和长度
    pub encoding: Encoding,
    /// 实际数据
    pub data: Vec<u8>,
}

/// 压缩列表 (ZipList)
/// Redis 用于小数据量时的内存优化存储
/// 特点: 连续内存、紧凑编码、顺序访问快
pub struct ZipList {
    /// 连续的字节数组
    data: Vec<u8>,
    /// 条目数量
    length: usize,
}

impl ZipList {
    /// ZipList 头部大小: 4(总字节数) + 4(尾偏移) + 2(条目数)
    const HEADER_SIZE: usize = 10;
    
    /// ZipList 尾部标记
    const END_MARKER: u8 = 0xFF;

    /// 创建新的压缩列表
    pub fn new() -> Self {
        let mut data = Vec::with_capacity(Self::HEADER_SIZE + 1);
        
        // 初始化header
        // TODO: 写入header信息
        // - 总字节数(4 bytes)
        // - 尾部偏移(4 bytes)  
        // - 条目数量(2 bytes)
        // - 结束标记(1 byte = 0xFF)
        
        ZipList {
            data,
            length: 0,
        }
    }

    /// 获取列表长度
    pub fn len(&self) -> usize {
        self.length
    }

    /// 判断是否为空
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// 在尾部追加条目
    pub fn push_back(&mut self, value: &[u8]) {
        // TODO: 实现尾部追加
        // 1. 选择合适的编码(整数或字节数组)
        // 2. 计算prev_len
        // 3. 编码并写入data
        // 4. 更新header信息
        todo!("实现尾部追加")
    }

    /// 在头部插入条目
    pub fn push_front(&mut self, value: &[u8]) {
        // TODO: 实现头部插入
        // 需要移动所有数据
        todo!("实现头部插入")
    }

    /// 删除尾部条目
    pub fn pop_back(&mut self) -> Option<Vec<u8>> {
        // TODO: 实现尾部删除
        todo!("实现尾部删除")
    }

    /// 删除头部条目
    pub fn pop_front(&mut self) -> Option<Vec<u8>> {
        // TODO: 实现头部删除
        todo!("实现头部删除")
    }

    /// 根据索引获取条目
    pub fn get(&self, index: usize) -> Option<Vec<u8>> {
        // TODO: 实现索引访问
        // 需要从头开始遍历解析
        todo!("实现索引访问")
    }

    /// 根据索引删除条目
    pub fn remove(&mut self, index: usize) -> Option<Vec<u8>> {
        // TODO: 实现删除
        // 需要移动后续数据
        todo!("实现删除")
    }

    /// 尝试将值编码为整数
    fn try_encode_int(&self, value: &[u8]) -> Option<(Encoding, i64)> {
        // TODO: 实现整数编码判断
        // 尝试解析为整数,选择合适的编码类型
        todo!("实现整数编码")
    }

    /// 编码prevlen字段
    fn encode_prev_len(&self, len: usize) -> Vec<u8> {
        // TODO: 实现prevlen编码
        // 如果 len < 254, 用1字节
        // 否则用5字节: 0xFE + 4字节长度
        todo!("实现prevlen编码")
    }

    /// 解码prevlen字段
    fn decode_prev_len(&self, offset: usize) -> (usize, usize) {
        // TODO: 实现prevlen解码
        // 返回: (prevlen值, prevlen字段占用字节数)
        todo!("实现prevlen解码")
    }

    /// 编码条目
    fn encode_entry(&self, prev_len: usize, value: &[u8]) -> Vec<u8> {
        // TODO: 实现条目编码
        // prevlen + encoding + data
        todo!("实现条目编码")
    }

    /// 解码条目
    fn decode_entry(&self, offset: usize) -> Option<ZipListEntry> {
        // TODO: 实现条目解码
        todo!("实现条目解码")
    }

    /// 获取总字节数
    fn get_total_bytes(&self) -> usize {
        self.data.len()
    }

    /// 更新header信息
    fn update_header(&mut self) {
        // TODO: 更新header中的总字节数、尾偏移、长度
        todo!("更新header")
    }

    /// 清空列表
    pub fn clear(&mut self) {
        self.data.clear();
        self.length = 0;
        // 重新初始化header
        // TODO
    }

    /// 迭代器
    pub fn iter(&self) -> ZipListIter {
        ZipListIter {
            ziplist: self,
            offset: Self::HEADER_SIZE,
        }
    }

    /// 检查是否需要转换为其他数据结构
    /// Redis在ziplist过大时会转换为hashtable或skiplist
    pub fn should_convert(&self) -> bool {
        // TODO: 实现转换判断
        // 通常判断: 条目数 > 512 或 单个条目 > 64字节
        self.length > 512 || self.get_total_bytes() > 1024 * 64
    }
}

impl Default for ZipList {
    fn default() -> Self {
        Self::new()
    }
}

/// ZipList 迭代器
pub struct ZipListIter<'a> {
    ziplist: &'a ZipList,
    offset: usize,
}

impl<'a> Iterator for ZipListIter<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: 实现迭代器
        // 解析当前offset的entry,返回data并移动offset
        todo!("实现迭代器")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_ziplist() {
        let list = ZipList::new();
        assert_eq!(list.len(), 0);
        assert!(list.is_empty());
    }

    #[test]
    #[ignore]
    fn test_push_back() {
        let mut list = ZipList::new();
        list.push_back(b"hello");
        list.push_back(b"world");
        assert_eq!(list.len(), 2);
    }

    #[test]
    #[ignore]
    fn test_get() {
        let mut list = ZipList::new();
        list.push_back(b"hello");
        assert_eq!(list.get(0), Some(b"hello".to_vec()));
    }

    #[test]
    #[ignore]
    fn test_integer_encoding() {
        let mut list = ZipList::new();
        list.push_back(b"123");
        // 应该被编码为整数
    }
}
