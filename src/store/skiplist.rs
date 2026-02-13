use std::fmt::Debug;

/// 跳表的最大层数
const MAX_LEVEL: usize = 32;

/// 层级提升的概率 (p = 1/4)
const LEVEL_UP_PROBABILITY: f64 = 0.25;

/// 跳表节点
pub struct SkipListNode<K, V> {
    pub key: K,
    pub value: V,
    /// 每一层的前进指针
    pub forward: Vec<Option<Box<SkipListNode<K, V>>>>,
    /// 后退指针(用于反向遍历)
    pub backward: *mut SkipListNode<K, V>,
    /// 每层跨越的节点数(用于计算排名)
    pub span: Vec<usize>,
}

impl<K, V> SkipListNode<K, V> {
    pub fn new(key: K, value: V, level: usize) -> Self {
        let mut forward = Vec::with_capacity(level);
        for _ in 0..level {
            forward.push(None);
        }
        SkipListNode {
            key,
            value,
            forward,
            backward: std::ptr::null_mut(),
            span: vec![0; level],
        }
    }
}

/// 跳表 (Skip List)
/// Redis ZSET 的底层实现之一
pub struct SkipList<K, V> {
    /// 头节点(哨兵节点,不存储实际数据)
    head: Box<SkipListNode<K, V>>,
    /// 尾节点
    tail: *mut SkipListNode<K, V>,
    /// 当前最大层级
    level: usize,
    /// 节点数量
    length: usize,
}

impl<K: Ord + Default, V: Default> SkipList<K, V> {
    /// 创建新的跳表
    pub fn new() -> Self {
        SkipList {
            head: Box::new(SkipListNode::new(
                K::default(),
                V::default(),
                MAX_LEVEL,
            )),
            tail: std::ptr::null_mut(),
            level: 1,
            length: 0,
        }
    }

    /// 获取跳表长度
    pub fn len(&self) -> usize {
        self.length
    }

    /// 判断是否为空
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// 随机生成节点层级
    fn random_level(&self) -> usize {
        // TODO: 实现随机层级生成
        // 使用几何分布: P(level = k) = p^(k-1) * (1-p)
        // 默认 p = 0.25
        todo!("实现随机层级生成")
    }

    /// 插入或更新键值对
    /// 返回: 是否是新插入(true)还是更新(false)
    pub fn insert(&mut self, key: K, value: V) -> bool {
        // TODO: 实现插入逻辑
        // 1. 找到每一层需要更新的节点
        // 2. 生成随机层级
        // 3. 创建新节点并插入
        // 4. 更新每层的forward指针和span
        // 5. 更新backward指针
        todo!("实现插入逻辑")
    }

    /// 根据键查找值
    pub fn get(&self, key: &K) -> Option<&V> {
        // TODO: 实现查找逻辑
        // 从最高层开始,逐层向下查找
        todo!("实现查找逻辑")
    }

    /// 根据键删除节点
    pub fn remove(&mut self, key: &K) -> Option<V> {
        // TODO: 实现删除逻辑
        // 1. 找到每一层需要更新的节点
        // 2. 删除节点并更新指针
        // 3. 更新span和backward
        // 4. 可能需要降低level
        todo!("实现删除逻辑")
    }

    /// 根据排名获取节点 (0-based)
    pub fn get_by_rank(&self, rank: usize) -> Option<(&K, &V)> {
        // TODO: 实现按排名查找
        // 利用span信息快速定位
        todo!("实现按排名查找")
    }

    /// 获取键的排名 (0-based)
    pub fn get_rank(&self, key: &K) -> Option<usize> {
        // TODO: 实现获取排名
        // 查找过程中累加span
        todo!("实现获取排名")
    }

    /// 范围查询: 获取排名在 [start, end) 之间的所有节点
    pub fn range(&self, start: usize, end: usize) -> Vec<(&K, &V)> {
        // TODO: 实现范围查询
        todo!("实现范围查询")
    }

    /// 正向遍历
    pub fn iter(&self) -> SkipListIter<K, V> {
        // TODO: 实现迭代器
        todo!("实现迭代器")
    }

    /// 清空跳表
    pub fn clear(&mut self) {
        self.head.forward.iter_mut().for_each(|f| *f = None);
        self.tail = std::ptr::null_mut();
        self.level = 1;
        self.length = 0;
    }
}

impl<K: Ord + Default, V: Default> Default for SkipList<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

/// 跳表迭代器
pub struct SkipListIter<'a, K, V> {
    current: Option<&'a SkipListNode<K, V>>,
}

impl<'a, K, V> Iterator for SkipListIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: 实现迭代器next
        todo!("实现迭代器")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_skiplist() {
        let list: SkipList<i32, String> = SkipList::new();
        assert_eq!(list.len(), 0);
        assert!(list.is_empty());
    }

    #[test]
    #[ignore]
    fn test_insert() {
        let mut list = SkipList::new();
        assert!(list.insert(1, "one"));
        assert!(list.insert(2, "two"));
        assert_eq!(list.len(), 2);
    }

    #[test]
    #[ignore]
    fn test_get() {
        let mut list = SkipList::new();
        list.insert(1, "one");
        assert_eq!(list.get(&1), Some(&"one"));
        assert_eq!(list.get(&2), None);
    }

    #[test]
    #[ignore]
    fn test_rank() {
        let mut list = SkipList::new();
        list.insert(10, "ten");
        list.insert(20, "twenty");
        list.insert(30, "thirty");
        assert_eq!(list.get_rank(&10), Some(0));
        assert_eq!(list.get_rank(&20), Some(1));
        assert_eq!(list.get_rank(&30), Some(2));
    }
}
