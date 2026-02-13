use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// 时间轮槽位
pub struct Slot<T> {
    /// 该槽位中的任务列表
    tasks: VecDeque<TimedTask<T>>,
}

impl<T> Slot<T> {
    fn new() -> Self {
        Slot {
            tasks: VecDeque::new(),
        }
    }
}

/// 定时任务
pub struct TimedTask<T> {
    /// 任务数据
    pub data: T,
    /// 到期时间
    pub deadline: Instant,
    /// 剩余轮数(用于多轮时间轮)
    pub rounds: usize,
}

/// 时间轮 (Timing Wheel)
/// Redis 用于高效管理大量定时任务(如键过期)
/// 时间复杂度: 插入O(1), 删除O(1), 触发O(1)
pub struct TimingWheel<T> {
    /// 时间轮槽位数组
    slots: Vec<Slot<T>>,
    /// 当前指针位置
    current_pos: usize,
    /// 每个槽位的时间间隔
    tick_duration: Duration,
    /// 时间轮大小
    size: usize,
    /// 时间轮启动时间
    start_time: Instant,
    /// 总任务数
    task_count: usize,
}

impl<T> TimingWheel<T> {
    /// 创建时间轮
    /// 
    /// # 参数
    /// - `size`: 槽位数量(通常是2的幂,如512, 1024)
    /// - `tick_duration`: 每个槽位代表的时间间隔(如100ms)
    /// 
    /// # 示例
    /// ```
    /// // 创建512个槽位,每个槽位100ms的时间轮
    /// // 可以表示的最大延迟: 512 * 100ms = 51.2秒
    /// let wheel = TimingWheel::new(512, Duration::from_millis(100));
    /// ```
    pub fn new(size: usize, tick_duration: Duration) -> Self {
        let mut slots = Vec::with_capacity(size);
        for _ in 0..size {
            slots.push(Slot::new());
        }

        TimingWheel {
            slots,
            current_pos: 0,
            tick_duration,
            size,
            start_time: Instant::now(),
            task_count: 0,
        }
    }

    /// 添加定时任务
    /// 
    /// # 参数
    /// - `data`: 任务数据
    /// - `delay`: 延迟时间
    pub fn add_task(&mut self, data: T, delay: Duration) {
        // TODO: 实现添加任务
        // 1. 计算任务应该放在哪个槽位
        // 2. 计算需要多少轮(rounds)
        // 3. 将任务添加到对应槽位
        todo!("实现添加任务")
    }

    /// 推进时间轮,触发到期的任务
    /// 返回所有到期的任务
    pub fn tick(&mut self) -> Vec<T> {
        // TODO: 实现tick
        // 1. 检查当前槽位的所有任务
        // 2. 对于rounds=0的任务,收集并返回
        // 3. 对于rounds>0的任务,减少rounds
        // 4. 移动current_pos到下一个槽位
        todo!("实现tick")
    }

    /// 批量推进时间轮
    /// 根据实际经过的时间推进多个tick
    pub fn advance(&mut self, elapsed: Duration) -> Vec<T> {
        // TODO: 实现批量推进
        // 计算需要推进多少个tick
        // 循环调用tick()
        todo!("实现批量推进")
    }

    /// 计算任务应该放在哪个槽位
    fn calculate_slot(&self, delay: Duration) -> (usize, usize) {
        // TODO: 实现槽位计算
        // 返回: (槽位索引, 轮数)
        // 槽位 = (current_pos + ticks) % size
        // 轮数 = ticks / size
        todo!("实现槽位计算")
    }

    /// 获取当前任务数
    pub fn len(&self) -> usize {
        self.task_count
    }

    /// 判断是否为空
    pub fn is_empty(&self) -> bool {
        self.task_count == 0
    }

    /// 清空时间轮
    pub fn clear(&mut self) {
        for slot in &mut self.slots {
            slot.tasks.clear();
        }
        self.task_count = 0;
        self.current_pos = 0;
    }
}

impl<T> Default for TimingWheel<T> {
    fn default() -> Self {
        // 默认: 512个槽位,每个槽位100ms
        Self::new(512, Duration::from_millis(100))
    }
}

/// 层级时间轮 (Hierarchical Timing Wheel)
/// 用于支持更大的时间范围
pub struct HierarchicalTimingWheel<T> {
    /// 秒级时间轮 (60个槽位,每个1秒)
    seconds_wheel: TimingWheel<T>,
    /// 分钟级时间轮 (60个槽位,每个1分钟)  
    minutes_wheel: TimingWheel<T>,
    /// 小时级时间轮 (24个槽位,每个1小时)
    hours_wheel: TimingWheel<T>,
}

impl<T> HierarchicalTimingWheel<T> {
    /// 创建层级时间轮
    pub fn new() -> Self {
        HierarchicalTimingWheel {
            seconds_wheel: TimingWheel::new(60, Duration::from_secs(1)),
            minutes_wheel: TimingWheel::new(60, Duration::from_secs(60)),
            hours_wheel: TimingWheel::new(24, Duration::from_secs(3600)),
        }
    }

    /// 添加任务
    pub fn add_task(&mut self, data: T, delay: Duration) {
        // TODO: 实现层级添加
        // 根据delay选择合适的时间轮
        // 秒 -> seconds_wheel
        // 分钟 -> minutes_wheel
        // 小时 -> hours_wheel
        todo!("实现层级添加")
    }

    /// 推进时间轮
    pub fn tick(&mut self) -> Vec<T> {
        // TODO: 实现层级推进
        // 1. 推进秒级时间轮
        // 2. 必要时级联推进分钟级和小时级
        todo!("实现层级推进")
    }
}

impl<T> Default for HierarchicalTimingWheel<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_timing_wheel() {
        let wheel: TimingWheel<String> = TimingWheel::new(512, Duration::from_millis(100));
        assert_eq!(wheel.len(), 0);
        assert!(wheel.is_empty());
    }

    #[test]
    #[ignore]
    fn test_add_task() {
        let mut wheel = TimingWheel::new(10, Duration::from_millis(100));
        wheel.add_task("task1".to_string(), Duration::from_millis(500));
        assert_eq!(wheel.len(), 1);
    }

    #[test]
    #[ignore]
    fn test_tick() {
        let mut wheel = TimingWheel::new(10, Duration::from_millis(100));
        wheel.add_task("task1".to_string(), Duration::from_millis(150));
        
        // 第一次tick,不应触发
        let expired = wheel.tick();
        assert_eq!(expired.len(), 0);
        
        // 第二次tick,应该触发
        let expired = wheel.tick();
        assert_eq!(expired.len(), 1);
    }
}
