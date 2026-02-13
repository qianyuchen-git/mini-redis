use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};
pub struct CommandQueue<T> {
    queue: Mutex<VecDeque<T>>,
    cond: Condvar,
}

impl<T> CommandQueue<T> {
    pub fn new() -> Self {
        CommandQueue {
            queue: Mutex::new(VecDeque::new()),
            cond: Condvar::new(),
        }
    }

    pub fn push(&self, item: T) {
        let mut guard = self.queue.lock().unwrap();
        guard.push_back(item);
        self.cond.notify_one();
    }

    pub fn pop(&self) -> T {
        let mut guard = self.queue.lock().unwrap();
        loop {
            if let Some(item) = guard.pop_front() {
                return item;
            }
            guard = self.cond.wait(guard).unwrap();
        }
    }
}
