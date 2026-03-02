use tokio::sync::broadcast;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct PubSub {
    // 频道名 → broadcast Sender
    channels: HashMap<String, broadcast::Sender<String>>,
}

impl PubSub {
    pub fn new() -> Self {
        PubSub {
            channels: HashMap::new(),
        }
    }

    // 订阅频道，返回一个 Receiver
    pub fn subscribe(&mut self, channel: &str) -> broadcast::Receiver<String> {
        self.channels
        .entry(channel.to_string())
        .or_insert_with(|| {
            // 创建一个新的频道，容量为 1000
            broadcast::channel(1000).0
        })
        .subscribe()
    }

    // 取消订阅
    pub fn unsubscribe(&mut self, channel: &str) {
        self.channels.remove(channel);
    }

    // 发布消息，返回收到消息的订阅者数量
    pub fn publish(&self, channel: &str, message: &str) -> usize {
        if let Some(sender) = self.channels.get(channel) {
            sender.send(message.to_string()).unwrap_or(0) as usize
        } else {
            0
        } 
     }
}
