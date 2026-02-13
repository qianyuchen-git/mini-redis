use rand::Rng;
use std::ops::Add;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use tokio::time::{Duration, sleep, timeout};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    exercise_4_producer_consumer();
    Ok(())
}

fn exercise_1_basic_spawn() {
    println!("=== 练习1: 基础线程创建 ===");
    let mut handles = Vec::new();
    // TODO: 在这里实现
    // 提示：创建3个线程，每个打印ID并sleep
    handles.push(thread::spawn(move || {
        thread::sleep(Duration::from_secs(1));
        println!("Thread 1 done");
    }));

    handles.push(thread::spawn(move || {
        thread::sleep(Duration::from_secs(2));
        println!("Thread 2 done");
    }));
    handles.push(thread::spawn(move || {
        thread::sleep(Duration::from_secs(3));
        println!("Thread 3 done");
    }));
    for handle in handles {
        handle.join().unwrap();
    }
    println!("All done\n");
}

fn exercise_2_shared_counter() {
    println!("=== 练习2: 共享计数器 ===");
    let counter = Arc::new(Mutex::new(0));
    let mut handles = Vec::new();
    for i in 0..10 {
        let counter_clone = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            for j in 0..100 {
                *counter_clone.lock().unwrap() += 1;
            }
            println!("Thread {} incremented counter", i);
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    println!("Final counter value: {}\n", *counter.lock().unwrap());
}

fn exercise_3_producer_consumer() {
    println!("=== 练习3: 生产者消费者 ===");
    let (tx, rx) = mpsc::channel::<String>();
    let mut handles = Vec::new();
    {
        let tx_clone = tx.clone();
        handles.push(thread::spawn(move || {
            for i in 0..10 {
                let msg = format!("Thread 1 counter:{}", i + 1);
                tx_clone.send(msg).unwrap();
                thread::sleep(Duration::from_secs(1));
            }
        }));
    }
    {
        let tx_clone = tx.clone();
        handles.push(thread::spawn(move || {
            for i in 0..10 {
                let msg = format!("Thread 2 counter:{}", i + 1);
                tx_clone.send(msg).unwrap();
                thread::sleep(Duration::from_secs(1));
            }
        }));
    }
    {
        let tx_clone = tx.clone();
        handles.push(thread::spawn(move || {
            for i in 0..10 {
                let msg = format!("Thread 3 counter:{}", i + 1);
                tx_clone.send(msg).unwrap();
                thread::sleep(Duration::from_secs(1));
            }
        }));
    }
    drop(tx);
    handles.push(thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(2));
            match rx.recv() {
                Ok(msg) => {
                    println!("Received: {}", msg);
                }
                Err(_) => {
                    return;
                }
            }
        }
    }));
    for handle in handles {
        handle.join().unwrap();
    }
}

fn exercise_4_producer_consumer() {
    type Task = Box<dyn FnOnce() + Send + 'static>;
    let (tx, rx) = mpsc::channel::<Task>();

    let rx_arc = Arc::new(Mutex::new(rx));
    let mut handles = Vec::new();
    let tx_clone = tx.clone();
    handles.push(thread::spawn(move || {
        for i in 0..20 {
            tx_clone.send(Box::new(|| {
                let mut rng = rand::thread_rng();
                let n: u64 = rng.gen_range(1..=10);
                thread::sleep(Duration::from_secs(n));
                println!("This task sleeping {} seconds", n);
            })).unwrap();
        }
    }));
    drop(tx);
    for _ in 0..4 {
        let rx_clone = Arc::clone(&rx_arc);
        handles.push(thread::spawn(move || {
            loop {
                let task = {
                    let receiver = rx_clone.lock().unwrap();
                    receiver.recv()
                };
                match task {
                    Ok(task) => {
                        task();
                    }
                    Err(_) => {
                        return;
                    }
                }
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
}
