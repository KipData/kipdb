use std::sync::{Arc, mpsc, Mutex};
use std::thread;

/// 任务定义为执行为一次并允许发送、全局生命周期的闭包
type Job = Box<dyn FnOnce() + Send + 'static>;

/// 线程池
pub struct ThreadPool {
    // 工作节点集合
    workers: Vec<Worker>,
    // 任务发送器
    sender: mpsc::Sender<Message>,
}

/// 线程池工作节点
struct Worker {
    job: Option<thread::JoinHandle<()>>
}

//Message枚举、用于执行信号
enum Message {
    NewJob(Job),
    Terminate,
}

impl Worker {
    pub fn new(receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Worker {
        // 初始化Worker的执行闭包，用于响应执行传入的闭包任务
        let handle = thread::spawn(move || loop {
            // 任务阻塞获取
            let message = receiver.lock()
                .unwrap()
                .recv()
                .unwrap();
            
            // 匹配信号枚举
            match message {
                Message::NewJob(job) => {
                    job();
                }

                Message::Terminate => {
                    break;
                }
            }
        });

        Worker {
            job: Some(handle)
        }
    }
}

impl ThreadPool {
    /// 线程池实例化方法
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        // 获取配对的发送器与接收器
        let (sender, receiver) = mpsc::channel();

        // 对接收器进行多持有者与阻塞封装
        let receiver = Arc::new(Mutex::new(receiver));

        let mut threads = Vec::with_capacity(size);
        
        // 将接收器克隆并顺序组装工作节点插入工作节点集合中
        for _ in 0..size {
            threads.push(Worker::new(Arc::clone(&receiver)));
        }

        ThreadPool { workers: threads, sender }
    }

    // 执行任务
    pub fn execute<F>(&self, f: F)
        where F:FnOnce() + Send +'static,
    {
        // 将任务封装为Box
        let job = Box::new(f);

        // 发送至工作节点
        self.sender.send(Message::NewJob(job))
            .unwrap();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        // 轮流发送结束信号
        for _ in &self.workers {
            self.sender
                .send(Message::Terminate)
                .unwrap()
        }

        // 等待任务执行完毕
        for worker in &mut self.workers {
            if let Some(job) = worker.job.take() {
                job.join()
                    .unwrap();
            }
        }
    }
}