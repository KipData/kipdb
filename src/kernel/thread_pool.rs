use std::sync::{Arc, mpsc, Mutex};
use std::thread;
use tracing::info;

type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Message>,
}

pub struct Worker {
    id: usize,
    job: Option<thread::JoinHandle<()>>
}

pub enum Message {
    NewJob(Job),
    Terminate,
}

impl Worker {
    pub fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Worker {
        let handle = thread::spawn(move || loop {
            let message = receiver.lock()
                .unwrap()
                .recv()
                .unwrap();

            match message {
                Message::NewJob(job) => {
                    info!("[Worker: {}][Executing][Get a Job]", id);
                    job();
                    info!("[Worker: {}][Executing][Done]", id);
                }

                Message::Terminate => {
                    info!("[Worker: {}][Terminate]", id);
                    break;
                }
            }
        });

        Worker {
            id,
            job: Some(handle)
        }
    }
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel();

        let receiver = Arc::new(Mutex::new(receiver));

        let mut threads = Vec::with_capacity(size);

        for index in 0..size {
            threads.push(Worker::new(index, Arc::clone(&receiver)));
        }

        ThreadPool { workers: threads, sender }
    }

    pub fn execute<F>(&self, f: F)
        where F:FnOnce() + Send +'static,
    {
        let job = Box::new(f);

        self.sender.send(Message::NewJob(job))
            .unwrap();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        info!("[ThreadPool][drop][Sending terminate message to all workers]");
        for _ in &self.workers {
            self.sender
                .send(Message::Terminate)
                .unwrap()
        }

        for worker in &mut self.workers {
            info!("[Worker: {}][Shutting Down]", worker.id);

            if let Some(job) = worker.job.take() {
                job.join()
                    .unwrap();
            }
        }
    }
}