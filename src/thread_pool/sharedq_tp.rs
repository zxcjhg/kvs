use crate::common::Result;
use crate::thread_pool::ThreadPool;
use crossbeam_channel;
use crossbeam_channel::bounded;
use std::thread;
pub struct SharedQueueThreadPool {
    sender: crossbeam_channel::Sender<Message>,
    num_threads: u32,
}

type Task = Box<dyn FnOnce() + Send + 'static>;

enum Message {
    Task(Task),
    Shutdown,
}

#[derive(Clone)]
struct TaskHandler {
    receiver: crossbeam_channel::Receiver<Message>,
}

impl TaskHandler {
    fn run(&mut self) {
        while let Message::Task(task) = self.receiver.recv().unwrap() {
            task();
        }
    }
}

impl Drop for TaskHandler {
    fn drop(&mut self) {
        if thread::panicking() {
            let mut th = self.clone();
            thread::spawn(move || {
                th.run();
            });
        }
    }
}
impl ThreadPool for SharedQueueThreadPool {
    fn new(num_threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        let (sender, receiver) = bounded::<Message>(4 * num_threads as usize);

        for _ in 0..num_threads {
            let mut th = TaskHandler {
                receiver: receiver.clone(),
            };
            thread::spawn(move || th.run());
        }
        Ok(SharedQueueThreadPool {
            num_threads,
            sender,
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender.send(Message::Task(Box::new(job))).unwrap();
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        for _ in 0..self.num_threads {
            self.sender.send(Message::Shutdown).unwrap()
        }
    }
}
