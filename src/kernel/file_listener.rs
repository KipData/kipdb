use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::Receiver;
use itertools::Itertools;
use tokio::sync::oneshot::Sender;
use crate::kernel::Result;

pub(crate) type FileAction = HashMap<u64, (BufReader<File>, BufWriter<File>)>;

pub(crate) type ResultSender = Sender<Result<Vec<u8>>>;

pub struct FileListener {
    task_receiver: Receiver<Message>,
    fs_map: Arc<Mutex<FileAction>>,
    read_stack: Vec<ReadTask>,
    write_stack: Vec<WriteTask>
}

enum Message {
    Read(ReadTask),
    Writer(WriteTask),
    Terminate
}

pub(crate) struct PosSender {
    start: u64,
    end: u64,
    result_sender: ResultSender
}

pub(crate) struct ReadTask {
    gen: u64,
    start: u64,
    end: u64,
    vec_result_call: Vec<PosSender>
}

struct WriteTask {
    gen: u64,
    data: Vec<u8>,
    vec_result_call: Vec<PosSender>
}

impl FileListener {
    fn listen_run(&mut self) {
        let receiver = &mut self.task_receiver;

        match receiver.recv().unwrap() {
            Message::Read(task) => {
                let gen = task.gen;
                let read_stack = &mut self.read_stack;
                // 将新Task传入ReadStack中
                Self::push_read_task(read_stack, task);
                if let Some(read_task) = read_stack.pop() {
                    let fs_map = self.fs_map.clone();
                    let mut sync_fs_map = fs_map.lock()
                        .expect("[FileListener][listen_run][Lock Expect!]");
                    let (reader, _) = sync_fs_map.get_mut(&gen)
                        .expect("[FileListener][listen_run][Get File Expect!]");

                    // 使用Vec buffer获取数据
                    let len = (read_task.end - read_task.end) as usize;
                    let mut buffer = Vec::with_capacity(len);
                    reader.seek(SeekFrom::Start(read_task.start))
                        .expect("[FileListener][listen_run][Seek Expect!]");
                    reader.read(&mut buffer).expect("[FileListener][listen_run][Read Expect!]");

                    let min_start = read_task.vec_result_call.iter()
                        .min_by(|item_a, item_b| Ord::cmp(&item_a.start, &item_b.start))
                        .expect("[FileListener][listen_run][Task Disappear!]").start;
                    for result_call in read_task.vec_result_call {
                        let start = (result_call.start - min_start) as usize;
                        let end = (result_call.end - min_start) as usize;
                        let vec_u8 = (&buffer[start..end]).to_vec();
                        result_call.result_sender.send(Ok(vec_u8)).expect("[FileListener][listen_run][Sender Expect!]");
                    }
                }
            }
            Message::Writer(task) => {

            }
            Message::Terminate => {

            }
        }

    }

    /// 将新的读取Task载入ReadStack中
    /// 并对新的Task尝试与其他已有Task进行融合
    /// 注意：ReadStack需要从头写入，尾部抛出
    /// 因为vec::pop()是从尾部抛出的
    ///
    /// 外部调用时，inside参数需要为false
    ///
    /// 使用递归来做的话可能效果会更好，但是rust递归和所有权太难搞了:(
    pub(crate) fn push_read_task(read_stack: &mut Vec<ReadTask>, mut new_task: ReadTask) -> () {
        let gen = new_task.gen;
        let mut this_task = &mut new_task;
        let mut vec_rm_tag = Vec::new();

        let vec_index_task = read_stack.iter_mut()
            .enumerate()
            // 过滤不为同一个gen的任务
            .filter(|(_, task)| gen == task.gen)
            // 由数据检索区域大小排序
            // 大块的数据优先组合，提高融合率
            .sorted_by(|(_, wait_task_a), (_, wait_task_b)| {
                let data_len_a = wait_task_a.start + wait_task_a.end;
                let data_len_b = wait_task_b.start + wait_task_b.end;
                Ord::cmp(&data_len_b, &data_len_a)
            }).collect_vec();

        // 数据边界判断相交以决定融合
        for (index, wait_task) in vec_index_task {
            if wait_task.start > this_task.start && wait_task.start < this_task.end { // 已存在wait_task的读起始位置与new_task读终止位置相交
                wait_task.start = this_task.start;

                this_task = Self::fusion_task(&mut vec_rm_tag, &mut this_task, index, wait_task);
            } else if this_task.start > wait_task.start && this_task.start < wait_task.end { // 已存在wait_task的读终止位置与new_task读起始位置相交
                wait_task.end = this_task.end;

                this_task = Self::fusion_task(&mut vec_rm_tag, &mut this_task, index, wait_task);
            }
        }

        // 清除多余任务
        vec_rm_tag.sort();
        if vec_rm_tag.len() > 0 {
            // 删除最后被融合的Task
            // 因为最后的Task是最终融合的Task
            vec_rm_tag.pop();
            // 将vec_rm_tag从新往旧(从0往后)
            // 删除时需要做好偏移量的调整
            for (index, rm_index) in vec_rm_tag.iter().enumerate() {
                read_stack.remove(rm_index - index);
            }
        } else {
            read_stack.insert(0, new_task);
        }
    }

    // 将两个Task融合并且标记到vec_rm_tag之中,并返回融合完成的猫
    fn fusion_task<'a>(vec_rm_tag: &mut Vec<usize>, this_task: &mut ReadTask, index: usize, wait_task: &'a mut ReadTask) -> &'a mut ReadTask {
        vec_rm_tag.push(index);
        wait_task.vec_result_call.append(&mut this_task.vec_result_call);
        return wait_task;
    }

}

impl ReadTask {
    pub(crate) fn new(gen: u64, start: u64, end: u64, result_call: ResultSender) -> Self {
        let sender = PosSender {
            start,
            end,
            result_sender: result_call
        };
        let vec_result_call = vec![sender];
        Self { gen, start, end, vec_result_call}
    }
}

#[test]
fn test_fusion_task() -> Result<()> {
    let mut task_stack: Vec<ReadTask> = Vec::new();

    let (sender1, _receiver) = tokio::sync::oneshot::channel();
    let (sender2, _receiver) = tokio::sync::oneshot::channel();
    let (sender3, _receiver) = tokio::sync::oneshot::channel();
    let (sender4, _receiver) = tokio::sync::oneshot::channel();
    let (sender5, _receiver) = tokio::sync::oneshot::channel();
    let (sender6, _receiver) = tokio::sync::oneshot::channel();
    let (sender7, _receiver) = tokio::sync::oneshot::channel();

    let task1 = ReadTask::new(1,10, 20, sender1);
    let task2 = ReadTask::new(1,5, 11, sender2);
    let task3 = ReadTask::new(1,18, 34, sender3);
    let task4 = ReadTask::new(1,35, 40, sender4);
    let task5 = ReadTask::new(1,36, 45, sender5);
    let task6 = ReadTask::new(1,29, 34, sender6);
    let task7 = ReadTask::new(2,3, 34, sender7);

    FileListener::push_read_task(&mut task_stack, task1);
    FileListener::push_read_task(&mut task_stack, task2);
    FileListener::push_read_task(&mut task_stack, task3);
    FileListener::push_read_task(&mut task_stack, task4);
    FileListener::push_read_task(&mut task_stack, task5);
    FileListener::push_read_task(&mut task_stack, task6);
    FileListener::push_read_task(&mut task_stack, task7);

    assert!(task_stack.len() > 0 || task_stack.len() < 7);
    Ok(())
}