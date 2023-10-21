use crate::kernel::lsm::table::loader::TableLoader;
use itertools::Itertools;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::error;

#[derive(Debug)]
pub enum CleanTag {
    Clean(u64),
    Add { version: u64, gens: Vec<i64> },
}

/// SSTable的文件删除器
///
/// 整体的设计思路是由`Version::drop`进行删除驱动
/// 考虑过在Compactor中进行文件删除，但这样会需要进行额外的阈值判断以触发压缩(Compactor的阈值判断是通过传入的KV进行累计)
pub(crate) struct Cleaner {
    ss_table_loader: Arc<TableLoader>,
    tag_rx: UnboundedReceiver<CleanTag>,
    del_gens: Vec<(u64, Vec<i64>)>,
}

impl Cleaner {
    pub(crate) fn new(
        ss_table_loader: &Arc<TableLoader>,
        tag_rx: UnboundedReceiver<CleanTag>,
    ) -> Self {
        Self {
            ss_table_loader: Arc::clone(ss_table_loader),
            tag_rx,
            del_gens: Vec::new(),
        }
    }

    /// 监听tag_rev传递的信号
    ///
    /// 当tag_tx drop后自动关闭
    pub(crate) async fn listen(&mut self) {
        loop {
            match self.tag_rx.recv().await {
                Some(CleanTag::Clean(ver_num)) => self.clean(ver_num),
                Some(CleanTag::Add { version, gens }) => {
                    self.del_gens.push((version, gens));
                }
                // 关闭时对此次运行中的暂存Version全部进行删除
                None => {
                    let all_ver_num = self
                        .del_gens
                        .iter()
                        .map(|(ver_num, _)| ver_num)
                        .cloned()
                        .collect_vec();
                    for ver_num in all_ver_num {
                        self.clean(ver_num)
                    }
                    return;
                }
            }
        }
    }

    /// 传入ver_num进行冗余SSTable的删除
    ///
    /// 整体删除逻辑: 当某个Version Drop时，以它的version_num作为基准，
    /// 检测该version_num在del_gens(应以version_num为顺序)的位置
    /// 当为第一位时说明无前置Version在使用，因此可以直接将此version_num的vec_gens全部删除
    /// 否则将对应位置的vec_gens添加至前一位的vec_gens中，使前一个Version开始clean时能将转移过来的vec_gens一起删除
    fn clean(&mut self, ver_num: u64) {
        if let Some(index) = Self::find_index_with_ver_num(&self.del_gens, ver_num) {
            let (_, mut vec_gen) = self.del_gens.remove(index);
            if index == 0 {
                let ss_table_loader = &self.ss_table_loader;
                // 当此Version处于第一位时，直接将其删除
                for gen in vec_gen {
                    if let Err(err) = ss_table_loader.clean(gen) {
                        error!(
                            "[Cleaner][clean][SSTable: {}]: Remove Error!: {:?}",
                            gen, err
                        );
                    };
                }
            } else {
                // 若非Version并非第一位，为了不影响前面Version对SSTable的读取处理，将待删除的SSTable的gen转移至前一位
                if let Some((_, pre_vec_gen)) = self.del_gens.get_mut(index - 1) {
                    pre_vec_gen.append(&mut vec_gen);
                }
            }
        }
    }

    fn find_index_with_ver_num(del_gen: &[(u64, Vec<i64>)], ver_num: u64) -> Option<usize> {
        del_gen
            .iter()
            .enumerate()
            .find(|(_, (vn, _))| vn == &ver_num)
            .map(|(index, _)| index)
    }
}
