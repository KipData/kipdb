use std::sync::Arc;
use chrono::Local;
use tokio::sync::RwLock;
use crate::HashStore;
use crate::kernel::io_handler::IOHandlerFactory;
use crate::kernel::Result;
use crate::kernel::lsm::lsm_kv::{CommandCodec, Config, LsmStore, wal_put};
use crate::kernel::lsm::Manifest;
use crate::kernel::lsm::ss_table::{LEVEL_0, SsTable};

pub(crate) struct Compactor {
    manifest: Arc<RwLock<Manifest>>,
    config: Arc<Config>,
    io_handler_factory: Arc<IOHandlerFactory>,
    wal: Arc<HashStore>,
}

impl Compactor {

    pub(crate) fn new(manifest: Arc<RwLock<Manifest>>, config: Arc<Config>, io_handler_factory: Arc<IOHandlerFactory>, wal: Arc<HashStore>) -> Self {
        Self { manifest, config, io_handler_factory, wal }
    }

    /// 持久化immutable_table为SSTable
    /// 此处manifest参数需要传入是因为Rust的锁不可重入所以需要从外部将锁对象传入
    pub(crate) async fn minor_compaction(&self) -> Result<()> {
        let mut manifest = self.manifest.write().await;

        // 获取当前时间戳当gen
        let time_stamp = Local::now().timestamp_millis() as u64;
        let io_handler = self.io_handler_factory.create(time_stamp)?;
        // 切换mem_table并准备持久化
        let (vec_keys, vec_values) = manifest.table_swap();
        let vec_ts_u8 = CommandCodec::encode_gen(time_stamp)?;

        // 将这些索引的key序列化后预先存入wal中作防灾准备
        // 当持久化异常时将对应gen的key反序列化出来并从wal找到对应值
        wal_put(&self.wal, vec_ts_u8, CommandCodec::encode_keys(&vec_keys)?);
        // 从内存表中将数据持久化为ss_table
        let ss_table = SsTable::create_for_immutable_table(&self.config
                                                           , io_handler
                                                           , &vec_values
                                                           , LEVEL_0).await?;
        manifest.insert_ss_table(time_stamp, ss_table);
        Ok(())
    }

    pub(crate) fn from_lsm_kv(lsm_kv: &LsmStore) -> Self {
        let manifest = Arc::clone(&lsm_kv.manifest());
        let config = Arc::clone(&lsm_kv.config());
        let wal = Arc::clone(&lsm_kv.wal());
        let io_handler_factory = Arc::clone(&lsm_kv.io_handler_factory());

        Compactor::new(manifest, config, io_handler_factory, wal)
    }
}
