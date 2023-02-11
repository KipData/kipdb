use std::sync::Arc;
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLockReadGuard;
use crate::kernel::{CommandData, Result};
use crate::kernel::lsm::lsm_kv::{Config, wal_put};
use crate::kernel::lsm::{key_encode_with_seq, MemTable, TableInner};
use crate::kernel::lsm::log::LogLoader;
use crate::kernel::lsm::version::Version;
use crate::KvsError;

pub struct Transaction<'a> {
    seq_id: i64,
    read_inner: RwLockReadGuard<'a, TableInner>,
    version: Arc<Version>,
    writer_buf: SkipMap<Vec<u8>, CommandData>,
    wal: Arc<LogLoader>,
    config: Arc<Config>,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(
        config: &Arc<Config>,
        version: Arc<Version>,
        read_inner: RwLockReadGuard<'a, TableInner>,
        wal: &Arc<LogLoader>
    ) -> Result<Transaction<'a>> {
        let seq_id = config.create_gen();
        Ok(Self {
            seq_id,
            read_inner,
            version,
            writer_buf: SkipMap::new(),
            wal: Arc::clone(wal),
            config: Arc::clone(config),
        })
    }

    /// 通过Key获取对应的Value
    ///
    /// 此处不需要等待压缩，因为在Transaction存活时不会触发Compaction
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(value) = self.writer_buf.get(key)
            .map(|entry| entry.value().get_value_clone())
            .flatten()
            .or_else(|| MemTable::find_with_inner(key, self.seq_id, &self.read_inner))
        {
            return Ok(Some(value));
        }

        if let Some(value) = self.version.find_data_for_ss_tables(key).await? {
            return Ok(Some(value));
        }

        Ok(None)
    }

    pub fn set(&mut self, key: &[u8], value: Vec<u8>) {
        let _ignore = self.writer_buf.insert(
            key.to_vec(),
            CommandData::set(key.to_vec(), value)
        );
    }

    pub async fn remove(&mut self, key: &[u8]) -> Result<()> {
        if let Some(_) = self.get(key).await? {
            let _ignore = self.writer_buf.insert(
                key.to_vec(),
                CommandData::remove(key.to_vec())
            );
        } else { return Err(KvsError::KeyNotFound); }

        Ok(())
    }

    pub async fn commit(self) -> Result<()> {
        let Transaction {
            read_inner,
            writer_buf,
            config,
            wal,
            ..
        } = self;

        // Wal与MemTable双写
        if config.wal_enable {
            for entry in writer_buf.iter() {
                wal_put(
                    &wal, entry.value(), !config.wal_async_put_enable
                ).await;
            }
        }

        // TODO: 待压缩反馈
        let _todo = Self::insert_batch_data_and_is_exceeded(
            &read_inner,
            writer_buf.into_iter().collect(),
            &config
        );

        Ok(())
    }

    pub(crate) fn insert_batch_data_and_is_exceeded(
        inner: &TableInner,
        vec_data: Vec<(Vec<u8>, CommandData)>,
        config: &Config,
    ) -> bool {
        // 将seq_id作为低位
        let seq_id = config.create_gen();

        vec_data.into_iter()
            .for_each(|(cmd_key, cmd)| {
                let key = key_encode_with_seq(cmd_key, seq_id);
                let _ignore = inner.mem_table.insert(key, (cmd, seq_id));
            });

        inner.mem_table.len() >= config.minor_threshold_with_len
    }

}

/// TODO: 更多的Test Case
#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use tempfile::TempDir;
    use crate::kernel::lsm::lsm_kv::{Config, LsmStore};
    use crate::kernel::{KVStore, Result};

    #[test]
    fn test_transaction() -> Result<()> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        tokio_test::block_on(async move {
            let times = 5000;

            let value = b"Stray birds of summer come to my window to sing and fly away.
            And yellow leaves of autumn, which have no songs, flutter and fall
            there with a sign.";

            let config = Config::new(temp_dir.into_path(), 0, 0)
                .wal_enable(false)
                .minor_threshold_with_len(1000)
                .major_threshold_with_sst_size(4);
            let kv_store = LsmStore::open_with_config(config).await?;

            let mut transaction = kv_store.transaction().await?;

            let mut vec_kv = Vec::new();

            for i in 0..times {
                let vec_u8 = bincode::serialize(&i)?;
                vec_kv.push((
                    vec_u8.clone(),
                    vec_u8.into_iter()
                        .chain(value.to_vec())
                        .collect_vec()
                ));
            }

            for i in 0..times {
                transaction.set(&vec_kv[i].0, vec_kv[i].1.clone());
            }

            transaction.remove(&vec_kv[times - 1].0).await?;

            for i in 0..times - 1 {
                assert_eq!(transaction.get(&vec_kv[i].0).await?, Some(vec_kv[i].1.clone()));
            }

            assert_eq!(transaction.get(&vec_kv[times - 1].0).await?, None);

            // 提交前不应该读取到数据
            for i in 0..times {
                assert_eq!(kv_store.get(&vec_kv[i].0).await?, None);
            }

            transaction.commit().await?;

            for i in 0..times - 1 {
                assert_eq!(kv_store.get(&vec_kv[i].0).await?, Some(vec_kv[i].1.clone()));
            }

            Ok(())
        })
    }
}