use tempfile::TempDir;
use walkdir::WalkDir;
use kip_db::kernel::hash_kv::HashStore;
use kip_db::kernel::io::{FileExtension, IoFactory, IoType};
use kip_db::kernel::KVStore;
use kip_db::kernel::lsm::lsm_kv::LsmStore;
use kip_db::kernel::Result;
use kip_db::kernel::sled_kv::SledStore;

#[test]
fn get_stored_value() -> Result<()> {
    get_stored_value_with_kv_store::<HashStore>()?;
    get_stored_value_with_kv_store::<SledStore>()?;
    get_stored_value_with_kv_store::<LsmStore>()?;
    Ok(())
}

fn get_stored_value_with_kv_store<T: KVStore>() -> Result<()> {
    tokio_test::block_on(async move {
        let key1: Vec<u8> = encode_key("key1")?;
        let key2: Vec<u8> = encode_key("key2")?;
        let value1: Vec<u8> = encode_key("value1")?;
        let value2: Vec<u8> = encode_key("value2")?;

        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kv_store = T::open(temp_dir.path()).await?;
        kv_store.set(&key1, value1.clone()).await?;
        kv_store.set(&key2, value2.clone()).await?;

        kv_store.flush().await?;

        kv_store.get(&key1).await?;
        kv_store.get(&key2).await?;
        kv_store.flush().await?;
        // Open from disk again and check persistent data.
        drop(kv_store);
        let kv_store = T::open(temp_dir.path()).await?;
        assert_eq!(kv_store.get(&key1).await?, Some(value1));
        assert_eq!(kv_store.get(&key2).await?, Some(value2));

        Ok(())
    })
}

// Should overwrite existent value.
#[test]
fn overwrite_value() -> Result<()> {
    overwrite_value_with_kv_store::<HashStore>()?;
    overwrite_value_with_kv_store::<SledStore>()?;
    overwrite_value_with_kv_store::<LsmStore>()?;

    Ok(())
}

fn overwrite_value_with_kv_store<T: KVStore>() -> Result<()> {
    tokio_test::block_on(async move {
        let key1: Vec<u8> = encode_key("key1")?;
        let value1: Vec<u8> = encode_key("value1")?;
        let value2: Vec<u8> = encode_key("value2")?;
        let value3: Vec<u8> = encode_key("value3")?;

        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kv_store = T::open(temp_dir.path()).await?;

        kv_store.set(&key1, value1.clone()).await?;
        kv_store.flush().await?;
        assert_eq!(kv_store.get(&key1).await?, Some(value1.clone()));
        kv_store.set(&key1, value2.clone()).await?;
        kv_store.flush().await?;
        assert_eq!(kv_store.get(&key1).await?, Some(value2.clone()));

        drop(kv_store);
        let kv_store = T::open(temp_dir.path()).await?;
        assert_eq!(kv_store.get(&key1).await?, Some(value2.clone()));
        kv_store.set(&key1, value3.clone()).await?;
        kv_store.flush().await?;
        assert_eq!(kv_store.get(&key1).await?, Some(value3.clone()));

        Ok(())
    })
}

// Should get `None` when getting a non-existent key.
#[test]
fn get_non_existent_value() -> Result<()> {
    get_non_existent_value_with_kv_store::<HashStore>()?;
    get_non_existent_value_with_kv_store::<SledStore>()?;
    get_non_existent_value_with_kv_store::<LsmStore>()?;

    Ok(())
}

fn get_non_existent_value_with_kv_store<T: KVStore>() -> Result<()> {
    tokio_test::block_on(async move {
        let key1: Vec<u8> = encode_key("key1")?;
        let key2: Vec<u8> = encode_key("key2")?;
        let value1: Vec<u8> = encode_key("value1")?;

        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kv_store = T::open(temp_dir.path()).await?;

        kv_store.set(&key1, value1).await?;
        assert_eq!(kv_store.get(&key2).await?, None);

        // Open from disk again and check persistent data.
        kv_store.flush().await?;
        drop(kv_store);
        let kv_store = T::open(temp_dir.path()).await?;
        assert_eq!(kv_store.get(&key2).await?, None);

        Ok(())
    })
}

#[test]
fn remove_non_existent_key() -> Result<()> {
    remove_non_existent_key_with_kv_store::<HashStore>()?;
    remove_non_existent_key_with_kv_store::<SledStore>()?;
    remove_non_existent_key_with_kv_store::<LsmStore>()?;

    Ok(())
}
fn remove_non_existent_key_with_kv_store<T: KVStore>() -> Result<()> {
    tokio_test::block_on(async move {
        let key1: Vec<u8> = encode_key("key1")?;

        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kv_store = T::open(temp_dir.path()).await?;
        assert!(kv_store.remove(&key1).await.is_err());

        Ok(())
    })
}

#[test]
fn remove_key() -> Result<()> {
    remove_key_with_kv_store::<HashStore>()?;
    remove_key_with_kv_store::<SledStore>()?;
    remove_key_with_kv_store::<LsmStore>()?;

    Ok(())
}

fn remove_key_with_kv_store<T: KVStore>() -> Result<()> {
    tokio_test::block_on(async move {
        let key1: Vec<u8> = encode_key("key1")?;
        let value1: Vec<u8> = encode_key("value1")?;

        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kv_store = T::open(temp_dir.path()).await?;
        kv_store.set(&key1, value1).await?;
        assert!(kv_store.remove(&key1).await.is_ok());
        assert_eq!(kv_store.get(&key1).await?, None);

        Ok(())
    })
}

// Insert data until total size of the directory decreases.
// Test data correctness after compaction.
#[test]
fn compaction() -> Result<()> {
    compaction_with_kv_store::<HashStore>()?;
    compaction_with_kv_store::<SledStore>()?;
    compaction_with_kv_store::<LsmStore>()?;

    Ok(())
}

// 如果此处出现异常，可以尝试降低压缩阈值或者提高检测时间
fn compaction_with_kv_store<T: KVStore>() -> Result<()> {
    tokio_test::block_on(async move {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kv_store = T::open(temp_dir.path()).await?;
        let dir_size = || {
            let entries = WalkDir::new(temp_dir.path()).into_iter();
            let len: walkdir::Result<u64> = entries
                .map(|res| {
                    res.and_then(|entry| entry.metadata())
                        .map(|metadata| metadata.len())
                })
                .sum();
            len.expect("fail to get directory size")
        };

        let mut current_size = dir_size();
        for iter in 0..1000 {
            for key_id in 0..1000 {
                let key = format!("key{}", key_id);
                let value = format!("{}", iter);
                kv_store.set(&encode_key(key.as_str())?,
                             encode_key(value.as_str())?).await?
            }

            kv_store.flush().await?;

            let new_size = dir_size();
            if new_size > current_size {
                current_size = new_size;
                continue;
            }
            // Compaction triggered.
            drop(kv_store);
            // reopen and check content.
            let kv_store = T::open(temp_dir.path()).await?;
            for key_id in 0..1000 {
                let key = format!("key{}", key_id);
                assert_eq!(kv_store.get(&encode_key(key.as_str())?).await?,
                           Some(encode_key(format!("{}", iter).as_str())?));
            }
            return Ok(());
        }

        panic!("No compaction detected");
    })
}

#[test]
fn test_io() -> Result<()> {
    let temp_dir = TempDir::new()
        .expect("unable to create temporary working directory");
    let factory = IoFactory::new(temp_dir.path(), FileExtension::Log).unwrap();

    io_type_test(&factory, IoType::Buf)?;
    io_type_test(&factory, IoType::MMap)?;
    io_type_test(&factory, IoType::Direct)?;

    Ok(())
}

fn io_type_test(factory: &IoFactory, io_type: IoType) -> Result<()> {
    let mut writer = factory.writer(1, io_type)?;
    let reader = factory.reader(1, io_type)?;
    let data_write1 = vec![b'1', b'2', b'3'];
    let data_write2 = vec![b'4', b'5', b'6'];
    let (pos_1, len_1) = writer.write(data_write1)?;
    let (pos_2, len_2) = writer.write(data_write2)?;
    writer.flush()?;
    let data_read = reader.read_with_pos(0, 6)?;

    assert_eq!(vec![b'1', b'2', b'3', b'4', b'5', b'6'], data_read);
    assert_eq!(pos_1, 0);
    assert_eq!(pos_2, 3);
    assert_eq!(len_1, 3);
    assert_eq!(len_2, 3);

    assert_eq!(reader.file_size()?, 6);

    Ok(())
}

fn encode_key(key: &str) -> Result<Vec<u8>>{
    Ok(bincode::serialize(key)?)
}