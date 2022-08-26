use tempfile::TempDir;
use walkdir::WalkDir;
use kip_db::kernel::hash_kv::HashStore;
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
    let key1: Vec<u8> = encode_key("key1")?;
    let key2: Vec<u8> = encode_key("key2")?;
    let value1: Vec<u8> = encode_key("value1")?;
    let value2: Vec<u8> = encode_key("value2")?;

    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut kv_store = T::open(temp_dir.path())?;
    kv_store.set(&key1, value1.clone())?;
    kv_store.set(&key2, value2.clone())?;

    kv_store.get(&key1)?;
    kv_store.get(&key2)?;
    // Open from disk again and check persistent data.
    drop(kv_store);
    let kv_store = T::open(temp_dir.path())?;
    assert_eq!(kv_store.get(&key1)?, Some(value1));
    assert_eq!(kv_store.get(&key2)?, Some(value2));

    Ok(())
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
    let key1: Vec<u8> = encode_key("key1")?;
    let value1: Vec<u8> = encode_key("value1")?;
    let value2: Vec<u8> = encode_key("value2")?;
    let value3: Vec<u8> = encode_key("value3")?;

    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut kv_store = T::open(temp_dir.path())?;

    kv_store.set(&key1, value1.clone())?;
    assert_eq!(kv_store.get(&key1)?, Some(value1.clone()));
    kv_store.set(&key1, value2.clone())?;
    assert_eq!(kv_store.get(&key1)?, Some(value2.clone()));

    // Open from disk again and check persistent data.
    drop(kv_store);
    let mut kv_store = T::open(temp_dir.path())?;
    assert_eq!(kv_store.get(&key1)?, Some(value2.clone()));
    kv_store.set(&key1, value3.clone())?;
    assert_eq!(kv_store.get(&key1)?, Some(value3.clone()));

    Ok(())
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
    let key1: Vec<u8> = encode_key("key1")?;
    let key2: Vec<u8> = encode_key("key2")?;
    let value1: Vec<u8> = encode_key("value1")?;

    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut kv_store = T::open(temp_dir.path())?;

    kv_store.set(&key1, value1)?;
    assert_eq!(kv_store.get(&key2)?, None);

    // Open from disk again and check persistent data.
    drop(kv_store);
    let kv_store = T::open(temp_dir.path())?;
    assert_eq!(kv_store.get(&key2)?, None);

    Ok(())
}

#[test]
fn remove_non_existent_key() -> Result<()> {
    remove_non_existent_key_with_kv_store::<HashStore>()?;
    remove_non_existent_key_with_kv_store::<SledStore>()?;
    remove_non_existent_key_with_kv_store::<LsmStore>()?;

    Ok(())
}
fn remove_non_existent_key_with_kv_store<T: KVStore>() -> Result<()> {
    let key1: Vec<u8> = encode_key("key1")?;

    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut kv_store = T::open(temp_dir.path())?;
    assert!(kv_store.remove(&key1).is_err());
    Ok(())
}

#[test]
fn remove_key() -> Result<()> {
    remove_key_with_kv_store::<HashStore>()?;
    remove_key_with_kv_store::<SledStore>()?;
    remove_key_with_kv_store::<LsmStore>()?;

    Ok(())
}

fn remove_key_with_kv_store<T: KVStore>() -> Result<()> {
    let key1: Vec<u8> = encode_key("key1")?;
    let value1: Vec<u8> = encode_key("value1")?;

    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut kv_store = T::open(temp_dir.path())?;
    kv_store.set(&key1, value1)?;
    assert!(kv_store.remove(&key1).is_ok());
    assert_eq!(kv_store.get(&key1)?, None);
    Ok(())
}

// Insert data until total size of the directory decreases.
// Test data correctness after compaction.
#[test]
fn compaction() -> Result<()> {
    // compaction_with_kv_store::<HashStore>()?;
    // compaction_with_kv_store::<SledStore>()?;
    compaction_with_kv_store::<LsmStore>()?;

    Ok(())
}

fn compaction_with_kv_store<T: KVStore>() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut kv_store = T::open(temp_dir.path())?;
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
                         encode_key(value.as_str())?)?
        }

        let new_size = dir_size();
        if new_size > current_size {
            current_size = new_size;
            continue;
        }
        // Compaction triggered.

        drop(kv_store);
        // reopen and check content.
        let kv_store = T::open(temp_dir.path())?;
        for key_id in 0..1000 {
            let key = format!("key{}", key_id);
            assert_eq!(kv_store.get(&encode_key(key.as_str())?)?,
                       Some(encode_key(format!("{}", iter).as_str())?));
        }
        return Ok(());
    }

    panic!("No compaction detected");
}

fn encode_key(key: &str) -> Result<Vec<u8>>{
    Ok(rmp_serde::encode::to_vec(key)?)
}