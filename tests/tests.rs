use tempfile::TempDir;
use walkdir::WalkDir;
use kip_db::core::hash_kv::HashStore;
use kip_db::core::KVStore;
use kip_db::core::Result;
use kip_db::core::sled_kv::SledStore;

#[test]
fn get_stored_value() -> Result<()> {
    get_stored_value_with_kv_store::<HashStore>()?;
    get_stored_value_with_kv_store::<SledStore>()?;
    Ok(())
}

fn get_stored_value_with_kv_store<T: KVStore>() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut kv_store = T::open(temp_dir.path())?;
    kv_store.set("key1".to_owned(), "value1".to_owned())?;
    kv_store.set("key2".to_owned(), "value2".to_owned())?;

    kv_store.get("key1".to_owned())?;
    kv_store.get("key2".to_owned())?;
    // Open from disk again and check persistent data.
    drop(kv_store);
    let kv_store = T::open(temp_dir.path())?;
    assert_eq!(kv_store.get("key1".to_owned())?, Some("value1".to_owned()));
    assert_eq!(kv_store.get("key2".to_owned())?, Some("value2".to_owned()));

    Ok(())
}

// Should overwrite existent value.
#[test]
fn overwrite_value() -> Result<()> {
    overwrite_value_with_kv_store::<HashStore>()?;
    overwrite_value_with_kv_store::<SledStore>()?;

    Ok(())
}

fn overwrite_value_with_kv_store<T: KVStore>() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut kv_store = T::open(temp_dir.path())?;

    kv_store.set("key1".to_owned(), "value1".to_owned())?;
    assert_eq!(kv_store.get("key1".to_owned())?, Some("value1".to_owned()));
    kv_store.set("key1".to_owned(), "value2".to_owned())?;
    assert_eq!(kv_store.get("key1".to_owned())?, Some("value2".to_owned()));

    // Open from disk again and check persistent data.
    drop(kv_store);
    let mut kv_store = T::open(temp_dir.path())?;
    assert_eq!(kv_store.get("key1".to_owned())?, Some("value2".to_owned()));
    kv_store.set("key1".to_owned(), "value3".to_owned())?;
    assert_eq!(kv_store.get("key1".to_owned())?, Some("value3".to_owned()));

    Ok(())
}

// Should get `None` when getting a non-existent key.
#[test]
fn get_non_existent_value() -> Result<()> {
    get_non_existent_value_with_kv_store::<HashStore>()?;
    get_non_existent_value_with_kv_store::<SledStore>()?;

    Ok(())
}

fn get_non_existent_value_with_kv_store<T: KVStore>() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut kv_store = T::open(temp_dir.path())?;

    kv_store.set("key1".to_owned(), "value1".to_owned())?;
    assert_eq!(kv_store.get("key2".to_owned())?, None);

    // Open from disk again and check persistent data.
    drop(kv_store);
    let kv_store = T::open(temp_dir.path())?;
    assert_eq!(kv_store.get("key2".to_owned())?, None);

    Ok(())
}

#[test]
fn remove_non_existent_key() -> Result<()> {
    remove_non_existent_key_with_kv_store::<HashStore>()?;
    remove_non_existent_key_with_kv_store::<SledStore>()?;

    Ok(())
}
fn remove_non_existent_key_with_kv_store<T: KVStore>() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut kv_store = T::open(temp_dir.path())?;
    assert!(kv_store.remove("key1".to_owned()).is_err());
    Ok(())
}

#[test]
fn remove_key() -> Result<()> {
    remove_key_with_kv_store::<HashStore>()?;
    remove_key_with_kv_store::<SledStore>()?;

    Ok(())
}

fn remove_key_with_kv_store<T: KVStore>() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut kv_store = T::open(temp_dir.path())?;
    kv_store.set("key1".to_owned(), "value1".to_owned())?;
    assert!(kv_store.remove("key1".to_owned()).is_ok());
    assert_eq!(kv_store.get("key1".to_owned())?, None);
    Ok(())
}

// Insert data until total size of the directory decreases.
// Test data correctness after compaction.
#[test]
fn compaction() -> Result<()> {
    compaction_with_kv_store::<HashStore>()?;
    compaction_with_kv_store::<SledStore>()?;

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
            kv_store.set(key, value)?
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
            assert_eq!(kv_store.get(key)?, Some(format!("{}", iter)));
        }
        return Ok(());
    }

    panic!("No compaction detected");
}