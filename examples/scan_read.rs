use bytes::Bytes;
use kip_db::kernel::lsm::iterator::Iter;
use kip_db::kernel::lsm::storage::{Config, KipStorage};
use kip_db::kernel::Storage;
use kip_db::KernelError;
use std::collections::Bound;
use tempfile::TempDir;

#[tokio::main]
async fn main() -> Result<(), KernelError> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let config = Config::new(temp_dir.into_path()).enable_level_0_memorization();
    let kip_storage = KipStorage::open_with_config(config).await?;

    println!("Set KeyValue -> (key_1, value_1)");
    kip_storage
        .set(b"key_1", Bytes::copy_from_slice(b"value_1"))
        .await?;
    println!("Set KeyValue -> (key_2, value_2)");
    kip_storage
        .set(b"key_2", Bytes::copy_from_slice(b"value_2"))
        .await?;
    println!("Set KeyValue -> (key_3, value_3)");
    kip_storage
        .set(b"key_3", Bytes::copy_from_slice(b"value_3"))
        .await?;

    println!("New Transaction");
    let tx = kip_storage.new_transaction().await;

    println!("Iter without key_3 By Transaction:");
    let mut iter = tx.iter(Bound::Unbounded, Bound::Excluded(b"key_3"))?;

    while let Some(item) = iter.try_next()? {
        println!("Item: {:?}", item);
    }

    Ok(())
}
