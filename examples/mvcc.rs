use bytes::Bytes;
use kip_db::kernel::lsm::storage::{Config, KipStorage};
use kip_db::kernel::Storage;
use kip_db::KernelError;
use tempfile::TempDir;

#[tokio::main]
async fn main() -> Result<(), KernelError> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let config = Config::new(temp_dir.into_path()).enable_level_0_memorization();
    let kip_storage = KipStorage::open_with_config(config).await?;

    println!("New Transaction");
    let mut tx = kip_storage.new_transaction().await;

    println!("Set KeyValue after the transaction -> (key_1, value_1)");
    kip_storage
        .set(
            Bytes::copy_from_slice(b"key_1"),
            Bytes::copy_from_slice(b"value_1"),
        )
        .await?;

    println!("Read key_1 on the transaction: {:?}", tx.get(b"key_1")?);

    println!("Set KeyValue on the transaction -> (key_2, value_2)");
    tx.set(b"key_2", Bytes::copy_from_slice(b"value_2"));

    println!("Read key_2 on the transaction: {:?}", tx.get(b"key_2")?);

    println!(
        "Read key_2 on the storage: {:?}",
        kip_storage.get(b"key_2").await?
    );

    println!("Commit this transaction");
    tx.commit().await?;

    println!(
        "Read key_2 on the storage again!: {:?}",
        kip_storage.get(b"key_2").await?
    );

    Ok(())
}
