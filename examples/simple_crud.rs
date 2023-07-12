use bytes::Bytes;
use kip_db::kernel::lsm::storage::{Config, KipStorage};
use kip_db::kernel::{CommandData, Storage};
use kip_db::KernelError;
use tempfile::TempDir;

#[tokio::main]
async fn main() -> Result<(), KernelError> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let config = Config::new(temp_dir.into_path()).enable_level_0_memorization();
    let kip_storage = KipStorage::open_with_config(config).await?;

    println!("Set KeyValue -> (apple, banana)");
    kip_storage
        .set(b"apple", Bytes::copy_from_slice(b"banana"))
        .await?;

    println!(
        "Get Key: apple -> Value: {:?}",
        kip_storage.get(b"apple").await?
    );
    println!("SizeOfDisk: {}", kip_storage.size_of_disk().await?);
    println!("Len: {}", kip_storage.len().await?);
    println!("IsEmpty: {}", kip_storage.is_empty().await);

    kip_storage.flush().await?;

    let join_cmd_1 = vec![
        CommandData::set(b"moon".to_vec(), b"star".to_vec()),
        // CommandData::remove(b"apple".to_vec()),
    ];
    println!(
        "Join 1: {:?} -> {:?}",
        join_cmd_1.clone(),
        kip_storage.join(join_cmd_1).await?
    );
    let join_cmd_2 = vec![
        CommandData::get(b"moon".to_vec()),
        CommandData::get(b"apple".to_vec()),
    ];
    println!(
        "Join 2: {:?} -> {:?}",
        join_cmd_2.clone(),
        kip_storage.join(join_cmd_2).await?
    );

    Ok(())
}
