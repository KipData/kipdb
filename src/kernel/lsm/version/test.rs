use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use tempfile::TempDir;
use tokio::time;
use crate::kernel::io::{FileExtension, IoFactory, IoType};
use crate::kernel::lsm::log::LogLoader;
use crate::kernel::lsm::ss_table::SSTable;
use crate::kernel::lsm::ss_table::sst_meta::SSTableMeta;
use crate::kernel::lsm::storage::Config;
use crate::kernel::lsm::version::{DEFAULT_SS_TABLE_PATH, DEFAULT_VERSION_PATH};
use crate::kernel::lsm::version::Version;
use crate::kernel::lsm::version::version_edit::VersionEdit;
use crate::kernel::lsm::version::version_status::VersionStatus;
use crate::kernel::Result;

#[test]
fn test_version_clean() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    tokio_test::block_on(async move {
        let config = Config::new(temp_dir.into_path());

        let (wal, _, _) = LogLoader::reload(
            config.path(),
            (DEFAULT_VERSION_PATH, Some(1)),
            IoType::Direct,
            |_| Ok(()),
        )?;

        // 注意：将ss_table的创建防止VersionStatus的创建前
        // 因为VersionStatus检测无Log时会扫描当前文件夹下的SSTable进行重组以进行容灾
        let ver_status =
            VersionStatus::load_with_path(config.clone(), wal.clone())?;


        let sst_factory = IoFactory::new(
            config.dir_path.join(DEFAULT_SS_TABLE_PATH),
            FileExtension::SSTable,
        )?;

        let (ss_table_1, scope_1) = SSTable::create_for_mem_table(
            &config,
            1,
            &sst_factory,
            vec![(Bytes::from_static(b"test"), None)],
            0,
            IoType::Direct,
        )?;

        let (ss_table_2, scope_2) = SSTable::create_for_mem_table(
            &config,
            2,
            &sst_factory,
            vec![(Bytes::from_static(b"test"), None)],
            0,
            IoType::Direct,
        )?;

        let meta_1 = SSTableMeta::from(&ss_table_1);
        let meta_2 = SSTableMeta::from(&ss_table_2);

        ver_status.insert_vec_ss_table(vec![ss_table_1])?;
        ver_status.insert_vec_ss_table(vec![ss_table_2])?;

        let vec_edit_1 = vec![
            VersionEdit::NewFile((vec![scope_1], 0), 0, meta_1),
        ];

        ver_status.log_and_apply(vec_edit_1, 2).await?;

        let version_1 = Arc::clone(&ver_status.current().await);

        let vec_edit_2 = vec![
            VersionEdit::NewFile((vec![scope_2.clone()], 0), 0,meta_2),
            VersionEdit::DeleteFile((vec![1], 0),meta_1),
        ];

        ver_status.log_and_apply(vec_edit_2, 2).await?;

        let version_2 = Arc::clone(&ver_status.current().await);

        let vec_edit_3 = vec![
            VersionEdit::DeleteFile((vec![2], 0),meta_2),
        ];

        // 用于去除version2的引用计数
        ver_status.log_and_apply(vec_edit_3, 2).await?;

        // 测试对比快照
        let (_, snapshot, _) = LogLoader::reload(
            config.path(),
            (DEFAULT_VERSION_PATH, None),
            IoType::Direct,
            |bytes| Ok(bincode::deserialize::<Vec<VersionEdit>>(bytes)?),
        )?;

        assert_eq!(
            snapshot,
            vec![
                vec![VersionEdit::NewFile((vec![scope_2], 0), 0, meta_2)],
                vec![VersionEdit::DeleteFile((vec![2], 0), meta_2)],
            ]
        );


        assert!(sst_factory.exists(1)?);
        assert!(sst_factory.exists(2)?);

        drop(version_2);

        assert!(sst_factory.exists(1)?);
        assert!(sst_factory.exists(2)?);

        drop(version_1);
        time::sleep(Duration::from_secs(1)).await;

        assert!(!sst_factory.exists(1)?);
        assert!(sst_factory.exists(2)?);

        drop(ver_status);
        time::sleep(Duration::from_secs(1)).await;

        assert!(!sst_factory.exists(1)?);
        assert!(!sst_factory.exists(2)?);

        Ok(())
    })
}

#[test]
fn test_version_apply_and_log() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    tokio_test::block_on(async move {
        let config = Config::new(temp_dir.into_path());

        let (wal, _, _) = LogLoader::reload(
            config.path(),
            (DEFAULT_VERSION_PATH, Some(1)),
            IoType::Direct,
            |_| Ok(()),
        )?;

        // 注意：将ss_table的创建防止VersionStatus的创建前
        // 因为VersionStatus检测无Log时会扫描当前文件夹下的SSTable进行重组以进行容灾
        let ver_status_1 =
            VersionStatus::load_with_path(config.clone(), wal.clone())?;


        let sst_factory = IoFactory::new(
            config.dir_path.join(DEFAULT_SS_TABLE_PATH),
            FileExtension::SSTable,
        )?;

        let (ss_table_1, scope_1) = SSTable::create_for_mem_table(
            &config,
            1,
            &sst_factory,
            vec![(Bytes::from_static(b"test"), None)],
            0,
            IoType::Direct,
        )?;

        let (ss_table_2, scope_2) = SSTable::create_for_mem_table(
            &config,
            2,
            &sst_factory,
            vec![(Bytes::from_static(b"test"), None)],
            0,
            IoType::Direct,
        )?;

        let vec_edit = vec![
            VersionEdit::NewFile((vec![scope_1], 0), 0, SSTableMeta::from(&ss_table_1)),
            VersionEdit::NewFile((vec![scope_2], 0), 0, SSTableMeta::from(&ss_table_2)),
            VersionEdit::DeleteFile((vec![2], 0), SSTableMeta::from(&ss_table_2)),
        ];

        ver_status_1.insert_vec_ss_table(vec![ss_table_1])?;
        ver_status_1.insert_vec_ss_table(vec![ss_table_2])?;
        ver_status_1.log_and_apply(vec_edit, 10).await?;

        let (ss_table_3, scope_3) = SSTable::create_for_mem_table(
            &config,
            3,
            &sst_factory,
            vec![(Bytes::from_static(b"test3"), None)],
            0,
            IoType::Direct,
        )?;

        let (ss_table_4, scope_4) = SSTable::create_for_mem_table(
            &config,
            4,
            &sst_factory,
            vec![(Bytes::from_static(b"test4"), None)],
            0,
            IoType::Direct,
        )?;

        let vec_edit2 = vec![
            VersionEdit::NewFile((vec![scope_3], 0), 0,SSTableMeta::from(&ss_table_3)),
            VersionEdit::NewFile((vec![scope_4], 0), 0,SSTableMeta::from(&ss_table_4)),
        ];

        ver_status_1.insert_vec_ss_table(vec![ss_table_3])?;
        ver_status_1.insert_vec_ss_table(vec![ss_table_4])?;
        ver_status_1.log_and_apply(vec_edit2, 10).await?;

        let version_1 = Version::clone(ver_status_1.current().await.as_ref());

        drop(ver_status_1);

        let ver_status_2 =
            VersionStatus::load_with_path(config, wal.clone())?;
        let version_2 = ver_status_2.current().await;

        assert_eq!(version_1.level_slice, version_2.level_slice);
        assert_eq!(version_1.meta_data, version_2.meta_data);

        Ok(())
    })
}