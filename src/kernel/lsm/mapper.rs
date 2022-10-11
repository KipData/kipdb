use std::{sync::Arc};
use std::path::PathBuf;

use futures::future;
use itertools::Itertools;

use super::{lsm_kv::{Config, LsmStore}};
use crate::kernel::{Result, CommandData, io_handler::{IOHandler, IOHandlerFactory}, CommandPackage};

pub struct Mapper {
    io_handler_factory: IOHandlerFactory,
    config: Arc<Config>,
    vec_result_io_handler: Vec<IOHandler>,
}

impl Mapper {
    pub async fn map<F>(self, match_func: F) -> Result<Self> where F: Fn(&CommandData) -> bool {
        let map_fork_futures = self.vec_result_io_handler.iter()
                    .map(|io_handler| {
                        Self::map_fork(&self.io_handler_factory, &self.config, io_handler, &match_func)
                    });
        let vec_result_io_handler= future::try_join_all(map_fork_futures).await?;
        
        match self {
            Mapper { io_handler_factory, config, .. } => {
                Ok(Mapper { io_handler_factory, config, vec_result_io_handler  })
            },
        }
    }

    pub async fn reduce(&self) -> Result<Vec<CommandData>> {
        let reduce_futures = self.vec_result_io_handler.iter()
                    .map(|io_handler| CommandPackage::from_read_to_vec(io_handler));

        Ok(future::try_join_all(reduce_futures).await?
        .into_iter()
        .flatten()
        .map(CommandPackage::unpack)
        .collect())
    }

    pub async fn from_lsm_kv<F>(lsm_kv: &LsmStore, match_func: F, path: impl Into<PathBuf> + Send) -> Result<Mapper> where F: Fn(&CommandData) -> bool {
        let io_handler_factory = IOHandlerFactory::new(path);
        let config = Arc::clone(&lsm_kv.config());

        let manifest = lsm_kv.manifest().read().await;
        
        let map_fork_futures = manifest.ss_tables_map.iter()
                    .map(|(_, ss_table)| {
                        Self::map_fork(&io_handler_factory, &config, &ss_table.get_io_handler(), &match_func)
                    });
        let vec_result_io_handler= future::try_join_all(map_fork_futures).await?;
        
        Ok(Mapper{ io_handler_factory, config, vec_result_io_handler })
    } 

    pub(crate) async fn map_fork<F>(io_handler_factory: &IOHandlerFactory, config: &Arc<Config>, io_handler: &IOHandler, match_func: F) -> Result<IOHandler> where F: Fn(&CommandData) -> bool {
        let match_vec = CommandPackage::from_read_to_vec(io_handler).await?
            .into_iter()
            .map(CommandPackage::unpack)
            .filter(match_func)
            .collect_vec();
            
        let tmp_gen = config.create_gen();
        let io_handler = io_handler_factory.create(tmp_gen)?;
        
        CommandPackage::write_batch_first_pos(&io_handler, &match_vec).await?;
        
        Ok(io_handler)
    }
}