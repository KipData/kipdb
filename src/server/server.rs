use crate::error::ConnectionError;
use crate::kernel::lsm::storage::KipStorage;
use crate::kernel::Storage;
use crate::proto::kipdb_rpc_server::{KipdbRpc, KipdbRpcServer};
use crate::proto::{
    BatchGetReq, BatchGetResp, BatchRemoveReq, BatchRemoveResp, BatchSetReq, BatchSetResp, Empty,
    FlushResp, GetReq, GetResp, LenResp, RemoveReq, RemoveResp, SetReq, SetResp, SizeOfDiskResp,
};
use bytes::Bytes;
use std::sync::Arc;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub async fn serve(ip: &String, port: u16) -> Result<(), ConnectionError> {
    let addr = format!("{}:{}", ip, port).parse()?;
    let kv_store = Arc::new(KipStorage::open("./data").await?);
    let kipdb_server = KipdbServer::new(kv_store);
    Server::builder()
        .add_service(KipdbRpcServer::new(kipdb_server))
        .serve(addr)
        .await?;
    Ok(())
}

struct KipdbServer {
    kv_store: Arc<KipStorage>,
}
impl KipdbServer {
    pub fn new(kv_store: Arc<KipStorage>) -> Self {
        Self { kv_store }
    }
}

#[tonic::async_trait]
impl KipdbRpc for KipdbServer {
    async fn set(&self, request: Request<SetReq>) -> Result<Response<SetResp>, Status> {
        let req = request.into_inner();
        let success = self
            .kv_store
            .set(Bytes::from(req.key), Bytes::from(req.value))
            .await
            .map_or(false, |_| true);
        Ok(Response::new(SetResp { success }))
    }

    async fn remove(&self, request: Request<RemoveReq>) -> Result<Response<RemoveResp>, Status> {
        let req = request.into_inner();
        let success = self
            .kv_store
            .remove(req.key.as_slice())
            .await
            .map_or(false, |_| true);
        Ok(Response::new(RemoveResp { success }))
    }

    async fn get(&self, request: Request<GetReq>) -> Result<Response<GetResp>, Status> {
        let req = request.into_inner();
        let value = self
            .kv_store
            .get(req.key.as_slice())
            .await
            .map_or(None, |v| v);
        Ok(Response::new(GetResp {
            value: value.map(|v| v.to_vec()),
        }))
    }

    async fn batch_set(
        &self,
        request: Request<BatchSetReq>,
    ) -> Result<Response<BatchSetResp>, Status> {
        let req = request.into_inner();
        let mut failure = Vec::new();
        // TODO change kv_store.set return type for parallel processing
        for kv in req.kvs {
            let success = self
                .kv_store
                .set(Bytes::from(kv.key.clone()), Bytes::from(kv.value.clone()))
                .await
                .map_or(false, |_| true);
            if !success {
                failure.push(kv);
            }
        }
        Ok(Response::new(BatchSetResp { failure }))
    }

    async fn batch_remove(
        &self,
        request: Request<BatchRemoveReq>,
    ) -> Result<Response<BatchRemoveResp>, Status> {
        let req = request.into_inner();
        let mut failure = Vec::new();
        // TODO change kv_store.remove return type for parallel processing
        for key in req.keys {
            let success = self
                .kv_store
                .remove(key.as_slice())
                .await
                .map_or(false, |_| true);
            if !success {
                failure.push(key);
            }
        }
        Ok(Response::new(BatchRemoveResp { failure }))
    }

    async fn batch_get(
        &self,
        request: Request<BatchGetReq>,
    ) -> Result<Response<BatchGetResp>, Status> {
        let req = request.into_inner();
        let mut values = Vec::new();
        // TODO change kv_store.get return type for parallel processing
        for key in req.keys {
            values.push(
                self.kv_store
                    .get(key.as_slice())
                    .await
                    .map_or(None, |v| v)
                    .map_or(vec![], |v| v.to_vec()),
            );
        }
        Ok(Response::new(BatchGetResp { values }))
    }

    async fn size_of_disk(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<SizeOfDiskResp>, Status> {
        match self.kv_store.size_of_disk().await {
            Ok(size) => Ok(Response::new(SizeOfDiskResp { size })),
            Err(_) => Err(Status::internal("Failed to get size of disk")),
        }
    }

    async fn len(&self, _request: Request<Empty>) -> Result<Response<LenResp>, Status> {
        match self.kv_store.len().await {
            Ok(len) => Ok(Response::new(LenResp { len: len as u64 })),
            Err(_) => Err(Status::internal("Failed to get len")),
        }
    }

    async fn flush(&self, _request: Request<Empty>) -> Result<Response<FlushResp>, Status> {
        let success = self.kv_store.flush().await.map_or(false, |_| true);
        Ok(Response::new(FlushResp { success }))
    }
}
