use crate::error::ConnectionError;
use crate::proto::kipdb_rpc_client::KipdbRpcClient;
use crate::proto::{
    BatchGetReq, BatchRemoveReq, BatchSetReq, Empty, GetReq, Kv, RemoveReq, SetReq,
};
use tonic::transport::Channel;

pub type Result<T> = std::result::Result<T, ConnectionError>;
type Key = Vec<u8>;
type Value = Vec<u8>;
type KV = (Key, Value);

pub struct KipdbClient {
    conn: KipdbRpcClient<Channel>,
}

impl KipdbClient {
    pub async fn connect(addr: String) -> Result<Self> {
        let conn = KipdbRpcClient::connect(addr).await?;
        Ok(Self { conn })
    }

    #[inline]
    pub async fn set(&mut self, key: Key, value: Value) -> Result<()> {
        let req = tonic::Request::new(SetReq { key, value });
        let resp = self.conn.set(req).await?;
        if resp.into_inner().success {
            Ok(())
        } else {
            Err(ConnectionError::WriteFailed)
        }
    }

    #[inline]
    pub async fn remove(&mut self, key: Key) -> Result<()> {
        let req = tonic::Request::new(RemoveReq { key });
        let resp = self.conn.remove(req).await?;
        if resp.into_inner().success {
            Ok(())
        } else {
            Err(ConnectionError::WriteFailed)
        }
    }

    #[inline]
    pub async fn get(&mut self, key: Key) -> Result<Option<Value>> {
        let req = tonic::Request::new(GetReq { key });
        let resp = self.conn.get(req).await?;
        Ok(resp.into_inner().value)
    }

    #[inline]
    pub async fn batch_set(&mut self, kvs: Vec<KV>) -> Result<Vec<KV>> {
        let req = tonic::Request::new(BatchSetReq {
            kvs: kvs
                .into_iter()
                .map(|(key, value)| Kv { key, value })
                .collect(),
        });
        let resp = self.conn.batch_set(req).await?;
        Ok(resp
            .into_inner()
            .failure
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect())
    }

    #[inline]
    pub async fn batch_remove(&mut self, keys: Vec<Key>) -> Result<Vec<Key>> {
        let req = tonic::Request::new(BatchRemoveReq { keys });
        let resp = self.conn.batch_remove(req).await?;
        Ok(resp.into_inner().failure)
    }

    #[inline]
    pub async fn batch_get(&mut self, keys: Vec<Key>) -> Result<Vec<Value>> {
        let req = tonic::Request::new(BatchGetReq { keys });
        let resp = self.conn.batch_get(req).await?;
        Ok(resp.into_inner().values)
    }

    #[inline]
    pub async fn flush(&mut self) -> Result<()> {
        let req = tonic::Request::new(Empty {});
        let resp = self.conn.flush(req).await?;
        if resp.into_inner().success {
            Ok(())
        } else {
            Err(ConnectionError::FlushError)
        }
    }

    #[inline]
    pub async fn size_of_disk(&mut self) -> Result<u64> {
        let req = tonic::Request::new(Empty {});
        let resp = self.conn.size_of_disk(req).await?;
        Ok(resp.into_inner().size)
    }

    #[inline]
    pub async fn len(&mut self) -> Result<usize> {
        let req = tonic::Request::new(Empty {});
        let resp = self.conn.len(req).await?;
        Ok(resp.into_inner().len as usize)
    }
}
