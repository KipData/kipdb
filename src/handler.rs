use std::{sync::{Arc, RwLock}};

use axum::{Router,routing::get, extract::{Extension, Path}, Json};

use crate::{error::Result, core::kv::KvStore};

pub fn router() -> Router {
    Router::new()
        .route("/get/:key", get(get_kv))
        .route("/set/:key/:value", get(set_kv))
}

pub async fn set_kv(
    Extension(kv_store): Extension<Arc<RwLock<KvStore>>>,
    Path((key, value)): Path<(String, String)>
) -> Result<Json<String>> {
    kv_store.write().unwrap().set(key, value)?;
    Ok(Json("Done".to_string()))
}

// pub async fn remove(key: &str, Extension(state): Extension<Arc<KvStore>>) {

// }

pub async fn get_kv(
    Extension(kv_store): Extension<Arc<RwLock<KvStore>>>,
    Path(key): Path<String>
    ) -> Result<Json<String>> {
    if let Some(value) = kv_store.read()
                                    .unwrap()
                                    .get(key)? {
        return Ok(Json(value));
    } else {
        return Ok(Json("".to_string()));
    }
}