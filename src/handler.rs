use std::sync::Arc;

use axum::{Router, extract::Extension};

use crate::{error::Result, core::kv::KvStore};

// pub fn router() -> Router {
    
// }

pub async fn set(key: &str, Extension(state): Extension<Arc<KvStore>>) {

}

pub async fn remove(key: &str, Extension(state): Extension<Arc<KvStore>>) {

}

// pub async fn get(
//     key: String,
//     value: &str,
//     Extension(kv_store): Extension<Arc<KvStore>>
//     ) -> Option<String> {
//     kv_store.get(key)?
// }