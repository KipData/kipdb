use std::sync::{Arc, RwLock};

use axum::{Router, extract::Extension, Server};
use kvs::{KvStore, handler};

#[tokio::main]
async fn main() {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "axum_rs_blog=debug");
    }
    tracing_subscriber::fmt::init();

    tracing::info!("服务已启动");

    let kv_store = KvStore::open("./data")
        .expect("初始化存储引擎失败");

    let app = Router::new()
        .nest("/", handler::router())
        .layer(Extension(Arc::new(RwLock::new(kv_store))));

    Server::bind(&"127.0.0.1:7036".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

