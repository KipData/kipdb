use axum::Router;
use kvs::KvStore;

#[tokio::main]
async fn main() {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "axum_rs_blog=debug");
    }
    tracing_subscriber::fmt::init();

    tracing::info!("服务已启动");

    let kv_store = KvStore::open("./data")
        .expect("初始化存储引擎失败");


}