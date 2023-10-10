#![feature(type_ascription)]
#![feature(fs_try_exists)]
#![feature(cursor_remaining)]
#![feature(result_flattening)]
#![feature(buf_read_has_data_left)]
#![feature(slice_pattern)]
#![feature(bound_map)]
pub mod cmd;
pub mod config;
pub mod error;
pub mod kernel;
pub mod net;
pub mod proto;

pub use error::KernelError;

pub const DEFAULT_PORT: u16 = 6333;

pub const LOCAL_IP: &str = "127.0.0.1";
