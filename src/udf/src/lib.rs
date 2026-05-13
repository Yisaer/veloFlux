#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::unreachable, clippy::panic)
)]
#![forbid(unsafe_code)]

pub mod abi;
pub mod engine;
pub mod func;
pub mod metadata;

pub use abi::AbiError;
pub use engine::{WasmEngine, WasmError};
pub use func::WasmUdf;
pub use metadata::{UdfArg, UdfMetadata};
