//! veloFlux UDF SDK — helper library for writing WASM UDFs in Rust.
//!
//! # Quick start
//!
//! Add this crate as a dependency, then use [`define_udf!`]:
//!
//! ```ignore
//! use veloflux_udf_sdk::define_udf;
//!
//! define_udf! {
//!     name = "my_udf",
//!     description = "Multiplies sensor value by threshold",
//!     return_type = "float",
//!     fn my_udf(sensor_value: f64, threshold: f64) -> f64 {
//!         sensor_value * threshold
//!     }
//! }
//! ```
//!
//! Compile with:
//! ```text
//! cargo build --target wasm32-unknown-unknown --release
//! ```
//!
//! The generated `.wasm` file is ready to upload to veloFlux.

// WASM is single-threaded; mutable references to `static mut` are safe.
#![allow(static_mut_refs)]

// ---------------------------------------------------------------------------
// Allocator
// ---------------------------------------------------------------------------

/// Bump allocator offset stored at `MEMORY[0..4]`.
const HEAP_START: usize = 4;

/// WASM linear memory. The first 4 bytes hold the bump pointer.
static mut MEMORY: [u8; 65536] = [0u8; 65536];

/// Internal bump allocator.
///
/// Allocates `size` bytes from the static `MEMORY` buffer, returning a raw
/// pointer into `MEMORY`. Never frees — the host resets offset 0 between calls.
pub fn udf_alloc(size: usize) -> *mut u8 {
    let bump = u32::from_le_bytes([
        unsafe { MEMORY[0] },
        unsafe { MEMORY[1] },
        unsafe { MEMORY[2] },
        unsafe { MEMORY[3] },
    ]) as usize;

    let current = if bump == 0 { HEAP_START } else { bump };
    let aligned = (current + 3) & !3;
    let new_bump = aligned + size;

    let bytes = (new_bump as u32).to_le_bytes();
    unsafe {
        MEMORY[0] = bytes[0];
        MEMORY[1] = bytes[1];
        MEMORY[2] = bytes[2];
        MEMORY[3] = bytes[3];
    }

    unsafe { MEMORY.as_mut_ptr().add(aligned) }
}

/// Allocate a length-prefixed result buffer.
///
/// Layout: `[result_len: i32 LE (4 bytes)][data: result_len bytes]`.
/// Returns pointer to `data` (past the 4-byte header).
pub fn alloc_result(data: &[u8]) -> *mut u8 {
    let total = 4 + data.len();
    let buf = udf_alloc(total);
    let len_bytes = (data.len() as u32).to_le_bytes();
    unsafe {
        buf.write(len_bytes[0]);
        buf.add(1).write(len_bytes[1]);
        buf.add(2).write(len_bytes[2]);
        buf.add(3).write(len_bytes[3]);
        std::ptr::copy_nonoverlapping(data.as_ptr(), buf.add(4), data.len());
    }
    unsafe { buf.add(4) }
}

// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

/// Map Rust type names to veloFlux type name strings.
pub fn type_name<T>() -> &'static str {
    let name = std::any::type_name::<T>();
    if name.contains("f64") || name.contains("f32") {
        "float"
    } else if name.contains("i64")
        || name.contains("i32")
        || name.contains("i16")
        || name.contains("i8")
        || name.contains("u64")
        || name.contains("u32")
        || name.contains("u16")
        || name.contains("u8")
    {
        "int"
    } else if name.contains("String") || name.contains("str") {
        "string"
    } else if name.contains("bool") {
        "bool"
    } else {
        "any"
    }
}

// Re-export for macro-internal use.
#[doc(hidden)]
pub use serde;
#[doc(hidden)]
pub use serde_json;
#[doc(hidden)]
pub use std;

// ---------------------------------------------------------------------------
// The define_udf! macro
// ---------------------------------------------------------------------------

/// Generate all WASM exports for a user-defined function.
///
/// Accepts:
/// - `name = "..."`      — canonical function name
/// - `description = "..."` — human-readable description
/// - `return_type = "..."` — "int", "float", "string", "bool", "any"
/// - `fn function_name(arg: Type, ...) -> ReturnType { body }`
#[macro_export]
macro_rules! define_udf {
    (
        name = $name:expr,
        description = $desc:expr,
        return_type = $ret:expr,
        fn $fn_name:ident($($arg_name:ident : $arg_ty:ty),* $(,)?) -> $out_ty:ty $body:block
    ) => {
        // ── user function ────────────────────────────────────────
        fn $fn_name($($arg_name: $arg_ty),*) -> $out_ty {
            $body
        }

        // ── metadata (lazily serialized JSON) ────────────────────
        static UDF_META_JSON: $crate::std::sync::LazyLock<Vec<u8>> =
            $crate::std::sync::LazyLock::new(|| {
                let mut args = Vec::new();
                $(args.push($crate::serde_json::json!({
                    "name": stringify!($arg_name),
                    "type": $crate::type_name::<$arg_ty>(),
                }));)*
                let meta = $crate::serde_json::json!({
                    "abi_version": 1,
                    "name": $name,
                    "description": $desc,
                    "args": args,
                    "return_type": $ret,
                    "execution_mode": "batch",
                });
                $crate::serde_json::to_vec(&meta).unwrap()
            });

        #[no_mangle]
        pub extern "C" fn udf_metadata_ptr() -> i32 {
            UDF_META_JSON.as_ptr() as i32
        }

        #[no_mangle]
        pub extern "C" fn udf_metadata_len() -> i32 {
            UDF_META_JSON.len() as i32
        }

        // ── memory export (for host allocator) ───────────────────
        #[no_mangle]
        pub static mut memory: [u8; 65536] = [0u8; 65536];

        // ── udf_batch entry point ─────────────────────────────────
        #[no_mangle]
        pub extern "C" fn udf_batch(input_ptr: i32, input_len: i32) -> i32 {
            let __error = |message: String| {
                let json = $crate::serde_json::json!({
                    "error": {
                        "code": "invalid_argument",
                        "message": message,
                    }
                });
                let bytes = $crate::serde_json::to_vec(&json)
                    .expect("WASM UDF error envelope must serialize");
                $crate::alloc_result(&bytes) as i32
            };

            if input_ptr < 0 || input_len < 0 {
                return __error(format!(
                    "invalid input pointer or length: ptr={input_ptr}, len={input_len}"
                ));
            }
            let input = unsafe {
                $crate::std::slice::from_raw_parts(
                    input_ptr as *const u8,
                    input_len as usize,
                )
            };
            let envelope: $crate::serde_json::Value = match $crate::serde_json::from_slice(input) {
                Ok(envelope) => envelope,
                Err(error) => return __error(format!("invalid input JSON: {error}")),
            };
            let row_count = match envelope
                .get("row_count")
                .and_then($crate::serde_json::Value::as_u64)
                .and_then(|count| $crate::std::convert::TryFrom::try_from(count).ok())
            {
                Some(row_count) => row_count,
                None => {
                    return __error("input row_count must be a non-negative integer".to_string())
                }
            };
            let args = match envelope
                .get("args")
                .and_then($crate::serde_json::Value::as_array)
            {
                Some(args) => args,
                None => return __error("input args must be an array of columns".to_string()),
            };
            let __expected_args = 0usize $(+ { let _ = stringify!($arg_name); 1usize })*;
            if args.len() != __expected_args {
                return __error(format!(
                    "expected {__expected_args} argument columns, got {}",
                    args.len()
                ));
            }
            for (index, column) in args.iter().enumerate() {
                let column = match column.as_array() {
                    Some(column) => column,
                    None => return __error(format!("argument column {index} must be an array")),
                };
                if column.len() != row_count {
                    return __error(format!(
                        "argument column {index} has {} values, expected {row_count}",
                        column.len()
                    ));
                }
            }
            let mut results = Vec::with_capacity(row_count);
            for __row in 0..row_count {
                let mut __idx = 0usize;
                let __row_result = (|| -> Result<$out_ty, String> {
                    $(
                        let $arg_name: $arg_ty = match $crate::serde_json::from_value(
                            args[__idx].as_array().expect("validated argument column")[__row].clone(),
                        ) {
                            Ok(value) => value,
                            Err(error) => return Err(format!(
                                "argument `{}` at row {__row} has an incompatible type: {error}",
                                stringify!($arg_name)
                            )),
                        };
                        __idx += 1;
                    )*
                    Ok($fn_name($($arg_name),*))
                })();
                match __row_result {
                    Ok(value) => results.push($crate::serde_json::json!({ "value": value })),
                    Err(error) => results.push($crate::serde_json::json!({
                        "error": {
                            "code": "invalid_argument",
                            "message": error,
                        }
                    })),
                }
            }
            let json = match $crate::serde_json::to_vec(
                &$crate::serde_json::json!({ "results": results }),
            ) {
                Ok(json) => json,
                Err(error) => return __error(format!("failed to serialize UDF result: {error}")),
            };
            $crate::alloc_result(&json) as i32
        }
    };
}
