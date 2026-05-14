use crate::abi::{deserialize_result, serialize_args};
use crate::metadata::UdfMetadata;
use datatypes::Value;
use flow::expr::custom_func::CustomFunc;
use flow::expr::func::EvalError;
use std::sync::Arc;
use wasmtime::{Engine, Linker, Memory, Module, Store, TypedFunc};

/// A user-defined function backed by a WASM module.
///
/// Implements `flow::CustomFunc` so it can be registered in `CustomFuncRegistry`
/// and used in SQL expressions like any built-in function.
pub struct WasmUdf {
    name: String,
    metadata: UdfMetadata,
    /// Wasmtime engine (shared across all UDFs).
    engine: Engine,
    /// Compiled WASM module (cheap to instantiate per-call).
    module: Module,
    /// Host function linker (shared, pre-seeded with host functions).
    linker: Arc<Linker<()>>,
    /// Fuel budget per `eval_row` call.
    fuel_per_call: u64,
}

impl std::fmt::Debug for WasmUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmUdf")
            .field("name", &self.name)
            .field("metadata", &self.metadata)
            .field("fuel_per_call", &self.fuel_per_call)
            .finish_non_exhaustive()
    }
}

impl WasmUdf {
    /// Default fuel budget per `eval_row` call.
    pub const DEFAULT_FUEL_PER_CALL: u64 = 100_000;

    /// Create a new WasmUdf.
    pub(crate) fn new(
        name: String,
        metadata: UdfMetadata,
        engine: Engine,
        module: Module,
        linker: Arc<Linker<()>>,
    ) -> Result<Self, super::engine::WasmError> {
        Ok(Self {
            name,
            metadata,
            engine,
            module,
            linker,
            fuel_per_call: Self::DEFAULT_FUEL_PER_CALL,
        })
    }

    /// Set the fuel budget per row evaluation.
    #[allow(dead_code)]
    pub fn with_fuel_per_call(mut self, fuel: u64) -> Self {
        self.fuel_per_call = fuel;
        self
    }

    /// Bump-allocate `size` bytes in WASM linear memory.
    /// Returns the offset (pointer) to the allocated block.
    /// Mirrors the `host::alloc` function registered in the linker.
    fn wasm_alloc(memory: &Memory, store: &mut Store<()>, size: i32) -> Result<i32, EvalError> {
        if size <= 0 {
            return Ok(0);
        }
        let size = size as u32;
        const HEAP_START: u32 = 4;
        const PAGE_SIZE: u64 = 65536;
        const MAX_PAGES: u64 = 256;

        // Read current bump pointer
        let mut bump_bytes = [0u8; 4];
        memory
            .read(&*store, 0, &mut bump_bytes)
            .map_err(|e| EvalError::NotImplemented {
                feature: format!("read bump ptr: {e}"),
            })?;
        let mut bump = u32::from_le_bytes(bump_bytes);

        if bump == 0 {
            bump = HEAP_START;
        }

        // Align to 4 bytes
        let aligned = (bump + 3) & !3u32;
        let new_bump = aligned + size;

        // Check if we need to grow memory
        let current_pages = memory.size(&*store);
        let needed_bytes = new_bump as u64;
        let current_bytes = current_pages * PAGE_SIZE;
        if needed_bytes > current_bytes {
            let additional = needed_bytes - current_bytes;
            let pages_needed = additional.div_ceil(PAGE_SIZE);
            let new_pages = current_pages + pages_needed;
            if new_pages > MAX_PAGES {
                return Err(EvalError::NotImplemented {
                    feature: format!("exceeded max memory ({new_pages} > {MAX_PAGES} pages)"),
                });
            }
            memory
                .grow(&mut *store, pages_needed)
                .map_err(|e| EvalError::NotImplemented {
                    feature: format!("grow memory: {e}"),
                })?;
        }

        // Write back new bump pointer
        let new_bump_bytes = new_bump.to_le_bytes();
        memory
            .write(&mut *store, 0, &new_bump_bytes)
            .map_err(|e| EvalError::NotImplemented {
                feature: format!("write bump ptr: {e}"),
            })?;

        Ok(aligned as i32)
    }
}

impl CustomFunc for WasmUdf {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        let expected = self.metadata.args.len();
        let actual = args.len();
        if expected > 0 && actual != expected {
            return Err(EvalError::TypeMismatch {
                expected: format!("{} arguments", expected),
                actual: format!("{} arguments", actual),
            });
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        // NOTE: This creates a new Store + instantiates the WASM module on every
        // row evaluation. This is orders of magnitude more expensive than the
        // built-in scalar function path. For the initial MVP this is acceptable;
        // a follow-up must implement InstancePre caching or a Store/Instance pool
        // before WASM UDFs can be treated as production-ready for streaming
        // workloads. See docs/integrations/wasm-udf.md#performance.
        let mut store = Store::new(&self.engine, ());
        store
            .set_fuel(self.fuel_per_call)
            .map_err(|e| EvalError::NotImplemented {
                feature: format!("fuel budget: {e}"),
            })?;

        // Instantiate the module into this store
        let instance = self
            .linker
            .instantiate(&mut store, &self.module)
            .map_err(|e| EvalError::NotImplemented {
                feature: format!("instantiation: {e}"),
            })?;

        let memory =
            instance
                .get_memory(&mut store, "memory")
                .ok_or_else(|| EvalError::NotImplemented {
                    feature: "missing WASM memory export".to_string(),
                })?;

        let udf_row: TypedFunc<(i32, i32), i32> = instance
            .get_typed_func(&mut store, "udf_row")
            .map_err(|e| EvalError::NotImplemented {
                feature: format!("udf_row function: {e}"),
            })?;

        // Serialize arguments to JSON
        let input_json = serialize_args(args).map_err(|e| EvalError::TypeMismatch {
            expected: "serializable arguments".to_string(),
            actual: e.to_string(),
        })?;

        let input_len = input_json.len() as i32;

        // Allocate memory in WASM via direct bump allocator
        let input_ptr = Self::wasm_alloc(&memory, &mut store, input_len)?;
        if input_ptr == 0 && input_len > 0 {
            return Err(EvalError::NotImplemented {
                feature: "alloc returned null".to_string(),
            });
        }

        // Write input JSON to WASM memory
        memory
            .write(&mut store, input_ptr as usize, &input_json)
            .map_err(|e| EvalError::NotImplemented {
                feature: format!("failed to write input to WASM memory: {e}"),
            })?;

        // Call the UDF
        let result_ptr = udf_row
            .call(&mut store, (input_ptr, input_len))
            .map_err(|e| {
                if store.get_fuel().unwrap_or(0) == 0 {
                    EvalError::NotImplemented {
                        feature: "UDF exceeded fuel budget (timeout)".to_string(),
                    }
                } else {
                    EvalError::NotImplemented {
                        feature: format!("UDF evaluation failed: {e}"),
                    }
                }
            })?;

        if result_ptr == 0 {
            return Err(EvalError::NotImplemented {
                feature: "udf_row returned null".to_string(),
            });
        }

        // Result protocol: the UDF allocates a buffer via host.alloc,
        // writes the length (i32 LE) at offset 0, writes the JSON data at offset 4,
        // and returns pointer to the data (buffer + 4).
        let len_offset = (result_ptr as usize).saturating_sub(4);
        let mut len_bytes = [0u8; 4];
        memory
            .read(&store, len_offset, &mut len_bytes)
            .map_err(|e| EvalError::NotImplemented {
                feature: format!("failed to read result length: {e}"),
            })?;
        let result_len = i32::from_le_bytes(len_bytes);

        if result_len <= 0 || result_len > 1024 * 1024 {
            return Err(EvalError::TypeMismatch {
                expected: "result length in (0, 1MB]".to_string(),
                actual: format!("result length: {result_len}"),
            });
        }

        let mut result_buf = vec![0u8; result_len as usize];
        memory
            .read(&store, result_ptr as usize, &mut result_buf)
            .map_err(|e| EvalError::NotImplemented {
                feature: format!("failed to read result: {e}"),
            })?;

        deserialize_result(&result_buf).map_err(|e| EvalError::TypeMismatch {
            expected: "valid JSON result".to_string(),
            actual: e.to_string(),
        })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::WasmEngine;
    use datatypes::Value;

    /// A minimal WASM module that properly implements the UDF protocol.
    /// It returns a JSON-encoded copy of the first argument (identity).
    fn identity_wasm_module() -> Vec<u8> {
        wat::parse_str(
            r#"
            (module
                (import "host" "alloc" (func $alloc (param i32) (result i32)))

                (memory (export "memory") 1 256)
                (data (i32.const 0) "\00\00\00\00")

                ;; Metadata JSON at offset 128 (exactly 118 bytes)
                (data (i32.const 128) "{\"name\":\"test_udf\",\"description\":\"identity\",\"args\":[{\"name\":\"x\",\"type\":\"any\"}],\"return_type\":\"any\",\"batch_mode\":false}")

                (func (export "udf_metadata_ptr") (result i32)
                    i32.const 128)
                (func (export "udf_metadata_len") (result i32)
                    i32.const 118)

                ;; udf_row: allocates output, writes length header + copies data
                (func (export "udf_row") (param $input_ptr i32) (param $input_len i32) (result i32)
                    (local $buf i32)
                    local.get $input_len
                    i32.const 4
                    i32.add
                    call $alloc
                    local.set $buf

                    local.get $buf
                    local.get $input_len
                    i32.store

                    local.get $buf
                    i32.const 4
                    i32.add
                    local.get $input_ptr
                    local.get $input_len
                    memory.copy

                    local.get $buf
                    i32.const 4
                    i32.add)
            )
            "#,
        )
        .expect("wat parse")
        .into()
    }

    #[test]
    fn identity_udf_int64() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = identity_wasm_module();
        let udf = engine.instantiate("test_udf", &wasm).expect("instantiate");

        let input = vec![Value::Int64(42)];
        let result = udf.eval_row(&input).expect("eval_row");
        assert_eq!(result, Value::Int64(42));
    }

    #[test]
    fn identity_udf_string() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = identity_wasm_module();
        let udf = engine.instantiate("test_udf", &wasm).expect("instantiate");

        let input = vec![Value::String("hello wasm".to_string())];
        let result = udf.eval_row(&input).expect("eval_row");
        assert_eq!(result, Value::String("hello wasm".to_string()));
    }

    #[test]
    fn identity_udf_float64() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = identity_wasm_module();
        let udf = engine.instantiate("test_udf", &wasm).expect("instantiate");

        let input = vec![Value::Float64(3.14)];
        let result = udf.eval_row(&input).expect("eval_row");
        assert_eq!(result, Value::Float64(3.14));
    }

    #[test]
    fn identity_udf_bool() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = identity_wasm_module();
        let udf = engine.instantiate("test_udf", &wasm).expect("instantiate");

        let input = vec![Value::Bool(true)];
        let result = udf.eval_row(&input).expect("eval_row");
        assert_eq!(result, Value::Bool(true));

        let input = vec![Value::Bool(false)];
        let result = udf.eval_row(&input).expect("eval_row");
        assert_eq!(result, Value::Bool(false));
    }

    // ── multiple calls, no state leak ──────────────────────────────

    #[test]
    fn multiple_calls_no_state_leak() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = identity_wasm_module();
        let udf = engine.instantiate("test_udf", &wasm).expect("instantiate");

        for i in 0..10 {
            let input = vec![Value::Int64(i)];
            let result = udf.eval_row(&input).expect("eval_row");
            assert_eq!(result, Value::Int64(i), "failed at iteration {i}");
        }
    }

    // ── large data (triggers memory growth) ───────────────────────

    #[test]
    fn large_string_roundtrip() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = identity_wasm_module();
        let udf = engine.instantiate("test_udf", &wasm).expect("instantiate");

        // 10 KB string — one WASM page is 64 KB, but input + output + allocator
        // bookkeeping should still fit in one page.
        let big = "x".repeat(10_000);
        let input = vec![Value::String(big.clone())];
        let result = udf.eval_row(&input).expect("eval_row");
        assert_eq!(result, Value::String(big));
    }

    // ── validate_row happy path ────────────────────────────────────

    #[test]
    fn validate_row_accepts_correct_arity() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = identity_wasm_module();
        let udf = engine.instantiate("test_udf", &wasm).expect("instantiate");

        // Metadata declares 1 arg; passing exactly 1 arg should pass.
        udf.validate_row(&[Value::Int64(1)])
            .expect("1 arg should be accepted");
    }
}
