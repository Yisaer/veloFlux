use crate::abi::{deserialize_batch_result, serialize_batch_args};
use crate::engine::{MAX_PAGES, PAGE_SIZE};
use crate::metadata::UdfMetadata;
use datatypes::Value;
use flow::expr::custom_func::{BatchFunc, CustomFunc};
use flow::expr::func::EvalError;
use std::sync::{Arc, Mutex};
use wasmtime::{Engine, InstancePre, Memory, Store, StoreLimits, TypedFunc};

const HEAP_START: u32 = 4;

/// A user-defined function backed by a WASM module.
///
/// Implements `flow::CustomFunc` so it can be registered in `CustomFuncRegistry`
/// and used in SQL expressions like any built-in function.
pub struct WasmUdf {
    name: String,
    metadata: UdfMetadata,
    /// Wasmtime engine (shared across all UDFs).
    engine: Engine,
    /// Pre-linked instantiation plan reused by processor-owned runtimes.
    instance_pre: InstancePre<StoreLimits>,
    /// Epoch ticks allowed for one batch call.
    epoch_ticks_per_call: u64,
}

impl std::fmt::Debug for WasmUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmUdf")
            .field("name", &self.name)
            .field("metadata", &self.metadata)
            .field("epoch_ticks_per_call", &self.epoch_ticks_per_call)
            .finish_non_exhaustive()
    }
}

impl WasmUdf {
    /// Default epoch budget per batch call.
    pub const DEFAULT_EPOCH_TICKS_PER_CALL: u64 = 10;

    /// Create a new WasmUdf.
    pub(crate) fn new(
        name: String,
        metadata: UdfMetadata,
        engine: Engine,
        instance_pre: InstancePre<StoreLimits>,
    ) -> Result<Self, super::engine::WasmError> {
        Ok(Self {
            name,
            metadata,
            engine,
            instance_pre,
            epoch_ticks_per_call: Self::DEFAULT_EPOCH_TICKS_PER_CALL,
        })
    }

    pub(crate) fn create_runtime(&self) -> Result<WasmUdfRuntime, EvalError> {
        WasmUdfRuntime::new(self)
    }

    /// Bump-allocate `size` bytes in WASM linear memory.
    /// Returns the offset (pointer) to the allocated block.
    /// Mirrors the `host::alloc` function registered in the linker.
    fn wasm_alloc(
        memory: &Memory,
        store: &mut Store<StoreLimits>,
        size: i32,
    ) -> Result<i32, EvalError> {
        if size <= 0 {
            return Ok(0);
        }
        let size = size as u32;

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
    fn is_batch(&self) -> bool {
        true
    }

    fn bind_batch_runtime(&self) -> Result<Option<Arc<dyn BatchFunc>>, String> {
        self.create_runtime()
            .map(|runtime| {
                Some(Arc::new(BoundWasmUdf {
                    name: self.name.clone(),
                    metadata: self.metadata.clone(),
                    runtime: Mutex::new(runtime),
                }) as Arc<dyn BatchFunc>)
            })
            .map_err(|error| error.to_string())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl WasmUdf {
    fn instance_pre(&self) -> &InstancePre<StoreLimits> {
        &self.instance_pre
    }
}

/// A worker-owned WASM execution state for one UDF definition.
pub struct WasmUdfRuntime {
    store: Store<StoreLimits>,
    memory: Memory,
    udf_batch: TypedFunc<(i32, i32), i32>,
    epoch_ticks_per_call: u64,
}

/// Explicit architecture name for the immutable registry-side UDF definition.
pub type WasmUdfDefinition = WasmUdf;

/// Explicit architecture name for the worker-owned evaluator.
pub type WasmUdfEvaluator = WasmUdfRuntime;

struct BoundWasmUdf {
    name: String,
    metadata: UdfMetadata,
    runtime: Mutex<WasmUdfRuntime>,
}

impl std::fmt::Debug for BoundWasmUdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundWasmUdf")
            .field("name", &self.name)
            .field("metadata", &self.metadata)
            .finish_non_exhaustive()
    }
}

impl BatchFunc for BoundWasmUdf {
    fn eval_batch(
        &self,
        args: &[Vec<Value>],
        row_count: usize,
    ) -> Result<Vec<Result<Value, EvalError>>, EvalError> {
        let expected = self.metadata.args.len();
        if args.len() != expected {
            return Err(EvalError::TypeMismatch {
                expected: format!("{} arguments", expected),
                actual: args.len().to_string(),
            });
        }
        self.runtime
            .lock()
            .map_err(|_| EvalError::NotImplemented {
                feature: "WASM UDF runtime lock poisoned".to_string(),
            })?
            .eval_batch(args, row_count)
    }
}

impl WasmUdfRuntime {
    fn new(udf: &WasmUdf) -> Result<Self, EvalError> {
        let mut store = crate::engine::new_store(&udf.engine);
        store.set_epoch_deadline(udf.epoch_ticks_per_call);
        let instance =
            udf.instance_pre()
                .instantiate(&mut store)
                .map_err(|e| EvalError::NotImplemented {
                    feature: format!("instantiation: {e}"),
                })?;
        let memory =
            instance
                .get_memory(&mut store, "memory")
                .ok_or_else(|| EvalError::NotImplemented {
                    feature: "missing WASM memory export".to_string(),
                })?;
        let udf_batch = instance
            .get_typed_func(&mut store, "udf_batch")
            .map_err(|e| EvalError::NotImplemented {
                feature: format!("udf_batch function: {e}"),
            })?;
        Ok(Self {
            store,
            memory,
            udf_batch,
            epoch_ticks_per_call: udf.epoch_ticks_per_call,
        })
    }

    fn reset_scratch_arena(&mut self) -> Result<(), EvalError> {
        self.memory
            .write(&mut self.store, 0, &HEAP_START.to_le_bytes())
            .map_err(|e| EvalError::NotImplemented {
                feature: format!("reset WASM scratch arena: {e}"),
            })
    }

    fn eval_batch(
        &mut self,
        args: &[Vec<Value>],
        row_count: usize,
    ) -> Result<Vec<Result<Value, EvalError>>, EvalError> {
        let result = self.eval_batch_inner(args, row_count);
        let reset_result = self.reset_scratch_arena();

        match (result, reset_result) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(value), Ok(())) => Ok(value),
        }
    }

    fn eval_batch_inner(
        &mut self,
        args: &[Vec<Value>],
        row_count: usize,
    ) -> Result<Vec<Result<Value, EvalError>>, EvalError> {
        self.store.set_epoch_deadline(self.epoch_ticks_per_call);

        // Serialize arguments to JSON
        let input_json =
            serialize_batch_args(args, row_count).map_err(|e| EvalError::TypeMismatch {
                expected: "serializable arguments".to_string(),
                actual: e.to_string(),
            })?;

        let input_len = input_json.len() as i32;

        // Allocate memory in WASM via direct bump allocator
        let input_ptr = WasmUdf::wasm_alloc(&self.memory, &mut self.store, input_len)?;
        if input_ptr == 0 && input_len > 0 {
            return Err(EvalError::NotImplemented {
                feature: "alloc returned null".to_string(),
            });
        }

        // Write input JSON to WASM memory
        self.memory
            .write(&mut self.store, input_ptr as usize, &input_json)
            .map_err(|e| EvalError::NotImplemented {
                feature: format!("failed to write input to WASM memory: {e}"),
            })?;

        // Call the UDF
        let result_ptr = self
            .udf_batch
            .call(&mut self.store, (input_ptr, input_len))
            .map_err(|e| EvalError::NotImplemented {
                feature: format!("UDF evaluation failed or timed out: {e}"),
            })?;

        if result_ptr == 0 {
            return Err(EvalError::NotImplemented {
                feature: "udf_batch returned null".to_string(),
            });
        }

        // Result protocol: the UDF allocates a buffer via host.alloc,
        // writes the length (i32 LE) at offset 0, writes the JSON data at offset 4,
        // and returns pointer to the data (buffer + 4).
        let len_offset = (result_ptr as usize).saturating_sub(4);
        let mut len_bytes = [0u8; 4];
        self.memory
            .read(&self.store, len_offset, &mut len_bytes)
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
        self.memory
            .read(&self.store, result_ptr as usize, &mut result_buf)
            .map_err(|e| EvalError::NotImplemented {
                feature: format!("failed to read result: {e}"),
            })?;

        let batch_results =
            deserialize_batch_result(&result_buf).map_err(|e| EvalError::TypeMismatch {
                expected: "valid JSON result".to_string(),
                actual: e.to_string(),
            })?;
        Ok(batch_results
            .into_iter()
            .map(|result| {
                result.map_err(|error| EvalError::TypeMismatch {
                    expected: "successful UDF result".to_string(),
                    actual: error.to_string(),
                })
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::WasmEngine;
    use datatypes::Value;

    #[test]
    fn worker_store_limits_guest_table_growth() {
        let metadata = r#"{"abi_version":1,"name":"test_udf","description":"limits","args":[],"return_type":"any","execution_mode":"batch"}"#;
        let data = metadata
            .bytes()
            .map(|byte| format!("\\{byte:02x}"))
            .collect::<String>();
        let wasm = wat::parse_str(format!(
            r#"
            (module
                (memory (export "memory") 1 256)
                (table 1 funcref)
                (data (i32.const 128) "{data}")
                (func (export "udf_metadata_ptr") (result i32) i32.const 128)
                (func (export "udf_metadata_len") (result i32) i32.const {})
                (func (export "udf_batch") (param i32 i32) (result i32)
                    ref.null func i32.const 65536 table.grow)
            )"#,
            metadata.len()
        ))
        .expect("module");
        let engine = WasmEngine::new().expect("engine");
        let udf = engine.instantiate("test_udf", &wasm).expect("UDF");
        let mut runtime = udf.create_runtime().expect("worker runtime");
        let allocation = runtime
            .udf_batch
            .call(&mut runtime.store, (0, 0))
            .expect("guest call");
        assert_eq!(
            allocation, -1,
            "guest table.grow must fail at the store limit"
        );
    }

    /// A minimal WASM module that returns two batch values.
    fn batch_wasm_module() -> Vec<u8> {
        wat::parse_str(
            r#"
            (module
                (import "host" "alloc" (func $alloc (param i32) (result i32)))

                (memory (export "memory") 1 256)
                (data (i32.const 0) "\00\00\00\00")

                (data (i32.const 128) "{\"abi_version\":1,\"name\":\"test_udf\",\"description\":\"batch\",\"args\":[{\"name\":\"x\",\"type\":\"any\"}],\"return_type\":\"any\",\"execution_mode\":\"batch\"}")
                (data (i32.const 32768) "{\"values\":[42,43]}")

                (func (export "udf_metadata_ptr") (result i32)
                    i32.const 128)
                (func (export "udf_metadata_len") (result i32)
                    i32.const 137)

                (func (export "udf_batch") (param i32) (param i32) (result i32)
                    (local $buf i32)
                    i32.const 18
                    i32.const 4
                    i32.add
                    call $alloc
                    local.set $buf

                    local.get $buf
                    i32.const 18
                    i32.store

                    local.get $buf
                    i32.const 4
                    i32.add
                    i32.const 32768
                    i32.const 18
                    memory.copy

                    local.get $buf
                    i32.const 4
                    i32.add)
            )
            "#,
        )
        .expect("wat parse")
    }

    fn looping_wasm_module() -> Vec<u8> {
        wat::parse_str(
            r#"
            (module
                (import "host" "alloc" (func $alloc (param i32) (result i32)))

                (memory (export "memory") 1 1)
                (data (i32.const 128) "{\"abi_version\":1,\"name\":\"loop_udf\",\"description\":\"loop\",\"args\":[],\"return_type\":\"any\",\"execution_mode\":\"batch\"}")

                (func (export "udf_metadata_ptr") (result i32)
                    i32.const 128)
                (func (export "udf_metadata_len") (result i32)
                    i32.const 111)

                (func (export "udf_batch") (param i32) (param i32) (result i32)
                    (loop
                        br 0)
                    unreachable)
            )
            "#,
        )
        .expect("wat parse")
    }

    #[test]
    fn batch_udf_returns_all_rows() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = batch_wasm_module();
        let udf = engine.instantiate("test_udf", &wasm).expect("instantiate");
        let udf = udf
            .bind_batch_runtime()
            .expect("bind runtime")
            .expect("WASM runtime");

        let input = vec![Value::Int64(42)];
        let result = udf.eval_batch(&[input], 1).expect("eval_batch");
        assert_eq!(result, vec![Ok(Value::Int64(42)), Ok(Value::Int64(43))]);
    }

    #[test]
    fn batch_udf_accepts_string_input() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = batch_wasm_module();
        let udf = engine.instantiate("test_udf", &wasm).expect("instantiate");
        let udf = udf
            .bind_batch_runtime()
            .expect("bind runtime")
            .expect("WASM runtime");

        let input = vec![Value::String("hello wasm".to_string())];
        let result = udf.eval_batch(&[input], 1).expect("eval_batch");
        assert_eq!(result, vec![Ok(Value::Int64(42)), Ok(Value::Int64(43))]);
    }

    #[test]
    fn batch_udf_accepts_float_input() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = batch_wasm_module();
        let udf = engine.instantiate("test_udf", &wasm).expect("instantiate");
        let udf = udf
            .bind_batch_runtime()
            .expect("bind runtime")
            .expect("WASM runtime");

        let input = vec![Value::Float64(2.5)];
        let result = udf.eval_batch(&[input], 1).expect("eval_batch");
        assert_eq!(result, vec![Ok(Value::Int64(42)), Ok(Value::Int64(43))]);
    }

    #[test]
    fn batch_udf_accepts_bool_input() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = batch_wasm_module();
        let udf = engine.instantiate("test_udf", &wasm).expect("instantiate");
        let udf = udf
            .bind_batch_runtime()
            .expect("bind runtime")
            .expect("WASM runtime");

        let input = vec![Value::Bool(true)];
        let result = udf.eval_batch(&[input], 1).expect("eval_batch");
        assert_eq!(result, vec![Ok(Value::Int64(42)), Ok(Value::Int64(43))]);

        let input = vec![Value::Bool(false)];
        let result = udf.eval_batch(&[input], 1).expect("eval_batch");
        assert_eq!(result, vec![Ok(Value::Int64(42)), Ok(Value::Int64(43))]);
    }

    // ── multiple calls, no state leak ──────────────────────────────

    #[test]
    fn multiple_calls_no_state_leak() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = batch_wasm_module();
        let udf = engine.instantiate("test_udf", &wasm).expect("instantiate");
        let udf = udf
            .bind_batch_runtime()
            .expect("bind runtime")
            .expect("WASM runtime");

        for i in 0..10 {
            let input = vec![Value::Int64(i)];
            let result = udf.eval_batch(&[input], 1).expect("eval_batch");
            assert_eq!(
                result,
                vec![Ok(Value::Int64(42)), Ok(Value::Int64(43))],
                "failed at iteration {i}"
            );
        }
    }

    #[test]
    fn repeated_calls_reuse_linear_memory() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = batch_wasm_module();
        let udf = engine.instantiate("test_udf", &wasm).expect("instantiate");
        let mut runtime = WasmUdfRuntime::new(&udf).expect("create runtime");
        let initial_pages = runtime.memory.size(&runtime.store);

        for i in 0..10_000 {
            let result = runtime
                .eval_batch(&[vec![Value::Int64(i), Value::Int64(i + 1)]], 2)
                .expect("eval_batch should reuse scratch memory");
            assert_eq!(result, vec![Ok(Value::Int64(42)), Ok(Value::Int64(43))]);
        }

        assert_eq!(runtime.memory.size(&runtime.store), initial_pages);
    }

    #[test]
    fn infinite_wasm_call_is_interrupted() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = looping_wasm_module();
        let udf = engine.instantiate("loop_udf", &wasm).expect("instantiate");
        let udf = udf
            .bind_batch_runtime()
            .expect("bind runtime")
            .expect("WASM runtime");

        let error = udf
            .eval_batch(&[], 0)
            .expect_err("infinite call should time out");
        assert!(error.to_string().contains("timed out"));
    }

    // ── large batch input ─────────────────────────────────────────

    #[test]
    fn large_batch_input_is_serialized() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = batch_wasm_module();
        let udf = engine.instantiate("test_udf", &wasm).expect("instantiate");
        let udf = udf
            .bind_batch_runtime()
            .expect("bind runtime")
            .expect("WASM runtime");

        // 10 KB string — one WASM page is 64 KB, but input + output + allocator
        // bookkeeping should still fit in one page.
        let big = "x".repeat(10_000);
        let input = vec![Value::String(big.clone())];
        let result = udf.eval_batch(&[input], 1).expect("eval_batch");
        assert_eq!(result, vec![Ok(Value::Int64(42)), Ok(Value::Int64(43))]);
    }

    // ── batch argument validation ─────────────────────────────────

    #[test]
    fn batch_runtime_rejects_wrong_arity() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = batch_wasm_module();
        let udf = engine.instantiate("test_udf", &wasm).expect("instantiate");
        let udf = udf
            .bind_batch_runtime()
            .expect("bind batch runtime")
            .expect("WASM runtime");

        let error = udf
            .eval_batch(&[], 0)
            .expect_err("missing batch argument should fail");
        assert!(error.to_string().contains("1 arguments"));
    }
}
