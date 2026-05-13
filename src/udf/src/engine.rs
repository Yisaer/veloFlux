use crate::func::WasmUdf;
use crate::metadata::UdfMetadata;
use std::sync::Arc;
use thiserror::Error;
use wasmtime::{Config, Engine, Linker, Module, Store};

/// Global WASM execution engine shared across all UDF instances.
///
/// The engine owns the JIT compilation context and the host function linker.
/// It is cheap to clone (internally reference-counted).
#[derive(Clone)]
pub struct WasmEngine {
    engine: Engine,
    linker: Arc<Linker<()>>,
}

/// Errors that can occur during WASM compilation or instantiation.
#[derive(Debug, Error)]
pub enum WasmError {
    #[error("WASM compilation failed: {0}")]
    Compile(#[from] wasmtime::Error),
    #[error("missing required export '{0}'")]
    MissingExport(&'static str),
    #[error("invalid UDF metadata: {0}")]
    InvalidMetadata(String),
    #[error("UDF metadata name '{wasm_name}' does not match expected '{expected}'")]
    NameMismatch { expected: String, wasm_name: String },
}

impl WasmEngine {
    /// Create a new engine with default safety limits.
    pub fn new() -> Result<Self, WasmError> {
        let mut config = Config::new();
        // Enable fuel consumption for per-call budgeting (eval_row sets fuel).
        // Epoch interruption is left disabled; we rely on fuel alone for timeout.
        config.consume_fuel(true);

        let engine = Engine::new(&config)?;

        let mut linker = Linker::new(&engine);
        linker.func_wrap("host", "alloc", host_alloc)?;

        Ok(Self {
            engine,
            linker: Arc::new(linker),
        })
    }

    /// Validate a WASM module: compile it and check for required exports.
    /// Returns the parsed metadata.
    pub fn validate(&self, wasm_bytes: &[u8]) -> Result<UdfMetadata, WasmError> {
        let module = Module::new(&self.engine, wasm_bytes)?;
        let mut store = Store::new(&self.engine, ());
        // Set a large fuel budget for validation (metadata reading requires WASM execution)
        store.set_fuel(10_000_000).ok();
        let instance = self.linker.instantiate(&mut store, &module)?;

        // Verify required exports exist
        instance
            .get_typed_func::<(i32, i32), i32>(&mut store, "udf_row")
            .map_err(|_| WasmError::MissingExport("udf_row"))?;
        instance
            .get_memory(&mut store, "memory")
            .ok_or(WasmError::MissingExport("memory"))?;

        // Read metadata
        let metadata = read_metadata(&instance, &mut store)?;
        validate_metadata(&metadata)?;

        Ok(metadata)
    }

    /// Compile a WASM module and return a callable UDF.
    ///
    /// Prefer calling `validate` first to catch errors before persisting.
    pub fn instantiate(&self, name: &str, wasm_bytes: &[u8]) -> Result<WasmUdf, WasmError> {
        let module = Module::new(&self.engine, wasm_bytes)?;
        let mut store = Store::new(&self.engine, ());

        // Set a generous fuel budget for metadata reading.
        // Per-call fuel is set separately in eval_row.
        store.set_fuel(10_000_000)?;

        let instance = self.linker.instantiate(&mut store, &module)?;

        let metadata = read_metadata(&instance, &mut store)?;
        validate_metadata(&metadata)?;

        if metadata.name != name {
            return Err(WasmError::NameMismatch {
                expected: name.to_string(),
                wasm_name: metadata.name.clone(),
            });
        }

        WasmUdf::new(
            name.to_string(),
            metadata,
            self.engine.clone(),
            module,
            Arc::clone(&self.linker),
        )
    }
}

impl Default for WasmEngine {
    fn default() -> Self {
        Self::new().expect("default WasmEngine should always construct successfully")
    }
}

/// Host-provided allocator: allocates `size` bytes in WASM linear memory,
/// returning the offset (pointer) to the start of the allocated block.
///
/// Simple bump-allocator using memory.grow when the current block is exhausted.
/// We reserve one i32 at offset 0 in memory to track the current offset.
fn host_alloc(mut caller: wasmtime::Caller<'_, ()>, size: i32) -> Result<i32, wasmtime::Error> {
    if size <= 0 {
        return Ok(0);
    }
    let size = size as u32;

    let memory = caller
        .get_export("memory")
        .and_then(|e| e.into_memory())
        .ok_or_else(|| wasmtime::Error::msg("host_alloc: memory export missing"))?;

    // Reserve the first 4 bytes for the bump pointer
    const HEAP_START: u32 = 4;

    // Read current bump pointer
    let mut bump_bytes = [0u8; 4];
    memory
        .read(&caller, 0, &mut bump_bytes)
        .map_err(|e| wasmtime::Error::msg(format!("host_alloc: read bump ptr: {e}")))?;
    let mut bump = u32::from_le_bytes(bump_bytes);

    if bump == 0 {
        // First allocation: start after the bump pointer slot
        bump = HEAP_START;
    }

    // Align to 4 bytes
    let aligned = (bump + 3) & !3u32;
    let new_bump = aligned + size;
    let current_pages = memory.size(&caller);

    // Check if we need to grow
    const PAGE_SIZE: u64 = 65536;
    let needed_bytes = new_bump as u64;
    let current_bytes = current_pages * PAGE_SIZE;
    if needed_bytes > current_bytes {
        let additional = needed_bytes - current_bytes;
        let pages_needed = additional.div_ceil(PAGE_SIZE);
        memory
            .grow(&mut caller, pages_needed)
            .map_err(|e| wasmtime::Error::msg(format!("host_alloc: grow memory: {e}")))?;
    }

    // Write back the new bump pointer
    let new_bump_bytes = new_bump.to_le_bytes();
    memory
        .write(&mut caller, 0, &new_bump_bytes)
        .map_err(|e| wasmtime::Error::msg(format!("host_alloc: write bump ptr: {e}")))?;

    Ok(aligned as i32)
}

fn read_metadata(
    instance: &wasmtime::Instance,
    store: &mut Store<()>,
) -> Result<UdfMetadata, WasmError> {
    let memory = instance
        .get_memory(&mut *store, "memory")
        .ok_or(WasmError::MissingExport("memory"))?;

    let ptr_fn = instance
        .get_typed_func::<(), i32>(&mut *store, "udf_metadata_ptr")
        .map_err(|_| WasmError::MissingExport("udf_metadata_ptr"))?;
    let len_fn = instance
        .get_typed_func::<(), i32>(&mut *store, "udf_metadata_len")
        .map_err(|_| WasmError::MissingExport("udf_metadata_len"))?;

    let ptr = ptr_fn.call(&mut *store, ())?;
    let len = len_fn.call(&mut *store, ())?;

    if ptr == 0 || len <= 0 {
        return Err(WasmError::InvalidMetadata(
            "metadata pointer is null or length is zero".to_string(),
        ));
    }

    let mut buf = vec![0u8; len as usize];
    memory.read(&*store, ptr as usize, &mut buf).map_err(|e| {
        WasmError::InvalidMetadata(format!("failed to read metadata from WASM memory: {e}"))
    })?;

    serde_json::from_slice(&buf)
        .map_err(|e| WasmError::InvalidMetadata(format!("invalid metadata JSON: {e}")))
}

fn validate_metadata(metadata: &UdfMetadata) -> Result<(), WasmError> {
    if metadata.name.trim().is_empty() {
        return Err(WasmError::InvalidMetadata(
            "UDF name must not be empty".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow::expr::custom_func::CustomFunc;

    /// A minimal WASM module that implements the UDF protocol correctly.
    fn minimal_wasm_module() -> Vec<u8> {
        wat::parse_str(
            r#"
            (module
                (import "host" "alloc" (func $alloc (param i32) (result i32)))

                (memory (export "memory") 1)
                (data (i32.const 0) "\00\00\00\00")

                ;; Metadata JSON at offset 128 (exactly 97 bytes)
                (data (i32.const 128) "{\"name\":\"test_udf\",\"description\":\"minimal test\",\"args\":[],\"return_type\":\"any\",\"batch_mode\":false}")

                (func (export "udf_metadata_ptr") (result i32)
                    i32.const 128)
                (func (export "udf_metadata_len") (result i32)
                    i32.const 97)

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
    fn engine_validate_accepts_valid_module() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = minimal_wasm_module();
        let metadata = engine.validate(&wasm).expect("validate");
        assert_eq!(metadata.name, "test_udf");
        assert_eq!(metadata.description, "minimal test");
    }

    #[test]
    fn engine_instantiate_returns_callable_udf() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = minimal_wasm_module();
        let udf = engine.instantiate("test_udf", &wasm).expect("instantiate");
        assert_eq!(udf.name(), "test_udf");
    }

    #[test]
    fn engine_instantiate_rejects_name_mismatch() {
        let engine = WasmEngine::new().expect("create engine");
        let wasm = minimal_wasm_module();
        let err = engine
            .instantiate("wrong_name", &wasm)
            .expect_err("should reject name mismatch");
        assert!(err.to_string().contains("wrong_name"));
    }
}
