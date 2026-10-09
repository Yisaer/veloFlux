use crate::func::WasmUdf;
use crate::metadata::{UdfExecutionMode, UdfMetadata};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use thiserror::Error;
use wasmtime::{Config, Engine, Linker, Module, Store, StoreLimits, StoreLimitsBuilder};

static GLOBAL_ENGINE: OnceLock<Engine> = OnceLock::new();
static EPOCH_TICKER: OnceLock<Result<(), String>> = OnceLock::new();

const MAX_MEMORY_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const PAGE_SIZE: u64 = 64 * 1024;
pub(crate) const MAX_PAGES: u64 = MAX_MEMORY_BYTES as u64 / PAGE_SIZE;
const MAX_METADATA_BYTES: usize = 64 * 1024;

/// Apply the same limits to validation and processor-owned execution stores.
pub(crate) fn new_store(engine: &Engine) -> Store<StoreLimits> {
    let limits = StoreLimitsBuilder::new()
        .memory_size(MAX_MEMORY_BYTES)
        .memories(1)
        .table_elements(65_536)
        .tables(1)
        .instances(1)
        .build();
    let mut store = Store::new(engine, limits);
    store.limiter(|limits| limits);
    store
}

/// Global WASM execution engine shared across all UDF instances.
///
/// The engine owns the JIT compilation context and the host function linker.
/// It is cheap to clone (internally reference-counted).
#[derive(Clone)]
pub struct WasmEngine {
    engine: Engine,
    linker: Arc<Linker<StoreLimits>>,
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
        config.epoch_interruption(true);

        let engine = match GLOBAL_ENGINE.get() {
            Some(engine) => engine.clone(),
            None => {
                let candidate = Engine::new(&config)?;
                let _ = GLOBAL_ENGINE.set(candidate);
                GLOBAL_ENGINE.get().cloned().ok_or_else(|| {
                    WasmError::Compile(wasmtime::Error::msg(
                        "global WASM engine initialization failed",
                    ))
                })?
            }
        };

        let ticker_result = EPOCH_TICKER.get_or_init(|| {
            let engine_weak = engine.weak();
            std::thread::Builder::new()
                .name("wasm-udf-epoch".to_string())
                .spawn(move || loop {
                    std::thread::sleep(Duration::from_millis(10));
                    let Some(engine) = engine_weak.upgrade() else {
                        break;
                    };
                    engine.increment_epoch();
                })
                .map(|_| ())
                .map_err(|error| format!("failed to start WASM epoch ticker: {error}"))
        });
        if let Err(error) = ticker_result {
            return Err(WasmError::Compile(wasmtime::Error::msg(error.clone())));
        }

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

        // Reject modules whose declared memory exceeds the cap before
        // instantiation, so we never commit large initial memory.
        enforce_memory_cap(&module)?;

        let mut store = new_store(&self.engine);
        store.set_epoch_deadline(100);
        let instance = self.linker.instantiate(&mut store, &module)?;

        // Verify required exports exist
        instance
            .get_typed_func::<(i32, i32), i32>(&mut store, "udf_batch")
            .map_err(|_| WasmError::MissingExport("udf_batch"))?;
        if instance.get_func(&mut store, "udf_row").is_some() {
            return Err(WasmError::InvalidMetadata(
                "legacy udf_row export is not supported".to_string(),
            ));
        }
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

        // Reject modules whose declared memory exceeds the cap before
        // instantiation, so we never commit large initial memory.
        enforce_memory_cap(&module)?;

        let mut store = new_store(&self.engine);

        store.set_epoch_deadline(100);

        let instance = self.linker.instantiate(&mut store, &module)?;

        let metadata = read_metadata(&instance, &mut store)?;
        validate_metadata(&metadata)?;

        if metadata.name != name {
            return Err(WasmError::NameMismatch {
                expected: name.to_string(),
                wasm_name: metadata.name.clone(),
            });
        }

        let instance_pre = self.linker.instantiate_pre(&module)?;
        WasmUdf::new(
            name.to_string(),
            metadata,
            self.engine.clone(),
            instance_pre,
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
fn host_alloc(
    mut caller: wasmtime::Caller<'_, StoreLimits>,
    size: i32,
) -> Result<i32, wasmtime::Error> {
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
    let needed_bytes = new_bump as u64;
    let current_bytes = current_pages * PAGE_SIZE;
    if needed_bytes > current_bytes {
        let additional = needed_bytes - current_bytes;
        let pages_needed = additional.div_ceil(PAGE_SIZE);
        // Enforce hard memory cap before growing
        let new_pages = current_pages + pages_needed;
        if new_pages > MAX_PAGES {
            return Err(wasmtime::Error::msg(format!(
                "host_alloc: exceeded max memory ({new_pages} > {MAX_PAGES} pages)"
            )));
        }
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
    store: &mut Store<StoreLimits>,
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

    if ptr <= 0 || len <= 0 {
        return Err(WasmError::InvalidMetadata(
            "metadata pointer is null or length is zero".to_string(),
        ));
    }

    let len = len as usize;
    if len > MAX_METADATA_BYTES {
        return Err(WasmError::InvalidMetadata(format!(
            "metadata length {len} exceeds {MAX_METADATA_BYTES} bytes"
        )));
    }
    let ptr = ptr as usize;
    let bytes = memory
        .data(&*store)
        .get(ptr..ptr.saturating_add(len))
        .ok_or_else(|| {
            WasmError::InvalidMetadata("metadata range is outside WASM memory".to_string())
        })?;

    serde_json::from_slice(bytes)
        .map_err(|e| WasmError::InvalidMetadata(format!("invalid metadata JSON: {e}")))
}

fn validate_metadata(metadata: &UdfMetadata) -> Result<(), WasmError> {
    if metadata.abi_version != 1 {
        return Err(WasmError::InvalidMetadata(format!(
            "unsupported ABI version: {}",
            metadata.abi_version
        )));
    }
    if metadata.execution_mode != UdfExecutionMode::Batch {
        return Err(WasmError::InvalidMetadata(
            "only batch execution mode is supported".to_string(),
        ));
    }
    if metadata.name.trim().is_empty() {
        return Err(WasmError::InvalidMetadata(
            "UDF name must not be empty".to_string(),
        ));
    }
    Ok(())
}

/// Check that the module's declared memory does not exceed the cap.
/// Both the minimum (initial size) and maximum (growth limit) are checked
/// before instantiation. A missing maximum is treated as unbounded.
fn enforce_memory_cap(module: &Module) -> Result<(), WasmError> {
    for export in module.exports() {
        if export.name() == "memory" {
            if let wasmtime::ExternType::Memory(mem) = export.ty() {
                if mem.minimum() > MAX_PAGES {
                    return Err(WasmError::Compile(wasmtime::Error::msg(format!(
                        "module memory minimum ({} pages) exceeds max ({MAX_PAGES} pages)",
                        mem.minimum(),
                    ))));
                }
                match mem.maximum() {
                    None => {
                        return Err(WasmError::Compile(wasmtime::Error::msg(
                            "module memory must declare a maximum size".to_string(),
                        )));
                    }
                    Some(max) if max > MAX_PAGES => {
                        return Err(WasmError::Compile(wasmtime::Error::msg(format!(
                            "module memory maximum ({} pages) exceeds max ({MAX_PAGES} pages)",
                            max,
                        ))));
                    }
                    _ => {}
                }
            }
        }
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

                (memory (export "memory") 1 256)
                (data (i32.const 0) "\00\00\00\00")

                ;; Metadata JSON at offset 128 (exactly 119 bytes)
                (data (i32.const 128) "{\"abi_version\":1,\"name\":\"test_udf\",\"description\":\"minimal test\",\"args\":[],\"return_type\":\"any\",\"execution_mode\":\"batch\"}")

                (func (export "udf_metadata_ptr") (result i32)
                    i32.const 128)
                (func (export "udf_metadata_len") (result i32)
                    i32.const 119)

                (func (export "udf_batch") (param i32) (param i32) (result i32)
                    i32.const 0)
            )
            "#,
        )
        .expect("wat parse")
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

    fn metadata_module(ptr: i32, len: i32, extra: &str) -> Vec<u8> {
        let metadata = r#"{"abi_version":1,"name":"test_udf","description":"limits","args":[],"return_type":"any","execution_mode":"batch"}"#;
        let data = metadata
            .bytes()
            .map(|byte| format!("\\{byte:02x}"))
            .collect::<String>();
        wat::parse_str(format!(
            r#"
            (module
                (memory (export "memory") 1 256)
                (data (i32.const 128) "{data}")
                (func (export "udf_metadata_ptr") (result i32) i32.const {ptr})
                (func (export "udf_metadata_len") (result i32) i32.const {len})
                (func (export "udf_batch") (param i32 i32) (result i32) i32.const 0)
                {extra}
            )"#
        ))
        .expect("test module")
    }

    #[test]
    fn metadata_limits_reject_invalid_lengths_and_ranges_before_parsing() {
        let engine = WasmEngine::new().expect("engine");
        for (ptr, len, reason) in [
            (128, i32::MAX, "exceeds"),
            (128, 65_537, "exceeds"),
            (-1, 8, "pointer"),
            (65_532, 8, "outside"),
            (i32::MAX, 8, "outside"),
        ] {
            let wasm = metadata_module(ptr, len, "");
            for result in [
                engine.validate(&wasm).map(|_| ()),
                engine.instantiate("test_udf", &wasm).map(|_| ()),
            ] {
                assert!(
                    result
                        .expect_err("reject metadata")
                        .to_string()
                        .contains(reason),
                    "{ptr}/{len}"
                );
            }
        }
    }

    #[test]
    fn validation_enforces_initial_memory_and_table_limits() {
        let engine = WasmEngine::new().expect("engine");
        let metadata_len = r#"{"abi_version":1,"name":"test_udf","description":"limits","args":[],"return_type":"any","execution_mode":"batch"}"#.len() as i32;
        for extra in [
            "(memory 1)",
            "(table 65537 funcref)",
            "(table 1 funcref) (table 1 funcref)",
        ] {
            let wasm = metadata_module(128, metadata_len, extra);
            assert!(engine.validate(&wasm).is_err(), "{extra}");
            assert!(engine.instantiate("test_udf", &wasm).is_err(), "{extra}");
        }
        let wasm = metadata_module(
            128,
            metadata_len,
            r#"
            (table (export "scratch") 1 funcref)
            (func (export "grow_scratch") (result i32)
                ref.null func i32.const 65536 table.grow)
        "#,
        );
        engine.validate(&wasm).expect("valid initial resources");
        let udf = engine.instantiate("test_udf", &wasm).expect("UDF");
        udf.create_runtime().expect("worker runtime");
    }

    #[test]
    fn engines_share_process_global_wasm_engine() {
        let first = WasmEngine::new().expect("create first engine");
        let second = WasmEngine::new().expect("create second engine");

        assert!(Engine::same(&first.engine, &second.engine));
    }
}
