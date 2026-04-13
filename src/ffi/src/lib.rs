#![deny(clippy::unwrap_used, clippy::unreachable, clippy::panic)]

use std::ffi::{c_char, c_int, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};

const VELOFLUX_FFI_OK: c_int = 0;
const VELOFLUX_FFI_INVALID_ARGUMENT: c_int = 1;
const VELOFLUX_FFI_START_FAILED: c_int = 2;
const VELOFLUX_FFI_STOP_FAILED: c_int = 3;
const VELOFLUX_FFI_PANIC: c_int = 255;

#[repr(C)]
pub struct veloflux_handle {
    _private: [u8; 0],
}

fn with_ffi_boundary(f: impl FnOnce() -> c_int) -> c_int {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(code) => code,
        Err(_) => VELOFLUX_FFI_PANIC,
    }
}

fn raw_handle_to_inner(
    handle: *mut veloflux_handle,
) -> *mut veloflux::embedded::EmbeddedServerHandle {
    handle.cast()
}

#[no_mangle]
pub extern "C" fn veloflux_start(
    config_path: *const c_char,
    out_handle: *mut *mut veloflux_handle,
) -> c_int {
    with_ffi_boundary(|| {
        if config_path.is_null() || out_handle.is_null() {
            return VELOFLUX_FFI_INVALID_ARGUMENT;
        }

        let config_path = unsafe { CStr::from_ptr(config_path) };
        let config_path = match config_path.to_str() {
            Ok(config_path) if !config_path.is_empty() => config_path,
            _ => return VELOFLUX_FFI_INVALID_ARGUMENT,
        };

        match veloflux::embedded::start_embedded(config_path) {
            Ok(handle) => {
                let raw_handle = Box::into_raw(Box::new(handle)).cast::<veloflux_handle>();
                unsafe {
                    *out_handle = raw_handle;
                }
                VELOFLUX_FFI_OK
            }
            Err(_) => VELOFLUX_FFI_START_FAILED,
        }
    })
}

#[no_mangle]
pub extern "C" fn veloflux_stop(handle: *mut *mut veloflux_handle) -> c_int {
    with_ffi_boundary(|| {
        if handle.is_null() {
            return VELOFLUX_FFI_INVALID_ARGUMENT;
        }

        let raw_handle = unsafe { *handle };
        if raw_handle.is_null() {
            return VELOFLUX_FFI_OK;
        }

        unsafe {
            *handle = std::ptr::null_mut();
        }

        let mut handle = unsafe { Box::from_raw(raw_handle_to_inner(raw_handle)) };
        match handle.stop() {
            Ok(()) => VELOFLUX_FFI_OK,
            Err(_) => VELOFLUX_FFI_STOP_FAILED,
        }
    })
}
