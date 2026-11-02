use std::env;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::fs::File;
use std::io::Read;
use std::mem;
use std::ptr;

const RTLD_NOW: c_int = 2;
const GCM_TAG_LEN: usize = 16;

#[repr(C)]
struct X509SymCryptoParam {
    key_type: c_int,
    input: *mut u8,
    input_len: u32,
    iv: *mut u8,
    iv_len: u32,
    aad: *mut u8,
    aad_len: u32,
    tag: *mut u8,
    tag_len: u32,
    padding: u32,
}

type InitFn = unsafe extern "C" fn() -> bool;
type KeyStateFn = unsafe extern "C" fn(c_int, *mut u8) -> i32;
type CryptoFn = unsafe extern "C" fn(*mut X509SymCryptoParam, *mut u8, *mut u32, u8) -> i32;

unsafe extern "C" {
    fn dlopen(filename: *const c_char, flags: c_int) -> *mut c_void;
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    fn dlerror() -> *const c_char;
    fn dlclose(handle: *mut c_void) -> c_int;
}

struct Library(*mut c_void);

impl Library {
    fn open(name: &str) -> Result<Self, String> {
        let name = CString::new(name).map_err(|error| error.to_string())?;
        let handle = unsafe { dlopen(name.as_ptr(), RTLD_NOW) };
        if handle.is_null() {
            return Err(format!("dlopen failed: {}", last_dl_error()));
        }
        Ok(Self(handle))
    }

    unsafe fn symbol<T>(&self, name: &str) -> Result<T, String> {
        let name = CString::new(name).map_err(|error| error.to_string())?;
        unsafe { dlerror() };
        let symbol = unsafe { dlsym(self.0, name.as_ptr()) };
        if symbol.is_null() {
            return Err(format!("dlsym failed: {}", last_dl_error()));
        }
        Ok(unsafe { mem::transmute_copy(&symbol) })
    }
}

impl Drop for Library {
    fn drop(&mut self) {
        unsafe { dlclose(self.0) };
    }
}

fn last_dl_error() -> String {
    let error = unsafe { dlerror() };
    if error.is_null() {
        return "unknown dynamic loader error".to_string();
    }
    unsafe { CStr::from_ptr(error) }
        .to_string_lossy()
        .into_owned()
}

fn round_trip(crypto: CryptoFn, key_type: c_int, aad: Option<&[u8]>) -> Result<(), String> {
    let plaintext = b"veloFlux QNX Rust FFI crypto probe";
    let mut iv = [0u8; 12];
    File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut iv))
        .map_err(|error| format!("random IV failed: {error}"))?;
    let mut tag = [0u8; GCM_TAG_LEN];
    let mut ciphertext = [0u8; 128];
    let mut ciphertext_len = ciphertext.len() as u32;
    let mut param = X509SymCryptoParam {
        key_type,
        input: plaintext.as_ptr().cast_mut(),
        input_len: plaintext.len() as u32,
        iv: iv.as_mut_ptr(),
        iv_len: iv.len() as u32,
        aad: aad.map_or(ptr::null_mut(), |bytes| bytes.as_ptr().cast_mut()),
        aad_len: aad.map_or(0, |bytes| bytes.len() as u32),
        tag: tag.as_mut_ptr(),
        tag_len: tag.len() as u32,
        padding: 0,
    };
    let status = unsafe { crypto(&mut param, ciphertext.as_mut_ptr(), &mut ciphertext_len, 0) };
    if status != 0 {
        return Err(format!("encryption failed: {status}"));
    }
    if ciphertext_len == 0
        || ciphertext_len as usize > ciphertext.len()
        || param.tag_len != GCM_TAG_LEN as u32
    {
        return Err("encryption returned invalid ciphertext or tag length".to_string());
    }

    let mut decrypted = [0u8; 128];
    let mut decrypted_len = decrypted.len() as u32;
    param.input = ciphertext.as_mut_ptr();
    param.input_len = ciphertext_len;
    let status = unsafe { crypto(&mut param, decrypted.as_mut_ptr(), &mut decrypted_len, 1) };
    if status != 0 {
        return Err(format!("decryption failed: {status}"));
    }
    if decrypted_len as usize != plaintext.len() || &decrypted[..plaintext.len()] != plaintext {
        return Err("decrypted plaintext differs from input".to_string());
    }
    Ok(())
}

fn run() -> Result<(), String> {
    if mem::size_of::<X509SymCryptoParam>() != 72 {
        return Err("unexpected X509SymCryptoParam layout".to_string());
    }
    let key_type = env::var("PKI_KEY_TYPE")
        .unwrap_or_else(|_| "1".to_string())
        .parse::<c_int>()
        .map_err(|error| format!("invalid PKI_KEY_TYPE: {error}"))?;
    if !matches!(key_type, 0 | 1 | 3) {
        return Err("PKI_KEY_TYPE must be 0, 1, or 3 for CCU GCM".to_string());
    }

    let library = Library::open("libcn_x509_proxy.so")?;
    let init: InitFn = unsafe { library.symbol("pkiLibraryInit")? };
    let key_state: KeyStateFn = unsafe { library.symbol("getCryptoKeyState")? };
    let crypto: CryptoFn = unsafe { library.symbol("symCryptoGeneric")? };
    println!("PASS: QNX SDK library and C symbols loaded");

    if !unsafe { init() } {
        return Err("pkiLibraryInit returned false".to_string());
    }
    println!("PASS: SDK initialized");

    let mut state = 1u8;
    let status = unsafe { key_state(key_type, &mut state) };
    if status != 0 || state != 0 {
        return Err(format!(
            "business key unavailable: status={status}, state={state}"
        ));
    }
    println!("PASS: existing business key {key_type} is available");

    round_trip(crypto, key_type, None)?;
    println!("PASS: empty AAD encryption and decryption round trip");
    round_trip(crypto, key_type, Some(b"veloFlux QNX AAD"))?;
    println!("PASS: nonempty AAD encryption and decryption round trip");
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("FAIL: {error}");
        std::process::exit(1);
    }
}
