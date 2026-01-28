#[cfg(all(feature = "allocator-jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use veloflux::server;

#[cfg(target_os = "linux")]
fn maybe_disable_thp() {
    // Best-effort: disable Transparent Huge Pages for this process to reduce RSS inflation.
    // PR_SET_THP_DISABLE is a Linux prctl option (value 41 in current kernels).
    if std::env::var_os("VELOFLUX_DISABLE_THP")
        .and_then(|v| v.to_str().map(|s| s != "0" && !s.eq_ignore_ascii_case("false")))
        .unwrap_or(false)
    {
        use std::os::raw::c_int;

        const PR_SET_THP_DISABLE: c_int = 41;
        extern "C" {
            fn prctl(option: c_int, arg2: usize, arg3: usize, arg4: usize, arg5: usize) -> c_int;
        }

        let rc = unsafe { prctl(PR_SET_THP_DISABLE, 1, 0, 0, 0) };
        if rc != 0 {
            eprintln!(
                "warning: failed to disable THP via prctl(PR_SET_THP_DISABLE): {}",
                std::io::Error::last_os_error()
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn maybe_disable_thp() {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    maybe_disable_thp();
    let result = veloflux::bootstrap::default_init()?;
    // Keep logging guard alive for the duration of the application
    let _logging_guard = result.logging_guard;

    let ctx = server::init(result.options, result.instance).await?;
    server::start(ctx).await
}
