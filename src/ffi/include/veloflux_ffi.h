#ifndef VELOFLUX_FFI_H
#define VELOFLUX_FFI_H

#ifdef __cplusplus
extern "C" {
#endif

#define VELOFLUX_FFI_OK 0
#define VELOFLUX_FFI_INVALID_ARGUMENT 1
#define VELOFLUX_FFI_START_FAILED 2
#define VELOFLUX_FFI_STOP_FAILED 3
#define VELOFLUX_FFI_PANIC 255

typedef struct veloflux_handle veloflux_handle_t;

/*
 * Start an embedded veloFlux manager runtime in the current process.
 *
 * `config_path` must point to a NUL-terminated UTF-8 path string.
 * On success, `*out_handle` receives a non-null opaque handle and the
 * function returns `VELOFLUX_FFI_OK`.
 */
int veloflux_start(const char* config_path, veloflux_handle_t** out_handle);

/*
 * Stop a previously started embedded veloFlux runtime.
 *
 * `handle` must point to the caller-owned handle variable. On success the
 * runtime is fully stopped and `*handle` is set to NULL.
 */
int veloflux_stop(veloflux_handle_t** handle);

#ifdef __cplusplus
}
#endif

#endif /* VELOFLUX_FFI_H */
