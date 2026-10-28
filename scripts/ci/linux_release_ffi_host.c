#define _POSIX_C_SOURCE 200809L
#include "veloflux_ffi.h"
#include <signal.h>
#include <stdio.h>
#include <time.h>

static volatile sig_atomic_t stopping;

static void stop(int signal_number) {
    (void)signal_number;
    stopping = 1;
}

int main(int argc, char **argv) {
    if (argc != 2) return 2;
    signal(SIGTERM, stop);
    signal(SIGINT, stop);
    veloflux_handle_t *handle = NULL;
    int result = veloflux_start(argv[1], &handle);
    if (result != VELOFLUX_FFI_OK || handle == NULL) return 3;
    while (!stopping) {
        const struct timespec interval = {0, 100000000};
        nanosleep(&interval, NULL);
    }
    result = veloflux_stop(&handle);
    if (result != VELOFLUX_FFI_OK || handle != NULL) return 4;
    return 0;
}
