# CI And Benchmark Operations

This directory contains CI, benchmark, and automation operational notes.

## Android release

The Android ARM64 job in `.github/workflows/release.yml` uses NDK r28c
(`28.2.13676358`), which defaults to 16 KB ELF alignment. The minimum API
remains 24 (Android 7.0). The release uses the system allocator.

Before packaging, the job prints the ELF program headers with `llvm-readelf`
and runs `python3 scripts/ci/check_elf_16k.py <binary>`. The check rejects LOAD
segments with insufficient alignment or incongruent file and virtual offsets,
and RELRO layouts whose 16 KB page protection overlaps writable data.
This also accepts an isolated RELRO segment that ends within a page, provided
that the rest of the protected page contains no writable data.

ELF validation does not replace device testing. Before declaring Android 16
support, run startup and a representative pipeline on a 16 KB Android 16 device
(`adb shell getconf PAGE_SIZE` must report `16384`), and repeat on a supported
4 KB device, including the oldest Android version in use. Exercise profiling
as well, since it is enabled in the Android release. Keep API 24 when updating
the NDK to preserve the minimum supported Android version.

Physical hardware is optional for a smoke test. Android Studio's SDK Manager
offers experimental 16 KB page size system images; choose an ARM64 image to
run this ARM64 release, on a host supported by the emulator. ARM64 Cuttlefish
with 16 KB pages is another option. Confirm the guest page size with the
command above before testing. An x86_64 image alone does not validate the
ARM64 release binary. Run the same startup and pipeline checks in the virtual
device; ELF checks alone do not establish runtime compatibility.

Reference: [Android 16 KB page size guidance](https://developer.android.com/guide/practices/page-sizes).
