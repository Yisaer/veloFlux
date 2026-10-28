//! Standalone converter timing and allocation probe; not a cargo bench target.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use arxml_converter::ArxmlCodec;

struct CountingAllocator;
static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn main() {
    let args: Vec<_> = std::env::args().collect();
    assert!(
        args.len() >= 3,
        "usage: decode_profile ARXML RAW_PAYLOAD [ITERATIONS]"
    );
    let iterations: usize = args.get(3).map(|n| n.parse().unwrap()).unwrap_or(2000);
    assert!(iterations > 0);
    let codec = ArxmlCodec::load(&args[1]).unwrap();
    let payload = std::fs::read(&args[2]).unwrap();
    let expected = codec.decode(0xAB04, 0x8003, &payload).unwrap();
    let checksum = format!("{expected:?}")
        .bytes()
        .fold(0xcbf29ce484222325u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
        });
    for _ in 0..64 {
        black_box(codec.decode(0xAB04, 0x8003, black_box(&payload)).unwrap());
    }
    ALLOCATIONS.store(0, Ordering::Relaxed);
    COUNTING.store(true, Ordering::Relaxed);
    let counted = codec.decode(0xAB04, 0x8003, &payload).unwrap();
    COUNTING.store(false, Ordering::Relaxed);
    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    assert_eq!(counted, expected);
    println!(
        "payload_bytes={} checksum={checksum:016x} allocations_per_decode={allocations}",
        payload.len()
    );
    for sample in 0..5 {
        let start = Instant::now();
        for _ in 0..iterations {
            black_box(codec.decode(0xAB04, 0x8003, black_box(&payload)).unwrap());
        }
        println!(
            "sample={sample} iterations={iterations} elapsed_ms={:.3}",
            start.elapsed().as_secs_f64() * 1000.0
        );
    }
}
