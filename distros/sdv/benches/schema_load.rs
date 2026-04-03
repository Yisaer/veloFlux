//! Benchmarks for comparing JSON vs DBC schema loading performance.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::path::PathBuf;

use veloflux_sdv::schema::dbc::{load_can_schema, load_dbc_json};

fn benchmark_schema_load(c: &mut Criterion) {
    let json_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/comprehensive.json");
    let dbc_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/tests/1_TestBus.dbc");

    let mut group = c.benchmark_group("schema_load");

    group.bench_function("json", |b| {
        b.iter(|| {
            black_box(load_dbc_json(json_path.to_str().unwrap()).unwrap());
        });
    });

    group.bench_function("dbc", |b| {
        b.iter(|| {
            black_box(load_can_schema(dbc_path.to_str().unwrap()).unwrap());
        });
    });

    group.finish();
}

criterion_group!(benches, benchmark_schema_load);
criterion_main!(benches);
