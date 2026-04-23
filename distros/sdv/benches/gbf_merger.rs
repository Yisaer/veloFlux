use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use flow::Merger;
use veloflux_sdv::codec::GbfMerger;
use veloflux_sdv::schema::gbf::GbfSchema;

fn get_bench_schema() -> GbfSchema {
    let json = r#"
    {
        "structure": {
            "type": "struct",
            "fields": [
                { "name": "ts", "type": "u64be" },
                { "name": "total_len", "type": "u16be" },
                {
                    "name": "frames",
                    "type": "sequence",
                    "structure": {
                        "type": "struct",
                        "fields": [
                            { "name": "magic", "type": "u8", "const": 85 },
                            { "name": "can_id", "type": "u16be" },
                            { "name": "data_len", "type": "u8" },
                            {
                                "name": "payload",
                                "type": "bytes",
                                "length_ref": "data_len",
                                "format": { "id_ref": "can_id" }
                            }
                        ]
                    },
                    "length_ref": "total_len",
                    "length_unit": "bytes"
                }
            ]
        }
    }
    "#;
    serde_json::from_str(json).expect("parse schema")
}

/// Create a GBF packet with the given number of frames and payload size
fn create_gbf_packet(num_frames: usize, payload_size: usize) -> Vec<u8> {
    let mut data = Vec::new();

    // Timestamp: 1000000000 = 0x3B9ACA00
    data.extend_from_slice(&0x3B9ACA00u64.to_be_bytes());

    // Calculate total frame bytes
    let frame_size = 1 + 2 + 1 + payload_size; // magic + can_id + data_len + payload
    let total_len = (num_frames * frame_size) as u16;
    data.extend_from_slice(&total_len.to_be_bytes());

    // Add frames
    for i in 0..num_frames {
        data.push(0x55); // magic
        data.extend_from_slice(&((0x0100 + i) as u16).to_be_bytes()); // can_id
        data.push(payload_size as u8); // data_len
        data.extend(vec![0x41u8 + (i % 26) as u8; payload_size]); // payload
    }

    data
}

fn bench_merge_only(c: &mut Criterion) {
    let schema = get_bench_schema();

    // Create 10 separate GBF packets, each with 1 frame
    let packets: Vec<Vec<u8>> = (0..10)
        .map(|i| {
            let mut data = Vec::new();
            data.extend_from_slice(&0x3B9ACA00u64.to_be_bytes());
            data.extend_from_slice(&8u16.to_be_bytes()); // 1 frame = 1+2+1+4 = 8 bytes
            data.push(0x55);
            data.extend_from_slice(&((0x0100 + i) as u16).to_be_bytes());
            data.push(4);
            data.extend_from_slice(b"ABCD");
            data
        })
        .collect();

    let mut merger = GbfMerger::new(schema).unwrap();

    c.bench_function("gbf_merger_merge_10_packets", |b| {
        b.iter(|| {
            merger.reset();
            for packet in &packets {
                merger.merge(black_box(packet)).unwrap();
            }
        })
    });
}

fn bench_round_trip(c: &mut Criterion) {
    let schema = get_bench_schema();

    // Create one GBF packet with 10 frames
    let packet = create_gbf_packet(10, 8);

    let mut merger = GbfMerger::new(schema).unwrap();

    c.bench_function("gbf_merger_round_trip_10_frames", |b| {
        b.iter(|| {
            merger.reset();
            merger.merge(black_box(&packet)).unwrap();
            black_box(merger.trigger().unwrap())
        })
    });
}

fn bench_payload_sizes(c: &mut Criterion) {
    let schema = get_bench_schema();

    let mut group = c.benchmark_group("payload_size");

    for payload_size in [8, 64, 255].iter() {
        // Create packet with 10 frames of given payload size
        let packet = create_gbf_packet(10, *payload_size);

        let mut merger = GbfMerger::new(schema.clone()).unwrap();

        group.bench_with_input(
            BenchmarkId::new("round_trip", payload_size),
            payload_size,
            |b, _| {
                b.iter(|| {
                    merger.reset();
                    merger.merge(black_box(&packet)).unwrap();
                    black_box(merger.trigger().unwrap())
                })
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_merge_only,
    bench_round_trip,
    bench_payload_sizes
);
criterion_main!(benches);
