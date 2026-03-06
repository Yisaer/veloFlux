use flow::connector::{MemoryData, MemoryTopicKind, DEFAULT_MEMORY_PUBSUB_CAPACITY};
use flow::{FlowInstance, FlowInstanceDedicatedRuntimeOptions};
use tokio::time::{timeout, Duration};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_pubsub_isolated_across_instances() {
    let instance_a = FlowInstance::new(flow::instance::FlowInstanceOptions::dedicated_runtime(
        "instance_a",
        None,
        FlowInstanceDedicatedRuntimeOptions::default(),
    ));
    let shared_registries = instance_a.shared_registries();
    let instance_b = FlowInstance::new(flow::instance::FlowInstanceOptions::dedicated_runtime(
        "instance_b",
        Some(shared_registries),
        FlowInstanceDedicatedRuntimeOptions::default(),
    ));

    let topic = "tests.memory_pubsub.isolation.bytes";
    instance_a
        .declare_memory_topic(
            topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare topic in instance_a");
    instance_b
        .declare_memory_topic(
            topic,
            MemoryTopicKind::Bytes,
            DEFAULT_MEMORY_PUBSUB_CAPACITY,
        )
        .expect("declare topic in instance_b");

    let mut sub_a = instance_a
        .open_memory_subscribe_bytes(topic)
        .expect("subscribe in instance_a");
    let mut sub_b = instance_b
        .open_memory_subscribe_bytes(topic)
        .expect("subscribe in instance_b");

    let pub_a = instance_a
        .open_memory_publisher_bytes(topic)
        .expect("open publisher in instance_a");
    pub_a
        .publish_bytes("from_instance_a")
        .expect("publish in a");

    let got_a = timeout(Duration::from_secs(2), sub_a.recv())
        .await
        .expect("receive in a timeout")
        .expect("receive in a failed");
    match got_a {
        MemoryData::Bytes(bytes) => assert_eq!(bytes.as_ref(), b"from_instance_a"),
        MemoryData::Collection(_) => panic!("expected bytes payload in instance_a"),
    }

    assert!(
        timeout(Duration::from_millis(200), sub_b.recv())
            .await
            .is_err(),
        "instance_b unexpectedly received message from instance_a"
    );

    let pub_b = instance_b
        .open_memory_publisher_bytes(topic)
        .expect("open publisher in instance_b");
    pub_b
        .publish_bytes("from_instance_b")
        .expect("publish in b");

    let got_b = timeout(Duration::from_secs(2), sub_b.recv())
        .await
        .expect("receive in b timeout")
        .expect("receive in b failed");
    match got_b {
        MemoryData::Bytes(bytes) => assert_eq!(bytes.as_ref(), b"from_instance_b"),
        MemoryData::Collection(_) => panic!("expected bytes payload in instance_b"),
    }

    assert!(
        timeout(Duration::from_millis(200), sub_a.recv())
            .await
            .is_err(),
        "instance_a unexpectedly received message from instance_b"
    );
}
