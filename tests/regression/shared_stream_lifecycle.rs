use super::{bind_manager_listener_or_skip, default_flow_instances, make_client, random_suffix};
use sdk::PipelineCreateRequest;
use sdk::StopOptions;
use sdk::StreamCreateRequest;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_stream_rapid_start_stop_cycles_via_rest() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = manager::new_default_flow_instance();

    let Some(listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let addr = listener.local_addr().expect("read listener addr");

    let server = tokio::spawn(async move {
        manager::start_server_with_listener(
            listener,
            instance,
            storage,
            default_flow_instances(),
            Vec::new(),
        )
        .await
        .expect("start manager server");
    });

    let client = make_client(addr);

    let stream_name = format!("reg_shared_stream_{}", random_suffix());
    let stream_req = StreamCreateRequest::mock_shared_i64_value(stream_name.clone());
    client
        .create_stream(&stream_req)
        .await
        .expect("create stream");

    let sql = format!("SELECT value FROM {stream_name}");

    for cycle in 0..5usize {
        let pipeline_a = format!("pipe_a_{cycle}_{}", random_suffix());
        let pipeline_b = format!("pipe_b_{cycle}_{}", random_suffix());

        let req_a = PipelineCreateRequest::nop(pipeline_a.clone(), sql.clone());
        let req_b = PipelineCreateRequest::nop(pipeline_b.clone(), sql.clone());

        client
            .create_pipeline(&req_a)
            .await
            .expect("create pipeline_a");
        client
            .create_pipeline(&req_b)
            .await
            .expect("create pipeline_b");

        client
            .start_pipeline(&pipeline_a)
            .await
            .expect("start pipeline_a");
        client
            .start_pipeline(&pipeline_b)
            .await
            .expect("start pipeline_b");

        tokio::time::sleep(Duration::from_millis(200)).await;

        let opt = StopOptions::graceful(5000);
        let stop_a = client.stop_pipeline(&pipeline_a, opt.clone());
        let stop_b = client.stop_pipeline(&pipeline_b, opt.clone());
        let (ra, rb) = tokio::join!(stop_a, stop_b);

        ra.expect("stop pipeline_a");
        rb.expect("stop pipeline_b");

        client
            .delete_pipeline(&pipeline_a)
            .await
            .expect("delete pipeline_a");
        client
            .delete_pipeline(&pipeline_b)
            .await
            .expect("delete pipeline_b");
    }

    client
        .delete_stream(&stream_name)
        .await
        .expect("delete stream");

    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_stream_slow_unsubscribe_during_restart_via_rest() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let storage = storage::StorageManager::new(temp_dir.path()).expect("create storage manager");
    let instance = manager::new_default_flow_instance();

    let Some(listener) = bind_manager_listener_or_skip().await else {
        return;
    };
    let addr = listener.local_addr().expect("read listener addr");

    let server = tokio::spawn(async move {
        manager::start_server_with_listener(
            listener,
            instance,
            storage,
            default_flow_instances(),
            Vec::new(),
        )
        .await
        .expect("start manager server");
    });

    let client = make_client(addr);

    let stream_name = format!("reg_shared_stream_slow_unsub_{}", random_suffix());
    let stream_req = StreamCreateRequest::mock_shared_i64_value(stream_name.clone());
    client
        .create_stream(&stream_req)
        .await
        .expect("create stream");

    let sql = format!("SELECT value FROM {stream_name}");
    let pipeline_a = format!("pipe_a_slow_unsub_{}", random_suffix());
    let pipeline_b_v1 = format!("pipe_b_v1_slow_unsub_{}", random_suffix());
    let pipeline_b_v2 = format!("pipe_b_v2_slow_unsub_{}", random_suffix());

    let req_a = PipelineCreateRequest::nop(pipeline_a.clone(), sql.clone());
    let req_b_v1 = PipelineCreateRequest::nop(pipeline_b_v1.clone(), sql.clone());

    client
        .create_pipeline(&req_a)
        .await
        .expect("create pipeline_a");
    client
        .create_pipeline(&req_b_v1)
        .await
        .expect("create pipeline_b_v1");

    client
        .start_pipeline(&pipeline_a)
        .await
        .expect("start pipeline_a");
    client
        .start_pipeline(&pipeline_b_v1)
        .await
        .expect("start pipeline_b_v1");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let client_for_slow = client.clone();
    let pipeline_a_for_slow = pipeline_a.clone();
    let slow_stop_a = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        client_for_slow
            .stop_pipeline(&pipeline_a_for_slow, StopOptions::graceful(5000))
            .await
    });

    client
        .stop_pipeline(&pipeline_b_v1, StopOptions::graceful(5000))
        .await
        .expect("stop pipeline_b_v1");
    client
        .delete_pipeline(&pipeline_b_v1)
        .await
        .expect("delete pipeline_b_v1");

    let req_b_v2 = PipelineCreateRequest::nop(pipeline_b_v2.clone(), sql.clone());
    client
        .create_pipeline(&req_b_v2)
        .await
        .expect("create pipeline_b_v2");
    client
        .start_pipeline(&pipeline_b_v2)
        .await
        .expect("start pipeline_b_v2");

    slow_stop_a
        .await
        .expect("slow stop task panicked")
        .expect("stop pipeline_a");

    client
        .stop_pipeline(&pipeline_b_v2, StopOptions::graceful(5000))
        .await
        .expect("stop pipeline_b_v2");
    client
        .delete_pipeline(&pipeline_b_v2)
        .await
        .expect("delete pipeline_b_v2");

    client
        .delete_pipeline(&pipeline_a)
        .await
        .expect("delete pipeline_a");

    client
        .delete_stream(&stream_name)
        .await
        .expect("delete stream");

    server.abort();
    let _ = server.await;
}
