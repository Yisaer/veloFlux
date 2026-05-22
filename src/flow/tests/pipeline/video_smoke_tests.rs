use flow::catalog::{
    StreamDecoderConfig, StreamDefinition, StreamProps, VideoReconnectConfig, VideoRtspTransport,
    VideoStreamProps,
};
use flow::pipeline::{
    NopSinkProps, PipelineDefinition, VideoCodec, VideoContainer, VideoRollingConfig,
    VideoSinkProps,
};
use flow::{
    CreatePipelineRequest, ExplainPipelineTarget, FlowInstance, PipelineError, SinkDefinition,
    SinkEncoderConfig, SinkProps, SinkType,
};
use serde_json::Map as JsonMap;
use std::process::Command;
use std::sync::Arc;

fn command_exists(name: &str) -> bool {
    Command::new("sh")
        .arg("-c")
        .arg(format!("command -v {name} >/dev/null 2>&1"))
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn pkg_config_exists(name: &str) -> bool {
    Command::new("pkg-config")
        .arg("--exists")
        .arg(name)
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[test]
fn video_rtsp_smoke_prerequisites_are_reported() {
    if std::env::var_os("VF_VIDEO_RTSP_SMOKE").is_none() {
        eprintln!(
            "skipping video RTSP smoke prerequisite check; set VF_VIDEO_RTSP_SMOKE=1 to enforce local tools"
        );
        return;
    }

    let mut missing = Vec::new();
    for command in ["ffmpeg", "mediamtx", "gst-inspect-1.0"] {
        if !command_exists(command) {
            missing.push(command.to_string());
        }
    }
    for package in ["gstreamer-1.0", "gstreamer-base-1.0", "gstreamer-app-1.0"] {
        if !pkg_config_exists(package) {
            missing.push(format!("pkg-config:{package}"));
        }
    }

    assert!(
        missing.is_empty(),
        "missing video RTSP smoke prerequisites: {}",
        missing.join(", ")
    );
}

async fn install_video_stream_for_planner_test(instance: &FlowInstance, stream_name: &str) {
    let stream = StreamDefinition::new(
        stream_name.to_string(),
        Arc::new(flow::codec::default_video_schema(stream_name)),
        StreamProps::Video(VideoStreamProps {
            url: "rtsp://127.0.0.1:8554/camera_1".to_string(),
            rtsp_transport: VideoRtspTransport::Tcp,
            reconnect: VideoReconnectConfig::default(),
        }),
        StreamDecoderConfig::none(),
    );
    instance
        .create_stream(stream, false)
        .await
        .expect("create video stream");
}

fn video_file_sink_for_planner_test() -> SinkDefinition {
    let sink_props = VideoSinkProps::new(
        "/tmp/veloflux-video/camera_1".to_string(),
        VideoRollingConfig::Duration { seconds: 10 },
    )
    .with_filename_prefix("camera_1")
    .with_codec(VideoCodec::H264)
    .with_container(VideoContainer::Mp4);

    SinkDefinition::new("video_sink", SinkType::Video, SinkProps::Video(sink_props))
        .with_encoder(SinkEncoderConfig::new("none", JsonMap::new()))
}

fn test_instance(name: &str) -> FlowInstance {
    FlowInstance::new(flow::instance::FlowInstanceOptions::shared_current_runtime(
        name, None,
    ))
    .expect("create flow instance")
}

// coverage-covers: sink.connector.video_output
#[tokio::test]
async fn flow_planner_rejects_payload_only_video_sink_projection() {
    let instance = test_instance("video-planner-payload-only");
    install_video_stream_for_planner_test(&instance, "camera_1").await;

    let pipeline = PipelineDefinition::new(
        "record_camera_1",
        "SELECT payload FROM camera_1",
        vec![video_file_sink_for_planner_test()],
    );
    let err = instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect_err("payload-only projection should not satisfy video sink tuple shape");

    match err {
        PipelineError::BuildFailure(message) => assert!(
            message.contains("video sink `video_sink` requires output column `width`"),
            "unexpected error: {message}"
        ),
        other => panic!("unexpected error: {other:?}"),
    }
}

// coverage-covers: source.video.tuple_input
#[tokio::test]
async fn flow_planner_accepts_video_with_regular_sink() {
    let instance = test_instance("video-planner-regular-sink");
    install_video_stream_for_planner_test(&instance, "camera_1").await;

    let pipeline = PipelineDefinition::new(
        "record_camera_1",
        "SELECT * FROM camera_1",
        vec![SinkDefinition::new(
            "nop_sink",
            SinkType::Nop,
            SinkProps::Nop(NopSinkProps::default()),
        )],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("video streams should plan like normal tuple streams");
}

// coverage-covers: source.video.tuple_input, sink.connector.video_output, planner.logical.video_sink_identity_project_elimination
#[tokio::test]
async fn flow_planner_eliminates_video_passthrough_project() {
    let instance = test_instance("video-planner-passthrough");
    install_video_stream_for_planner_test(&instance, "camera_1").await;

    let pipeline_id = "record_camera_1";
    let pipeline = PipelineDefinition::new(
        pipeline_id,
        "SELECT payload, width, height, format, timestamp FROM camera_1",
        vec![video_file_sink_for_planner_test()],
    );
    instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect("create video pipeline");

    let explain = instance
        .explain_pipeline(ExplainPipelineTarget::Id(pipeline_id))
        .expect("explain video pipeline")
        .to_json();
    let explain_json = explain.to_string();
    assert!(
        !explain_json.contains("\"operator\":\"Project\""),
        "logical video passthrough should eliminate Project: {explain_json}"
    );
    assert!(
        !explain_json.contains("\"operator\":\"PhysicalProject\""),
        "physical video passthrough should eliminate PhysicalProject: {explain_json}"
    );
    assert!(
        explain_json.contains("\"operator\":\"DataSource\"")
            && explain_json.contains("\"operator\":\"DataSink\""),
        "video passthrough plan should connect source to sink: {explain_json}"
    );
}

#[tokio::test]
async fn flow_planner_rejects_unsafe_video_filename_prefix() {
    let instance = test_instance("video-planner-prefix");
    install_video_stream_for_planner_test(&instance, "camera_1").await;

    let mut sink = video_file_sink_for_planner_test();
    let SinkProps::Video(props) = &mut sink.props else {
        panic!("expected video sink props");
    };
    props.filename_prefix = Some("camera/../escape".to_string());

    let pipeline = PipelineDefinition::new("record_camera_1", "SELECT * FROM camera_1", vec![sink]);
    let err = instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect_err("unsafe video filename prefix should fail planning");

    match err {
        PipelineError::BuildFailure(message) => assert_eq!(
            message,
            "invalid video sink filename_prefix for video_sink: filename_prefix must not contain path separators"
        ),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn flow_planner_rejects_zero_video_rolling_duration() {
    let instance = test_instance("video-planner-rolling");
    install_video_stream_for_planner_test(&instance, "camera_1").await;

    let mut sink = video_file_sink_for_planner_test();
    let SinkProps::Video(props) = &mut sink.props else {
        panic!("expected video sink props");
    };
    props.rolling = VideoRollingConfig::Duration { seconds: 0 };

    let pipeline = PipelineDefinition::new("record_camera_1", "SELECT * FROM camera_1", vec![sink]);
    let err = instance
        .create_pipeline(CreatePipelineRequest::new(pipeline))
        .expect_err("zero video rolling duration should fail planning");

    match err {
        PipelineError::BuildFailure(message) => assert_eq!(
            message,
            "sink video_sink video rolling duration requires positive seconds"
        ),
        other => panic!("unexpected error: {other:?}"),
    }
}
