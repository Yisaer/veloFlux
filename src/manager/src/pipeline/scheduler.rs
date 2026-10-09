use chrono::Utc;
use croner::{
    Cron,
    parser::{CronParser, Seconds, Year},
};
use std::time::Duration;
use storage::StoredPipelineDesiredState;

use super::types::{PipelineDatetimeRangeRequest, PipelineScheduleRequest, ScheduleStatus};
use crate::storage_bridge;

pub(crate) fn schedule_has_future_datetime_range(
    schedule_config: &PipelineScheduleRequest,
    now: chrono::DateTime<Utc>,
) -> bool {
    let now_ms = now.timestamp_millis();
    schedule_config
        .datetime_ranges
        .iter()
        .any(|range| range.end_timestamp_ms > now_ms)
}

pub(crate) fn schedule_has_ended(
    schedule_config: &PipelineScheduleRequest,
    now: chrono::DateTime<Utc>,
) -> bool {
    !schedule_config.datetime_ranges.is_empty()
        && !schedule_has_future_datetime_range(schedule_config, now)
}

/// Parse a Linux-compatible 5-field cron expression.
fn parse_cron_expression(expr: &str) -> Result<Cron, String> {
    let trimmed = expr.trim();
    if trimmed.is_empty() {
        return Err("cron expression must not be empty".to_string());
    }

    if trimmed.eq_ignore_ascii_case("@reboot") {
        return Err("@reboot is not supported for recurring pipeline schedules".to_string());
    }

    let normalized = if trimmed.eq_ignore_ascii_case("@midnight") {
        "@daily"
    } else {
        trimmed
    };

    CronParser::builder()
        .seconds(Seconds::Disallowed)
        .year(Year::Disallowed)
        .build()
        .parse(normalized)
        .map_err(|err| format!("invalid cron expression: {err}"))
}

/// Validate a Linux-compatible 5-field cron expression.
pub(crate) fn validate_cron_expression(expr: &str) -> Result<(), String> {
    parse_cron_expression(expr).map(|_| ())
}

/// Return the end of the cron window containing `now`.
///
/// A cron window is open on both sides to match eKuiper schedule semantics:
/// `fire < now < fire + duration_secs`.
fn active_cron_window_end_ms(
    schedule: &Cron,
    now: chrono::DateTime<Utc>,
    duration_secs: u64,
) -> Result<Option<i64>, String> {
    let duration_ms = i64::try_from(duration_secs)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1000))
        .ok_or_else(|| "schedule duration is too large".to_string())?;
    let search_now = chrono::DateTime::from_timestamp(now.timestamp(), 0)
        .ok_or_else(|| "schedule evaluation time is out of range".to_string())?;
    // Cron occurrences have second precision. Include the truncated search second only when
    // `now` is already past it; at an exact fire boundary, select the preceding occurrence so
    // an overlapping earlier window remains active.
    let include_search_second = search_now < now;
    let Some(fire) = schedule
        .find_previous_occurrence(&search_now, include_search_second)
        .ok()
    else {
        return Ok(None);
    };
    let fire_ms = fire.timestamp_millis();
    let window_end_ms = fire_ms
        .checked_add(duration_ms)
        .ok_or_else(|| "schedule window end is out of range".to_string())?;
    let now_ms = now.timestamp_millis();
    Ok((fire < now && now_ms < window_end_ms).then_some(window_end_ms))
}

fn datetime_range_contains(range: &PipelineDatetimeRangeRequest, timestamp_ms: i64) -> bool {
    range.begin_timestamp_ms <= timestamp_ms && timestamp_ms < range.end_timestamp_ms
}

fn active_datetime_range_end_ms(
    schedule_config: &PipelineScheduleRequest,
    now: chrono::DateTime<Utc>,
) -> Option<i64> {
    let now_ms = now.timestamp_millis();
    schedule_config
        .datetime_ranges
        .iter()
        .filter(|range| datetime_range_contains(range, now_ms))
        .map(|range| range.end_timestamp_ms)
        .max()
}

struct ScheduleEvaluation {
    active_window_end_ms: Option<i64>,
    previous_fire_at: Option<chrono::DateTime<Utc>>,
    next_fire_at: Option<chrono::DateTime<Utc>>,
}

fn evaluate_schedule_at(
    schedule_config: &PipelineScheduleRequest,
    now: chrono::DateTime<Utc>,
) -> Result<ScheduleEvaluation, String> {
    let cron = match (&schedule_config.cron, schedule_config.duration_secs) {
        (Some(expression), Some(duration_secs)) => {
            Some((parse_cron_expression(expression)?, duration_secs))
        }
        (None, None) if !schedule_config.datetime_ranges.is_empty() => None,
        (Some(_), None) => return Err("schedule duration is missing".to_string()),
        (None, Some(_)) => return Err("schedule cron expression is missing".to_string()),
        (None, None) => return Err("schedule has no run window".to_string()),
    };

    let active_range_end_ms = active_datetime_range_end_ms(schedule_config, now);
    let has_datetime_restriction = !schedule_config.datetime_ranges.is_empty();

    let (active_window_end_ms, previous_fire_at, next_fire_at) =
        if let Some((cron, duration_secs)) = cron {
            let cron_window_end_ms = active_cron_window_end_ms(&cron, now, duration_secs)?;
            let active_window_end_ms = cron_window_end_ms.and_then(|cron_end| {
                if has_datetime_restriction {
                    active_range_end_ms.map(|range_end| cron_end.min(range_end))
                } else {
                    Some(cron_end)
                }
            });
            let search_now = chrono::DateTime::from_timestamp(now.timestamp(), 0)
                .ok_or_else(|| "schedule evaluation time is out of range".to_string())?;
            let previous_fire_at = cron.find_previous_occurrence(&search_now, true).ok();
            let next_fire_at = cron.find_next_occurrence(&search_now, false).ok();
            (active_window_end_ms, previous_fire_at, next_fire_at)
        } else {
            (active_range_end_ms, None, None)
        };

    Ok(ScheduleEvaluation {
        active_window_end_ms,
        previous_fire_at,
        next_fire_at,
    })
}

/// Compute the `ScheduleStatus` for a pipeline at the current time.
pub(crate) fn compute_schedule_status(schedule_config: &PipelineScheduleRequest) -> ScheduleStatus {
    compute_schedule_status_at(schedule_config, Utc::now())
}

fn compute_schedule_status_at(
    schedule_config: &PipelineScheduleRequest,
    now: chrono::DateTime<Utc>,
) -> ScheduleStatus {
    let evaluation = evaluate_schedule_at(schedule_config, now).ok();
    let active_window_end_ms = evaluation
        .as_ref()
        .and_then(|evaluation| evaluation.active_window_end_ms);
    let auto_stop_at = active_window_end_ms
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map(|t| t.to_rfc3339());

    ScheduleStatus {
        cron: schedule_config.cron.clone(),
        duration_secs: schedule_config.duration_secs,
        datetime_ranges: schedule_config.datetime_ranges.clone(),
        in_window: active_window_end_ms.is_some(),
        previous_fire_at: evaluation
            .as_ref()
            .and_then(|evaluation| evaluation.previous_fire_at)
            .map(|time| time.to_rfc3339()),
        next_fire_at: evaluation
            .and_then(|evaluation| evaluation.next_fire_at)
            .map(|time| time.to_rfc3339()),
        auto_stop_at,
    }
}

/// The patrol loop that periodically checks all pipelines with schedules
/// and reconciles their actual state with the expected schedule windows.
pub(crate) async fn run_patrol(state: super::state::AppState, interval: Duration) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await;

    loop {
        tick.tick().await;
        let pipelines = match state.storage.list_pipelines() {
            Ok(p) => p,
            Err(err) => {
                tracing::error!(%err, "patrol: failed to list pipelines");
                continue;
            }
        };

        for stored in &pipelines {
            patrol_pipeline(&stored.id, &state).await;
        }
    }
}

#[derive(Clone, Copy)]
enum PatrolAction {
    Start,
    Stop,
}

async fn patrol_pipeline(pipeline_id: &str, state: &super::state::AppState) {
    if patrol_action(pipeline_id, state).is_none() {
        return;
    }
    let _metadata_permit = match state.acquire_metadata_operation().await {
        Ok(permit) => permit,
        Err(err) => {
            tracing::error!(%err, "patrol: metadata operation guard closed");
            return;
        }
    };
    if crate::attachments::needs_recovery(&state.storage) {
        return;
    }
    let _permit = match state.try_acquire_pipeline_op(pipeline_id).await {
        Ok(permit) => permit,
        Err(tokio::sync::TryAcquireError::NoPermits) => {
            tracing::debug!(pipeline_id, "patrol: pipeline operation is busy");
            return;
        }
        Err(tokio::sync::TryAcquireError::Closed) => {
            tracing::error!(pipeline_id, "patrol: pipeline operation guard closed");
            return;
        }
    };
    // Configuration and runtime may have changed while waiting for the boundary.
    let Some((instance, action)) = patrol_action(pipeline_id, state) else {
        return;
    };
    match action {
        PatrolAction::Start => {
            tracing::info!(pipeline_id, "patrol: auto-starting pipeline");
            if let Err(err) = instance.start_pipeline(pipeline_id).await {
                tracing::error!(pipeline_id, %err, "patrol: failed to auto-start pipeline");
            }
        }
        PatrolAction::Stop => {
            tracing::info!(
                pipeline_id,
                "patrol: auto-stopping pipeline (scheduled window ended)"
            );
            if let Err(err) = instance
                .stop_pipeline(
                    pipeline_id,
                    flow::pipeline::PipelineStopMode::Graceful,
                    Duration::from_secs(30),
                )
                .await
            {
                tracing::error!(pipeline_id, %err, "patrol: failed to auto-stop pipeline");
            }
        }
    }
}

fn patrol_action(
    pipeline_id: &str,
    state: &super::state::AppState,
) -> Option<(std::sync::Arc<flow::FlowInstance>, PatrolAction)> {
    let storage = state.storage.as_ref();
    let instances = &state.instances;
    let stored = match storage.get_pipeline(pipeline_id) {
        Ok(Some(stored)) => stored,
        Ok(None) => return None,
        Err(err) => {
            tracing::warn!(pipeline_id, %err, "patrol: failed to read pipeline");
            return None;
        }
    };
    let req = match storage_bridge::pipeline_request_from_stored(&stored) {
        Ok(r) => r,
        Err(err) => {
            tracing::warn!(pipeline_id, %err, "patrol: failed to decode stored pipeline");
            return None;
        }
    };

    let Some(schedule_config) = &req.options.schedule else {
        return None;
    };

    let now = Utc::now();
    let evaluation = match evaluate_schedule_at(schedule_config, now) {
        Ok(evaluation) => evaluation,
        Err(err) => {
            tracing::warn!(
                pipeline_id,
                cron = ?schedule_config.cron,
                %err,
                "patrol: invalid schedule configuration"
            );
            return None;
        }
    };
    let in_window = evaluation.active_window_end_ms.is_some();
    let current_desired_state = match storage.get_pipeline_run_state(pipeline_id) {
        Ok(Some(state)) => state.desired_state,
        Ok(None) => StoredPipelineDesiredState::Stopped,
        Err(err) => {
            tracing::warn!(pipeline_id, %err, "patrol: failed to read run state");
            return None;
        }
    };

    if matches!(current_desired_state, StoredPipelineDesiredState::Stopped) {
        tracing::debug!(
            pipeline_id,
            "patrol: scheduled pipeline is manually stopped"
        );
        return None;
    }

    let flow_instance_id = req
        .flow_instance_id
        .as_deref()
        .unwrap_or(crate::instances::DEFAULT_FLOW_INSTANCE_ID);

    let Some(instance) = instances.get(flow_instance_id) else {
        tracing::debug!(
            pipeline_id,
            flow_instance_id,
            "patrol: flow instance not available"
        );
        return None;
    };

    let runtime_status = instance
        .list_pipelines()
        .into_iter()
        .find(|snapshot| snapshot.definition.id() == pipeline_id)
        .map(|snapshot| snapshot.status);

    if matches!(
        runtime_status,
        Some(flow::pipeline::PipelineStatus::Stopping)
    ) {
        tracing::debug!(pipeline_id, "patrol: pipeline stop is still in progress");
        return None;
    }

    if matches!(runtime_status, Some(flow::pipeline::PipelineStatus::Failed)) {
        tracing::debug!(
            pipeline_id,
            "patrol: skipping pipeline with a runtime failure until explicit recovery"
        );
        return None;
    }

    let is_running = matches!(
        runtime_status,
        Some(flow::pipeline::PipelineStatus::Running)
    );

    if in_window && !is_running {
        Some((instance, PatrolAction::Start))
    } else if !in_window && is_running {
        Some((instance, PatrolAction::Stop))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn schedule(
        cron: &str,
        duration_secs: u64,
        datetime_ranges: Vec<PipelineDatetimeRangeRequest>,
    ) -> PipelineScheduleRequest {
        PipelineScheduleRequest {
            cron: Some(cron.to_string()),
            duration_secs: Some(duration_secs),
            datetime_ranges,
        }
    }

    fn datetime_range_schedule(
        datetime_ranges: Vec<PipelineDatetimeRangeRequest>,
    ) -> PipelineScheduleRequest {
        PipelineScheduleRequest {
            cron: None,
            duration_secs: None,
            datetime_ranges,
        }
    }

    #[test]
    fn schedule_status_requires_datetime_range_match() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 10, 0, 10).unwrap();
        let req = schedule(
            "* * * * *",
            30,
            vec![PipelineDatetimeRangeRequest {
                begin_timestamp_ms: now.timestamp_millis() + 60_000,
                end_timestamp_ms: now.timestamp_millis() + 120_000,
            }],
        );

        let status = compute_schedule_status_at(&req, now);

        assert!(!status.in_window);
        assert_eq!(status.auto_stop_at, None);
    }

    #[test]
    fn schedule_status_clips_auto_stop_at_to_datetime_range_end() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 10, 1, 0).unwrap();
        let range_end = Utc.with_ymd_and_hms(2026, 1, 1, 10, 2, 0).unwrap();
        let req = schedule(
            "0 10 * * *",
            300,
            vec![PipelineDatetimeRangeRequest {
                begin_timestamp_ms: Utc
                    .with_ymd_and_hms(2026, 1, 1, 10, 0, 0)
                    .unwrap()
                    .timestamp_millis(),
                end_timestamp_ms: range_end.timestamp_millis(),
            }],
        );

        let status = compute_schedule_status_at(&req, now);

        assert!(status.in_window);
        assert_eq!(status.auto_stop_at, Some(range_end.to_rfc3339()));
    }

    #[test]
    fn schedule_status_without_datetime_ranges_uses_cron_window() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 10, 0, 10).unwrap();
        let req = schedule("* * * * *", 30, Vec::new());

        let status = compute_schedule_status_at(&req, now);

        assert!(status.in_window);
        assert_eq!(
            status.auto_stop_at,
            Some(
                Utc.with_ymd_and_hms(2026, 1, 1, 10, 0, 30)
                    .unwrap()
                    .to_rfc3339()
            )
        );
    }

    #[test]
    fn datetime_range_only_schedule_uses_left_closed_right_open_boundaries() {
        let begin = Utc.with_ymd_and_hms(2026, 1, 1, 10, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2026, 1, 1, 11, 0, 0).unwrap();
        let req = datetime_range_schedule(vec![PipelineDatetimeRangeRequest {
            begin_timestamp_ms: begin.timestamp_millis(),
            end_timestamp_ms: end.timestamp_millis(),
        }]);

        for (now, expected) in [
            (begin - chrono::Duration::milliseconds(1), false),
            (begin, true),
            (begin + chrono::Duration::milliseconds(1), true),
            (end, false),
            (end + chrono::Duration::milliseconds(1), false),
        ] {
            assert_eq!(
                compute_schedule_status_at(&req, now).in_window,
                expected,
                "unexpected range-only result at {now}"
            );
        }

        let status = compute_schedule_status_at(&req, begin + chrono::Duration::seconds(1));
        assert_eq!(status.cron, None);
        assert_eq!(status.duration_secs, None);
        assert_eq!(status.previous_fire_at, None);
        assert_eq!(status.next_fire_at, None);
        assert_eq!(status.auto_stop_at, Some(end.to_rfc3339()));
    }

    #[test]
    fn datetime_range_only_schedule_enters_scheduled_end_after_last_range() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        let end = now - chrono::Duration::seconds(1);
        let req = datetime_range_schedule(vec![PipelineDatetimeRangeRequest {
            begin_timestamp_ms: (end - chrono::Duration::hours(1)).timestamp_millis(),
            end_timestamp_ms: end.timestamp_millis(),
        }]);

        let evaluation = compute_schedule_status_at(&req, now);
        assert!(!evaluation.in_window);
        assert!(!schedule_has_future_datetime_range(&req, now));
        assert!(schedule_has_ended(&req, now));
    }

    #[test]
    fn datetime_range_only_schedule_matches_any_range() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        let second_end = now + chrono::Duration::hours(1);
        let req = datetime_range_schedule(vec![
            PipelineDatetimeRangeRequest {
                begin_timestamp_ms: (now - chrono::Duration::hours(2)).timestamp_millis(),
                end_timestamp_ms: (now - chrono::Duration::hours(1)).timestamp_millis(),
            },
            PipelineDatetimeRangeRequest {
                begin_timestamp_ms: (now - chrono::Duration::hours(1)).timestamp_millis(),
                end_timestamp_ms: second_end.timestamp_millis(),
            },
        ]);

        let status = compute_schedule_status_at(&req, now);

        assert!(status.in_window);
        assert_eq!(status.auto_stop_at, Some(second_end.to_rfc3339()));
    }

    #[test]
    fn cron_schedule_uses_open_boundaries() {
        let fire = Utc.with_ymd_and_hms(2026, 1, 1, 10, 0, 0).unwrap();
        let req = schedule("0 10 * * *", 30, Vec::new());

        assert!(!compute_schedule_status_at(&req, fire).in_window);
        assert!(
            compute_schedule_status_at(&req, fire + chrono::Duration::milliseconds(1)).in_window
        );
        assert!(!compute_schedule_status_at(&req, fire + chrono::Duration::seconds(30)).in_window);
    }

    #[test]
    fn cron_schedule_keeps_overlapping_window_active_at_next_fire() {
        let next_fire = Utc.with_ymd_and_hms(2026, 1, 1, 10, 10, 0).unwrap();
        let req = schedule("*/10 * * * *", 15 * 60, Vec::new());

        let at_fire = compute_schedule_status_at(&req, next_fire);
        assert!(at_fire.in_window);
        assert_eq!(
            at_fire.auto_stop_at,
            Some(
                Utc.with_ymd_and_hms(2026, 1, 1, 10, 15, 0)
                    .unwrap()
                    .to_rfc3339()
            )
        );

        let after_fire =
            compute_schedule_status_at(&req, next_fire + chrono::Duration::milliseconds(1));
        assert!(after_fire.in_window);
        assert_eq!(
            after_fire.auto_stop_at,
            Some(
                Utc.with_ymd_and_hms(2026, 1, 1, 10, 25, 0)
                    .unwrap()
                    .to_rfc3339()
            )
        );
    }

    #[test]
    fn linux_weekday_numbers_use_sunday_zero_or_seven_and_monday_one() {
        let saturday = Utc.with_ymd_and_hms(2026, 1, 3, 23, 59, 0).unwrap();
        let sunday = Utc.with_ymd_and_hms(2026, 1, 4, 0, 0, 0).unwrap();
        let monday = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();

        for expression in ["0 0 * * 0", "0 0 * * 7"] {
            let schedule = parse_cron_expression(expression).expect("parse Sunday schedule");
            assert_eq!(
                schedule
                    .find_next_occurrence(&saturday, false)
                    .expect("find Sunday occurrence"),
                sunday
            );
        }

        let schedule = parse_cron_expression("0 0 * * 1").expect("parse Monday schedule");
        assert_eq!(
            schedule
                .find_next_occurrence(&saturday, false)
                .expect("find Monday occurrence"),
            monday
        );
    }

    #[test]
    fn linux_day_of_month_and_day_of_week_use_or_semantics() {
        let schedule =
            parse_cron_expression("30 4 1,15 * FRI").expect("parse combined day schedule");
        let before_month_day = Utc.with_ymd_and_hms(2026, 1, 1, 4, 29, 0).unwrap();
        let month_day = Utc.with_ymd_and_hms(2026, 1, 1, 4, 30, 0).unwrap();
        let friday = Utc.with_ymd_and_hms(2026, 1, 2, 4, 30, 0).unwrap();

        assert_eq!(
            schedule
                .find_next_occurrence(&before_month_day, false)
                .expect("find day-of-month occurrence"),
            month_day
        );
        assert_eq!(
            schedule
                .find_next_occurrence(&month_day, false)
                .expect("find day-of-week occurrence"),
            friday
        );
    }

    #[test]
    fn linux_cron_parser_accepts_supported_forms_and_rejects_invalid_forms() {
        for expression in [
            "*/10 8-17 * JAN,MAR MON-FRI",
            "@yearly",
            "@annually",
            "@monthly",
            "@weekly",
            "@daily",
            "@midnight",
            "@hourly",
        ] {
            validate_cron_expression(expression).expect("accept supported cron expression");
        }

        for expression in ["0 0 * *", "0 0 0 * * *", "0 0 * * 8", "@reboot"] {
            assert!(
                validate_cron_expression(expression).is_err(),
                "expected expression to be rejected: {expression}"
            );
        }
    }

    async fn scheduled_patrol_state() -> (
        tempfile::TempDir,
        super::super::state::AppState,
        super::super::types::CreatePipelineRequest,
    ) {
        use flow::catalog::{
            MemoryStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps,
        };
        let temp = tempfile::tempdir().expect("temp directory");
        let state = super::super::state::AppState::new(
            crate::new_default_flow_instance(),
            storage::StorageManager::new(temp.path()).expect("storage"),
            vec![crate::FlowInstanceSpec {
                id: "default".to_string(),
                ..Default::default()
            }],
            0,
        )
        .expect("state");
        let instance = state.instances.default_instance();
        instance
            .declare_memory_topic("patrol_input", flow::connector::MemoryTopicKind::Bytes, 16)
            .expect("topic");
        instance
            .create_stream(
                StreamDefinition::new(
                    "src",
                    std::sync::Arc::new(flow::Schema::new(Vec::new())),
                    StreamProps::Memory(MemoryStreamProps::new("patrol_input")),
                    StreamDecoderConfig::json(),
                ),
                false,
            )
            .await
            .expect("stream");
        let now = Utc::now();
        let request: super::super::types::CreatePipelineRequest =
            serde_json::from_value(serde_json::json!({
                "id":"scheduled", "revision":1, "sql":"SELECT * FROM src",
                "sinks":[{"type":"nop", "props":{}, "encoder":{"type":"json","props":{}}}],
                "options":{"schedule":{"datetime_ranges":[{
                    "begin_timestamp_ms":(now - chrono::Duration::hours(1)).timestamp_millis(),
                    "end_timestamp_ms":(now + chrono::Duration::hours(1)).timestamp_millis()
                }]}}
            }))
            .expect("request");
        let stored = storage_bridge::stored_pipeline_from_request(&request).expect("stored");
        state
            .storage
            .create_pipeline(stored.clone())
            .expect("persist");
        state
            .storage
            .put_pipeline_run_state(storage::StoredPipelineRunState {
                pipeline_id: request.id.clone(),
                desired_state: StoredPipelineDesiredState::Running,
            })
            .expect("intent");
        crate::storage_apply::install_pipeline(&state, &stored)
            .await
            .expect("stopped scheduled runtime");
        (temp, state, request)
    }

    #[tokio::test]
    async fn patrol_does_not_lock_idle_pipelines_and_rechecks_waiting_operations() {
        for change in ["manual_stop", "schedule", "delete"] {
            let (_temp, state, mut request) = scheduled_patrol_state().await;
            let instance = state.instances.default_instance();
            let stored = state.storage.get_pipeline(&request.id).unwrap().unwrap();
            let exclusive = state
                .try_acquire_metadata_operation(true)
                .expect("exclusive");
            state
                .storage
                .put_pipeline_run_state(storage::StoredPipelineRunState {
                    pipeline_id: request.id.clone(),
                    desired_state: StoredPipelineDesiredState::Stopped,
                })
                .unwrap();
            tokio::time::timeout(
                Duration::from_millis(100),
                patrol_pipeline(&stored.id, &state),
            )
            .await
            .expect("idle patrol must not wait for metadata");
            state
                .storage
                .put_pipeline_run_state(storage::StoredPipelineRunState {
                    pipeline_id: request.id.clone(),
                    desired_state: StoredPipelineDesiredState::Running,
                })
                .unwrap();
            let waiting_state = state.clone();
            let waiting =
                tokio::spawn(async move { patrol_pipeline(&stored.id, &waiting_state).await });
            tokio::task::yield_now().await;
            assert!(
                !waiting.is_finished(),
                "active patrol must wait for metadata"
            );
            assert_eq!(
                instance.list_pipelines()[0].status,
                flow::pipeline::PipelineStatus::Stopped
            );
            match change {
                "manual_stop" => state
                    .storage
                    .put_pipeline_run_state(storage::StoredPipelineRunState {
                        pipeline_id: request.id.clone(),
                        desired_state: StoredPipelineDesiredState::Stopped,
                    })
                    .unwrap(),
                "schedule" => {
                    request.revision = 2;
                    let now = Utc::now();
                    request.options.schedule = Some(datetime_range_schedule(vec![
                        PipelineDatetimeRangeRequest {
                            begin_timestamp_ms: (now + chrono::Duration::hours(1))
                                .timestamp_millis(),
                            end_timestamp_ms: (now + chrono::Duration::hours(2)).timestamp_millis(),
                        },
                    ]));
                    let mut snapshot = state.storage.export_metadata_snapshot().unwrap();
                    snapshot.pipelines[0] =
                        storage_bridge::stored_pipeline_from_request(&request).unwrap();
                    state
                        .storage
                        .commit_metadata_snapshot(snapshot, Default::default())
                        .unwrap();
                }
                "delete" => {
                    state.storage.delete_pipeline(&request.id).unwrap();
                }
                _ => unreachable!(),
            }
            drop(exclusive);
            tokio::time::timeout(Duration::from_secs(5), waiting)
                .await
                .expect("patrol resumes")
                .unwrap();
            assert_eq!(
                instance.list_pipelines()[0].status,
                flow::pipeline::PipelineStatus::Stopped,
                "{change}"
            );
            let _exclusive = state
                .try_acquire_metadata_operation(true)
                .expect("permit released after cancellation");
        }
    }

    #[tokio::test]
    async fn patrol_releases_metadata_after_each_start_and_stop() {
        let (_temp, state, mut request) = scheduled_patrol_state().await;
        let instance = state.instances.default_instance();
        let stored = state.storage.get_pipeline(&request.id).unwrap().unwrap();
        patrol_pipeline(&stored.id, &state).await;
        assert_eq!(
            instance.list_pipelines()[0].status,
            flow::pipeline::PipelineStatus::Running
        );
        let exclusive = state
            .try_acquire_metadata_operation(true)
            .expect("start released metadata");
        // A running pipeline inside its window requires no operation or metadata permit.
        tokio::time::timeout(
            Duration::from_millis(100),
            patrol_pipeline(&stored.id, &state),
        )
        .await
        .expect("running patrol remains idle");
        let now = Utc::now();
        request.revision = 2;
        request.options.schedule = Some(datetime_range_schedule(vec![
            PipelineDatetimeRangeRequest {
                begin_timestamp_ms: (now - chrono::Duration::hours(2)).timestamp_millis(),
                end_timestamp_ms: (now - chrono::Duration::hours(1)).timestamp_millis(),
            },
        ]));
        let stored = storage_bridge::stored_pipeline_from_request(&request).unwrap();
        let mut snapshot = state.storage.export_metadata_snapshot().unwrap();
        snapshot.pipelines[0] = stored.clone();
        state
            .storage
            .commit_metadata_snapshot(snapshot, Default::default())
            .unwrap();
        let waiting_state = state.clone();
        let waiting =
            tokio::spawn(async move { patrol_pipeline(&stored.id, &waiting_state).await });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        assert_eq!(
            instance.list_pipelines()[0].status,
            flow::pipeline::PipelineStatus::Running
        );
        drop(exclusive);
        tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("stop resumes")
            .unwrap();
        assert_eq!(
            instance.list_pipelines()[0].status,
            flow::pipeline::PipelineStatus::Stopped
        );
        let _exclusive = state
            .try_acquire_metadata_operation(true)
            .expect("stop released metadata");
    }

    #[tokio::test(start_paused = true)]
    async fn patrol_skips_missed_ticks_without_repeating_complete_scans() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tracing::instrument::WithSubscriber;

        struct ScanWarnings(Arc<AtomicUsize>);
        impl tracing::Subscriber for ScanWarnings {
            fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
                metadata.level() == &tracing::Level::WARN
                    && metadata.target().ends_with("pipeline::scheduler")
            }
            fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
                tracing::span::Id::from_u64(1)
            }
            fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
            fn event(&self, event: &tracing::Event<'_>) {
                if self.enabled(event.metadata()) {
                    self.0.fetch_add(1, Ordering::SeqCst);
                }
            }
            fn enter(&self, _: &tracing::span::Id) {}
            fn exit(&self, _: &tracing::span::Id) {}
        }

        let (_temp, state, _) = scheduled_patrol_state().await;
        let mut snapshot = state.storage.export_metadata_snapshot().unwrap();
        snapshot.pipeline_run_states.clear();
        snapshot.pipelines[0].raw_json = "invalid JSON".to_string();
        state
            .storage
            .commit_metadata_snapshot(snapshot, Default::default())
            .unwrap();
        // This invalid stored definition emits one existing warning per real scan.
        // Observe the task's warnings without adding production hooks or global subscribers.
        let scans = Arc::new(AtomicUsize::new(0));
        let interval = Duration::from_secs(10);
        let patrol = tokio::spawn(
            run_patrol(state, interval).with_subscriber(ScanWarnings(Arc::clone(&scans))),
        );
        tokio::task::yield_now().await;
        tokio::time::advance(interval * 3 + Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            scans.load(Ordering::SeqCst),
            1,
            "missed ticks must produce one scan, not a burst"
        );
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            scans.load(Ordering::SeqCst),
            2,
            "next scan must follow the next regular tick"
        );
        patrol.abort();
        let _ = patrol.await;
    }

    #[tokio::test(start_paused = true)]
    async fn patrol_waits_for_metadata_and_uses_latest_schedule_before_next_tick() {
        use flow::catalog::{
            MemoryStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps,
        };
        use flow::pipeline::{PipelineStatus, PipelineStopMode};

        let temp = tempfile::tempdir().expect("temp directory");
        let state = super::super::state::AppState::new(
            crate::new_default_flow_instance(),
            storage::StorageManager::new(temp.path()).expect("storage"),
            vec![crate::FlowInstanceSpec {
                id: "default".to_string(),
                ..Default::default()
            }],
            0,
        )
        .expect("state");
        let instance = state.instances.default_instance();
        instance
            .declare_memory_topic("patrol_input", flow::connector::MemoryTopicKind::Bytes, 16)
            .expect("topic");
        instance
            .create_stream(
                StreamDefinition::new(
                    "src",
                    std::sync::Arc::new(flow::Schema::new(Vec::new())),
                    StreamProps::Memory(MemoryStreamProps::new("patrol_input")),
                    StreamDecoderConfig::json(),
                ),
                false,
            )
            .await
            .expect("stream");
        let now = Utc::now();
        let mut request: super::super::types::CreatePipelineRequest =
            serde_json::from_value(serde_json::json!({
                "id": "waiting", "revision": 1, "sql": "SELECT * FROM src",
                "sinks": [{"type":"nop", "props":{}, "encoder":{"type":"json","props":{}}}],
                "options":{"schedule":{"datetime_ranges":[{
                    "begin_timestamp_ms":(now + chrono::Duration::hours(1)).timestamp_millis(),
                    "end_timestamp_ms":(now + chrono::Duration::hours(2)).timestamp_millis()
                }]}}
            }))
            .expect("request");
        let mut guarded = request.clone();
        guarded.id = "guarded".to_string();
        guarded.options.schedule = Some(datetime_range_schedule(vec![
            PipelineDatetimeRangeRequest {
                begin_timestamp_ms: (now - chrono::Duration::hours(1)).timestamp_millis(),
                end_timestamp_ms: (now + chrono::Duration::hours(1)).timestamp_millis(),
            },
        ]));
        for request in [&request, &guarded] {
            let stored =
                storage_bridge::stored_pipeline_from_request(request).expect("stored request");
            state
                .storage
                .create_pipeline(stored.clone())
                .expect("persist pipeline");
            state
                .storage
                .put_pipeline_run_state(storage::StoredPipelineRunState {
                    pipeline_id: request.id.clone(),
                    desired_state: StoredPipelineDesiredState::Running,
                })
                .expect("running intent");
            crate::storage_apply::install_pipeline(&state, &stored)
                .await
                .expect("stopped scheduled runtime");
        }

        let exclusive = state
            .try_acquire_metadata_operation(true)
            .expect("exclusive boundary");
        let interval = Duration::from_secs(3600);
        let patrol = tokio::spawn(run_patrol(state.clone(), interval));
        tokio::task::yield_now().await;
        tokio::time::advance(interval * 3 + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(
            instance
                .list_pipelines()
                .iter()
                .all(|pipeline| pipeline.status == PipelineStatus::Stopped)
        );

        request.revision = 2;
        request.options.schedule = Some(datetime_range_schedule(vec![
            PipelineDatetimeRangeRequest {
                begin_timestamp_ms: (now - chrono::Duration::hours(1)).timestamp_millis(),
                end_timestamp_ms: (now + chrono::Duration::hours(1)).timestamp_millis(),
            },
        ]));
        let mut snapshot = state.storage.export_metadata_snapshot().expect("snapshot");
        *snapshot
            .pipelines
            .iter_mut()
            .find(|pipeline| pipeline.id == request.id)
            .expect("waiting pipeline") =
            storage_bridge::stored_pipeline_from_request(&request).expect("updated pipeline");
        state
            .storage
            .commit_metadata_snapshot(snapshot, Default::default())
            .expect("commit active schedule while patrol waits");
        assert!(
            instance
                .list_pipelines()
                .iter()
                .all(|pipeline| pipeline.status == PipelineStatus::Stopped)
        );

        drop(exclusive);
        // Resume time for the failure timeout; the next patrol tick is still almost an hour away.
        tokio::time::resume();
        let started = tokio::time::timeout(Duration::from_secs(5), async {
            while !instance
                .list_pipelines()
                .iter()
                .all(|pipeline| pipeline.status == PipelineStatus::Running)
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        patrol.abort();
        let _ = patrol.await;
        for id in [&request.id, &guarded.id] {
            instance
                .stop_pipeline(id, PipelineStopMode::Quick, Duration::from_secs(5))
                .await
                .expect("cleanup");
        }
        started.expect("patrol must start with the latest schedule before the next tick");
    }

    #[tokio::test]
    async fn scheduled_recovery_resets_current_failure_and_respects_windows() {
        use axum::{
            extract::{Path, State},
            response::IntoResponse,
        };
        use flow::catalog::{
            MemoryStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps,
        };
        use flow::pipeline::PipelineStatus;
        for (label, begin_offset, end_offset, expected) in [
            ("future", 3600, 7200, PipelineStatus::Stopped),
            ("ended", -7200, -3600, PipelineStatus::Stopped),
            ("active", -3600, 3600, PipelineStatus::Running),
        ] {
            let temp = tempfile::tempdir().expect("temp directory");
            let state = super::super::state::AppState::new(
                crate::new_default_flow_instance(),
                storage::StorageManager::new(temp.path()).expect("storage"),
                vec![crate::FlowInstanceSpec {
                    id: "default".to_string(),
                    ..Default::default()
                }],
                0,
            )
            .expect("state");
            let instance = state.instances.default_instance();
            instance
                .declare_memory_topic(
                    "scheduled_input",
                    flow::connector::MemoryTopicKind::Bytes,
                    16,
                )
                .expect("topic");
            instance
                .create_stream(
                    StreamDefinition::new(
                        "src",
                        std::sync::Arc::new(flow::Schema::new(Vec::new())),
                        StreamProps::Memory(MemoryStreamProps::new("scheduled_input")),
                        StreamDecoderConfig::json(),
                    ),
                    false,
                )
                .await
                .expect("stream");
            let now = Utc::now();
            let request: super::super::types::CreatePipelineRequest = serde_json::from_value(serde_json::json!({
                "id": label, "revision": 1, "sql": "SELECT * FROM src",
                "sinks": [{"id":"sink", "type":"nop", "props":{}, "encoder":{"type":"json","props":{}}}],
                "options":{"schedule":{"datetime_ranges":[{
                    "begin_timestamp_ms":(now + chrono::Duration::seconds(begin_offset)).timestamp_millis(),
                    "end_timestamp_ms":(now + chrono::Duration::seconds(end_offset)).timestamp_millis()
                }]}}
            })).expect("request");
            let stored =
                storage_bridge::stored_pipeline_from_request(&request).expect("stored request");
            state
                .storage
                .create_pipeline(stored.clone())
                .expect("persist definition");
            state
                .storage
                .put_pipeline_run_state(storage::StoredPipelineRunState {
                    pipeline_id: label.to_string(),
                    desired_state: StoredPipelineDesiredState::Running,
                })
                .expect("intent");
            let definition = super::super::spec::build_pipeline_definition(
                &request,
                instance.encoder_registry().as_ref(),
                instance.as_ref(),
                Default::default(),
            )
            .expect("definition");
            instance
                .create_pipeline(flow::CreatePipelineRequest::new(definition))
                .expect("runtime");
            instance
                .mark_pipeline_failed(label, "processor", "current failure".to_string())
                .expect("fail runtime");
            patrol_pipeline(&stored.id, &state).await;
            let snapshot = instance.list_pipelines().pop().expect("snapshot");
            assert_eq!(
                snapshot.status,
                PipelineStatus::Failed,
                "{label}: patrol must not retry failed runtime"
            );
            let response = super::super::handlers::start_pipeline_handler(
                State(state.clone()),
                Path(label.to_string()),
            )
            .await
            .into_response();
            assert_eq!(response.status(), axum::http::StatusCode::OK);
            let snapshot = instance.list_pipelines().pop().expect("reset snapshot");
            assert_eq!(snapshot.status, PipelineStatus::Stopped);
            assert!(snapshot.last_runtime_error.is_none());
            let response = super::super::handlers::list_pipelines(State(state.clone()))
                .await
                .into_response();
            let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .expect("body");
            let body: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
            assert_eq!(
                body[0]["status"],
                if label == "ended" {
                    "scheduled_end"
                } else {
                    "scheduled_stopped"
                }
            );
            assert!(body[0].get("last_runtime_error").is_none());
            patrol_pipeline(&stored.id, &state).await;
            assert_eq!(
                instance
                    .list_pipelines()
                    .pop()
                    .expect("patrol snapshot")
                    .status,
                expected
            );
            instance
                .stop_pipeline(
                    label,
                    flow::pipeline::PipelineStopMode::Quick,
                    Duration::from_secs(5),
                )
                .await
                .expect("cleanup");
        }
    }
}
