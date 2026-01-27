mod bodies;
mod config;
mod output;
mod payload;
mod wide;

use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = match config::Config::parse() {
        Ok(v) => v,
        Err(help_or_err) => {
            if help_or_err.contains("USAGE:") {
                print!("{help_or_err}");
                return Ok(());
            }
            eprintln!("{help_or_err}");
            std::process::exit(2);
        }
    };

    let column_names = wide::column_names(cfg.cols);
    let sql = wide::select_sql(&column_names, config::STREAM_NAME);

    // Pre-generate a small set of payloads in memory. Later steps will publish these bytes to MQTT.
    let _payload_cases = payload::generate_payload_cases(cfg.cols, cfg.cases, cfg.str_len);

    // These JSON bodies match the manager REST API shape documented in `scripts/sf_rest_tool.md`.
    // Step 1-3 only: generate bodies for review; no network calls yet.
    let stream_body = bodies::build_stream_body(
        config::STREAM_NAME,
        config::DEFAULT_BROKER_URL,
        config::DEFAULT_TOPIC,
        config::DEFAULT_QOS,
        &column_names,
    );
    let pipeline_body = bodies::build_pipeline_body(config::PIPELINE_ID, &sql);

    let info = output::write_review_artifacts(&cfg.out_dir, &sql, &stream_body, &pipeline_body)?;

    let summary = json!({
        "cols": cfg.cols,
        "cases": cfg.cases,
        "str_len": cfg.str_len,
        "stream_name": config::STREAM_NAME,
        "pipeline_id": config::PIPELINE_ID,
        "sql_len_bytes": sql.len(),
        "stream_json_len_bytes": info.stream_json_len_bytes,
        "pipeline_json_len_bytes": info.pipeline_json_len_bytes,
        "out_dir": cfg.out_dir,
        "broker_url_default": config::DEFAULT_BROKER_URL,
        "topic_default": config::DEFAULT_TOPIC,
        "qos_default": config::DEFAULT_QOS,
    });
    output::write_summary(&cfg.out_dir, &summary)?;

    println!(
        "generated cols={} cases={} str_len={} sql_len={} stream_json={} pipeline_json={} out_dir={}",
        cfg.cols,
        cfg.cases,
        cfg.str_len,
        sql.len(),
        info.stream_json_len_bytes,
        info.pipeline_json_len_bytes,
        cfg.out_dir.display()
    );

    Ok(())
}
