use serde_json::Value;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct WriteInfo {
    pub stream_json_len_bytes: u64,
    pub pipeline_json_len_bytes: u64,
}

fn write_text(path: &Path, content: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut f = fs::File::create(path)?;
    f.write_all(content.as_bytes())?;
    Ok(())
}

pub fn write_review_artifacts(
    out_dir: &Path,
    sql: &str,
    stream_body: &Value,
    pipeline_body: &Value,
) -> std::io::Result<WriteInfo> {
    fs::create_dir_all(out_dir)?;

    let sql_path = out_dir.join("sql.txt");
    let stream_path = out_dir.join("stream.json");
    let pipeline_path = out_dir.join("pipeline.json");

    write_text(&sql_path, sql)?;
    fs::write(
        &stream_path,
        serde_json::to_vec(stream_body).expect("stream body is json"),
    )?;
    fs::write(
        &pipeline_path,
        serde_json::to_vec(pipeline_body).expect("pipeline body is json"),
    )?;

    Ok(WriteInfo {
        stream_json_len_bytes: fs::metadata(&stream_path)?.len(),
        pipeline_json_len_bytes: fs::metadata(&pipeline_path)?.len(),
    })
}

pub fn write_summary(out_dir: &Path, summary: &Value) -> std::io::Result<PathBuf> {
    fs::create_dir_all(out_dir)?;
    let summary_path = out_dir.join("summary.json");
    fs::write(
        &summary_path,
        serde_json::to_vec_pretty(summary).expect("summary is json"),
    )?;
    Ok(summary_path)
}
