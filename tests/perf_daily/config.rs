use std::env;
use std::path::PathBuf;

pub const STREAM_NAME: &str = "perf_daily_stream";
pub const PIPELINE_ID: &str = "perf_daily_pipeline";

pub const DEFAULT_COLS: usize = 15_000;
pub const DEFAULT_CASES: usize = 10;
pub const DEFAULT_STR_LEN: usize = 16;

pub const DEFAULT_BROKER_URL: &str = "tcp://127.0.0.1:1883";
pub const DEFAULT_TOPIC: &str = "/perf/daily";
pub const DEFAULT_QOS: i64 = 0;
pub const DEFAULT_OUT_DIR: &str = "tmp/perf_daily";

#[derive(Debug, Clone)]
pub struct Config {
    pub cols: usize,
    pub cases: usize,
    pub str_len: usize,
    pub out_dir: PathBuf,
}

fn usage() -> &'static str {
    r#"perf_daily (dry-run generator for wide stream + pipeline)

USAGE:
  cargo run --bin perf_daily -- [--cols N] [--cases N] [--str-len N] [--out-dir PATH]

FLAGS:
  --cols N        Number of string columns to generate (default: 15000, env: COLS)
  --cases N       Number of pre-generated payload cases (default: 10, env: CASES; 0 disables)
  --str-len N     String length for each cell value (default: 16, env: STR_LEN)
  --out-dir PATH  Output directory (default: tmp/perf_daily, env: OUT_DIR)
  --help          Show this help

NOTES:
  - Column names are fixed to a1..aN (prefix "a")
  - Column type is fixed to string
  - Stream name / pipeline id are fixed to:
      stream_name = perf_daily_stream
      pipeline_id = perf_daily_pipeline
  - SQL format is `select a1,a2,...,aN from perf_daily_stream` (no spaces after commas)
"#
}

impl Config {
    pub fn parse() -> Result<Self, String> {
        let mut cols = env::var("COLS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_COLS);
        let mut cases = env::var("CASES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_CASES);
        let mut str_len = env::var("STR_LEN")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_STR_LEN);
        let mut out_dir = env::var("OUT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(DEFAULT_OUT_DIR));

        let mut it = env::args().skip(1);
        while let Some(arg) = it.next() {
            match arg.as_str() {
                "--help" | "-h" => return Err(usage().to_string()),
                "--cols" => {
                    let v = it
                        .next()
                        .ok_or_else(|| "missing value for --cols".to_string())?;
                    cols = v
                        .parse::<usize>()
                        .map_err(|_| format!("invalid --cols value: {v}"))?;
                }
                "--cases" => {
                    let v = it
                        .next()
                        .ok_or_else(|| "missing value for --cases".to_string())?;
                    cases = v
                        .parse::<usize>()
                        .map_err(|_| format!("invalid --cases value: {v}"))?;
                }
                "--str-len" => {
                    let v = it
                        .next()
                        .ok_or_else(|| "missing value for --str-len".to_string())?;
                    str_len = v
                        .parse::<usize>()
                        .map_err(|_| format!("invalid --str-len value: {v}"))?;
                }
                "--out-dir" => {
                    let v = it
                        .next()
                        .ok_or_else(|| "missing value for --out-dir".to_string())?;
                    out_dir = PathBuf::from(v);
                }
                other => return Err(format!("unknown arg: {other}\n\n{}", usage())),
            }
        }

        if cols == 0 {
            return Err("--cols must be > 0".to_string());
        }
        if cases > 0 && str_len == 0 {
            return Err("--str-len must be > 0 (or set --cases 0)".to_string());
        }

        Ok(Self {
            cols,
            cases,
            str_len,
            out_dir,
        })
    }
}
