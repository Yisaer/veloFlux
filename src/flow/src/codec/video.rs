use crate::model::{Collection, Message, RecordBatch, Tuple};
use bytes::Bytes;
use datatypes::{
    BytesType, ColumnSchema, ConcreteDatatype, Int64Type, Schema, StringType, TimestampType,
    TimestampValue, Value,
};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const VIDEO_PAYLOAD_COLUMN: &str = "payload";
pub const VIDEO_WIDTH_COLUMN: &str = "width";
pub const VIDEO_HEIGHT_COLUMN: &str = "height";
pub const VIDEO_FORMAT_COLUMN: &str = "format";
pub const VIDEO_TIMESTAMP_COLUMN: &str = "timestamp";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoRawFrame {
    pub payload: Bytes,
    pub width: i64,
    pub height: i64,
    pub format: String,
    pub timestamp: TimestampValue,
}

#[derive(Debug, thiserror::Error)]
pub enum VideoCodecError {
    #[error("video frame width and height must be positive, got {width}x{height}")]
    InvalidDimensions { width: i64, height: i64 },
    #[error("video tuple requires `{0}` field")]
    MissingField(&'static str),
    #[error("video tuple field `{field}` must be {expected}")]
    InvalidFieldType {
        field: &'static str,
        expected: &'static str,
    },
    #[error("video tuple field `{field}` is out of range")]
    FieldOutOfRange { field: &'static str },
    #[error("video collection error: {0}")]
    Collection(#[from] crate::model::CollectionError),
}

impl VideoRawFrame {
    pub fn new(
        payload: Bytes,
        width: i64,
        height: i64,
        format: impl Into<String>,
        timestamp: TimestampValue,
    ) -> Result<Self, VideoCodecError> {
        if width <= 0 || height <= 0 {
            return Err(VideoCodecError::InvalidDimensions { width, height });
        }
        Ok(Self {
            payload,
            width,
            height,
            format: format.into(),
            timestamp,
        })
    }
}

pub fn default_video_schema(stream_name: impl Into<String>) -> Schema {
    let stream_name = stream_name.into();
    Schema::new(vec![
        ColumnSchema::new(
            stream_name.clone(),
            VIDEO_PAYLOAD_COLUMN.to_string(),
            ConcreteDatatype::Bytes(BytesType),
        ),
        ColumnSchema::new(
            stream_name.clone(),
            VIDEO_WIDTH_COLUMN.to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            stream_name.clone(),
            VIDEO_HEIGHT_COLUMN.to_string(),
            ConcreteDatatype::Int64(Int64Type),
        ),
        ColumnSchema::new(
            stream_name.clone(),
            VIDEO_FORMAT_COLUMN.to_string(),
            ConcreteDatatype::String(StringType),
        ),
        ColumnSchema::new(
            stream_name,
            VIDEO_TIMESTAMP_COLUMN.to_string(),
            ConcreteDatatype::Timestamp(TimestampType),
        ),
    ])
}

pub fn timestamp_from_system_time(time: SystemTime) -> TimestampValue {
    let epoch_micros = time
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_micros();
    TimestampValue::from_epoch_micros(i64::try_from(epoch_micros).unwrap_or(i64::MAX))
}

pub fn raw_frame_to_record_batch(
    stream_name: impl Into<Arc<str>>,
    frame: VideoRawFrame,
) -> Result<RecordBatch, VideoCodecError> {
    let stream_name = stream_name.into();
    let keys = vec![
        Arc::<str>::from(VIDEO_PAYLOAD_COLUMN),
        Arc::<str>::from(VIDEO_WIDTH_COLUMN),
        Arc::<str>::from(VIDEO_HEIGHT_COLUMN),
        Arc::<str>::from(VIDEO_FORMAT_COLUMN),
        Arc::<str>::from(VIDEO_TIMESTAMP_COLUMN),
    ];
    let values = vec![
        Arc::new(Value::Bytes(frame.payload)),
        Arc::new(Value::Int64(frame.width)),
        Arc::new(Value::Int64(frame.height)),
        Arc::new(Value::String(frame.format)),
        Arc::new(Value::Timestamp(frame.timestamp)),
    ];
    let message = Arc::new(Message::new(stream_name, keys, values));
    let timestamp = UNIX_EPOCH
        + Duration::from_micros(u64::try_from(frame.timestamp.epoch_micros()).unwrap_or(0));
    Ok(RecordBatch::new(vec![Tuple::with_timestamp(
        Arc::from(vec![message]),
        timestamp,
    )])?)
}

pub fn raw_frame_from_tuple(row: &Tuple) -> Result<VideoRawFrame, VideoCodecError> {
    let payload = match required_field(row, VIDEO_PAYLOAD_COLUMN)? {
        Value::Bytes(payload) => payload.clone(),
        _ => {
            return Err(VideoCodecError::InvalidFieldType {
                field: VIDEO_PAYLOAD_COLUMN,
                expected: "bytes",
            });
        }
    };
    let width = required_i64_field(row, VIDEO_WIDTH_COLUMN)?;
    let height = required_i64_field(row, VIDEO_HEIGHT_COLUMN)?;
    let format = match required_field(row, VIDEO_FORMAT_COLUMN)? {
        Value::String(format) => format.clone(),
        _ => {
            return Err(VideoCodecError::InvalidFieldType {
                field: VIDEO_FORMAT_COLUMN,
                expected: "string",
            });
        }
    };
    let timestamp = match required_field(row, VIDEO_TIMESTAMP_COLUMN)? {
        Value::Timestamp(timestamp) => *timestamp,
        _ => {
            return Err(VideoCodecError::InvalidFieldType {
                field: VIDEO_TIMESTAMP_COLUMN,
                expected: "timestamp",
            });
        }
    };
    VideoRawFrame::new(payload, width, height, format, timestamp)
}

pub fn raw_frames_from_collection(
    collection: &dyn Collection,
) -> Result<Vec<VideoRawFrame>, VideoCodecError> {
    collection.rows().iter().map(raw_frame_from_tuple).collect()
}

fn required_field<'a>(row: &'a Tuple, field: &'static str) -> Result<&'a Value, VideoCodecError> {
    row.value_by_name("", field)
        .or_else(|| {
            row.messages()
                .iter()
                .find_map(|message| message.value(field))
        })
        .ok_or(VideoCodecError::MissingField(field))
}

fn required_i64_field(row: &Tuple, field: &'static str) -> Result<i64, VideoCodecError> {
    match required_field(row, field)? {
        Value::Int64(value) => Ok(*value),
        Value::Int32(value) => Ok(i64::from(*value)),
        Value::Uint32(value) => Ok(i64::from(*value)),
        Value::Uint64(value) => {
            i64::try_from(*value).map_err(|_| VideoCodecError::FieldOutOfRange { field })
        }
        _ => Err(VideoCodecError::InvalidFieldType {
            field,
            expected: "integer",
        }),
    }
}
