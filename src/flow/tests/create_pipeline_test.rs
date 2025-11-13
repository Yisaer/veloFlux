//! Tests for create_pipeline function
//!
//! This module tests the high-level create_pipeline function that creates
//! a complete processing pipeline from SQL queries.

use flow::create_pipeline;
use flow::processor::{StreamData};
use flow::model::{Column, RecordBatch as FlowRecordBatch};
use datatypes::Value;
use tokio::time::{timeout, Duration};

/// Test create_pipeline with a simple projection query
#[tokio::test]
async fn test_create_pipeline_projection() {
    // Create pipeline from SQL
    let mut pipeline = create_pipeline("SELECT a + 1, b + 2 FROM stream")
        .expect("Failed to create pipeline");

    pipeline.start();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create test data
    let col_a_values = vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)];
    let col_b_values = vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)];
    let col_c_values = vec![Value::Int64(1000), Value::Int64(2000), Value::Int64(3000)];
    
    let column_a = Column::new("".to_string(), "a".to_string(), col_a_values);
    let column_b = Column::new("".to_string(), "b".to_string(), col_b_values);
    let column_c = Column::new("".to_string(), "c".to_string(), col_c_values);
    
    let test_batch = FlowRecordBatch::new(vec![column_a, column_b, column_c])
        .expect("Failed to create test RecordBatch");

    let stream_data = StreamData::Collection(Box::new(test_batch));
    pipeline.input.send(stream_data).await.expect("Failed to send test data");

    let timeout_duration = Duration::from_secs(5);

    let received_data = timeout(timeout_duration, pipeline.output.recv())
        .await
        .expect("Timeout waiting for output")
        .expect("Failed to receive output");

    match received_data {
        StreamData::Collection(result_collection) => {
            let batch = result_collection.as_ref();
            assert_eq!(batch.num_rows(), 3, "Should have 3 rows");
            assert_eq!(batch.num_columns(), 2, "Should have 2 columns (a+1, b+2)");
            let col1 = batch.column(0).expect("Should have first column");
            let col2 = batch.column(1).expect("Should have second column");
            assert_eq!(col1.name, "a + 1".to_string(), "First column should contain '+'");
            assert_eq!(col2.name, "b + 2".to_string(), "First column should contain '+'");
            assert_eq!(col1.values(),vec![Value::Int64(11), Value::Int64(21), Value::Int64(31)],"" );
            assert_eq!(col2.values(),vec![Value::Int64(102), Value::Int64(202), Value::Int64(302)],"" );
        }
        StreamData::Control(_) => {
            panic!("Expected Collection data, but received control signal");
        }
        StreamData::Error(e) => {
            panic!("Expected Collection data, but received error: {}", e.message);
        }
    }
    pipeline.close().await.expect("Failed to close pipeline");
}

/// Test create_pipeline with a filter query
#[tokio::test]
async fn test_create_pipeline_filter() {
    // Create pipeline from SQL with filter
    let mut pipeline = create_pipeline("SELECT a, b FROM stream WHERE a > 15")
        .expect("Failed to create pipeline");

    pipeline.start();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create test data: a=10,20,30, b=100,200,300
    let col_a_values = vec![Value::Int64(10), Value::Int64(20), Value::Int64(30)];
    let col_b_values = vec![Value::Int64(100), Value::Int64(200), Value::Int64(300)];
    
    let column_a = Column::new("".to_string(), "a".to_string(), col_a_values);
    let column_b = Column::new("".to_string(), "b".to_string(), col_b_values);
    
    let test_batch = FlowRecordBatch::new(vec![column_a, column_b])
        .expect("Failed to create test RecordBatch");

    let stream_data = StreamData::Collection(Box::new(test_batch));
    pipeline.input.send(stream_data).await.expect("Failed to send test data");

    let timeout_duration = Duration::from_secs(5);

    let received_data = timeout(timeout_duration, pipeline.output.recv())
        .await
        .expect("Timeout waiting for output")
        .expect("Failed to receive output");

    match received_data {
        StreamData::Collection(result_collection) => {
            let batch = result_collection.as_ref();
            assert_eq!(batch.num_rows(), 2, "Should have 2 rows (filtered)");
            assert_eq!(batch.num_columns(), 2, "Should have 2 columns (a, b)");
            let col1 = batch.column(0).expect("Should have first column");
            let col2 = batch.column(1).expect("Should have second column");
            assert_eq!(col1.name, "a".to_string());
            assert_eq!(col2.name, "b".to_string());
            // Should only have rows where a > 15 (a=20, a=30)
            assert_eq!(col1.values(),vec![Value::Int64(20), Value::Int64(30)],"" );
            assert_eq!(col2.values(),vec![Value::Int64(200), Value::Int64(300)],"" );
        }
        StreamData::Control(_) => {
            panic!("Expected Collection data, but received control signal");
        }
        StreamData::Error(e) => {
            panic!("Expected Collection data, but received error: {}", e.message);
        }
    }
    pipeline.close().await.expect("Failed to close pipeline");
}

/// Test create_pipeline with invalid SQL
#[tokio::test]
async fn test_create_pipeline_invalid_sql() {
    // Test with invalid SQL
    let result = create_pipeline("INVALID SQL SYNTAX");
    assert!(result.is_err(), "Should fail with invalid SQL");
}

#[tokio::test]
async fn test_pipeline() {
    let sql = "select a + 1, b + 2 from stream";
    // Create pipeline from SQL
    let mut pipeline = create_pipeline(sql)
        .expect("Failed to create pipeline");

    pipeline.start();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let col_a_values = vec![
        Value::Int64(10),
        Value::Int64(20),
        Value::Int64(30)
    ];
    let col_b_values = vec![
        Value::Int64(100),
        Value::Int64(200),
        Value::Int64(300)
    ];
    let col_c_values = vec![
        Value::Int64(1000),
        Value::Int64(2000),
        Value::Int64(3000)
    ];

    let column_a = Column::new("".to_string(), "a".to_string(), col_a_values);
    let column_b = Column::new("".to_string(), "b".to_string(), col_b_values);
    let column_c = Column::new("".to_string(), "c".to_string(), col_c_values);

    let test_batch = FlowRecordBatch::new(vec![column_a, column_b, column_c])
        .expect("Failed to create test RecordBatch");

    let stream_data = StreamData::Collection(Box::new(test_batch));
    pipeline.input.send(stream_data).await.expect("Failed to send test data");

    let timeout_duration = Duration::from_secs(5);

    let received_data = timeout(timeout_duration, pipeline.output.recv())
        .await
        .expect("Timeout waiting for output")
        .expect("Failed to receive output");

    match received_data {
        StreamData::Collection(result_collection) => {
            let batch = result_collection.as_ref();
            assert_eq!(batch.num_rows(), 3, "Should have 3 rows");
            assert_eq!(batch.num_columns(), 2, "Should have 2 columns (a+1, b+2)");
            let col1 = batch.column(0).expect("Should have first column");
            let col2 = batch.column(1).expect("Should have second column");
            assert_eq!(col1.name, "a + 1".to_string(), "First column should contain '+'");
            assert_eq!(col2.name, "b + 2".to_string(), "First column should contain '+'");
            assert_eq!(col1.values(),vec![Value::Int64(11), Value::Int64(21), Value::Int64(31)],"" );
            assert_eq!(col2.values(),vec![Value::Int64(102), Value::Int64(202), Value::Int64(302)],"" );

        }
        StreamData::Control(_) => {
            panic!("Expected Collection data, but received control signal");
        }
        StreamData::Error(e) => {
            panic!("Expected Collection data, but received error: {}", e.message);
        }
    }
    pipeline.close().await.expect("Failed to close pipeline");
}