use flow::planner::logical::create_logical_plan;
use flow::planner::create_physical_plan;
use flow::processor::{StreamData};
use flow::processor::create_processor_pipeline;
use flow::model::{Column, RecordBatch as FlowRecordBatch};
use datatypes::Value;
use tokio::time::{timeout, Duration};
use parser;

#[tokio::test]
async fn test_end_to_end_projection_pipeline() {
    let sql = "select a + 1, b + 2 from stream";
    let select_stmt = parser::parse_sql(sql).expect("Failed to parse SQL");

    let logical_plan = create_logical_plan(select_stmt).expect("Failed to create logical plan");

    let physical_plan = create_physical_plan(logical_plan).expect("Failed to create physical plan");

    let mut pipeline = create_processor_pipeline(physical_plan)
        .expect("Failed to create processor pipeline");

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
    
    let column_a = Column::new("a".to_string(), "".to_string(), col_a_values);
    let column_b = Column::new("b".to_string(), "".to_string(), col_b_values);
    let column_c = Column::new("c".to_string(), "".to_string(), col_c_values);
    
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