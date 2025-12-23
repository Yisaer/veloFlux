use datatypes::{ColumnSchema, ConcreteDatatype, Schema, Value};
use flow::catalog::{MockStreamProps, StreamDecoderConfig, StreamDefinition, StreamProps};
use flow::expr::custom_func::CustomFunc;
use flow::expr::func::EvalError;
use flow::model::batch_from_columns_simple;
use flow::processor::StreamData;
use flow::FlowInstance;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

async fn install_stream_schema(instance: &FlowInstance) {
    let schema = Schema::new(vec![
        ColumnSchema::new(
            "stream".to_string(),
            "a".to_string(),
            ConcreteDatatype::String(datatypes::types::StringType),
        ),
        ColumnSchema::new(
            "stream".to_string(),
            "b".to_string(),
            ConcreteDatatype::String(datatypes::types::StringType),
        ),
    ]);
    let definition = StreamDefinition::new(
        "stream".to_string(),
        Arc::new(schema),
        StreamProps::Mock(MockStreamProps::default()),
        StreamDecoderConfig::json(),
    );
    instance
        .create_stream(definition, false)
        .await
        .expect("create stream");
}

#[tokio::test]
async fn custom_func_registry_allows_external_registration() {
    #[derive(Debug)]
    struct WrapFn;

    impl CustomFunc for WrapFn {
        fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
            if args.len() != 1 {
                return Err(EvalError::TypeMismatch {
                    expected: "1 argument".to_string(),
                    actual: format!("{} arguments", args.len()),
                });
            }
            match &args[0] {
                Value::String(_) => Ok(()),
                other => Err(EvalError::TypeMismatch {
                    expected: "String".to_string(),
                    actual: format!("{:?}", other),
                }),
            }
        }

        fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
            self.validate_row(args)?;
            let Value::String(s) = &args[0] else {
                unreachable!("validated as string")
            };
            Ok(Value::String(format!("[{s}]")))
        }

        fn name(&self) -> &str {
            "wrap"
        }
    }

    let instance = FlowInstance::new();
    install_stream_schema(&instance).await;
    instance
        .register_custom_func(Arc::new(WrapFn))
        .expect("register wrap");

    let mut pipeline = instance
        .build_pipeline_with_log_sink("SELECT wrap(a) AS out FROM stream", true)
        .expect("create pipeline");
    pipeline.start();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let batch = batch_from_columns_simple(vec![
        (
            "stream".to_string(),
            "a".to_string(),
            vec![
                Value::String("x".to_string()),
                Value::String("y".to_string()),
            ],
        ),
        (
            "stream".to_string(),
            "b".to_string(),
            vec![
                Value::String("ignored".to_string()),
                Value::String("ignored".to_string()),
            ],
        ),
    ])
    .expect("create batch");
    pipeline
        .send_stream_data("stream", StreamData::collection(Box::new(batch)))
        .await
        .expect("send data");

    let mut output = pipeline.take_output().expect("output receiver");
    let received = timeout(Duration::from_secs(5), output.recv())
        .await
        .expect("timeout")
        .expect("missing output");
    match received {
        StreamData::Collection(collection) => {
            let rows = collection.rows();
            assert_eq!(rows.len(), 2);
            let values: Vec<Value> = rows
                .iter()
                .map(|row| row.value_by_name("", "out").expect("out").clone())
                .collect();
            assert_eq!(
                values,
                vec![
                    Value::String("[x]".to_string()),
                    Value::String("[y]".to_string())
                ]
            );
        }
        other => panic!("expected collection, got {}", other.description()),
    }

    pipeline.close().await.expect("close");
}
