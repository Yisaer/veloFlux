//! Collection-level processor for batch-only WASM UDF evaluations.

use crate::expr::custom_func::BatchFunc;
use crate::expr::EvalRowContext;
use crate::model::{Collection, RecordBatch};
use crate::planner::physical::{PhysicalPlan, PhysicalWasmUdfCompute, PhysicalWasmUdfEvaluation};
use crate::processor::base::{
    default_channel_capacities, fan_in_control_streams, fan_in_streams,
    send_control_with_backpressure, send_with_backpressure, LinkOutput, LinkReceiver,
    ProcessorChannelCapacities,
};
use crate::processor::{
    ControlSignal, Processor, ProcessorError, ProcessorStart, ProcessorStats, StreamData,
};
use crate::runtime::TaskSpawner;
use futures::stream::StreamExt;
use std::sync::Arc;

struct BoundEvaluation {
    output_name: Arc<String>,
    definition: Arc<dyn crate::expr::custom_func::CustomFunc>,
    udf: Arc<dyn BatchFunc>,
    args: Vec<crate::expr::ScalarExpr>,
}

pub struct WasmUdfComputeProcessor {
    id: String,
    evaluations: Arc<Vec<BoundEvaluation>>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}

impl WasmUdfComputeProcessor {
    pub fn new(
        id: impl Into<String>,
        plan: Arc<PhysicalWasmUdfCompute>,
    ) -> Result<Self, ProcessorError> {
        Self::new_with_channel_capacities(id, plan, default_channel_capacities())
    }

    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        plan: Arc<PhysicalWasmUdfCompute>,
        channel_capacities: ProcessorChannelCapacities,
    ) -> Result<Self, ProcessorError> {
        let evaluations = bind_evaluations(&plan.evaluations)?;
        let output = LinkOutput::new(channel_capacities.data_link_kind, channel_capacities.data);
        let control_output = LinkOutput::new(
            channel_capacities.control_link_kind,
            channel_capacities.control,
        );
        Ok(Self {
            id: id.into(),
            evaluations: Arc::new(evaluations),
            inputs: Vec::new(),
            control_inputs: Vec::new(),
            output,
            control_output,
            channel_capacities,
            stats: Arc::new(ProcessorStats::collection_in_out()),
        })
    }

    pub fn from_physical_plan(
        id: impl Into<String>,
        plan: Arc<PhysicalPlan>,
    ) -> Result<Option<Self>, ProcessorError> {
        match plan.as_ref() {
            PhysicalPlan::WasmUdfCompute(compute) => {
                Ok(Some(Self::new(id, Arc::new(compute.clone()))?))
            }
            _ => Ok(None),
        }
    }

    pub fn set_stats(&mut self, stats: Arc<ProcessorStats>) {
        stats.declare_collection_in_out();
        self.stats = stats;
    }
}

fn bind_evaluation(
    evaluation: &PhysicalWasmUdfEvaluation,
) -> Result<BoundEvaluation, ProcessorError> {
    let udf = evaluation
        .udf
        .bind_batch_runtime()
        .map_err(ProcessorError::InvalidConfiguration)?
        .ok_or_else(|| {
            ProcessorError::InvalidConfiguration(format!(
                "WASM UDF '{}' does not provide a batch runtime",
                evaluation.output_column
            ))
        })?;
    Ok(BoundEvaluation {
        output_name: Arc::new(evaluation.output_column.clone()),
        definition: Arc::clone(&evaluation.udf),
        udf,
        args: evaluation.args.clone(),
    })
}

fn bind_evaluations(
    evaluations: &[PhysicalWasmUdfEvaluation],
) -> Result<Vec<BoundEvaluation>, ProcessorError> {
    let mut bound: Vec<BoundEvaluation> = Vec::with_capacity(evaluations.len());
    for evaluation in evaluations {
        if let Some(existing) = bound
            .iter()
            .find(|candidate| Arc::ptr_eq(&candidate.definition, &evaluation.udf))
        {
            bound.push(BoundEvaluation {
                output_name: Arc::new(evaluation.output_column.clone()),
                definition: Arc::clone(&evaluation.udf),
                udf: Arc::clone(&existing.udf),
                args: evaluation.args.clone(),
            });
        } else {
            bound.push(bind_evaluation(evaluation)?);
        }
    }
    Ok(bound)
}

fn apply_compute(
    input: Box<dyn Collection>,
    evaluations: &[BoundEvaluation],
) -> Result<(Box<dyn Collection>, Vec<String>), ProcessorError> {
    let metadata = input.metadata().clone();
    let mut rows = input.into_rows().map_err(|error| {
        ProcessorError::ProcessingError(format!("failed to extract rows: {error}"))
    })?;
    let mut errors = Vec::new();
    for evaluation in evaluations {
        let mut columns = vec![Vec::with_capacity(rows.len()); evaluation.args.len()];
        let mut argument_errors = vec![None; rows.len()];
        for (row_index, tuple) in rows.iter().enumerate() {
            let context = EvalRowContext {
                tuple,
                collection_metadata: &metadata,
            };
            for (index, arg) in evaluation.args.iter().enumerate() {
                match arg.eval_with_context(&context) {
                    Ok(value) => columns[index].push(value),
                    Err(error) => {
                        columns[index].push(datatypes::Value::Null);
                        argument_errors[row_index] = Some(error.to_string());
                    }
                }
            }
        }
        let values = evaluation
            .udf
            .eval_batch(&columns, rows.len())
            .map_err(|error| {
                ProcessorError::ProcessingError(format!(
                    "WASM UDF batch evaluation failed: {error}"
                ))
            })?;
        if values.len() != rows.len() {
            return Err(ProcessorError::ProcessingError(format!(
                "WASM UDF returned {} values for {} input rows",
                values.len(),
                rows.len()
            )));
        }
        let mut next_rows = Vec::with_capacity(rows.len());
        for (row_index, (mut tuple, value)) in rows.into_iter().zip(values).enumerate() {
            let result = match argument_errors[row_index].take() {
                Some(error) => Err(error),
                None => value.map_err(|error| error.to_string()),
            };
            match result {
                Ok(value) => {
                    tuple.add_affiliate_column(Arc::clone(&evaluation.output_name), value);
                    next_rows.push(tuple);
                }
                Err(error) => errors.push(format!("row {row_index}: {error}")),
            }
        }
        rows = next_rows;
    }
    let output = RecordBatch::new_with_metadata(rows, metadata).map_err(|error| {
        ProcessorError::ProcessingError(format!("failed to build WASM UDF output: {error}"))
    })?;
    Ok((Box::new(output), errors))
}

impl Processor for WasmUdfComputeProcessor {
    fn id(&self) -> &str {
        &self.id
    }

    fn start(&mut self, spawner: &TaskSpawner) -> ProcessorStart {
        let mut inputs = fan_in_streams(std::mem::take(&mut self.inputs));
        let mut controls = fan_in_control_streams(std::mem::take(&mut self.control_inputs));
        let mut control_active = !controls.is_empty();
        let output = self.output.clone();
        let control_output = self.control_output.clone();
        let evaluations = Arc::clone(&self.evaluations);
        let capacities = self.channel_capacities;
        let stats = Arc::clone(&self.stats);
        ProcessorStart::ready(spawner.spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    item = controls.next(), if control_active => {
                        if let Some(Ok(signal)) = item {
                            let terminal = signal.is_terminal();
                            send_control_with_backpressure(&control_output, capacities.control, signal).await?;
                            if terminal { return Ok(()); }
                        } else { control_active = false; }
                    }
                    item = inputs.next() => match item {
                        Some(Ok(StreamData::Collection(collection))) => {
                            let start = std::time::Instant::now();
                            let result = apply_compute(collection, &evaluations);
                            stats.record_handle_duration(start.elapsed());
                            match result {
                                Ok((collection, errors)) => {
                                    for error in errors {
                                        stats.record_error_logged("WASM UDF compute error", error);
                                    }
                                    send_with_backpressure(&output, capacities.data, StreamData::collection(collection), Some(stats.as_ref())).await?
                                }
                                Err(error) => stats.record_error_logged("WASM UDF compute error", error.to_string()),
                            }
                        }
                        Some(Ok(data)) => {
                            let terminal = data.is_terminal();
                            send_with_backpressure(&output, capacities.data, data, Some(stats.as_ref())).await?;
                            if terminal { return Ok(()); }
                        }
                        Some(Err(_error)) => return Err(ProcessorError::ChannelClosed),
                        None => return Ok(()),
                    }
                }
            }
        }))
    }

    fn add_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<StreamData>>,
    {
        self.inputs.push(receiver.into());
    }

    fn add_control_input<R>(&mut self, receiver: R)
    where
        R: Into<LinkReceiver<ControlSignal>>,
    {
        self.control_inputs.push(receiver.into());
    }

    fn subscribe_output(&self) -> Option<LinkReceiver<StreamData>> {
        self.output.subscribe()
    }
    fn subscribe_control_output(&self) -> Option<LinkReceiver<ControlSignal>> {
        self.control_output.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::custom_func::BatchFunc;
    use crate::expr::scalar::ColumnRef;
    use crate::model::{Message, Tuple};
    use datatypes::Value;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct TestBatchFunc {
        calls: Arc<AtomicUsize>,
        offset: i64,
        truncate_result: bool,
    }

    #[derive(Debug)]
    struct TestDefinition;

    impl crate::expr::custom_func::CustomFunc for TestDefinition {
        fn name(&self) -> &str {
            "test_udf"
        }
    }

    impl BatchFunc for TestBatchFunc {
        fn eval_batch(
            &self,
            args: &[Vec<Value>],
            _row_count: usize,
        ) -> Result<Vec<Result<Value, crate::expr::func::EvalError>>, crate::expr::func::EvalError>
        {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let values =
                args.first()
                    .ok_or_else(|| crate::expr::func::EvalError::TypeMismatch {
                        expected: "one argument".to_string(),
                        actual: "zero arguments".to_string(),
                    })?;
            let mut output = values
                .iter()
                .map(|value| match value {
                    Value::Int64(value) => Ok(Value::Int64(value + self.offset)),
                    other => Err(crate::expr::func::EvalError::TypeMismatch {
                        expected: "int64".to_string(),
                        actual: format!("{other:?}"),
                    }),
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(Ok)
                .collect::<Vec<_>>();
            if self.truncate_result {
                output.pop();
            }
            Ok(output)
        }
    }

    #[derive(Debug)]
    struct ZeroArgBatchFunc;

    impl BatchFunc for ZeroArgBatchFunc {
        fn eval_batch(
            &self,
            _args: &[Vec<Value>],
            row_count: usize,
        ) -> Result<Vec<Result<Value, crate::expr::func::EvalError>>, crate::expr::func::EvalError>
        {
            Ok((0..row_count).map(|_| Ok(Value::Int64(7))).collect())
        }
    }

    #[derive(Debug)]
    struct OneRowErrorBatchFunc;

    impl BatchFunc for OneRowErrorBatchFunc {
        fn eval_batch(
            &self,
            args: &[Vec<Value>],
            _row_count: usize,
        ) -> Result<Vec<Result<Value, crate::expr::func::EvalError>>, crate::expr::func::EvalError>
        {
            Ok(args[0]
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    if index == 1 {
                        Err(crate::expr::func::EvalError::TypeMismatch {
                            expected: "valid test row".to_string(),
                            actual: "invalid test row".to_string(),
                        })
                    } else {
                        Ok(value.clone())
                    }
                })
                .collect())
        }
    }

    fn input_batch() -> RecordBatch {
        let rows = [1_i64, 2]
            .into_iter()
            .map(|value| {
                Tuple::new(vec![Arc::new(Message::new(
                    "stream",
                    vec![Arc::from("a")],
                    vec![Arc::new(Value::Int64(value))],
                ))])
            })
            .collect();
        RecordBatch::new(rows).expect("create input batch")
    }

    fn source_column(index: usize) -> crate::expr::ScalarExpr {
        crate::expr::ScalarExpr::Column(ColumnRef::ByIndex {
            source_name: "stream".to_string(),
            column_index: index,
        })
    }

    fn affiliate_column(name: &str) -> crate::expr::ScalarExpr {
        crate::expr::ScalarExpr::Column(ColumnRef::ByName {
            column_name: name.to_string(),
        })
    }

    #[test]
    fn batch_compute_evaluates_full_collection_and_preserves_dependencies() {
        let first_calls = Arc::new(AtomicUsize::new(0));
        let second_calls = Arc::new(AtomicUsize::new(0));
        let evaluations = vec![
            BoundEvaluation {
                output_name: Arc::new("__vf_wasm_1".to_string()),
                definition: Arc::new(TestDefinition),
                udf: Arc::new(TestBatchFunc {
                    calls: Arc::clone(&first_calls),
                    offset: 1,
                    truncate_result: false,
                }),
                args: vec![source_column(0)],
            },
            BoundEvaluation {
                output_name: Arc::new("__vf_wasm_2".to_string()),
                definition: Arc::new(TestDefinition),
                udf: Arc::new(TestBatchFunc {
                    calls: Arc::clone(&second_calls),
                    offset: 10,
                    truncate_result: false,
                }),
                args: vec![affiliate_column("__vf_wasm_1")],
            },
        ];

        let (output, errors) =
            apply_compute(Box::new(input_batch()), &evaluations).expect("batch compute");
        assert!(errors.is_empty());
        let rows = output.rows();
        assert_eq!(first_calls.load(Ordering::Relaxed), 1);
        assert_eq!(second_calls.load(Ordering::Relaxed), 1);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].affiliate().unwrap().value("__vf_wasm_1"),
            Some(&Value::Int64(2))
        );
        assert_eq!(
            rows[0].affiliate().unwrap().value("__vf_wasm_2"),
            Some(&Value::Int64(12))
        );
        assert_eq!(
            rows[1].affiliate().unwrap().value("__vf_wasm_1"),
            Some(&Value::Int64(3))
        );
        assert_eq!(
            rows[1].affiliate().unwrap().value("__vf_wasm_2"),
            Some(&Value::Int64(13))
        );
    }

    #[test]
    fn batch_compute_rejects_result_length_mismatch() {
        let evaluations = vec![BoundEvaluation {
            output_name: Arc::new("__vf_wasm_1".to_string()),
            definition: Arc::new(TestDefinition),
            udf: Arc::new(TestBatchFunc {
                calls: Arc::new(AtomicUsize::new(0)),
                offset: 0,
                truncate_result: true,
            }),
            args: vec![source_column(0)],
        }];

        let error = match apply_compute(Box::new(input_batch()), &evaluations) {
            Ok(_) => panic!("length mismatch should fail"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("returned 1 values for 2 input rows"));
    }

    #[test]
    fn batch_compute_preserves_row_count_for_zero_argument_udf() {
        let evaluations = vec![BoundEvaluation {
            output_name: Arc::new("__vf_wasm_1".to_string()),
            definition: Arc::new(TestDefinition),
            udf: Arc::new(ZeroArgBatchFunc),
            args: Vec::new(),
        }];

        let (output, errors) =
            apply_compute(Box::new(input_batch()), &evaluations).expect("batch compute");
        assert!(errors.is_empty());
        assert_eq!(output.rows().len(), 2);
        for row in output.rows() {
            assert_eq!(
                row.affiliate().unwrap().value("__vf_wasm_1"),
                Some(&Value::Int64(7))
            );
        }
    }

    #[test]
    fn batch_compute_keeps_successful_rows_when_one_row_fails() {
        let evaluations = vec![BoundEvaluation {
            output_name: Arc::new("__vf_wasm_1".to_string()),
            definition: Arc::new(TestDefinition),
            udf: Arc::new(OneRowErrorBatchFunc),
            args: vec![source_column(0)],
        }];

        let (output, errors) =
            apply_compute(Box::new(input_batch()), &evaluations).expect("batch compute");
        assert_eq!(output.rows().len(), 1);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("row 1"));
        assert_eq!(
            output.rows()[0].affiliate().unwrap().value("__vf_wasm_1"),
            Some(&Value::Int64(1))
        );
    }
}
