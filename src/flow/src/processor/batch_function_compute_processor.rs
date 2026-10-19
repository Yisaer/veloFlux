//! Collection-level processor for batch-function evaluations.
use crate::expr::custom_func::BatchFunc;
use crate::expr::EvalRowContext;
use crate::model::{Collection, RecordBatch, Tuple};
use crate::planner::physical::{
    PhysicalBatchFunctionCompute, PhysicalBatchFunctionEvaluation, PhysicalPlan,
};
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
pub(crate) struct BoundBatchEvaluation {
    output_name: Arc<String>,
    definition: Arc<dyn crate::expr::custom_func::CustomFunc>,
    runtime: Arc<dyn BatchFunc>,
    args: Vec<crate::expr::ScalarExpr>,
}

/// A row that survived all preceding batch-function evaluations.
///
/// Batch evaluations compact the active row set after row-level failures. The original index is
/// retained so diagnostics continue to identify the row in the input collection rather than its
/// position in the compacted set.
struct ActiveRow {
    original_index: usize,
    tuple: Tuple,
}
pub struct BatchFunctionComputeProcessor {
    id: String,
    evaluations: Arc<Vec<BoundBatchEvaluation>>,
    inputs: Vec<LinkReceiver<StreamData>>,
    control_inputs: Vec<LinkReceiver<ControlSignal>>,
    output: LinkOutput<StreamData>,
    control_output: LinkOutput<ControlSignal>,
    channel_capacities: ProcessorChannelCapacities,
    stats: Arc<ProcessorStats>,
}
impl BatchFunctionComputeProcessor {
    pub fn new(
        id: impl Into<String>,
        plan: Arc<PhysicalBatchFunctionCompute>,
    ) -> Result<Self, ProcessorError> {
        Self::new_with_channel_capacities(id, plan, default_channel_capacities())
    }
    pub(crate) fn new_with_channel_capacities(
        id: impl Into<String>,
        plan: Arc<PhysicalBatchFunctionCompute>,
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
            PhysicalPlan::BatchFunctionCompute(compute) => {
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
    evaluation: &PhysicalBatchFunctionEvaluation,
) -> Result<BoundBatchEvaluation, ProcessorError> {
    let runtime = evaluation
        .function
        .bind_batch_runtime()
        .map_err(ProcessorError::InvalidConfiguration)?
        .ok_or_else(|| {
            ProcessorError::InvalidConfiguration(format!(
                "batch function '{}' does not provide a batch runtime",
                evaluation.output_column
            ))
        })?;
    Ok(BoundBatchEvaluation {
        output_name: Arc::new(evaluation.output_column.clone()),
        definition: Arc::clone(&evaluation.function),
        runtime,
        args: evaluation.args.clone(),
    })
}
pub(crate) fn bind_evaluations(
    evaluations: &[PhysicalBatchFunctionEvaluation],
) -> Result<Vec<BoundBatchEvaluation>, ProcessorError> {
    let mut bound: Vec<BoundBatchEvaluation> = Vec::with_capacity(evaluations.len());
    for evaluation in evaluations {
        if let Some(existing) = bound
            .iter()
            .find(|candidate| Arc::ptr_eq(&candidate.definition, &evaluation.function))
        {
            bound.push(BoundBatchEvaluation {
                output_name: Arc::new(evaluation.output_column.clone()),
                definition: Arc::clone(&evaluation.function),
                runtime: Arc::clone(&existing.runtime),
                args: evaluation.args.clone(),
            });
        } else {
            bound.push(bind_evaluation(evaluation)?);
        }
    }
    Ok(bound)
}
pub(crate) fn apply_compute(
    input: Box<dyn Collection>,
    evaluations: &[BoundBatchEvaluation],
) -> Result<(Box<dyn Collection>, Vec<String>), ProcessorError> {
    let metadata = input.metadata().clone();
    let input_rows = input.into_rows().map_err(|error| {
        ProcessorError::ProcessingError(format!("failed to extract rows: {error}"))
    })?;
    let mut rows: Vec<ActiveRow> = input_rows
        .into_iter()
        .enumerate()
        .map(|(original_index, tuple)| ActiveRow {
            original_index,
            tuple,
        })
        .collect();
    let mut errors = Vec::new();
    for evaluation in evaluations {
        let mut columns = vec![Vec::with_capacity(rows.len()); evaluation.args.len()];
        let mut argument_errors = vec![None; rows.len()];
        for (row_index, row) in rows.iter().enumerate() {
            let context = EvalRowContext {
                tuple: &row.tuple,
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
            .runtime
            .eval_batch(&columns, rows.len())
            .map_err(|error| {
                ProcessorError::ProcessingError(format!(
                    "batch function evaluation failed: {error}"
                ))
            })?;
        if values.len() != rows.len() {
            return Err(ProcessorError::ProcessingError(format!(
                "batch function returned {} values for {} input rows",
                values.len(),
                rows.len()
            )));
        }
        let mut next_rows = Vec::with_capacity(rows.len());
        for (row_index, (mut row, value)) in rows.into_iter().zip(values).enumerate() {
            let result = match argument_errors[row_index].take() {
                Some(error) => Err(error),
                None => value.map_err(|error| error.to_string()),
            };
            match result {
                Ok(value) => {
                    row.tuple
                        .add_affiliate_column(Arc::clone(&evaluation.output_name), value);
                    next_rows.push(row);
                }
                Err(error) => errors.push(format!("row {}: {error}", row.original_index)),
            }
        }
        rows = next_rows;
    }
    let output_rows = rows.into_iter().map(|row| row.tuple).collect();
    let output = RecordBatch::new_with_metadata(output_rows, metadata).map_err(|error| {
        ProcessorError::ProcessingError(format!("failed to build batch function output: {error}"))
    })?;
    Ok((Box::new(output), errors))
}
impl Processor for BatchFunctionComputeProcessor {
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
                                        stats.record_error_logged("batch function compute error", error);
                                    }
                                    send_with_backpressure(&output, capacities.data, StreamData::collection(collection), Some(stats.as_ref())).await?
                                }
                                Err(error) => stats.record_error_logged("batch function compute error", error.to_string()),
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
    #[derive(Debug)]
    struct SharedTestDefinition {
        bind_calls: Arc<AtomicUsize>,
        eval_calls: Arc<AtomicUsize>,
    }
    impl crate::expr::custom_func::CustomFunc for SharedTestDefinition {
        fn bind_batch_runtime(&self) -> Result<Option<Arc<dyn BatchFunc>>, String> {
            self.bind_calls.fetch_add(1, Ordering::Relaxed);
            Ok(Some(Arc::new(TestBatchFunc {
                calls: Arc::clone(&self.eval_calls),
                offset: 1,
                truncate_result: false,
            })))
        }

        fn name(&self) -> &str {
            "shared_test_udf"
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
    #[derive(Debug)]
    struct ErrorOnValueBatchFunc {
        rejected_value: i64,
    }
    impl BatchFunc for ErrorOnValueBatchFunc {
        fn eval_batch(
            &self,
            args: &[Vec<Value>],
            _row_count: usize,
        ) -> Result<Vec<Result<Value, crate::expr::func::EvalError>>, crate::expr::func::EvalError>
        {
            Ok(args[0]
                .iter()
                .map(|value| match value {
                    Value::Int64(value) if *value == self.rejected_value => {
                        Err(crate::expr::func::EvalError::TypeMismatch {
                            expected: "accepted test row".to_string(),
                            actual: format!("rejected value {value}"),
                        })
                    }
                    _ => Ok(value.clone()),
                })
                .collect())
        }
    }
    fn input_batch() -> RecordBatch {
        input_batch_values(&[1, 2])
    }
    fn input_batch_values(values: &[i64]) -> RecordBatch {
        let rows = values
            .iter()
            .copied()
            .into_iter()
            .map(|value| {
                Tuple::new(vec![Arc::new(Message::new(
                    "stream",
                    vec![Arc::from("a")],
                    vec![Value::Int64(value)],
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
            BoundBatchEvaluation {
                output_name: Arc::new("__vf_batch_1".to_string()),
                definition: Arc::new(TestDefinition),
                runtime: Arc::new(TestBatchFunc {
                    calls: Arc::clone(&first_calls),
                    offset: 1,
                    truncate_result: false,
                }),
                args: vec![source_column(0)],
            },
            BoundBatchEvaluation {
                output_name: Arc::new("__vf_batch_2".to_string()),
                definition: Arc::new(TestDefinition),
                runtime: Arc::new(TestBatchFunc {
                    calls: Arc::clone(&second_calls),
                    offset: 10,
                    truncate_result: false,
                }),
                args: vec![affiliate_column("__vf_batch_1")],
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
            rows[0].affiliate().unwrap().value("__vf_batch_1"),
            Some(&Value::Int64(2))
        );
        assert_eq!(
            rows[0].affiliate().unwrap().value("__vf_batch_2"),
            Some(&Value::Int64(12))
        );
        assert_eq!(
            rows[1].affiliate().unwrap().value("__vf_batch_1"),
            Some(&Value::Int64(3))
        );
        assert_eq!(
            rows[1].affiliate().unwrap().value("__vf_batch_2"),
            Some(&Value::Int64(13))
        );
    }
    #[test]
    fn batch_compute_rejects_result_length_mismatch() {
        let evaluations = vec![BoundBatchEvaluation {
            output_name: Arc::new("__vf_batch_1".to_string()),
            definition: Arc::new(TestDefinition),
            runtime: Arc::new(TestBatchFunc {
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
        let evaluations = vec![BoundBatchEvaluation {
            output_name: Arc::new("__vf_batch_1".to_string()),
            definition: Arc::new(TestDefinition),
            runtime: Arc::new(ZeroArgBatchFunc),
            args: Vec::new(),
        }];
        let (output, errors) =
            apply_compute(Box::new(input_batch()), &evaluations).expect("batch compute");
        assert!(errors.is_empty());
        assert_eq!(output.rows().len(), 2);
        for row in output.rows() {
            assert_eq!(
                row.affiliate().unwrap().value("__vf_batch_1"),
                Some(&Value::Int64(7))
            );
        }
    }
    #[test]
    fn batch_compute_keeps_successful_rows_when_one_row_fails() {
        let evaluations = vec![BoundBatchEvaluation {
            output_name: Arc::new("__vf_batch_1".to_string()),
            definition: Arc::new(TestDefinition),
            runtime: Arc::new(OneRowErrorBatchFunc),
            args: vec![source_column(0)],
        }];
        let (output, errors) =
            apply_compute(Box::new(input_batch()), &evaluations).expect("batch compute");
        assert_eq!(output.rows().len(), 1);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("row 1"));
        assert_eq!(
            output.rows()[0].affiliate().unwrap().value("__vf_batch_1"),
            Some(&Value::Int64(1))
        );
    }

    #[test]
    fn batch_compute_reports_original_row_indexes_after_compaction() {
        let evaluations = vec![
            BoundBatchEvaluation {
                output_name: Arc::new("__vf_batch_1".to_string()),
                definition: Arc::new(TestDefinition),
                runtime: Arc::new(ErrorOnValueBatchFunc { rejected_value: 2 }),
                args: vec![source_column(0)],
            },
            BoundBatchEvaluation {
                output_name: Arc::new("__vf_batch_2".to_string()),
                definition: Arc::new(TestDefinition),
                runtime: Arc::new(ErrorOnValueBatchFunc { rejected_value: 3 }),
                args: vec![affiliate_column("__vf_batch_1")],
            },
        ];

        let (output, errors) =
            apply_compute(Box::new(input_batch_values(&[1, 2, 3])), &evaluations)
                .expect("batch compute");

        assert_eq!(output.rows().len(), 1);
        assert_eq!(errors.len(), 2);
        assert!(errors[0].contains("row 1"));
        assert!(errors[1].contains("row 2"));
        assert_eq!(
            output.rows()[0].affiliate().unwrap().value("__vf_batch_1"),
            Some(&Value::Int64(1))
        );
        assert_eq!(
            output.rows()[0].affiliate().unwrap().value("__vf_batch_2"),
            Some(&Value::Int64(1))
        );
    }

    #[test]
    fn batch_compute_reuses_one_runtime_for_multiple_evaluations() {
        let bind_calls = Arc::new(AtomicUsize::new(0));
        let eval_calls = Arc::new(AtomicUsize::new(0));
        let definition: Arc<dyn crate::expr::custom_func::CustomFunc> =
            Arc::new(SharedTestDefinition {
                bind_calls: Arc::clone(&bind_calls),
                eval_calls: Arc::clone(&eval_calls),
            });
        let evaluations = vec![
            PhysicalBatchFunctionEvaluation {
                output_column: "__vf_batch_1".to_string(),
                function: Arc::clone(&definition),
                args: vec![source_column(0)],
                original_expr: sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("x")),
            },
            PhysicalBatchFunctionEvaluation {
                output_column: "__vf_batch_2".to_string(),
                function: Arc::clone(&definition),
                args: vec![source_column(0)],
                original_expr: sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("x")),
            },
        ];
        let bound = bind_evaluations(&evaluations).expect("bind batch evaluations");
        assert_eq!(bind_calls.load(Ordering::Relaxed), 1);
        assert!(Arc::ptr_eq(&bound[0].runtime, &bound[1].runtime));

        let (output, errors) =
            apply_compute(Box::new(input_batch()), &bound).expect("batch compute");
        assert!(errors.is_empty());
        assert_eq!(eval_calls.load(Ordering::Relaxed), 2);
        assert_eq!(output.rows().len(), 2);
    }
}
