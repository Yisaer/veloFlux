use super::CustomFunc;
use crate::expr::func::EvalError;
use crate::expr::ScalarExpr;
use datatypes::{ConcreteDatatype, Value};
use model_runtime::ModelRegistry;
use std::collections::HashMap;
use std::sync::Arc;

/// Planner-visible and row-based runtime implementation of the ONNX function.
pub struct OnnxFunc {
    model_registry: Arc<ModelRegistry>,
    prepared_models: Arc<HashMap<String, Arc<model_runtime::PreparedOnnxModel>>>,
}

fn type_error(error: impl Into<String>) -> EvalError {
    EvalError::TypeMismatch {
        expected: "valid ONNX function input".to_string(),
        actual: error.into(),
    }
}

impl std::fmt::Debug for OnnxFunc {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("OnnxFunc").finish_non_exhaustive()
    }
}

impl OnnxFunc {
    pub fn new(model_registry: Arc<ModelRegistry>) -> Self {
        Self {
            model_registry,
            prepared_models: Arc::new(HashMap::new()),
        }
    }

    fn validate_call(&self, args: &[ScalarExpr]) -> Result<(), String> {
        let Some(ScalarExpr::Literal(Value::String(model_name), _)) = args.first() else {
            return Err("onnx model_name must be a string constant".to_string());
        };
        let metadata = self
            .model_registry
            .metadata(model_name)?
            .ok_or_else(|| format!("onnx model '{model_name}' is not registered"))?;
        let expected = metadata.model.inputs.len();
        if args.len().saturating_sub(1) != expected {
            return Err(format!(
                "onnx model '{model_name}' expects {expected} input arguments, got {}",
                args.len().saturating_sub(1)
            ));
        }
        metadata.input_types()?;
        metadata.output_type()?;
        Ok(())
    }
}

impl CustomFunc for OnnxFunc {
    fn bind_runtime(&self) -> Result<Option<Arc<dyn CustomFunc>>, String> {
        Ok(Some(Arc::new(Self {
            model_registry: Arc::clone(&self.model_registry),
            prepared_models: Arc::clone(&self.prepared_models),
        })))
    }

    fn bind_runtime_with_prepared_models(
        &self,
        prepared_models: &HashMap<String, Arc<model_runtime::PreparedOnnxModel>>,
    ) -> Result<Option<Arc<dyn CustomFunc>>, String> {
        Ok(Some(Arc::new(Self {
            model_registry: Arc::clone(&self.model_registry),
            prepared_models: Arc::new(prepared_models.clone()),
        })))
    }

    fn validate_call_with_schema(
        &self,
        args: &[ScalarExpr],
        bindings: &crate::expr::sql_conversion::SchemaBinding,
    ) -> Result<(), String> {
        self.validate_call(args)?;
        let Some(ScalarExpr::Literal(Value::String(model_name), _)) = args.first() else {
            return Ok(());
        };
        let metadata = self
            .model_registry
            .metadata(model_name)?
            .ok_or_else(|| "ONNX model metadata disappeared during planning".to_string())?;
        let expected_types = metadata.input_types()?;
        for (index, (argument, expected)) in args[1..].iter().zip(expected_types).enumerate() {
            if let Some(actual) = infer_scalar_type(argument, bindings) {
                if actual != expected && actual != ConcreteDatatype::Null {
                    return Err(format!(
                        "onnx input {} has type {:?}, expected {:?}",
                        index, actual, expected
                    ));
                }
            }
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "onnx"
    }

    fn return_type(&self, args: &[ScalarExpr]) -> Result<Option<ConcreteDatatype>, String> {
        let Some(ScalarExpr::Literal(Value::String(model_name), _)) = args.first() else {
            return Ok(None);
        };
        let metadata = self
            .model_registry
            .metadata(model_name)?
            .ok_or_else(|| format!("onnx model '{model_name}' is not registered"))?;
        Ok(Some(metadata.output_type()?))
    }

    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        let Some(Value::String(model_name)) = args.first() else {
            return Err(type_error("onnx model_name must be a string constant"));
        };
        let metadata = self
            .model_registry
            .metadata(model_name)
            .map_err(type_error)?
            .ok_or_else(|| type_error(format!("onnx model '{model_name}' is not registered")))?;
        let expected = metadata.model.inputs.len();
        if args.len().saturating_sub(1) != expected {
            return Err(type_error(format!(
                "onnx model '{model_name}' expects {expected} input arguments, got {}",
                args.len().saturating_sub(1)
            )));
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        self.validate_row(args)?;
        let Value::String(model_name) = &args[0] else {
            return Err(type_error("onnx model_name must be a string constant"));
        };
        let model = self
            .prepared_models
            .get(model_name.as_str())
            .ok_or_else(|| {
                type_error(format!(
                    "prepared ONNX model '{model_name}' is unavailable in the pipeline runtime"
                ))
            })?;
        model
            .run_values(&args[1..])
            .map_err(|error| type_error(format!("model '{model_name}': {error}")))
    }
}

fn infer_scalar_type(
    expression: &ScalarExpr,
    bindings: &crate::expr::sql_conversion::SchemaBinding,
) -> Option<datatypes::ConcreteDatatype> {
    match expression {
        ScalarExpr::Literal(_, data_type) => Some(data_type.clone()),
        ScalarExpr::Column(column) => match column {
            crate::expr::scalar::ColumnRef::ByIndex {
                source_name,
                column_index,
            } => bindings
                .entries()
                .iter()
                .find(|entry| entry.source_name == *source_name)
                .and_then(|entry| entry.schema.column_schemas().get(*column_index))
                .map(|column| column.data_type.clone()),
            crate::expr::scalar::ColumnRef::ByName { column_name } => bindings
                .entries()
                .iter()
                .filter_map(|entry| entry.schema.column_schema_by_name(column_name))
                .map(|column| column.data_type.clone())
                .next(),
        },
        ScalarExpr::FieldAccess { expr, field_name } => match infer_scalar_type(expr, bindings) {
            Some(datatypes::ConcreteDatatype::Struct(struct_type)) => struct_type
                .fields()
                .iter()
                .find(|field| field.name() == field_name)
                .map(|field| field.data_type().clone()),
            _ => None,
        },
        ScalarExpr::CallFunc { func, args } => func.return_type(args).ok().flatten(),
        _ => None,
    }
}
