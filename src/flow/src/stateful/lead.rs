use super::util::{bool_arg, lag_offset};
use super::{
    DeferredEvalInput, PreparedStatefulEval, ResolvedStatefulRow, StatefulEvalInput,
    StatefulEvalUpdate, StatefulFunction, StatefulFunctionInstance,
};
use crate::catalog::{
    FunctionArgSpec, FunctionContext, FunctionDef, FunctionKind, FunctionRequirement,
    FunctionSignatureSpec, StatefulFunctionSpec, TypeSpec,
};
use datatypes::{ConcreteDatatype, Value};
use std::any::Any;
use std::collections::{HashSet, VecDeque};

pub struct LeadFunction;

pub fn lead_function_def() -> FunctionDef {
    FunctionDef {
        kind: FunctionKind::Stateful,
        name: "lead".to_string(),
        aliases: vec![],
        signature: FunctionSignatureSpec {
            args: vec![
                FunctionArgSpec {
                    name: "x".to_string(),
                    r#type: TypeSpec::Any,
                    optional: false,
                    variadic: false,
                },
                FunctionArgSpec {
                    name: "offset".to_string(),
                    r#type: TypeSpec::Named {
                        name: "int64".to_string(),
                    },
                    optional: true,
                    variadic: false,
                },
                FunctionArgSpec {
                    name: "ignore_null".to_string(),
                    r#type: TypeSpec::Named {
                        name: "bool".to_string(),
                    },
                    optional: true,
                    variadic: false,
                },
            ],
            return_type: TypeSpec::Any,
        },
        description: "Return the value from offset rows ahead, optionally skipping NULL inputs."
            .to_string(),
        allowed_contexts: vec![FunctionContext::Select],
        requirements: vec![FunctionRequirement::DeterministicOrder],
        constraints: vec![
            "Requires 1 to 3 arguments.".to_string(),
            "Return type matches the argument type.".to_string(),
            "The optional offset must be a positive integer.".to_string(),
            "When ignore_null is true, NULL probe values do not advance the lead state."
                .to_string(),
            "An unresolved origin returns NULL at normal partition end.".to_string(),
        ],
        examples: vec![
            "SELECT lead(x) OVER (UNTIL ts - origin.ts > 5) FROM stream".to_string(),
            "SELECT lead(x, 2, true) FROM stream".to_string(),
        ],
        aggregate: None,
        stateful: Some(StatefulFunctionSpec {
            state_semantics:
                "Maintains pending lead origins and resolves them from later probe rows per partition."
                    .to_string(),
        }),
    }
}

impl LeadFunction {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LeadFunction {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default)]
struct LeadInstance {
    config: Option<LeadConfig>,
    pending: VecDeque<LeadRequest>,
}

#[derive(Clone, Copy)]
struct LeadConfig {
    offset: usize,
    ignore_null: bool,
}

#[derive(Clone, Copy)]
struct LeadRequest {
    origin_id: u64,
    matched_count: usize,
}

impl StatefulFunctionInstance for LeadInstance {
    fn prepare_eval(&self, _input: StatefulEvalInput<'_>) -> Result<PreparedStatefulEval, String> {
        Err("lead() requires deferred evaluation".to_string())
    }

    fn prepare_deferred_eval(
        &self,
        input: DeferredEvalInput<'_>,
    ) -> Result<PreparedStatefulEval, String> {
        if input.origin_args.is_empty() || input.origin_args.len() > 3 {
            return Err(format!(
                "lead() expects 1 to 3 arguments, got {}",
                input.origin_args.len()
            ));
        }

        let config = match self.config {
            Some(config) => config,
            None => LeadConfig {
                offset: match input.origin_args.get(1) {
                    Some(value) => lag_offset(value)?,
                    None => 1,
                },
                ignore_null: match input.origin_args.get(2) {
                    Some(value) => bool_arg("lead() third argument", value)?,
                    None => true,
                },
            },
        };
        let config_update = self.config.is_none().then_some(config);
        let mut resolved = Vec::new();
        let mut resolved_ids = HashSet::new();
        let mut matched_counts = Vec::with_capacity(self.pending.len());

        if input.probes.len() != self.pending.len() {
            return Err(format!(
                "lead() received {} probes for {} pending origins",
                input.probes.len(),
                self.pending.len()
            ));
        }

        for (request, probe) in self.pending.iter().zip(input.probes) {
            if request.origin_id != probe.origin_id {
                return Err(format!(
                    "lead() probe origin order mismatch: expected {}, got {}",
                    request.origin_id, probe.origin_id
                ));
            }

            let mut matched_count = request.matched_count;
            if probe.until {
                resolved.push(ResolvedStatefulRow {
                    origin_id: request.origin_id,
                    value: Value::Null,
                });
                resolved_ids.insert(request.origin_id);
                continue;
            }

            if probe.should_apply {
                let value = probe
                    .args
                    .first()
                    .ok_or_else(|| "lead() probe is missing the value argument".to_string())?;
                if !(config.ignore_null && value.is_null()) {
                    matched_count += 1;
                    if matched_count >= config.offset {
                        resolved.push(ResolvedStatefulRow {
                            origin_id: request.origin_id,
                            value: value.clone(),
                        });
                        resolved_ids.insert(request.origin_id);
                        continue;
                    }
                }
            }
            matched_counts.push(matched_count);
        }

        Ok(PreparedStatefulEval::deferred(
            resolved,
            Box::new(LeadUpdate {
                config: config_update,
                resolved_ids,
                matched_counts,
                origin_id: input.origin_id,
            }),
        ))
    }

    fn commit_eval(&mut self, update: Box<dyn StatefulEvalUpdate>) {
        let update = update
            .as_any()
            .downcast_ref::<LeadUpdate>()
            .expect("lead received incompatible update");
        if let Some(config) = update.config {
            self.config = Some(config);
        }
        let mut matched_counts = update.matched_counts.iter();
        self.pending.retain_mut(|request| {
            if update.resolved_ids.contains(&request.origin_id) {
                return false;
            }
            request.matched_count = *matched_counts
                .next()
                .expect("lead update is missing a pending request");
            true
        });
        self.pending.push_back(LeadRequest {
            origin_id: update.origin_id,
            matched_count: 0,
        });
    }

    fn finish(&mut self) -> Vec<ResolvedStatefulRow> {
        self.pending
            .drain(..)
            .map(|request| ResolvedStatefulRow {
                origin_id: request.origin_id,
                value: Value::Null,
            })
            .collect()
    }
}

struct LeadUpdate {
    config: Option<LeadConfig>,
    resolved_ids: HashSet<u64>,
    matched_counts: Vec<usize>,
    origin_id: u64,
}

impl StatefulEvalUpdate for LeadUpdate {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl StatefulFunction for LeadFunction {
    fn supports_inline_execution(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "lead"
    }

    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String> {
        if input_types.is_empty() || input_types.len() > 3 {
            return Err(format!(
                "lead() expects 1 to 3 argument types, got {}",
                input_types.len()
            ));
        }
        Ok(input_types[0].clone())
    }

    fn create_instance(&self) -> Box<dyn StatefulFunctionInstance> {
        Box::new(LeadInstance::default())
    }

    fn supports_deferred_eval(&self) -> bool {
        true
    }
}
