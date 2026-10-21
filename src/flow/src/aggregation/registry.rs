use super::prepared::PreparedAccumulatorFactory;
use super::AccumulatorState;
use crate::aggregation::{
    avg_function_def, count_function_def, deduplicate_function_def, last_row_function_def,
    max_function_def, median_function_def, merge_agg_function_def, min_function_def,
    ndv_function_def, stddev_function_def, stddevs_function_def, sum_function_def,
    var_function_def, vars_function_def, AvgFunction, CountFunction, DeduplicateFunction,
    LastRowFunction, MaxFunction, MedianFunction, MergeAggFunction, MinFunction, NdvFunction,
    StddevFunction, StddevsFunction, SumFunction, VarFunction, VarsFunction,
};
use crate::catalog::FunctionDef;
use datatypes::{ConcreteDatatype, Value};
use parking_lot::RwLock;
use parser::aggregate_registry::AggregateRegistry;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

pub trait AggregateUpdate: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

pub trait AggregateAccumulator: Send + Sync {
    fn prepare_update(&self, args: &[Value]) -> Result<Box<dyn AggregateUpdate>, String>;
    fn commit_update(&mut self, update: Box<dyn AggregateUpdate>);
    fn finalize(&self) -> Value;

    fn update(&mut self, args: &[Value]) -> Result<(), String> {
        let update = self.prepare_update(args)?;
        self.commit_update(update);
        Ok(())
    }
}

pub trait AggregateFunction: Send + Sync {
    /// Whether this engine-controlled implementation may execute inline in a region.
    /// Custom implementations remain isolated unless they explicitly opt in.
    fn supports_inline_execution(&self) -> bool {
        false
    }

    fn name(&self) -> &str;
    fn return_type(&self, input_types: &[ConcreteDatatype]) -> Result<ConcreteDatatype, String>;
    fn create_accumulator(&self) -> Box<dyn AggregateAccumulator>;
    /// Whether this aggregate supports incremental (streaming) updates.
    fn supports_incremental(&self) -> bool {
        false
    }
}

/// Execution metadata is selected when an implementation is registered.
#[derive(Clone)]
pub(crate) enum AccumulatorFactory {
    Prepared(PreparedAccumulatorFactory),
    Registered(Arc<dyn AggregateFunction>),
}

impl AccumulatorFactory {
    pub(crate) fn create(&self) -> AccumulatorState {
        match self {
            Self::Prepared(factory) => factory.create(),
            Self::Registered(function) => {
                AccumulatorState::registered(function.create_accumulator())
            }
        }
    }

    pub(crate) fn uses_non_null_wildcard_sentinel(&self) -> bool {
        matches!(self, Self::Prepared(factory) if factory.is_count())
    }
}

struct AggregateRegistration {
    function: Arc<dyn AggregateFunction>,
    factory: AccumulatorFactory,
}

pub struct AggregateFunctionRegistry {
    functions: RwLock<HashMap<String, AggregateRegistration>>,
}

impl AggregateFunctionRegistry {
    pub fn new() -> Self {
        Self {
            functions: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_builtins() -> Arc<Self> {
        let registry = Arc::new(Self::new());
        registry.register_builtin_functions();
        registry
    }

    /// Register through the boxed accumulator interface, replacing any same-name entry.
    /// Concrete builtin execution is reserved for the registry's builtin registrations.
    pub fn register_function(&self, function: Arc<dyn AggregateFunction>) {
        let factory = AccumulatorFactory::Registered(Arc::clone(&function));
        self.functions.write().insert(
            function.name().to_lowercase(),
            AggregateRegistration { function, factory },
        );
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn AggregateFunction>> {
        self.functions
            .read()
            .get(&name.to_lowercase())
            .map(|entry| Arc::clone(&entry.function))
    }

    pub(crate) fn accumulator_factory(&self, name: &str) -> Option<AccumulatorFactory> {
        self.functions
            .read()
            .get(&name.to_lowercase())
            .map(|entry| entry.factory.clone())
    }

    pub fn is_registered(&self, name: &str) -> bool {
        self.functions.read().contains_key(&name.to_lowercase())
    }

    /// Check if the given aggregate function supports incremental updates.
    pub fn supports_incremental(&self, name: &str) -> bool {
        self.functions
            .read()
            .get(&name.to_lowercase())
            .map(|entry| entry.function.supports_incremental())
            .unwrap_or(false)
    }

    fn register_prepared_builtin(
        &self,
        function: Arc<dyn AggregateFunction>,
        factory: PreparedAccumulatorFactory,
    ) {
        self.functions.write().insert(
            function.name().to_lowercase(),
            AggregateRegistration {
                function,
                factory: AccumulatorFactory::Prepared(factory),
            },
        );
    }

    fn register_builtin_functions(&self) {
        self.register_prepared_builtin(
            Arc::new(AvgFunction::new()),
            PreparedAccumulatorFactory::avg(),
        );
        self.register_prepared_builtin(
            Arc::new(CountFunction::new()),
            PreparedAccumulatorFactory::count(),
        );
        self.register_function(Arc::new(DeduplicateFunction::new()));
        self.register_prepared_builtin(
            Arc::new(SumFunction::new()),
            PreparedAccumulatorFactory::sum(),
        );
        self.register_prepared_builtin(
            Arc::new(MaxFunction::new()),
            PreparedAccumulatorFactory::max(),
        );
        self.register_prepared_builtin(
            Arc::new(MinFunction::new()),
            PreparedAccumulatorFactory::min(),
        );
        self.register_function(Arc::new(MedianFunction::new()));
        self.register_function(Arc::new(LastRowFunction::new()));
        self.register_function(Arc::new(MergeAggFunction::new()));
        self.register_function(Arc::new(NdvFunction::new()));
        self.register_function(Arc::new(StddevFunction::new()));
        self.register_function(Arc::new(StddevsFunction::new()));
        self.register_function(Arc::new(VarFunction::new()));
        self.register_function(Arc::new(VarsFunction::new()));
    }
}

impl Default for AggregateFunctionRegistry {
    fn default() -> Self {
        let registry = Self::new();
        registry.register_builtin_functions();
        registry
    }
}

impl AggregateRegistry for AggregateFunctionRegistry {
    fn is_aggregate_function(&self, name: &str) -> bool {
        self.is_registered(name)
    }
}

pub fn builtin_aggregation_defs() -> Vec<FunctionDef> {
    vec![
        avg_function_def(),
        count_function_def(),
        deduplicate_function_def(),
        max_function_def(),
        median_function_def(),
        min_function_def(),
        last_row_function_def(),
        merge_agg_function_def(),
        ndv_function_def(),
        stddev_function_def(),
        stddevs_function_def(),
        sum_function_def(),
        var_function_def(),
        vars_function_def(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use datatypes::types;

    struct DummyAgg;
    impl AggregateFunction for DummyAgg {
        fn name(&self) -> &str {
            "dummy"
        }
        fn return_type(
            &self,
            _input_types: &[ConcreteDatatype],
        ) -> Result<ConcreteDatatype, String> {
            Ok(ConcreteDatatype::Int64(types::Int64Type))
        }
        fn create_accumulator(&self) -> Box<dyn AggregateAccumulator> {
            struct Update;
            impl AggregateUpdate for Update {
                fn as_any(&self) -> &dyn Any {
                    self
                }

                fn into_any(self: Box<Self>) -> Box<dyn Any> {
                    self
                }
            }
            struct Acc;
            impl AggregateAccumulator for Acc {
                fn prepare_update(
                    &self,
                    _args: &[Value],
                ) -> Result<Box<dyn AggregateUpdate>, String> {
                    Ok(Box::new(Update))
                }

                fn commit_update(&mut self, _update: Box<dyn AggregateUpdate>) {}

                fn finalize(&self) -> Value {
                    Value::Null
                }
            }
            Box::new(Acc)
        }
        fn supports_incremental(&self) -> bool {
            false
        }
    }

    #[test]
    fn test_supports_incremental_lookup() {
        let registry = AggregateFunctionRegistry::new();
        registry.register_function(Arc::new(SumFunction::new()));
        registry.register_function(Arc::new(DummyAgg));

        assert!(registry.supports_incremental("sum"));
        assert!(!registry.supports_incremental("dummy"));
        assert!(!registry.supports_incremental("nonexistent"));
    }
}
