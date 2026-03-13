use super::{
    AbsFunc, AcosFunc, AsinFunc, Atan2Func, AtanFunc, BitAndFunc, BitNotFunc, BitOrFunc,
    BitXorFunc, CeilFunc, CeilingFunc, ConcatFunc, ConvFunc, CosFunc, CoshFunc, CotFunc,
    CustomFunc, DegreesFunc, ExpFunc, FloorFunc, LnFunc, LogFunc, ModFunc, PiFunc, PowFunc,
    PowerFunc, RadiansFunc, RandFunc, RoundFunc, SignFunc, SinFunc, SinhFunc, SqrtFunc, TanFunc,
    TanhFunc,
};

use std::collections::HashMap;
use std::sync::Arc;

/// Registry for scalar custom functions referenced in SQL (e.g. `concat(a, b)`).
pub struct CustomFuncRegistry {
    functions: HashMap<String, Arc<dyn CustomFunc>>,
}

impl CustomFuncRegistry {
    pub fn with_builtins() -> Arc<Self> {
        Arc::new(Self::builtins())
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn CustomFunc>> {
        self.functions.get(&name.to_lowercase()).cloned()
    }

    pub fn is_registered(&self, name: &str) -> bool {
        self.functions.contains_key(&name.to_lowercase())
    }

    pub fn list_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.functions.keys().cloned().collect();
        names.sort();
        names
    }

    fn builtins() -> Self {
        let mut functions: HashMap<String, Arc<dyn CustomFunc>> = HashMap::new();

        register(&mut functions, Arc::new(ConcatFunc));
        register_math_functions(&mut functions);

        Self { functions }
    }
}

fn register(functions: &mut HashMap<String, Arc<dyn CustomFunc>>, func: Arc<dyn CustomFunc>) {
    functions.insert(func.name().to_lowercase(), func);
}

fn register_math_functions(functions: &mut HashMap<String, Arc<dyn CustomFunc>>) {
    for func in [
        Arc::new(AbsFunc) as Arc<dyn CustomFunc>,
        Arc::new(AcosFunc),
        Arc::new(AsinFunc),
        Arc::new(AtanFunc),
        Arc::new(Atan2Func),
        Arc::new(BitAndFunc),
        Arc::new(BitOrFunc),
        Arc::new(BitXorFunc),
        Arc::new(BitNotFunc),
        Arc::new(CeilFunc),
        Arc::new(CeilingFunc),
        Arc::new(ConvFunc),
        Arc::new(CosFunc),
        Arc::new(CoshFunc),
        Arc::new(CotFunc),
        Arc::new(DegreesFunc),
        Arc::new(ExpFunc),
        Arc::new(FloorFunc),
        Arc::new(LnFunc),
        Arc::new(LogFunc),
        Arc::new(ModFunc),
        Arc::new(PiFunc),
        Arc::new(PowFunc),
        Arc::new(PowerFunc),
        Arc::new(RadiansFunc),
        Arc::new(RandFunc),
        Arc::new(RoundFunc),
        Arc::new(SignFunc),
        Arc::new(SinFunc),
        Arc::new(SinhFunc),
        Arc::new(SqrtFunc),
        Arc::new(TanFunc),
        Arc::new(TanhFunc),
    ] {
        register(functions, func);
    }
}

impl Default for CustomFuncRegistry {
    fn default() -> Self {
        Self::builtins()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtins_include_concat() {
        let registry = CustomFuncRegistry::default();
        assert!(registry.is_registered("concat"));
        assert!(registry.is_registered("CONCAT"));
        assert!(registry.get("concat").is_some());
        assert!(registry.get("CoNcAt").is_some());
    }

    #[test]
    fn builtins_include_math_functions() {
        let registry = CustomFuncRegistry::default();
        assert!(registry.is_registered("abs"));
        assert!(registry.is_registered("sin"));
        assert!(registry.is_registered("pow"));
        assert!(registry.is_registered("ceiling"));
        assert!(registry.is_registered("ceil"));
        assert!(registry.is_registered("conv"));
    }

    #[test]
    fn registry_rejects_unknown_functions() {
        let registry = CustomFuncRegistry::default();
        assert!(!registry.is_registered("dummy"));
        assert!(registry.get("dummy").is_none());
        assert!(registry.get("missing").is_none());
    }
}
