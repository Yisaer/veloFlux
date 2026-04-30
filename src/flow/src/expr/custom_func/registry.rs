use super::array_func::{
    ArrayConcatFunc, ArrayContainsAnyFunc, ArrayContainsFunc, ArrayCreateFunc,
    ArrayLastPositionFunc, ArrayPositionFunc, ArrayRemoveFunc, ElementAtFunc, RepeatFunc,
    SequenceFunc,
};
use super::math_func::{
    AbsFunc, AcosFunc, AsinFunc, Atan2Func, AtanFunc, BitAndFunc, BitNotFunc, BitOrFunc,
    BitXorFunc, CeilFunc, CeilingFunc, ConvFunc, CosFunc, CoshFunc, CotFunc, DegreesFunc, ExpFunc,
    FloorFunc, LnFunc, LogFunc, ModFunc, PiFunc, PowFunc, PowerFunc, RadiansFunc, RandFunc,
    RoundFunc, SignFunc, SinFunc, SinhFunc, SqrtFunc, TanFunc, TanhFunc,
};
use super::null_func::IsNullFunc;

use super::string_func::{
    ConcatFunc, EndsWithFunc, FormatFunc, IndexOfFunc, LPadFunc, LTrimFunc, LengthFunc, LowerFunc,
    NumBytesFunc, RPadFunc, RTrimFunc, RegexpMatchesFunc, RegexpReplaceFunc, RegexpSubstrFunc,
    ReverseFunc, SplitValueFunc, StartsWithFunc, SubstringFunc, TrimFunc, UpperFunc,
};

use super::obj_func::{
    EraseFunc, ItemsFunc, KeysFunc, ObjToKvPairArrayFunc, ObjectConcatFunc, ObjectConstructFunc,
    ObjectFunc, ObjectPickFunc, ObjectSizeFunc, ValuesFunc, ZipFunc,
};

use super::misc_func::{
    BypassFunc, CardinalityFunc, CastFunc, ChrFunc, CoalesceFunc, Crc32Func, Dec2HexFunc,
    DecodeFunc, DelayFunc, EncodeFunc, Hex2DecFunc, Md5Func, NewUuidFunc, ParseJsonFunc, Sha1Func,
    Sha256Func, Sha384Func, Sha512Func, ToJsonFunc, TruncFunc, TstampFunc,
};

use super::CustomFunc;

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

        register_math_functions(&mut functions);
        register_array_functions(&mut functions);
        register_null_functions(&mut functions);
        register_string_functions(&mut functions);
        register_object_functions(&mut functions);
        register_misc_functions(&mut functions);

        Self { functions }
    }
}

fn register(functions: &mut HashMap<String, Arc<dyn CustomFunc>>, func: Arc<dyn CustomFunc>) {
    functions.insert(func.name().to_lowercase(), Arc::clone(&func));
    for alias in func.aliases() {
        functions.insert(alias.to_lowercase(), Arc::clone(&func));
    }
}

fn register_array_functions(functions: &mut HashMap<String, Arc<dyn CustomFunc>>) {
    for func in [
        Arc::new(ArrayPositionFunc) as Arc<dyn CustomFunc>,
        Arc::new(ArrayLastPositionFunc),
        Arc::new(ElementAtFunc),
        Arc::new(ArrayContainsFunc),
        Arc::new(ArrayContainsAnyFunc),
        Arc::new(ArrayCreateFunc),
        Arc::new(ArrayRemoveFunc),
        Arc::new(RepeatFunc),
        Arc::new(SequenceFunc),
        Arc::new(ArrayConcatFunc),
    ] {
        register(functions, func);
    }
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

fn register_null_functions(functions: &mut HashMap<String, Arc<dyn CustomFunc>>) {
    register(functions, Arc::new(IsNullFunc));
}

fn register_string_functions(functions: &mut HashMap<String, Arc<dyn CustomFunc>>) {
    for func in [
        Arc::new(FormatFunc) as Arc<dyn CustomFunc>,
        Arc::new(ConcatFunc),
        Arc::new(EndsWithFunc),
        Arc::new(IndexOfFunc),
        Arc::new(LengthFunc),
        Arc::new(LowerFunc),
        Arc::new(LPadFunc),
        Arc::new(LTrimFunc),
        Arc::new(NumBytesFunc),
        Arc::new(RegexpMatchesFunc),
        Arc::new(RegexpReplaceFunc),
        Arc::new(RegexpSubstrFunc),
        Arc::new(ReverseFunc),
        Arc::new(RPadFunc),
        Arc::new(RTrimFunc),
        Arc::new(SubstringFunc),
        Arc::new(StartsWithFunc),
        Arc::new(SplitValueFunc),
        Arc::new(TrimFunc),
        Arc::new(UpperFunc),
    ] {
        register(functions, func);
    }
}

fn register_object_functions(functions: &mut HashMap<String, Arc<dyn CustomFunc>>) {
    for func in [
        Arc::new(KeysFunc) as Arc<dyn CustomFunc>,
        Arc::new(ValuesFunc),
        Arc::new(ObjectFunc),
        Arc::new(ZipFunc),
        Arc::new(ItemsFunc),
        Arc::new(ObjectConcatFunc),
        Arc::new(ObjectConstructFunc),
        Arc::new(EraseFunc),
        Arc::new(ObjectSizeFunc),
        Arc::new(ObjectPickFunc),
        Arc::new(ObjToKvPairArrayFunc),
    ] {
        register(functions, func);
    }
}

fn register_misc_functions(functions: &mut HashMap<String, Arc<dyn CustomFunc>>) {
    for func in [
        Arc::new(BypassFunc) as Arc<dyn CustomFunc>,
        Arc::new(ChrFunc),
        Arc::new(EncodeFunc),
        Arc::new(DecodeFunc),
        Arc::new(TruncFunc),
        Arc::new(Md5Func),
        Arc::new(Sha1Func),
        Arc::new(Sha256Func),
        Arc::new(Sha384Func),
        Arc::new(Sha512Func),
        Arc::new(Crc32Func),
        Arc::new(CoalesceFunc),
        Arc::new(Hex2DecFunc),
        Arc::new(Dec2HexFunc),
        Arc::new(ToJsonFunc),
        Arc::new(ParseJsonFunc),
        Arc::new(CardinalityFunc),
        Arc::new(NewUuidFunc),
        Arc::new(CastFunc),
        Arc::new(TstampFunc),
        Arc::new(DelayFunc),
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
    fn builtins_include_string_functions() {
        let registry = CustomFuncRegistry::default();

        assert!(registry.is_registered("concat"));
        assert!(registry.is_registered("format"));
        assert!(registry.is_registered("endswith"));
        assert!(registry.is_registered("indexof"));
        assert!(registry.is_registered("length"));
        assert!(registry.is_registered("lower"));
        assert!(registry.is_registered("lpad"));
        assert!(registry.is_registered("ltrim"));
        assert!(registry.is_registered("numbytes"));
        assert!(registry.is_registered("regexp_matches"));
        assert!(registry.is_registered("regexp_replace"));
        assert!(registry.is_registered("regexp_substring"));
        assert!(registry.is_registered("regexp_substr"));
        assert!(registry.is_registered("reverse"));
        assert!(registry.is_registered("rpad"));
        assert!(registry.is_registered("rtrim"));
        assert!(registry.is_registered("substring"));
        assert!(registry.is_registered("startswith"));
        assert!(registry.is_registered("split_value"));
        assert!(registry.is_registered("trim"));
        assert!(registry.is_registered("upper"));
    }

    #[test]
    fn builtins_include_array_functions() {
        let registry = CustomFuncRegistry::default();

        assert!(registry.is_registered("array_position"));
        assert!(registry.is_registered("array_last_position"));
        assert!(registry.is_registered("element_at"));
        assert!(registry.is_registered("array_contains"));
        assert!(registry.is_registered("array_contains_any"));
        assert!(registry.is_registered("array_create"));
        assert!(registry.is_registered("array_remove"));
        assert!(registry.is_registered("repeat"));
        assert!(registry.is_registered("sequence"));
        assert!(registry.is_registered("array_concat"));
    }

    #[test]
    fn builtins_include_object_functions() {
        let registry = CustomFuncRegistry::default();

        assert!(registry.is_registered("keys"));
        assert!(registry.is_registered("values"));
        assert!(registry.is_registered("object"));
        assert!(registry.is_registered("zip"));
        assert!(registry.is_registered("items"));
        assert!(registry.is_registered("object_concat"));
        assert!(registry.is_registered("object_construct"));
        assert!(registry.is_registered("erase"));
        assert!(registry.is_registered("object_size"));
    }

    #[test]
    fn builtins_include_misc_functions() {
        let registry = CustomFuncRegistry::default();

        assert!(registry.is_registered("bypass"));
        assert!(registry.is_registered("chr"));
        assert!(registry.is_registered("encode"));
        assert!(registry.is_registered("decode"));
        assert!(registry.is_registered("trunc"));

        assert!(registry.is_registered("md5"));
        assert!(registry.is_registered("sha1"));
        assert!(registry.is_registered("sha256"));
        assert!(registry.is_registered("sha384"));
        assert!(registry.is_registered("sha512"));
        assert!(registry.is_registered("crc32"));

        assert!(registry.is_registered("coalesce"));

        assert!(registry.is_registered("hex2dec"));
        assert!(registry.is_registered("dec2hex"));

        assert!(registry.is_registered("to_json"));
        assert!(registry.is_registered("parse_json"));

        assert!(registry.is_registered("cardinality"));
        assert!(registry.is_registered("newuuid"));
        assert!(registry.is_registered("cast"));
        assert!(registry.is_registered("tstamp"));
        assert!(registry.is_registered("delay"));

        assert!(registry.is_registered("NEWUUID"));
        assert!(registry.get("TsTaMp").is_some());
        assert!(registry.get("CAST").is_some());
    }

    #[test]
    fn builtins_include_null_functions() {
        let registry = CustomFuncRegistry::default();
        assert!(registry.is_registered("isnull"));
        assert!(registry.is_registered("ISNULL"));
        assert!(registry.get("isNull").is_some());
    }

    #[test]
    fn registry_rejects_unknown_functions() {
        let registry = CustomFuncRegistry::default();
        assert!(!registry.is_registered("dummy"));
        assert!(registry.get("dummy").is_none());
        assert!(registry.get("missing").is_none());
    }

    #[test]
    fn registry_resolves_runtime_aliases() {
        let registry = CustomFuncRegistry::default();

        let canonical = registry
            .get("regexp_substring")
            .expect("canonical regexp_substring should be registered");
        let alias = registry
            .get("regexp_substr")
            .expect("regexp_substr alias should be registered");

        assert_eq!(canonical.name(), alias.name());
    }
}
