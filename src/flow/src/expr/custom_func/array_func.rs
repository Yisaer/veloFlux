use crate::expr::custom_func::helpers::{
    any_null, array_distinct_values, array_to_value, map_to_value, nth_array_or_null,
    nth_i64_or_null, nth_string_or_null, validate_arity, validate_at_least_arity,
    validate_one_array_or_null, validate_two_arrays_or_null, value_sort_key, value_to_array,
    value_to_i64, value_to_map, value_to_string, value_to_string_lossy,
};
use crate::expr::custom_func::CustomFunc;
use crate::expr::func::EvalError;
use crate::CustomFuncRegistry;
use datatypes::Value;
use rand::seq::SliceRandom;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct CardinalityFunc;

#[derive(Debug, Clone)]
pub struct ArrayPositionFunc;

#[derive(Debug, Clone)]
pub struct ElementAtFunc;

#[derive(Debug, Clone)]
pub struct ArrayContainsFunc;

#[derive(Debug, Clone)]
pub struct ArrayCreateFunc;

#[derive(Debug, Clone)]
pub struct ArrayRemoveFunc;

#[derive(Debug, Clone)]
pub struct ArrayLastPositionFunc;

#[derive(Debug, Clone)]
pub struct ArrayContainsAnyFunc;

#[derive(Debug, Clone)]
pub struct ArrayIntersectFunc;

#[derive(Debug, Clone)]
pub struct ArrayUnionFunc;

#[derive(Debug, Clone)]
pub struct ArrayMaxFunc;

#[derive(Debug, Clone)]
pub struct ArrayMinFunc;

#[derive(Debug, Clone)]
pub struct ArrayExceptFunc;

#[derive(Debug, Clone)]
pub struct RepeatFunc;

#[derive(Debug, Clone)]
pub struct SequenceFunc;

#[derive(Debug, Clone)]
pub struct ArrayCardinalityFunc;

#[derive(Debug, Clone)]
pub struct ArrayFlattenFunc;

#[derive(Debug, Clone)]
pub struct ArrayDistinctFunc;

#[derive(Debug, Clone)]
pub struct ArrayJoinFunc;

#[derive(Debug, Clone)]
pub struct ArrayShuffleFunc;

#[derive(Debug, Clone)]
pub struct ArrayConcatFunc;

#[derive(Debug, Clone)]
pub struct ArraySortFunc;

#[derive(Debug, Clone)]
pub struct KvpairArrayToObjFunc;

fn type_mismatch(expected: &str, actual: &Value) -> EvalError {
    EvalError::TypeMismatch {
        expected: expected.to_string(),
        actual: format!("{:?}", actual),
    }
}

fn array_or_null_arg(args: &[Value], idx: usize) -> Result<Option<Vec<Value>>, EvalError> {
    match args.get(idx) {
        Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(value_to_array(v)?)),
        None => Err(EvalError::TypeMismatch {
            expected: format!("argument {}", idx),
            actual: format!("missing argument {}", idx),
        }),
    }
}

fn cmp_values_for_order(a: &Value, b: &Value) -> Result<Ordering, EvalError> {
    let ka = value_sort_key(a)?;
    let kb = value_sort_key(b)?;
    Ok(ka.cmp(&kb))
}

impl CustomFunc for CardinalityFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_array_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(array) = nth_array_or_null(args, 0)? else {
            return Ok(Value::Int64(0));
        };
        Ok(Value::Int64(array.len() as i64))
    }

    fn name(&self) -> &str {
        "cardinality"
    }
}

impl CustomFunc for ArrayPositionFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        if !matches!(args.first(), Some(Value::Null) | Some(Value::List(_))) {
            return Err(type_mismatch("array", &args[0]));
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;
        let Some(array) = array_or_null_arg(args, 0)? else {
            return Ok(Value::Int64(-1));
        };
        for (i, item) in array.iter().enumerate() {
            if item == &args[1] {
                return Ok(Value::Int64(i as i64));
            }
        }
        Ok(Value::Int64(-1))
    }

    fn name(&self) -> &str {
        "array_position"
    }
}

impl CustomFunc for ElementAtFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;

        if args[0].is_null() {
            return Ok(Value::Null);
        }

        match &args[0] {
            Value::List(array) => {
                let index = value_to_i64(&args[1])?;
                let len = array.len() as i64;

                if index >= len || -index > len {
                    return Err(EvalError::TypeMismatch {
                        expected: "valid array index".to_string(),
                        actual: index.to_string(),
                    });
                }

                let real_index = if index >= 0 {
                    index as usize
                } else {
                    (len + index) as usize
                };

                Ok(array.get(real_index).cloned().unwrap_or(Value::Null))
            }
            Value::Struct(map) => {
                let key = value_to_string(&args[1])?;
                Ok(map.get_field(&key).cloned().unwrap_or(Value::Null))
            }
            other => Err(type_mismatch("array or object", other)),
        }
    }

    fn name(&self) -> &str {
        "element_at"
    }
}

impl CustomFunc for ArrayContainsFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        if !matches!(args.first(), Some(Value::Null) | Some(Value::List(_))) {
            return Err(type_mismatch("array", &args[0]));
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;
        let Some(array) = array_or_null_arg(args, 0)? else {
            return Ok(Value::Null);
        };
        Ok(Value::Bool(array.iter().any(|x| x == &args[1])))
    }

    fn name(&self) -> &str {
        "array_contains"
    }
}

impl CustomFunc for ArrayCreateFunc {
    fn validate_row(&self, _args: &[Value]) -> Result<(), EvalError> {
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        array_to_value(args.to_vec())
    }

    fn name(&self) -> &str {
        "array_create"
    }
}

impl CustomFunc for ArrayRemoveFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        if !matches!(args.first(), Some(Value::Null) | Some(Value::List(_))) {
            return Err(type_mismatch("array", &args[0]));
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;
        let Some(array) = array_or_null_arg(args, 0)? else {
            return Ok(Value::Null);
        };

        array_to_value(array.into_iter().filter(|x| x != &args[1]).collect())
    }

    fn name(&self) -> &str {
        "array_remove"
    }
}

impl CustomFunc for ArrayLastPositionFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        if !matches!(args.first(), Some(Value::Null) | Some(Value::List(_))) {
            return Err(type_mismatch("array", &args[0]));
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;
        let Some(array) = array_or_null_arg(args, 0)? else {
            return Ok(Value::Int64(-1));
        };

        for i in (0..array.len()).rev() {
            if array[i] == args[1] {
                return Ok(Value::Int64(i as i64));
            }
        }

        Ok(Value::Int64(-1))
    }

    fn name(&self) -> &str {
        "array_last_position"
    }
}

impl CustomFunc for ArrayContainsAnyFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        if !matches!(args.first(), Some(Value::Null) | Some(Value::List(_))) {
            return Err(type_mismatch("array", &args[0]));
        }
        if !matches!(args.get(1), Some(Value::List(_))) {
            return Err(type_mismatch("array", &args[1]));
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;
        let Some(array1) = array_or_null_arg(args, 0)? else {
            return Ok(Value::Bool(false));
        };
        let array2 = value_to_array(&args[1])?;

        for a in &array1 {
            if array2.iter().any(|b| a == b) {
                return Ok(Value::Bool(true));
            }
        }
        Ok(Value::Bool(false))
    }

    fn name(&self) -> &str {
        "array_contains_any"
    }
}

impl CustomFunc for ArrayIntersectFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_two_arrays_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;
        let Some(array1) = nth_array_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        let Some(array2) = nth_array_or_null(args, 1)? else {
            return Ok(Value::Null);
        };

        let mut out = Vec::new();
        for v in array2 {
            if array1.iter().any(|x| x == &v) && !out.iter().any(|x| x == &v) {
                out.push(v);
            }
        }

        array_to_value(out)
    }

    fn name(&self) -> &str {
        "array_intersect"
    }
}

impl CustomFunc for ArrayUnionFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        if !matches!(args.first(), Some(Value::Null) | Some(Value::List(_))) {
            return Err(type_mismatch("array", &args[0]));
        }
        if !matches!(args.get(1), Some(Value::Null) | Some(Value::List(_))) {
            return Err(type_mismatch("array", &args[1]));
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;

        let mut out = Vec::new();

        if let Some(a1) = array_or_null_arg(args, 0)? {
            for v in a1 {
                if !out.iter().any(|x| x == &v) {
                    out.push(v);
                }
            }
        }

        if let Some(a2) = array_or_null_arg(args, 1)? {
            for v in a2 {
                if !out.iter().any(|x| x == &v) {
                    out.push(v);
                }
            }
        }

        array_to_value(out)
    }

    fn name(&self) -> &str {
        "array_union"
    }
}

impl CustomFunc for ArrayMaxFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_array_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(array) = nth_array_or_null(args, 0)? else {
            return Ok(Value::Null);
        };

        let mut best: Option<Value> = None;
        for v in array.into_iter().filter(|v| !v.is_null()) {
            match &best {
                None => best = Some(v),
                Some(cur) => {
                    if cmp_values_for_order(&v, cur)? == Ordering::Greater {
                        best = Some(v);
                    }
                }
            }
        }

        Ok(best.unwrap_or(Value::Null))
    }

    fn name(&self) -> &str {
        "array_max"
    }
}

impl CustomFunc for ArrayMinFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_array_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(array) = nth_array_or_null(args, 0)? else {
            return Ok(Value::Null);
        };

        let mut best: Option<Value> = None;
        for v in array.into_iter().filter(|v| !v.is_null()) {
            match &best {
                None => best = Some(v),
                Some(cur) => {
                    if cmp_values_for_order(&v, cur)? == Ordering::Less {
                        best = Some(v);
                    }
                }
            }
        }

        Ok(best.unwrap_or(Value::Null))
    }

    fn name(&self) -> &str {
        "array_min"
    }
}

impl CustomFunc for ArrayExceptFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        if !matches!(args.first(), Some(Value::Null) | Some(Value::List(_))) {
            return Err(type_mismatch("array", &args[0]));
        }
        if !matches!(args.get(1), Some(Value::Null) | Some(Value::List(_))) {
            return Err(type_mismatch("array", &args[1]));
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;

        let Some(array1) = array_or_null_arg(args, 0)? else {
            return Ok(Value::Null);
        };
        let array2 = array_or_null_arg(args, 1)?.unwrap_or_default();

        let mut out = Vec::new();
        for v in array1 {
            if !array2.iter().any(|x| x == &v) && !out.iter().any(|x| x == &v) {
                out.push(v);
            }
        }

        array_to_value(out)
    }

    fn name(&self) -> &str {
        "array_except"
    }
}

impl CustomFunc for RepeatFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        nth_i64_or_null(args, 1)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;
        if any_null(args) {
            return Ok(Value::Null);
        }

        let count = value_to_i64(&args[1])?;
        if count < 0 {
            return Err(EvalError::TypeMismatch {
                expected: "non-negative integer".to_string(),
                actual: count.to_string(),
            });
        }

        array_to_value(vec![args[0].clone(); count as usize])
    }

    fn name(&self) -> &str {
        "repeat"
    }
}

impl CustomFunc for SequenceFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2, 3])?;
        nth_i64_or_null(args, 0)?;
        nth_i64_or_null(args, 1)?;
        if args.len() == 3 {
            nth_i64_or_null(args, 2)?;
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2, 3])?;
        if any_null(args) {
            return Ok(Value::Null);
        }

        let start = value_to_i64(&args[0])?;
        let stop = value_to_i64(&args[1])?;
        let step = if args.len() == 3 {
            let s = value_to_i64(&args[2])?;
            if s == 0 {
                return Err(EvalError::TypeMismatch {
                    expected: "non-zero step".to_string(),
                    actual: "0".to_string(),
                });
            }
            s
        } else if start <= stop {
            1
        } else {
            -1
        };

        let mut out = Vec::new();

        if step > 0 {
            let mut cur = start;
            while cur <= stop {
                out.push(Value::Int64(cur));
                cur += step;
            }
        } else {
            let mut cur = start;
            while cur >= stop {
                out.push(Value::Int64(cur));
                cur += step;
            }
        }

        array_to_value(out)
    }

    fn name(&self) -> &str {
        "sequence"
    }
}

impl CustomFunc for ArrayCardinalityFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_array_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(array) = nth_array_or_null(args, 0)? else {
            return Ok(Value::Int64(0));
        };
        Ok(Value::Int64(array.len() as i64))
    }

    fn name(&self) -> &str {
        "array_cardinality"
    }
}

impl CustomFunc for ArrayFlattenFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_array_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(array) = nth_array_or_null(args, 0)? else {
            return Ok(Value::Null);
        };

        let mut out = Vec::new();
        for v in array {
            match v {
                Value::List(inner) => out.extend(inner.items().iter().cloned()),
                other => out.push(other),
            }
        }

        array_to_value(out)
    }

    fn name(&self) -> &str {
        "array_flatten"
    }
}

impl CustomFunc for ArrayDistinctFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_array_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(array) = nth_array_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        array_to_value(array_distinct_values(array))
    }

    fn name(&self) -> &str {
        "array_distinct"
    }
}

impl CustomFunc for ArrayJoinFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2, 3])?;
        nth_array_or_null(args, 0)?;
        nth_string_or_null(args, 1)?;
        if args.len() == 3 {
            nth_string_or_null(args, 2)?;
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2, 3])?;
        let Some(array) = nth_array_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        let Some(delimiter) = nth_string_or_null(args, 1)? else {
            return Ok(Value::Null);
        };

        let null_replacement = if args.len() == 3 {
            nth_string_or_null(args, 2)?
        } else {
            None
        };

        let mut parts = Vec::new();
        for v in array {
            if v.is_null() {
                if let Some(rep) = &null_replacement {
                    parts.push(rep.clone());
                }
            } else {
                parts.push(value_to_string_lossy(&v)?);
            }
        }

        Ok(Value::String(parts.join(&delimiter)))
    }

    fn name(&self) -> &str {
        "array_join"
    }
}

impl CustomFunc for ArrayShuffleFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_array_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(mut array) = nth_array_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        let mut rng = rand::rng();
        array.shuffle(&mut rng);
        array_to_value(array)
    }

    fn name(&self) -> &str {
        "array_shuffle"
    }
}

impl CustomFunc for ArrayConcatFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_at_least_arity(args, 1)?;
        for arg in args {
            if !matches!(arg, Value::Null | Value::List(_)) {
                return Err(type_mismatch("array", arg));
            }
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_at_least_arity(args, 1)?;
        let mut out = Vec::new();

        for arg in args {
            match arg {
                Value::Null => {}
                Value::List(v) => out.extend(v.items().iter().cloned()),
                other => return Err(type_mismatch("array", other)),
            }
        }

        array_to_value(out)
    }

    fn name(&self) -> &str {
        "array_concat"
    }
}

impl CustomFunc for ArraySortFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_array_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(mut array) = nth_array_or_null(args, 0)? else {
            return Ok(Value::Null);
        };

        array.sort_by(|a, b| cmp_values_for_order(a, b).unwrap_or(Ordering::Equal));
        array_to_value(array)
    }

    fn name(&self) -> &str {
        "array_sort"
    }
}

impl CustomFunc for KvpairArrayToObjFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_array_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(array) = nth_array_or_null(args, 0)? else {
            return Ok(Value::Null);
        };

        let mut out = BTreeMap::new();

        for item in array {
            let pair = value_to_map(&item)?;
            if pair.len() != 2 {
                return Err(EvalError::TypeMismatch {
                    expected: "array item should be key-value pair".to_string(),
                    actual: format!("{:?}", pair),
                });
            }

            let key = pair.get("key").ok_or_else(|| EvalError::TypeMismatch {
                expected: "array item should contain key".to_string(),
                actual: format!("{:?}", pair),
            })?;

            let value = pair.get("value").ok_or_else(|| EvalError::TypeMismatch {
                expected: "array item should contain value".to_string(),
                actual: format!("{:?}", pair),
            })?;

            let key_str = value_to_string(key)?;
            out.insert(key_str, value.clone());
        }

        map_to_value(out)
    }

    fn name(&self) -> &str {
        "kvpair_array_to_obj"
    }
}

#[derive(Clone)]
pub struct ArrayMapFunc {
    registry: Arc<CustomFuncRegistry>,
}

impl std::fmt::Debug for ArrayMapFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayMapFunc").finish()
    }
}

impl ArrayMapFunc {
    pub fn new(registry: Arc<CustomFuncRegistry>) -> Self {
        Self { registry }
    }
}

impl CustomFunc for ArrayMapFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        nth_string_or_null(args, 0)?;
        nth_array_or_null(args, 1)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;

        let Some(func_name) = nth_string_or_null(args, 0)? else {
            return Ok(Value::Null);
        };

        let Some(array) = nth_array_or_null(args, 1)? else {
            return Ok(Value::Null);
        };

        let func = self
            .registry
            .get(&func_name)
            .ok_or(EvalError::TypeMismatch {
                expected: "registered function".to_string(),
                actual: func_name.clone(),
            })?;

        let mut out = Vec::with_capacity(array.len());

        for value in array {
            let mapped = func.eval_row(&[value])?;
            out.push(mapped);
        }

        array_to_value(out)
    }

    fn name(&self) -> &str {
        "array_map"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::custom_func::helpers::{
        a, assert_array, assert_bool, assert_float, assert_int, assert_map, assert_null,
        assert_string, b, f, i, m, n, s,
    };
    use datatypes::Value;
    #[test]
    fn test_array_functions_correctness() {
        let cardinality = CardinalityFunc;
        assert_int(
            cardinality.eval_row(&[a(vec![i(1), i(2), i(3)])]).unwrap(),
            3,
        );
        assert_int(cardinality.eval_row(&[Value::Null]).unwrap(), 0);

        let array_position = ArrayPositionFunc;
        assert_int(
            array_position
                .eval_row(&[a(vec![s("a"), s("b"), s("a")]), s("a")])
                .unwrap(),
            0,
        );
        assert_int(
            array_position
                .eval_row(&[a(vec![s("a"), s("b")]), s("x")])
                .unwrap(),
            -1,
        );
        assert_int(array_position.eval_row(&[Value::Null, s("x")]).unwrap(), -1);

        let element_at = ElementAtFunc;
        assert_eq!(
            element_at
                .eval_row(&[a(vec![i(10), i(20), i(30)]), i(1)])
                .unwrap(),
            i(20)
        );
        assert_eq!(
            element_at
                .eval_row(&[a(vec![i(10), i(20), i(30)]), i(-1)])
                .unwrap(),
            i(30)
        );
        assert_null(element_at.eval_row(&[Value::Null, i(0)]).unwrap());

        let array_contains = ArrayContainsFunc;
        assert_bool(
            array_contains
                .eval_row(&[a(vec![i(1), i(2), i(3)]), i(2)])
                .unwrap(),
            true,
        );
        assert_bool(
            array_contains
                .eval_row(&[a(vec![i(1), i(2), i(3)]), i(9)])
                .unwrap(),
            false,
        );
        assert_null(array_contains.eval_row(&[Value::Null, i(1)]).unwrap());

        let array_create = ArrayCreateFunc;
        assert_array(
            array_create.eval_row(&[s("1"), s("x"), s("s")]).unwrap(),
            vec![s("1"), s("x"), s("s")],
        );
        assert_array(array_create.eval_row(&[]).unwrap(), vec![]);

        let array_remove = ArrayRemoveFunc;
        assert_array(
            array_remove
                .eval_row(&[a(vec![i(1), i(2), i(1), i(3)]), i(1)])
                .unwrap(),
            vec![i(2), i(3)],
        );
        assert_null(array_remove.eval_row(&[Value::Null, i(1)]).unwrap());

        let array_last_position = ArrayLastPositionFunc;
        assert_int(
            array_last_position
                .eval_row(&[a(vec![s("a"), s("b"), s("a")]), s("a")])
                .unwrap(),
            2,
        );
        assert_int(
            array_last_position
                .eval_row(&[a(vec![s("a"), s("b")]), s("x")])
                .unwrap(),
            -1,
        );
        assert_int(
            array_last_position
                .eval_row(&[Value::Null, s("x")])
                .unwrap(),
            -1,
        );

        let array_contains_any = ArrayContainsAnyFunc;
        assert_bool(
            array_contains_any
                .eval_row(&[a(vec![i(1), i(2), i(3)]), a(vec![i(5), i(2), i(8)])])
                .unwrap(),
            true,
        );
        assert_bool(
            array_contains_any
                .eval_row(&[a(vec![i(1), i(2)]), a(vec![i(7), i(8)])])
                .unwrap(),
            false,
        );
        assert_bool(
            array_contains_any
                .eval_row(&[Value::Null, a(vec![i(1)])])
                .unwrap(),
            false,
        );

        let array_intersect = ArrayIntersectFunc;
        assert_array(
            array_intersect
                .eval_row(&[
                    a(vec![i(1), i(2), i(2), i(3)]),
                    a(vec![i(2), i(2), i(4), i(1)]),
                ])
                .unwrap(),
            vec![i(2), i(1)],
        );
        assert_null(
            array_intersect
                .eval_row(&[Value::Null, a(vec![i(1)])])
                .unwrap(),
        );
        assert_null(
            array_intersect
                .eval_row(&[a(vec![i(1)]), Value::Null])
                .unwrap(),
        );

        let array_union = ArrayUnionFunc;
        assert_array(
            array_union
                .eval_row(&[a(vec![i(1), i(2), i(2)]), a(vec![i(2), i(3), i(1), i(4)])])
                .unwrap(),
            vec![i(1), i(2), i(3), i(4)],
        );
        assert_array(
            array_union
                .eval_row(&[Value::Null, a(vec![i(2), i(3)])])
                .unwrap(),
            vec![i(2), i(3)],
        );
        assert_array(
            array_union.eval_row(&[a(vec![i(1)]), Value::Null]).unwrap(),
            vec![i(1)],
        );

        let array_max = ArrayMaxFunc;
        assert_eq!(
            array_max
                .eval_row(&[a(vec![i(1), Value::Null, i(3), i(2)])])
                .unwrap(),
            i(3)
        );
        assert_null(array_max.eval_row(&[Value::Null]).unwrap());
        assert_null(
            array_max
                .eval_row(&[a(vec![Value::Null, Value::Null])])
                .unwrap(),
        );

        let array_min = ArrayMinFunc;
        assert_eq!(
            array_min
                .eval_row(&[a(vec![i(4), Value::Null, i(2), i(3)])])
                .unwrap(),
            i(2)
        );
        assert_null(array_min.eval_row(&[Value::Null]).unwrap());
        assert_null(
            array_min
                .eval_row(&[a(vec![Value::Null, Value::Null])])
                .unwrap(),
        );

        let array_except = ArrayExceptFunc;
        assert_array(
            array_except
                .eval_row(&[
                    a(vec![i(1), i(2), i(2), i(3), i(4)]),
                    a(vec![i(2), i(5), i(4)]),
                ])
                .unwrap(),
            vec![i(1), i(3)],
        );
        assert_null(
            array_except
                .eval_row(&[Value::Null, a(vec![i(1)])])
                .unwrap(),
        );
        assert_array(
            array_except
                .eval_row(&[a(vec![i(1), i(2)]), Value::Null])
                .unwrap(),
            vec![i(1), i(2)],
        );

        let repeat = RepeatFunc;
        assert_array(
            repeat.eval_row(&[s("x"), i(3)]).unwrap(),
            vec![s("x"), s("x"), s("x")],
        );
        assert_array(repeat.eval_row(&[i(7), i(2)]).unwrap(), vec![i(7), i(7)]);
        assert_null(repeat.eval_row(&[Value::Null, i(2)]).unwrap());
        assert_null(repeat.eval_row(&[s("x"), Value::Null]).unwrap());

        let sequence = SequenceFunc;
        assert_array(
            sequence.eval_row(&[i(1), i(5)]).unwrap(),
            vec![i(1), i(2), i(3), i(4), i(5)],
        );
        assert_array(
            sequence.eval_row(&[i(5), i(1)]).unwrap(),
            vec![i(5), i(4), i(3), i(2), i(1)],
        );
        assert_array(
            sequence.eval_row(&[i(1), i(5), i(2)]).unwrap(),
            vec![i(1), i(3), i(5)],
        );
        assert_array(
            sequence.eval_row(&[i(5), i(1), i(-2)]).unwrap(),
            vec![i(5), i(3), i(1)],
        );
        assert_null(sequence.eval_row(&[Value::Null, i(5)]).unwrap());

        let array_cardinality = ArrayCardinalityFunc;
        assert_int(
            array_cardinality
                .eval_row(&[a(vec![i(1), Value::Null, i(3)])])
                .unwrap(),
            3,
        );
        assert_int(array_cardinality.eval_row(&[Value::Null]).unwrap(), 0);

        let array_flatten = ArrayFlattenFunc;
        assert_array(
            array_flatten
                .eval_row(&[a(vec![a(vec![i(1), i(4)]), a(vec![i(2), i(3)])])])
                .unwrap(),
            vec![i(1), i(4), i(2), i(3)],
        );
        assert_array(
            array_flatten
                .eval_row(&[a(vec![a(vec![i(1)]), a(vec![i(2)]), a(vec![i(3), i(4)])])])
                .unwrap(),
            vec![i(1), i(2), i(3), i(4)],
        );
        assert_null(array_flatten.eval_row(&[Value::Null]).unwrap());

        let array_distinct = ArrayDistinctFunc;
        assert_array(
            array_distinct
                .eval_row(&[a(vec![i(1), i(2), i(1), i(3), i(2)])])
                .unwrap(),
            vec![i(1), i(2), i(3)],
        );
        assert_null(array_distinct.eval_row(&[Value::Null]).unwrap());

        let registry = CustomFuncRegistry::with_builtins();
        let array_map = ArrayMapFunc::new(registry);
        assert_array(
            array_map
                .eval_row(&[s("upper"), a(vec![s("a"), s("bc"), s("x")])])
                .unwrap(),
            vec![s("A"), s("BC"), s("X")],
        );
        assert_array(
            array_map
                .eval_row(&[s("abs"), a(vec![i(-1), i(2), i(-3)])])
                .unwrap(),
            vec![i(1), i(2), i(3)],
        );
        assert_null(array_map.eval_row(&[s("upper"), Value::Null]).unwrap());

        let array_join = ArrayJoinFunc;
        assert_string(
            array_join
                .eval_row(&[a(vec![i(1), i(2), i(3)]), s(",")])
                .unwrap(),
            "1,2,3",
        );
        assert_string(
            array_join
                .eval_row(&[a(vec![s("a"), Value::Null, s("b")]), s(","), s("null")])
                .unwrap(),
            "a,null,b",
        );
        assert_string(
            array_join
                .eval_row(&[a(vec![s("a"), Value::Null, s("b")]), s(",")])
                .unwrap(),
            "a,b",
        );
        assert_null(array_join.eval_row(&[Value::Null, s(",")]).unwrap());

        let array_shuffle = ArrayShuffleFunc;
        let shuffled = array_shuffle
            .eval_row(&[a(vec![i(1), i(2), i(3), i(4)])])
            .unwrap();
        match shuffled {
            Value::List(v) => {
                assert_eq!(v.len(), 4);
                assert!(v.items().contains(&i(1)));
                assert!(v.items().contains(&i(2)));
                assert!(v.items().contains(&i(3)));
                assert!(v.items().contains(&i(4)));
            }
            other => panic!("expected array, got {:?}", other),
        }
        assert_null(array_shuffle.eval_row(&[Value::Null]).unwrap());

        let array_concat = ArrayConcatFunc;
        assert_array(
            array_concat
                .eval_row(&[a(vec![i(1), i(2)]), a(vec![i(3)]), a(vec![i(4), i(5)])])
                .unwrap(),
            vec![i(1), i(2), i(3), i(4), i(5)],
        );
        assert_array(
            array_concat
                .eval_row(&[a(vec![i(1)]), Value::Null, a(vec![i(2)])])
                .unwrap(),
            vec![i(1), i(2)],
        );

        let array_sort = ArraySortFunc;
        assert_array(
            array_sort
                .eval_row(&[a(vec![i(3), i(2), i(5), i(1)])])
                .unwrap(),
            vec![i(1), i(2), i(3), i(5)],
        );
        assert_null(array_sort.eval_row(&[Value::Null]).unwrap());

        let kvpair_array_to_obj = KvpairArrayToObjFunc;
        assert_map(
            kvpair_array_to_obj
                .eval_row(&[a(vec![
                    m(vec![("key", s("key1")), ("value", i(1))]),
                    m(vec![("key", s("key2")), ("value", i(2))]),
                ])])
                .unwrap(),
            vec![("key1", i(1)), ("key2", i(2))],
        );
        assert_null(kvpair_array_to_obj.eval_row(&[Value::Null]).unwrap());
    }

    #[test]
    fn test_array_functions_arity_and_error_cases() {
        assert!(CardinalityFunc.eval_row(&[]).is_err());
        assert!(CardinalityFunc.eval_row(&[a(vec![]), a(vec![])]).is_err());

        assert!(ArrayPositionFunc.eval_row(&[a(vec![i(1)])]).is_err());
        assert!(ArrayPositionFunc
            .eval_row(&[a(vec![i(1)]), i(1), i(2)])
            .is_err());

        assert!(ElementAtFunc.eval_row(&[a(vec![i(1)])]).is_err());
        assert!(ElementAtFunc.eval_row(&[a(vec![i(1)]), i(5)]).is_err());
        assert!(ElementAtFunc.eval_row(&[a(vec![i(1)]), i(-2)]).is_err());

        assert!(ArrayContainsFunc.eval_row(&[a(vec![i(1)])]).is_err());
        assert!(ArrayContainsFunc
            .eval_row(&[a(vec![i(1)]), i(1), i(2)])
            .is_err());

        assert!(ArrayRemoveFunc.eval_row(&[a(vec![i(1)])]).is_err());

        assert!(ArrayLastPositionFunc.eval_row(&[a(vec![i(1)])]).is_err());

        assert!(ArrayContainsAnyFunc.eval_row(&[a(vec![i(1)])]).is_err());

        assert!(ArrayIntersectFunc.eval_row(&[a(vec![i(1)])]).is_err());

        assert!(ArrayUnionFunc.eval_row(&[a(vec![i(1)])]).is_err());

        assert!(ArrayMaxFunc.eval_row(&[]).is_err());
        assert!(ArrayMinFunc.eval_row(&[]).is_err());

        assert!(ArrayExceptFunc.eval_row(&[a(vec![i(1)])]).is_err());

        assert!(RepeatFunc.eval_row(&[s("x")]).is_err());
        assert!(RepeatFunc.eval_row(&[s("x"), i(-1)]).is_err());

        assert!(SequenceFunc.eval_row(&[i(1)]).is_err());
        assert!(SequenceFunc.eval_row(&[i(1), i(3), i(0)]).is_err());
        assert!(SequenceFunc.eval_row(&[i(1), i(3), i(1), i(2)]).is_err());

        assert!(ArrayCardinalityFunc.eval_row(&[]).is_err());

        assert!(ArrayFlattenFunc.eval_row(&[]).is_err());

        assert!(ArrayDistinctFunc.eval_row(&[]).is_err());

        let registry = CustomFuncRegistry::with_builtins();
        let array_map = ArrayMapFunc::new(registry);
        assert!(array_map.eval_row(&[s("upper")]).is_err());
        assert!(array_map
            .eval_row(&[s("not_exist"), a(vec![s("a")])])
            .is_err());
        assert!(array_map.eval_row(&[s("concat"), a(vec![s("a")])]).is_err());

        assert!(ArrayJoinFunc.eval_row(&[a(vec![i(1)])]).is_err());
        assert!(ArrayJoinFunc
            .eval_row(&[a(vec![i(1)]), s(","), s("x"), s("y")])
            .is_err());

        assert!(ArrayShuffleFunc.eval_row(&[]).is_err());

        assert!(ArrayConcatFunc.eval_row(&[]).is_err());

        assert!(ArraySortFunc.eval_row(&[]).is_err());

        assert!(KvpairArrayToObjFunc.eval_row(&[]).is_err());
        assert!(KvpairArrayToObjFunc
            .eval_row(&[a(vec![m(vec![("key", s("k1"))])])])
            .is_err());
        assert!(KvpairArrayToObjFunc
            .eval_row(&[a(vec![m(vec![("key", i(1)), ("value", i(2))])])])
            .is_err());
    }
}
