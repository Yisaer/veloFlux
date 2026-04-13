use super::helpers::*;
use crate::catalog::FunctionDef;
use crate::expr::custom_func::CustomFunc;
use crate::expr::func::EvalError;
use datatypes::Value;
use datatypes::{ConcreteDatatype, ListValue};
use std::collections::BTreeMap;
use std::sync::Arc;

const KV_PAIR_K_NAME: &str = "key";
const KV_PAIR_V_NAME: &str = "value";

#[derive(Debug, Clone)]
pub struct KeysFunc;

#[derive(Debug, Clone)]
pub struct ValuesFunc;

#[derive(Debug, Clone)]
pub struct ObjectFunc;

#[derive(Debug, Clone)]
pub struct ZipFunc;

#[derive(Debug, Clone)]
pub struct ItemsFunc;

#[derive(Debug, Clone)]
pub struct ObjectConcatFunc;

#[derive(Debug, Clone)]
pub struct ObjectConstructFunc;

#[derive(Debug, Clone)]
pub struct EraseFunc;

#[derive(Debug, Clone)]
pub struct ObjectSizeFunc;

pub fn builtin_function_defs() -> Vec<FunctionDef> {
    vec![
        keys_function_def(),
        values_function_def(),
        object_function_def(),
        zip_function_def(),
        items_function_def(),
        object_concat_function_def(),
        object_construct_function_def(),
        erase_function_def(),
        object_size_function_def(),
        object_pick_function_def(),
        obj_to_kvpair_array_function_def(),
    ]
}

pub fn keys_function_def() -> FunctionDef {
    scalar_function_def(
        "keys",
        vec![req_arg("object", object_type())],
        array_type(),
        "Return all keys from an object.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be an object or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT keys(meta)"],
    )
}

pub fn values_function_def() -> FunctionDef {
    scalar_function_def(
        "values",
        vec![req_arg("object", object_type())],
        array_type(),
        "Return all values from an object.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be an object or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT values(meta)"],
    )
}

pub fn object_function_def() -> FunctionDef {
    scalar_function_def(
        "object",
        vec![
            req_arg("keys", array_type()),
            req_arg("values", array_type()),
        ],
        object_type(),
        "Build an object from parallel key and value arrays.",
        vec![
            "Requires exactly 2 arguments.",
            "The first argument must be an array of strings or NULL.",
            "The second argument must be an array or NULL.",
            "Returns NULL if any argument is NULL.",
            "The two arrays must have the same length.",
        ],
        vec!["SELECT object(['a', 'b'], [1, 2])"],
    )
}

pub fn zip_function_def() -> FunctionDef {
    scalar_function_def(
        "zip",
        vec![req_arg("pairs", array_type())],
        object_type(),
        "Build an object from an array of [key, value] pairs.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be an array of 2-element arrays or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT zip([['a', 1], ['b', 2]])"],
    )
}

pub fn items_function_def() -> FunctionDef {
    scalar_function_def(
        "items",
        vec![req_arg("object", object_type())],
        array_type(),
        "Return an array of [key, value] pairs from an object.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be an object or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT items(meta)"],
    )
}

pub fn object_concat_function_def() -> FunctionDef {
    scalar_function_def(
        "object_concat",
        vec![
            req_arg("object1", object_type()),
            variadic_arg("objects", object_type()),
        ],
        object_type(),
        "Merge two or more objects. Later keys overwrite earlier keys.",
        vec![
            "Requires at least 2 arguments.",
            "Each argument must be an object or NULL.",
            "NULL arguments are skipped.",
        ],
        vec!["SELECT object_concat(obj1, obj2, obj3)"],
    )
}

pub fn object_construct_function_def() -> FunctionDef {
    scalar_function_def(
        "object_construct",
        vec![variadic_arg("key_values", any_type())],
        object_type(),
        "Construct an object from alternating key and value arguments.",
        vec![
            "Requires an even number of arguments.",
            "Keys are taken from even positions.",
            "NULL keys are skipped.",
            "Keys must be string-convertible scalars.",
        ],
        vec!["SELECT object_construct('a', 1, 'b', 2)"],
    )
}

pub fn erase_function_def() -> FunctionDef {
    scalar_function_def(
        "erase",
        vec![
            req_arg("object", object_type()),
            req_arg("keys", any_type()),
        ],
        object_type(),
        "Return a copy of an object with one or more keys removed.",
        vec![
            "Requires exactly 2 arguments.",
            "The first argument must be an object or NULL.",
            "The second argument must be a string, an array of strings, or NULL.",
            "Returns NULL if any argument is NULL.",
        ],
        vec![
            "SELECT erase(meta, 'debug')",
            "SELECT erase(meta, ['debug', 'trace'])",
        ],
    )
}

pub fn object_size_function_def() -> FunctionDef {
    scalar_function_def(
        "object_size",
        vec![req_arg("object", object_type())],
        int_type(),
        "Return the number of fields in an object.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be an object or NULL.",
            "Returns 0 if the argument is NULL.",
        ],
        vec!["SELECT object_size(meta)"],
    )
}

pub fn object_pick_function_def() -> FunctionDef {
    scalar_function_def(
        "object_pick",
        vec![
            req_arg("object", object_type()),
            req_arg("keys", any_type()),
            variadic_arg("more_keys", any_type()),
        ],
        object_type(),
        "Return a new object containing only the selected keys or nested key paths.",
        vec![
            "Requires at least 2 arguments.",
            "The first argument must be an object or NULL.",
            "Remaining arguments must be strings or arrays of strings.",
            "Returns NULL if the first argument is NULL.",
        ],
        vec![
            "SELECT object_pick(meta, 'foo')",
            "SELECT object_pick(meta, 'a', 'b')",
            "SELECT object_pick(meta, ['a', 'b'])",
            "SELECT object_pick(meta, 'k1.temp')",
        ],
    )
}

pub fn obj_to_kvpair_array_function_def() -> FunctionDef {
    scalar_function_def(
        "obj_to_kvpair_array",
        vec![req_arg("object", object_type())],
        array_type(),
        "Convert an object into an array of {key, value} objects.",
        vec![
            "Requires exactly 1 argument.",
            "The argument must be an object or NULL.",
            "Returns NULL if the argument is NULL.",
        ],
        vec!["SELECT obj_to_kvpair_array(meta)"],
    )
}

impl CustomFunc for KeysFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_object_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(map) = nth_object_or_null(args, 0)? else {
            return Ok(Value::Null);
        };

        let items = map.into_keys().map(Value::String).collect();
        array_to_value(items)
    }

    fn name(&self) -> &str {
        "keys"
    }
}

impl CustomFunc for ValuesFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_object_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;
        let Some(map) = nth_object_or_null(args, 0)? else {
            return Ok(Value::Null);
        };

        let items = map.into_values().collect();
        Ok(Value::List(ListValue::new(
            items,
            Arc::new(ConcreteDatatype::Null),
        )))
    }

    fn name(&self) -> &str {
        "values"
    }
}

impl CustomFunc for ObjectFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        nth_string_array_or_null(args, 0)?;
        nth_array_or_null(args, 1)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;

        let Some(keys) = nth_string_array_or_null(args, 0)? else {
            return Ok(Value::Null);
        };
        let Some(values) = nth_array_or_null(args, 1)? else {
            return Ok(Value::Null);
        };

        if keys.len() != values.len() {
            return Err(EvalError::TypeMismatch {
                expected: "same-length key and value arrays".to_string(),
                actual: format!("{} keys vs {} values", keys.len(), values.len()),
            });
        }

        let map: BTreeMap<String, Value> = keys.into_iter().zip(values).collect();
        map_to_value(map)
    }

    fn name(&self) -> &str {
        "object"
    }
}

impl CustomFunc for ZipFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_array_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;

        let Some(pairs) = nth_array_or_null(args, 0)? else {
            return Ok(Value::Null);
        };

        let mut map = BTreeMap::new();

        for pair in pairs {
            if pair.is_null() {
                continue;
            }

            match pair {
                Value::List(inner) => {
                    let items = inner.items();
                    if items.len() != 2 {
                        return Err(EvalError::TypeMismatch {
                            expected: "2-element [key, value] pair".to_string(),
                            actual: format!("{:?}", Value::List(inner)),
                        });
                    }
                    let key = value_to_string(&items[0])?;
                    map.insert(key, items[1].clone());
                }
                other => {
                    return Err(EvalError::TypeMismatch {
                        expected: "array of [key, value] pairs".to_string(),
                        actual: format!("{:?}", other),
                    });
                }
            }
        }

        map_to_value(map)
    }

    fn name(&self) -> &str {
        "zip"
    }
}

impl CustomFunc for ItemsFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_one_object_or_null(args)
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;

        let Some(map) = nth_object_or_null(args, 0)? else {
            return Ok(Value::Null);
        };

        let mut out = Vec::with_capacity(map.len());
        for (k, v) in map {
            out.push(ah(vec![Value::String(k), v]));
        }

        Ok(Value::List(ListValue::new(
            out,
            Arc::new(ConcreteDatatype::Null),
        )))
    }

    fn name(&self) -> &str {
        "items"
    }
}

impl CustomFunc for ObjectConcatFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        if args.len() < 2 {
            return Err(EvalError::TypeMismatch {
                expected: "at least 2 arguments".to_string(),
                actual: format!("{} arguments", args.len()),
            });
        }

        for arg in args {
            if !arg.is_null() {
                value_to_map(arg)?;
            }
        }

        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        if args.len() < 2 {
            return Err(EvalError::TypeMismatch {
                expected: "at least 2 arguments".to_string(),
                actual: format!("{} arguments", args.len()),
            });
        }

        let mut out = BTreeMap::new();

        for arg in args {
            if arg.is_null() {
                continue;
            }
            for (k, v) in value_to_map(arg)? {
                out.insert(k, v);
            }
        }

        map_to_value(out)
    }

    fn name(&self) -> &str {
        "object_concat"
    }
}

impl CustomFunc for ObjectConstructFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        if !args.len().is_multiple_of(2) {
            return Err(EvalError::TypeMismatch {
                expected: "even number of key/value arguments".to_string(),
                actual: format!("{} arguments", args.len()),
            });
        }

        for i in (0..args.len()).step_by(2) {
            if args[i].is_null() {
                continue;
            }
            value_to_string_lossy(&args[i])?;
        }

        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        if !args.len().is_multiple_of(2) {
            return Err(EvalError::TypeMismatch {
                expected: "even number of key/value arguments".to_string(),
                actual: format!("{} arguments", args.len()),
            });
        }

        let mut out = BTreeMap::new();

        for i in (0..args.len()).step_by(2) {
            if args[i].is_null() {
                continue;
            }
            let key = value_to_string_lossy(&args[i])?;
            out.insert(key, args[i + 1].clone());
        }

        map_to_value(out)
    }

    fn name(&self) -> &str {
        "object_construct"
    }
}

impl CustomFunc for EraseFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[2])?;
        nth_object_or_null(args, 0)?;

        match args.get(1) {
            Some(Value::Null) => Ok(()),
            Some(Value::String(_)) => Ok(()),
            Some(Value::List(_)) => {
                value_to_string_vec(&args[1])?;
                Ok(())
            }
            Some(other) => Err(EvalError::TypeMismatch {
                expected: "string or array of strings".to_string(),
                actual: format!("{:?}", other),
            }),
            None => Err(EvalError::TypeMismatch {
                expected: "argument 1".to_string(),
                actual: "missing argument 1".to_string(),
            }),
        }
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[2])?;

        let Some(map) = nth_object_or_null(args, 0)? else {
            return Ok(Value::Null);
        };

        if args[1].is_null() {
            return Ok(Value::Null);
        }

        let remove_keys: Vec<String> = match &args[1] {
            Value::String(s) => vec![s.clone()],
            Value::List(_) => value_to_string_vec(&args[1])?,
            other => {
                return Err(EvalError::TypeMismatch {
                    expected: "string or array of strings".to_string(),
                    actual: format!("{:?}", other),
                });
            }
        };

        let remove_set = string_set(&remove_keys);
        let out: BTreeMap<String, Value> = map
            .into_iter()
            .filter(|(k, _)| !remove_set.contains(k))
            .collect();

        map_to_value(out)
    }

    fn name(&self) -> &str {
        "erase"
    }
}

impl CustomFunc for ObjectSizeFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        if let Some(v) = args.first() {
            if !v.is_null() {
                value_to_map(v)?;
            }
        }
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;

        if args[0].is_null() {
            return Ok(Value::Int64(0));
        }

        let map = value_to_map(&args[0])?;
        Ok(Value::Int64(map.len() as i64))
    }

    fn name(&self) -> &str {
        "object_size"
    }
}

#[derive(Debug, Clone)]
pub struct ObjectPickFunc;

impl CustomFunc for ObjectPickFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        if args.len() < 2 {
            return Err(EvalError::TypeMismatch {
                expected: "at least 2 arguments".to_string(),
                actual: format!("{} arguments", args.len()),
            });
        }

        nth_object_or_null(args, 0)?;

        for arg in &args[1..] {
            match arg {
                Value::Null | Value::String(_) => {}
                Value::List(_) => {
                    value_to_string_vec(arg)?;
                }
                other => {
                    return Err(EvalError::TypeMismatch {
                        expected: "string or array of strings".to_string(),
                        actual: format!("{:?}", other),
                    });
                }
            }
        }

        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        if args.len() < 2 {
            return Err(EvalError::TypeMismatch {
                expected: "at least 2 arguments".to_string(),
                actual: format!("{} arguments", args.len()),
            });
        }

        let Some(source) = nth_object_or_null(args, 0)? else {
            return Ok(Value::Null);
        };

        let mut out = BTreeMap::new();

        for arg in &args[1..] {
            match arg {
                Value::Null => {}
                Value::String(path) => {
                    pick_path(&mut out, &source, path)?;
                }
                Value::List(_) => {
                    for path in value_to_string_vec(arg)? {
                        pick_path(&mut out, &source, &path)?;
                    }
                }
                other => {
                    return Err(EvalError::TypeMismatch {
                        expected: "string or array of strings".to_string(),
                        actual: format!("{:?}", other),
                    });
                }
            }
        }

        map_to_value(out)
    }

    fn name(&self) -> &str {
        "object_pick"
    }
}

fn pick_path(
    target: &mut BTreeMap<String, Value>,
    source: &BTreeMap<String, Value>,
    path: &str,
) -> Result<(), EvalError> {
    if !path.contains('.') {
        if let Some(v) = source.get(path) {
            target.insert(path.to_string(), v.clone());
        }
        return Ok(());
    }

    let keys: Vec<&str> = path.split('.').collect();
    let Some(value) = get_nested_value(source, &keys) else {
        return Ok(());
    };

    insert_nested_value(target, &keys, value)?;
    Ok(())
}

fn get_nested_value(current: &BTreeMap<String, Value>, keys: &[&str]) -> Option<Value> {
    if keys.is_empty() {
        return None;
    }

    let first = current.get(keys[0])?;
    if keys.len() == 1 {
        return Some(first.clone());
    }

    let next = value_to_map(first).ok()?;
    get_nested_value(&next, &keys[1..])
}

fn insert_nested_value(
    target: &mut BTreeMap<String, Value>,
    keys: &[&str],
    value: Value,
) -> Result<(), EvalError> {
    if keys.len() == 1 {
        target.insert(keys[0].to_string(), value);
        return Ok(());
    }

    let head = keys[0].to_string();

    let mut child = match target.get(&head) {
        Some(existing) => value_to_map(existing)?,
        None => BTreeMap::new(),
    };

    insert_nested_value(&mut child, &keys[1..], value)?;
    target.insert(head, map_to_value(child)?);
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ObjToKvPairArrayFunc;

impl CustomFunc for ObjToKvPairArrayFunc {
    fn validate_row(&self, args: &[Value]) -> Result<(), EvalError> {
        validate_arity(args, &[1])?;
        nth_object_or_null(args, 0)?;
        Ok(())
    }

    fn eval_row(&self, args: &[Value]) -> Result<Value, EvalError> {
        validate_arity(args, &[1])?;

        let Some(map) = nth_object_or_null(args, 0)? else {
            return Ok(Value::Null);
        };

        let mut out: Vec<Value> = Vec::with_capacity(map.len());

        for (k, v) in map {
            let mut pair = BTreeMap::new();
            pair.insert(KV_PAIR_K_NAME.to_string(), Value::String(k));
            pair.insert(KV_PAIR_V_NAME.to_string(), v);
            out.push(map_to_value(pair)?);
        }

        Ok(Value::List(ListValue::new(
            out,
            Arc::new(ConcreteDatatype::Null),
        )))
    }

    fn name(&self) -> &str {
        "obj_to_kvpair_array"
    }
}

#[cfg(test)]
mod tests {
    use super::super::helpers::*;
    use super::*;
    use crate::expr::custom_func::CustomFunc;
    use crate::expr::func::EvalError;

    fn type_mismatch(expected: &str, actual: &str) -> EvalError {
        EvalError::TypeMismatch {
            expected: expected.to_string(),
            actual: actual.to_string(),
        }
    }

    #[test]
    fn test_object_construct_basic() {
        let f = ObjectConstructFunc;

        assert_map(
            f.eval_row(&[s("foo"), s("bar")]).unwrap(),
            vec![("foo", s("bar"))],
        );

        assert_map(
            f.eval_row(&[s("key1"), s("bar"), s("key2"), s("foo")])
                .unwrap(),
            vec![("key1", s("bar")), ("key2", s("foo"))],
        );

        assert_map(
            f.eval_row(&[s("key1"), n(), s("key2"), s("foo"), s("key3"), n()])
                .unwrap(),
            vec![("key1", n()), ("key2", s("foo")), ("key3", n())],
        );
    }

    #[test]
    fn test_object_construct_nil_key_skipped() {
        let f = ObjectConstructFunc;

        assert_map(
            f.eval_row(&[n(), s("v1"), s("k2"), s("v2")]).unwrap(),
            vec![("k2", s("v2"))],
        );
    }

    #[test]
    fn test_object_construct_odd_arity_error() {
        let f = ObjectConstructFunc;

        let err = f.eval_row(&[s("foo")]).unwrap_err();
        assert_eq!(
            err,
            type_mismatch("even number of key/value arguments", "1 arguments")
        );
    }

    #[test]
    fn test_keys() {
        let f = KeysFunc;

        assert_array(
            f.eval_row(&[m(vec![("a", i(1)), ("b", i(2))])]).unwrap(),
            vec![s("a"), s("b")],
        );
    }

    #[test]
    fn test_keys_null() {
        let f = KeysFunc;
        assert_null(f.eval_row(&[n()]).unwrap());
    }

    #[test]
    fn test_keys_type_error() {
        let f = KeysFunc;
        let err = f.eval_row(&[i(1)]).unwrap_err();
        assert_eq!(err, type_mismatch("object", "Int64(1)"));
    }

    #[test]
    fn test_values() {
        let f = ValuesFunc;

        assert_array(
            f.eval_row(&[m(vec![("a", s("c")), ("b", s("d"))])])
                .unwrap(),
            vec![s("c"), s("d")],
        );
    }

    #[test]
    fn test_values_with_null() {
        let f = ValuesFunc;

        assert_array(f.eval_row(&[m(vec![("k", n())])]).unwrap(), vec![n()]);
    }

    #[test]
    fn test_object() {
        let f = ObjectFunc;

        assert_map(
            f.eval_row(&[a(vec![s("a"), s("b")]), a(vec![i(1), i(2)])])
                .unwrap(),
            vec![("a", i(1)), ("b", i(2))],
        );
    }

    #[test]
    fn test_object_value_can_be_null() {
        let f = ObjectFunc;

        assert_map(
            f.eval_row(&[a(vec![s("k1")]), a(vec![n()])]).unwrap(),
            vec![("k1", n())],
        );
    }

    #[test]
    fn test_object_null_arg() {
        let f = ObjectFunc;
        assert_null(f.eval_row(&[n(), a(vec![i(1)])]).unwrap());
        assert_null(f.eval_row(&[a(vec![s("a")]), n()]).unwrap());
    }

    #[test]
    fn test_object_first_arg_not_string_array() {
        let f = ObjectFunc;

        let err = f
            .eval_row(&[a(vec![i(1), i(2)]), a(vec![i(1), i(2)])])
            .unwrap_err();

        assert_eq!(err, type_mismatch("string", "Int64(1)"));
    }

    #[test]
    fn test_object_second_arg_not_array() {
        let f = ObjectFunc;

        let err = f.eval_row(&[a(vec![s("a"), s("b")]), i(1)]).unwrap_err();

        assert_eq!(err, type_mismatch("array argument 1", "Int64(1)"));
    }

    #[test]
    fn test_object_length_mismatch() {
        let f = ObjectFunc;

        let err = f
            .eval_row(&[a(vec![s("a"), s("b")]), a(vec![i(1), i(2), i(3)])])
            .unwrap_err();

        assert_eq!(
            err,
            type_mismatch("same-length key and value arrays", "2 keys vs 3 values")
        );
    }

    #[test]
    fn test_zip() {
        let f = ZipFunc;

        assert_map(
            f.eval_row(&[a(vec![ah(vec![s("a"), i(1)]), ah(vec![s("b"), i(2)])])])
                .unwrap(),
            vec![("a", i(1)), ("b", i(2))],
        );
    }

    #[test]
    fn test_zip_skip_null_pair() {
        let f = ZipFunc;

        assert_map(
            f.eval_row(&[a(vec![
                a(vec![s("k1"), s("v1")]),
                n(),
                a(vec![s("k2"), s("v2")]),
            ])])
            .unwrap(),
            vec![("k1", s("v1")), ("k2", s("v2"))],
        );
    }

    #[test]
    fn test_zip_not_array() {
        let f = ZipFunc;

        let err = f.eval_row(&[i(1)]).unwrap_err();
        assert_eq!(err, type_mismatch("array argument 0", "Int64(1)"));
    }

    #[test]
    fn test_zip_pair_not_array() {
        let f = ZipFunc;

        let err = f.eval_row(&[a(vec![i(1), i(2)])]).unwrap_err();
        assert_eq!(
            err,
            type_mismatch("array of [key, value] pairs", "Int64(1)")
        );
    }

    #[test]
    fn test_zip_pair_wrong_len() {
        let f = ZipFunc;

        let err = f
            .eval_row(&[a(vec![
                ah(vec![s("a"), i(1), i(3)]),
                ah(vec![s("b"), i(2), i(4)]),
            ])])
            .unwrap_err();

        assert_eq!(
            err,
            type_mismatch(
                "2-element [key, value] pair",
                r#"List(ListValue { items: [String("a"), Int64(1), Int64(3)], datatype: Null })"#
            )
        );
    }

    #[test]
    fn test_zip_first_element_not_string() {
        let f = ZipFunc;

        let err = f
            .eval_row(&[a(vec![a(vec![i(1), i(3)]), a(vec![i(2), i(4)])])])
            .unwrap_err();

        assert_eq!(err, type_mismatch("string", "Int64(1)"));
    }

    #[test]
    fn test_items() {
        let f = ItemsFunc;

        assert_array(
            f.eval_row(&[m(vec![("a", i(1)), ("b", i(2))])]).unwrap(),
            vec![ah(vec![s("a"), i(1)]), ah(vec![s("b"), i(2)])],
        );
    }

    #[test]
    fn test_items_with_null_value() {
        let f = ItemsFunc;

        assert_array(
            f.eval_row(&[m(vec![("k2", n())])]).unwrap(),
            vec![ah(vec![s("k2"), n()])],
        );
    }

    #[test]
    fn test_object_concat() {
        let f = ObjectConcatFunc;

        assert_map(
            f.eval_row(&[
                m(vec![("a", i(1)), ("b", i(2))]),
                m(vec![("b", i(3)), ("c", i(4))]),
                m(vec![("a", i(2)), ("d", i(1))]),
            ])
            .unwrap(),
            vec![("a", i(2)), ("b", i(3)), ("c", i(4)), ("d", i(1))],
        );
    }

    #[test]
    fn test_object_concat_skip_null() {
        let f = ObjectConcatFunc;

        assert_map(
            f.eval_row(&[m(vec![("k1", s("v1"))]), n(), m(vec![("k2", s("v2"))])])
                .unwrap(),
            vec![("k1", s("v1")), ("k2", s("v2"))],
        );
    }

    #[test]
    fn test_object_concat_type_error() {
        let f = ObjectConcatFunc;

        let err = f
            .eval_row(&[m(vec![("a", i(1))]), a(vec![i(1), i(2)])])
            .unwrap_err();

        assert_eq!(
            err,
            type_mismatch(
                "object",
                r#"List(ListValue { items: [Int64(1), Int64(2)], datatype: Int64(Int64Type) })"#
            )
        );
    }

    #[test]
    fn test_erase_single_key() {
        let f = EraseFunc;

        assert_map(
            f.eval_row(&[m(vec![("a", i(1)), ("b", i(2)), ("c", i(3))]), s("a")])
                .unwrap(),
            vec![("b", i(2)), ("c", i(3))],
        );
    }

    #[test]
    fn test_erase_multiple_keys() {
        let f = EraseFunc;

        assert_map(
            f.eval_row(&[
                m(vec![("a", i(1)), ("b", i(2)), ("c", i(3))]),
                a(vec![s("a"), s("b")]),
            ])
            .unwrap(),
            vec![("c", i(3))],
        );
    }

    #[test]
    fn test_erase_keep_null_value_logic() {
        let f = EraseFunc;

        assert_map(
            f.eval_row(&[m(vec![("k1", n()), ("k2", s("2"))]), s("k1")])
                .unwrap(),
            vec![("k2", s("2"))],
        );
    }

    #[test]
    fn test_erase_wrong_arity() {
        let f = EraseFunc;

        let err = f
            .eval_row(&[m(vec![("a", i(1))]), s("a"), s("c")])
            .unwrap_err();

        assert_eq!(err, type_mismatch("2 arguments", "3 arguments"));
    }

    #[test]
    fn test_object_size() {
        let f = ObjectSizeFunc;

        assert_int(
            f.eval_row(&[m(vec![("a", i(1)), ("b", a(vec![s("foo"), s("bar")]))])])
                .unwrap(),
            2,
        );
    }

    #[test]
    fn test_object_size_null_is_zero() {
        let f = ObjectSizeFunc;

        assert_int(f.eval_row(&[n()]).unwrap(), 0);
    }

    #[test]
    fn test_object_pick_single() {
        let func = ObjectPickFunc;

        let actual = func
            .eval_row(&[m(vec![("a", i(1)), ("b", i(2))]), s("a")])
            .unwrap();

        assert_map(actual, vec![("a", i(1))]);
    }

    #[test]
    fn test_object_pick_multiple_args() {
        let func = ObjectPickFunc;

        let actual = func
            .eval_row(&[
                m(vec![("a", i(1)), ("b", i(2)), ("c", i(3))]),
                s("a"),
                s("c"),
            ])
            .unwrap();

        assert_map(actual, vec![("a", i(1)), ("c", i(3))]);
    }

    #[test]
    fn test_object_pick_string_array() {
        let func = ObjectPickFunc;

        let actual = func
            .eval_row(&[
                m(vec![("a", i(1)), ("b", i(2)), ("c", i(3))]),
                a(vec![s("a"), s("c")]),
            ])
            .unwrap();

        assert_map(actual, vec![("a", i(1)), ("c", i(3))]);
    }

    #[test]
    fn test_object_pick_nested() {
        let func = ObjectPickFunc;

        let nested = m(vec![
            ("k1", m(vec![("temp", i(23)), ("hum", i(34))])),
            ("k2", s("2")),
        ]);

        let actual = func.eval_row(&[nested, s("k1.temp")]).unwrap();

        assert_map(actual, vec![("k1", m(vec![("temp", i(23))]))]);
    }

    #[test]
    fn test_obj_to_kvpair_array() {
        let func = ObjToKvPairArrayFunc;

        let actual = func
            .eval_row(&[m(vec![("key1", i(1)), ("key2", i(2))])])
            .unwrap();

        assert_array_of_maps(
            actual,
            vec![
                vec![("key", s("key1")), ("value", i(1))],
                vec![("key", s("key2")), ("value", i(2))],
            ],
        );
    }

    #[test]
    fn test_values_heterogeneous_object() {
        let f = ValuesFunc;

        assert_array(
            f.eval_row(&[m(vec![("a", i(1)), ("b", s("x"))])]).unwrap(),
            vec![i(1), s("x")],
        );
    }

    #[test]
    fn test_obj_to_kvpair_array_heterogeneous_object() {
        let func = ObjToKvPairArrayFunc;

        let actual = func
            .eval_row(&[m(vec![("a", i(1)), ("b", s("x"))])])
            .unwrap();

        assert_array_of_maps(
            actual,
            vec![
                vec![("key", s("a")), ("value", i(1))],
                vec![("key", s("b")), ("value", s("x"))],
            ],
        );
    }

    #[test]
    fn test_items_heterogeneous_object() {
        let f = ItemsFunc;

        assert_array(
            f.eval_row(&[m(vec![("a", i(1)), ("b", s("x"))])]).unwrap(),
            vec![ah(vec![s("a"), i(1)]), ah(vec![s("b"), s("x")])],
        );
    }
}
