use datatypes::{
    ConcreteDatatype, ListValue, StringType, StructField, StructType, StructValue, Value,
};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

fn fields() -> StructType {
    StructType::new(Arc::new(vec![StructField::new(
        "payload".to_owned(),
        ConcreteDatatype::String(StringType),
        false,
    )]))
}

#[test]
fn complex_clones_share_payload_and_survive_owner_release() {
    let text = Arc::new("x".repeat(1024 * 1024));
    let original = Value::String(text.clone());
    let cloned = original.clone();
    drop(original);
    let Value::String(cloned_text) = &cloned else {
        panic!("expected string")
    };
    assert!(Arc::ptr_eq(&text, cloned_text));
    let list = ListValue::new(vec![cloned], Arc::new(ConcreteDatatype::String(StringType)));
    let list_clone = list.clone();
    assert_eq!(list.items().as_ptr(), list_clone.items().as_ptr());
    drop(list);
    assert_eq!(list_clone.get(0), Some(&Value::String(text.clone())));
    let value = Value::List(list_clone);
    let structure_fields = StructType::new(Arc::new(vec![StructField::new(
        "payload".to_owned(),
        value.datatype(),
        false,
    )]));
    let structure = StructValue::new(vec![value], structure_fields);
    let structure_clone = structure.clone();
    assert_eq!(structure.items().as_ptr(), structure_clone.items().as_ptr());
    drop(structure);
    assert!(matches!(structure_clone.items()[0], Value::List(_)));
    #[cfg(target_pointer_width = "64")]
    assert_eq!(std::mem::size_of::<Value>(), 16);
}

#[test]
fn selected_value_does_not_retain_unselected_payload() {
    let selected = Arc::new("selected".to_owned());
    let unused = Arc::new("unused".repeat(4096));
    let unused_weak = Arc::downgrade(&unused);
    let row = vec![Value::String(selected.clone()), Value::String(unused)];
    let retained = row[0].clone();
    drop(row);
    assert!(unused_weak.upgrade().is_none());
    assert_eq!(retained, Value::String(selected));
}

#[test]
fn into_parts_moves_unique_buffer_and_shallow_clones_shared_buffer() {
    let unique = StructValue::new(vec![Value::String(Arc::new("unique".to_owned()))], fields());
    let pointer = unique.items().as_ptr();
    let (items, _) = unique.into_parts();
    assert_eq!(pointer, items.as_ptr());
    let text = Arc::new("shared".repeat(4096));
    let shared = StructValue::new(vec![Value::String(text.clone())], fields());
    let (mut items, _) = shared.clone().into_parts();
    assert_ne!(shared.items().as_ptr(), items.as_ptr());
    let Value::String(extracted) = &items[0] else {
        panic!("expected string")
    };
    assert!(Arc::ptr_eq(extracted, &text));
    items[0] = Value::Null;
    assert_eq!(shared.items(), &[Value::String(text)]);
}

#[test]
fn shared_payload_equality_and_hash_use_content() {
    fn hash(value: &Value) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }
    let make = |number| {
        Value::List(ListValue::new(
            vec![
                Value::String(Arc::new("same".to_owned())),
                Value::Float64(number),
            ],
            Arc::new(ConcreteDatatype::Null),
        ))
    };
    for (left, right) in [(make(f64::NAN), make(-f64::NAN)), (make(-0.0), make(0.0))] {
        assert_eq!(left, right);
        assert_eq!(hash(&left), hash(&right));
    }
    assert_ne!(make(1.0), make(2.0));
}
