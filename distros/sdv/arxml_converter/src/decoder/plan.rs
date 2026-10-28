//! Immutable layouts compiled once for repeated payload decoding.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::ast::resolver::{BasicType, resolve_basic_type};
use crate::ast::types::{DataType, DataTypeKind};

use super::{DecodeError, Decoder, Value, alias_target};

/// A self-contained payload layout with resolved aliases and primitive kinds.
/// Layout errors remain executable nodes so unused or zero-count children keep
/// their existing behavior. No mutable registry is borrowed by the plan.
#[derive(Debug, Clone)]
pub struct DecodePlan {
    root: Arc<Node>,
}

#[derive(Debug)]
enum Node {
    Primitive(BasicType),
    Array {
        length: Option<u64>,
        stop_on_payload_error: bool,
        element: Arc<Node>,
    },
    Structure {
        fields: Vec<(String, Arc<Node>)>,
        empty_request: Option<String>,
    },
    Error(String),
}

impl DecodePlan {
    pub(super) fn compile(types: &HashMap<String, DataType>, root: &DataType) -> Self {
        let mut compiler = Compiler {
            types,
            resolved: HashMap::new(),
            active: HashSet::new(),
        };
        Self {
            root: compiler.node(root),
        }
    }

    /// Execute the resolved layout without type-name parsing or registry lookup.
    pub fn decode(&self, data: &[u8]) -> Result<(usize, Value), String> {
        self.root.decode(data).map_err(DecodeError::into_message)
    }
}

struct Compiler<'a> {
    types: &'a HashMap<String, DataType>,
    resolved: HashMap<String, Arc<Node>>,
    active: HashSet<String>,
}

impl Compiler<'_> {
    fn reference(&mut self, path: &str) -> Option<Arc<Node>> {
        if let Some(node) = self.resolved.get(path) {
            return Some(Arc::clone(node));
        }
        if !self.active.insert(path.into()) {
            return None;
        }
        // The compatibility resolver is only used while compiling. Its scoped
        // lookup, primitive shadowing and cycle rules remain authoritative.
        let node = Decoder::new()
            .resolve_ref(self.types, path)
            .map(|dt| self.node(&dt));
        self.active.remove(path);
        if let Some(node) = &node {
            self.resolved.insert(path.into(), Arc::clone(node));
        }
        node
    }

    fn node(&mut self, dt: &DataType) -> Arc<Node> {
        let error = |message: String| Arc::new(Node::Error(message));
        Arc::new(match &dt.kind {
            DataTypeKind::Unsupported { reason } => Node::Error(reason.clone()),
            DataTypeKind::TypeReference(reference) => {
                if reference.type_name.contains('/')
                    && !self.types.contains_key(&reference.type_name)
                {
                    return error(format!("unknown type reference: {}", reference.type_name));
                }
                if alias_target(self.types, dt).is_some() {
                    return self.reference(&reference.type_name).unwrap_or_else(|| {
                        error(format!("cyclic type reference: {}", reference.type_name))
                    });
                }
                match resolve_basic_type(reference) {
                    Some(basic) => Node::Primitive(basic),
                    None => Node::Error(format!("unresolved basic type: {}", reference.type_name)),
                }
            }
            DataTypeKind::Array(array) => {
                if array
                    .bounds
                    .as_ref()
                    .is_some_and(|bounds| bounds.is_dynamic)
                {
                    return error(
                        "dynamic implementation arrays require a serialization length policy"
                            .into(),
                    );
                }
                let Some(element) = self.reference(&array.element_ref) else {
                    return error(format!("unknown array element type: {}", array.element_ref));
                };
                Node::Array {
                    length: (!(array.size == 0 && array.bounds.is_none())).then_some(array.size),
                    stop_on_payload_error: true,
                    element,
                }
            }
            DataTypeKind::Vector(vector) => {
                let Some(element) = self.reference(&vector.element_ref) else {
                    return error(format!(
                        "unknown vector element type: {}",
                        vector.element_ref
                    ));
                };
                Node::Array {
                    length: None,
                    stop_on_payload_error: false,
                    element,
                }
            }
            DataTypeKind::Structure(structure) => {
                let fields = structure
                    .fields
                    .iter()
                    .map(|field| {
                        let node = self.reference(&field.type_ref).unwrap_or_else(|| {
                            error(format!(
                                "unknown type '{}' for field '{}'",
                                field.type_ref, field.name
                            ))
                        });
                        (field.name.clone(), node)
                    })
                    .collect();
                Node::Structure {
                    fields,
                    empty_request: (dt.category == "REQUEST" && structure.fields.is_empty())
                        .then(|| dt.short_name.clone()),
                }
            }
        })
    }
}

impl Node {
    fn decode(&self, data: &[u8]) -> Result<(usize, Value), DecodeError> {
        match self {
            Self::Error(message) => Err(DecodeError::Layout(message.clone())),
            Self::Primitive(basic) => Decoder::new().decode_basic_type(data, *basic),
            Self::Structure {
                fields,
                empty_request,
            } => {
                if let Some(name) = empty_request
                    && !data.is_empty()
                {
                    return Err(DecodeError::Payload(format!(
                        "empty request '{name}' requires zero payload bytes, got {}",
                        data.len()
                    )));
                }
                let mut offset = 0;
                let mut values = Vec::with_capacity(fields.len());
                for (name, node) in fields {
                    let (consumed, value) = node.decode(&data[offset..])?;
                    offset += consumed;
                    values.push((name.clone(), value));
                }
                Ok((offset, Value::Struct(values)))
            }
            Self::Array {
                length,
                stop_on_payload_error,
                element,
            } => {
                let mut offset = 0;
                let mut values = length
                    .map(|length| Vec::with_capacity(length as usize))
                    .unwrap_or_default();
                if let Some(length) = length {
                    for _ in 0..*length {
                        let (consumed, value) = element.decode(&data[offset..])?;
                        offset += consumed;
                        values.push(value);
                    }
                } else {
                    while offset < data.len() {
                        match element.decode(&data[offset..]) {
                            Ok((0, _)) => break,
                            Ok((consumed, value)) => {
                                offset += consumed;
                                values.push(value);
                            }
                            Err(DecodeError::Payload(_)) if *stop_on_payload_error => break,
                            Err(error) => return Err(error),
                        }
                    }
                }
                Ok((offset, Value::Array(values)))
            }
        }
    }
}
