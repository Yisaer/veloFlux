//! ARXML schema parser for SOME/IP signal definitions.
//!
//! Parses AUTOSAR ARXML files via `arxml_converter` and generates
//! veloFlux column schemas from the discovered data type fields.

use std::sync::Arc;

use arxml_converter::ArxmlCodec;
use datatypes::{
    BooleanType, ColumnSchema, ConcreteDatatype, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, Schema, StringType, Uint8Type, Uint16Type, Uint32Type, Uint64Type,
};
use manager::{ParsedSchema, register_schema};
use serde_json::{Map as JsonMap, Value as JsonValue};

/// Register the `arxml` schema parser in the global schema registry.
pub fn register_arxml_schema() {
    register_schema("arxml", Arc::new(parse_arxml_schema));
}

/// Immutable ARXML schema artifact shared by planning and runtime decoders.
#[derive(Clone)]
pub struct CompiledArxmlSchema {
    codec: Arc<ArxmlCodec>,
    signal_name_pattern: Arc<str>,
}

impl CompiledArxmlSchema {
    pub fn codec(&self) -> Arc<ArxmlCodec> {
        Arc::clone(&self.codec)
    }

    pub fn signal_name_pattern(&self) -> &str {
        &self.signal_name_pattern
    }
}

fn parse_arxml_schema(
    stream_name: &str,
    props: &JsonMap<String, JsonValue>,
) -> Result<ParsedSchema, String> {
    let arxml_path = props
        .get("schema_path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "arxml schema requires `schema_path` prop".to_string())?;

    let pattern = props
        .get("signal_name_pattern")
        .and_then(|v| v.as_str())
        .unwrap_or("{field}");

    let (schema, artifact) = compile_arxml_schema(stream_name, arxml_path, pattern)?;
    Ok((schema, None, Some(artifact)))
}

/// Compile an ARXML source for either a standalone schema or a private GBF format.
pub fn compile_arxml_schema(
    stream_name: &str,
    arxml_path: &str,
    signal_name_pattern: &str,
) -> Result<(Schema, Arc<CompiledArxmlSchema>), String> {
    let codec = ArxmlCodec::load(arxml_path).map_err(|e| format!("failed to load ARXML: {e}"))?;

    // Enumerate configuration independently of successful payload resolution.
    let configured = codec.configured_entries();
    let mut field_specs: Vec<(String, ConcreteDatatype)> =
        vec![("ts".to_string(), ConcreteDatatype::Int64(Int64Type))];

    for (service_id, event_id) in configured {
        // PRS_SOMEIPSD_00151/00152 reserve this identity for discovery control
        // traffic, which has no application ARXML payload schema.
        if (service_id, event_id) == (0xFFFF, 0x8100) {
            continue;
        }
        let with_entry = |error| {
            format!(
                "failed to infer ARXML message (service_id={service_id}, event_id={event_id}): {error}"
            )
        };
        let resolved = codec.resolve(service_id, event_id).map_err(with_entry)?;
        // Validate the complete payload even when its shape yields no columns
        // or field-name deduplication would omit some fields below.
        concrete_datatype(&codec, resolved).map_err(with_entry)?;

        let (service_name, entry_name) = codec
            .resolve_entry_names(service_id, event_id)
            .unwrap_or_else(|_| (format!("0x{service_id:04X}"), format!("0x{event_id:04X}")));

        // Unwrap ARRAY to element type, then extract struct fields.
        let type_dt = match &resolved.kind {
            arxml_converter::ast::types::DataTypeKind::Array(arr) => {
                codec.resolve_ref(&arr.element_ref)
            }
            _ => Some(resolved.clone()),
        };
        let type_dt =
            type_dt.ok_or_else(|| with_entry("unresolved array element type".to_string()))?;

        if let arxml_converter::ast::types::DataTypeKind::Structure(st) = &type_dt.kind {
            for f in &st.fields {
                let name = apply_signal_name_pattern(
                    signal_name_pattern,
                    &service_name,
                    &entry_name,
                    &f.name,
                );
                if field_specs.iter().any(|(n, _)| n == &name) {
                    continue;
                }
                let dt = resolve_concrete_datatype(&codec, &f.type_ref).map_err(with_entry)?;
                field_specs.push((name, dt));
            }
        }
    }

    let columns: Vec<ColumnSchema> = field_specs
        .into_iter()
        .map(|(name, dt)| ColumnSchema::new(stream_name.to_string(), name, dt))
        .collect();

    let artifact = Arc::new(CompiledArxmlSchema {
        codec: Arc::new(codec),
        signal_name_pattern: Arc::from(signal_name_pattern),
    });
    Ok((Schema::new(columns), artifact))
}

/// Recursively resolve an ARXML type reference to a ConcreteDatatype.
fn resolve_concrete_datatype(
    codec: &ArxmlCodec,
    type_ref: &str,
) -> Result<ConcreteDatatype, String> {
    let dt = codec
        .resolve_ref(type_ref)
        .ok_or_else(|| format!("unresolved ARXML type: {type_ref}"))?;
    concrete_datatype(codec, &dt)
}

/// Convert a resolved layout, preserving diagnostics for every nested type.
fn concrete_datatype(
    codec: &ArxmlCodec,
    dt: &arxml_converter::ast::types::DataType,
) -> Result<ConcreteDatatype, String> {
    Ok(match &dt.kind {
        arxml_converter::ast::types::DataTypeKind::Unsupported { reason } => {
            return Err(reason.clone());
        }
        arxml_converter::ast::types::DataTypeKind::TypeReference(tr) => {
            if tr.type_name.contains('/')
                || arxml_converter::ast::resolver::resolve_basic_type(tr).is_none()
            {
                resolve_concrete_datatype(codec, &tr.type_name)?
            } else {
                base_type_to_concrete(&tr.type_name)
            }
        }
        arxml_converter::ast::types::DataTypeKind::Structure(st) => {
            let fields: Vec<datatypes::StructField> = st
                .fields
                .iter()
                .map(|f| {
                    let fdt = resolve_concrete_datatype(codec, &f.type_ref)?;
                    Ok(datatypes::StructField::new(f.name.clone(), fdt, true))
                })
                .collect::<Result<_, String>>()?;
            ConcreteDatatype::Struct(datatypes::StructType::new(Arc::new(fields)))
        }
        arxml_converter::ast::types::DataTypeKind::Array(arr) => {
            let elem_dt = resolve_concrete_datatype(codec, &arr.element_ref)?;
            ConcreteDatatype::List(datatypes::ListType::new(Arc::new(elem_dt)))
        }
        arxml_converter::ast::types::DataTypeKind::Vector(vec) => {
            let elem_dt = resolve_concrete_datatype(codec, &vec.element_ref)?;
            ConcreteDatatype::List(datatypes::ListType::new(Arc::new(elem_dt)))
        }
    })
}

/// Map an ARXML base type name to a ConcreteDatatype (leaf types only).
fn base_type_to_concrete(base: &str) -> ConcreteDatatype {
    match base.to_lowercase().as_str() {
        "uint8" | "uint8_t" => ConcreteDatatype::Uint8(Uint8Type),
        "uint16" | "uint16_t" => ConcreteDatatype::Uint16(Uint16Type),
        "uint32" | "uint32_t" => ConcreteDatatype::Uint32(Uint32Type),
        "uint64" | "uint64_t" => ConcreteDatatype::Uint64(Uint64Type),
        "sint8" | "int8" | "int8_t" => ConcreteDatatype::Int8(Int8Type),
        "sint16" | "int16" | "int16_t" => ConcreteDatatype::Int16(Int16Type),
        "sint32" | "int32" | "int32_t" => ConcreteDatatype::Int32(Int32Type),
        "sint64" | "int64" | "int64_t" => ConcreteDatatype::Int64(Int64Type),
        "float" | "float32" => ConcreteDatatype::Float32(Float32Type),
        "double" | "float64" => ConcreteDatatype::Float64(Float64Type),
        "boolean" | "bool" => ConcreteDatatype::Bool(BooleanType),
        "string" => ConcreteDatatype::String(StringType),
        _ => ConcreteDatatype::String(StringType),
    }
}

/// Apply a signal name pattern to produce a column name.
///
/// Placeholders: `{service}` (service interface), `{method}` or `{event}`
/// (synonyms for the entry name), and `{field}` (payload field name).
/// Default pattern is `"{field}"` which preserves raw ARXML field names.
pub(crate) fn apply_signal_name_pattern(
    pattern: &str,
    service: &str,
    entry: &str,
    field: &str,
) -> String {
    pattern
        .replace("{service}", service)
        .replace("{method}", entry)
        .replace("{event}", entry)
        .replace("{field}", field)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_only_service_discovery_is_excluded_from_application_schema() {
        let source =
            include_str!("../../arxml_converter/tests/test_data/schema_message_entries.arxml");
        for (service, event, succeeds) in [
            (0xFFFFu16, 0x8100u16, true),
            (0xFFFF, 0x8101, false),
            (0xFFFE, 0x8100, false),
        ] {
            let header = (u32::from(service) << 16) | u32::from(event);
            let xml = source.replace(
                "<HEADER-ID>655461</HEADER-ID>",
                &format!("<HEADER-ID>{header}</HEADER-ID>"),
            );
            let path = std::env::temp_dir().join(format!(
                "veloflux-sd-schema-{}-{header}.arxml",
                std::process::id()
            ));
            std::fs::write(&path, xml).unwrap();
            let result = compile_arxml_schema("test", path.to_str().unwrap(), "{field}");
            std::fs::remove_file(path).unwrap();
            assert_eq!(result.is_ok(), succeeds, "service={service}, event={event}");
            if let Err(error) = result {
                assert!(error.contains("no service found"), "{error}");
            }
        }
    }

    #[test]
    fn test_complete_baq_schema_includes_empty_getter_requests() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/test_data/baq.arxml");
        let (schema, artifact) =
            compile_arxml_schema("test", path, "{field}").expect("complete BAQ schema");
        assert!(schema.column_schemas().len() > 1);
        let codec = artifact.codec();
        assert!(codec.configured_entries().contains(&(43529, 1)));
        assert!(codec.known_entries().contains(&(43529, 1)));
        let getter = codec.resolve(43529, 1).unwrap();
        let arxml_converter::ast::types::DataTypeKind::Structure(request) = &getter.kind else {
            panic!("expected empty request")
        };
        assert!(request.fields.is_empty());
        assert_eq!(
            codec.decode(43529, 1, &[]).unwrap(),
            arxml_converter::Value::Struct(vec![])
        );
        assert!(
            codec
                .decode(43529, 1, &[1])
                .unwrap_err()
                .contains("zero payload bytes")
        );
    }

    #[test]
    fn test_ap_schema_reports_unresolved_configured_types_and_aliases() {
        let source = include_str!("../../arxml_converter/tests/test_data/s1_ap_test.xml");
        for (index, (xml, unresolved_path)) in [
            (
                source.replace("/dataTypes/WiFiSwitchStatus", "/Other/Missing"),
                "/Other/Missing",
            ),
            (
                source.replace("/AUTOSAR/StdTypes/int32_t", "/Other/Missing"),
                "/dataTypes/WiFiApNum",
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let path = std::env::temp_dir().join(format!(
                "veloflux-ap-schema-message-{}-{index}.arxml",
                std::process::id()
            ));
            std::fs::write(&path, xml).unwrap();
            let result = compile_arxml_schema("test", path.to_str().unwrap(), "{field}");
            std::fs::remove_file(path).unwrap();
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("configured AP type failures must not disappear"),
            };
            assert!(error.contains("service_id=33282"), "{error}");
            assert!(error.contains(unresolved_path), "{error}");
        }
    }

    #[test]
    fn test_configured_message_layout_errors_fail_schema_inference() {
        let source =
            include_str!("../../arxml_converter/tests/test_data/schema_message_entries.arxml");
        for (index, (target, diagnostic)) in [
            ("/DataTypes/Impl/OptionalPayload", None),
            (
                "/DataTypes/Impl/UnsupportedPayload",
                Some("unsupported implementation category: UNION"),
            ),
            (
                "/DataTypes/Impl/UnsupportedAlias",
                Some("unsupported implementation category: UNION"),
            ),
            (
                "/DataTypes/Impl/UnsupportedRecord",
                Some("unsupported implementation category: UNION"),
            ),
            (
                "/DataTypes/Impl/UnsupportedArray",
                Some("unsupported implementation category: UNION"),
            ),
            (
                "/DataTypes/Impl/NestedUnsupportedArrays",
                Some("unsupported implementation category: UNION"),
            ),
            (
                "/DataTypes/App/UnresolvedElements",
                Some("unresolved ARXML type: /Other/Missing"),
            ),
            ("/Other/Missing", Some("no data type for /Other/Missing")),
        ]
        .into_iter()
        .enumerate()
        {
            let xml = source.replace(
                "<TYPE-TREF>/DataTypes/Impl/OptionalPayload</TYPE-TREF>",
                &format!("<TYPE-TREF>{target}</TYPE-TREF>"),
            );
            let path = std::env::temp_dir().join(format!(
                "veloflux-schema-message-{}-{index}.arxml",
                std::process::id()
            ));
            std::fs::write(&path, xml).unwrap();
            let codec = ArxmlCodec::load(&path).unwrap();
            assert_eq!(codec.configured_entries(), vec![(10, 100), (10, 101)]);
            assert!(
                codec.resolve(10, 100).is_ok(),
                "the unrelated message remains usable"
            );
            let result = compile_arxml_schema("test", path.to_str().unwrap(), "{field}");
            std::fs::remove_file(path).unwrap();
            match (result, diagnostic) {
                (Ok((schema, _)), None) => {
                    assert_eq!(schema.column_schemas().len(), 2);
                    let value = schema
                        .column_schemas()
                        .iter()
                        .find(|c| c.name == "value")
                        .unwrap();
                    assert!(matches!(value.data_type, ConcreteDatatype::Uint32(_)));
                }
                (Err(error), Some(diagnostic)) => {
                    assert!(error.contains(diagnostic), "{target}: {error}");
                    assert!(error.contains("service_id=10, event_id=101"), "{error}");
                    if diagnostic.contains("UNION") {
                        assert!(
                            error.contains("/DataTypes/Impl/UnsupportedPayload"),
                            "{error}"
                        );
                    }
                }
                (Err(error), None) => {
                    panic!("unused unsupported definitions must not fail inference: {error}")
                }
                (Ok(_), Some(_)) => panic!("configured layout must not be omitted: {target}"),
            }
        }
    }

    #[test]
    fn test_unsupported_field_is_rejected_during_schema_inference() {
        let original = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/test_data/baq.arxml");
        let mut xml = std::fs::read_to_string(original).unwrap();
        let implementation = xml
            .find("<SHORT-NAME>IDT_ADAS_val_SlotID</SHORT-NAME>")
            .unwrap();
        let original_category = "<CATEGORY>VALUE</CATEGORY>";
        let category = implementation + xml[implementation..].find(original_category).unwrap();
        xml.replace_range(
            category..category + original_category.len(),
            "<CATEGORY>UNION</CATEGORY>",
        );
        let path = std::env::temp_dir().join(format!(
            "veloflux-unsupported-schema-{}.arxml",
            std::process::id()
        ));
        std::fs::write(&path, xml).unwrap();
        let result = compile_arxml_schema("test", path.to_str().unwrap(), "{field}");
        std::fs::remove_file(path).unwrap();
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("unsupported fields must not become string columns"),
        };
        assert!(
            error.contains("unsupported implementation category: UNION"),
            "{error}"
        );
        assert!(
            error.contains("/DataTypes/ImplementationDataTypes/IDT_ADAS_val_SlotID"),
            "{error}"
        );
    }

    /// Verify `entry_fields` returns correct field names and types.
    #[test]
    fn test_entry_fields_from_baq_arxml() {
        let arxml_path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/test_data/baq.arxml");
        let codec = ArxmlCodec::load(arxml_path).expect("load arxml");

        let fields = codec
            .entry_fields(0xAB04, 0x8003)
            .expect("entry_fields should return Some");

        // Verify specific fields.
        let slot_id = fields.iter().find(|(n, _)| n == "DTE_SlotID");
        assert!(slot_id.is_some(), "DTE_SlotID not found");
        assert_eq!(slot_id.unwrap().1, "uint16");

        let slot_type = fields.iter().find(|(n, _)| n == "DTE_SlotType");
        assert!(slot_type.is_some(), "DTE_SlotType not found");
        assert_eq!(slot_type.unwrap().1, "uint8");

        // Nested struct fields should have their own types resolved.
        let point_top = fields.iter().find(|(n, _)| n == "DTE_SlotPointTop1");
        assert!(point_top.is_some(), "DTE_SlotPointTop1 not found");
        assert_eq!(point_top.unwrap().1, "struct");
    }

    /// Verify schema inference produces correct types for leaf and nested fields.
    #[test]
    fn test_arxml_schema_inference() {
        let arxml_path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/test_data/baq.arxml");
        let (schema, _) =
            compile_arxml_schema("test", arxml_path, "{field}").expect("infer complete schema");

        assert!(
            schema.column_schemas().len() > 1,
            "expected more than ts column"
        );

        // Leaf: DTE_SlotID should be Uint16.
        let slot_id = schema
            .column_schemas()
            .iter()
            .find(|c| c.name == "DTE_SlotID")
            .expect("DTE_SlotID should be in schema");
        assert!(
            matches!(slot_id.data_type, ConcreteDatatype::Uint16(_)),
            "DTE_SlotID type mismatch: {:?}",
            slot_id.data_type
        );

        // Leaf: DTE_SlotType should be Uint8.
        let slot_type = schema
            .column_schemas()
            .iter()
            .find(|c| c.name == "DTE_SlotType")
            .expect("DTE_SlotType should be in schema");
        assert!(
            matches!(slot_type.data_type, ConcreteDatatype::Uint8(_)),
            "DTE_SlotType type mismatch: {:?}",
            slot_type.data_type
        );

        // Nested struct: DTE_SlotPointTop1 → should be a Struct with ≥2 float32 fields.
        let point_top = schema
            .column_schemas()
            .iter()
            .find(|c| c.name == "DTE_SlotPointTop1")
            .expect("DTE_SlotPointTop1 should be in schema");
        match &point_top.data_type {
            ConcreteDatatype::Struct(st) => {
                let fields = st.fields();
                assert!(
                    fields.len() >= 2,
                    "DTE_SlotPointTop1 should have >= 2 fields, got {}",
                    fields.len()
                );
                for f in fields.iter() {
                    assert!(
                        matches!(f.data_type(), ConcreteDatatype::Float32(_)),
                        "DTE_SlotPointTop1.{} should be Float32, got {:?}",
                        f.name(),
                        f.data_type()
                    );
                }
            }
            other => panic!("DTE_SlotPointTop1 should be Struct, got {other:?}"),
        }

        // Array: DTE_VPAParkingLotList (0xAB04, 0x8007) should be List.
        let parking_lot_list = schema
            .column_schemas()
            .iter()
            .find(|c| c.name == "DTE_VPAParkingLotList");
        // Note: this field only appears if (0xAB04, 0x8007) is in known_entries.
        // It exists in the ARXML but field_specs de-duplicates by name.
        if let Some(col) = parking_lot_list {
            assert!(
                matches!(col.data_type, ConcreteDatatype::List(_)),
                "DTE_VPAParkingLotList should be List, got {:?}",
                col.data_type
            );
        }
    }
}
