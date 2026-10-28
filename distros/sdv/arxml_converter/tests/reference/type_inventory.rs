//! Complete source-driven type inventory, including unrepresented definitions.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::process::Command;

use arxml_converter::ast::resolver::{BasicType, resolve_basic_type};
use arxml_converter::ast::types::{ApplicationTypeConstraints, DataType, DataTypeKind};
use arxml_converter::parser::datatypes::DataTypesParser;
use serde_json::{Value, json};

use super::{cp_mappings, normalize, root, short, text};

#[derive(Default)]
struct Registry {
    application_constraints: HashMap<String, ApplicationTypeConstraints>,
    scoped: HashMap<String, DataType>,
    ap: HashMap<String, DataType>,
    error: Option<String>,
}

impl Registry {
    fn load(xml: &str, parser: &str) -> Self {
        let mut registry = Self::default();
        let result = registry.parse(xml, parser);
        registry.error = result.err();
        registry
    }

    fn parse(&mut self, xml: &str, mode: &str) -> Result<(), String> {
        let doc = roxmltree::Document::parse(xml).map_err(|e| e.to_string())?;
        match mode {
            "cp" | "cp_types" => {
                // Supply the existing CP parser's mapping input. Expected types
                // and reference facts come independently from the Python model.
                let mappings = cp_mappings(&doc)?;
                let mut parser = DataTypesParser::new(mappings);
                let package = if mode == "cp_types" {
                    doc.root_element()
                } else {
                    doc.descendants()
                        .find(|node| {
                            node.has_tag_name("AR-PACKAGE")
                                && node.children().any(|child| {
                                    child.has_tag_name("SHORT-NAME")
                                        && child.text() == Some("DataTypes")
                                })
                        })
                        .ok_or("missing DataTypes package")?
                };
                parser.parse_data_types(package)?;
                self.scoped = parser.merged_data_types();
                self.application_constraints = parser.application_constraints;
            }
            "ap" => {
                // Match the production parser's package selection. Parsing
                // arbitrary extra packages here would overstate its coverage.
                let package = doc
                    .descendants()
                    .find(|node| {
                        node.has_tag_name("AR-PACKAGE")
                            && node.children().any(|child| {
                                child.has_tag_name("SHORT-NAME")
                                    && child.text() == Some("dataTypes")
                            })
                    })
                    .ok_or("missing dataTypes package")?;
                self.ap = arxml_converter::parser::ap::datatypes::parse_data_types(package)?;
                self.scoped = self.ap.clone();
            }
            other => return Err(format!("unknown inventory parser {other}")),
        }
        Ok(())
    }

    fn get(&self, row: &Value) -> Option<&DataType> {
        let name = short(text(row, "path"));
        match text(row, "element") {
            "SW-BASE-TYPE" => self.scoped.get(text(row, "path")),
            "IMPLEMENTATION-DATA-TYPE" => self.scoped.get(text(row, "path")),
            "STD-CPP-IMPLEMENTATION-DATA-TYPE" => self.ap.get(&name.to_lowercase()),
            "APPLICATION-PRIMITIVE-DATA-TYPE"
            | "APPLICATION-ARRAY-DATA-TYPE"
            | "APPLICATION-RECORD-DATA-TYPE" => self.scoped.get(text(row, "path")),
            _ => None,
        }
    }
}

fn implementation_output(dt: &DataType, registry: &Registry) -> (Value, BTreeMap<String, String>) {
    fn member(
        dt: &DataType,
        role: &str,
        registry: &Registry,
        refs: &mut BTreeMap<String, String>,
    ) -> Value {
        let mut result = json!({"name": dt.short_name, "category": dt.category});
        match &dt.kind {
            DataTypeKind::Unsupported { reason } => {
                result["unsupported"] = json!(reason);
            }
            DataTypeKind::TypeReference(reference) => {
                result["target"] = json!(reference.type_name);
                refs.insert(role.into(), reference.type_name.clone());
            }
            DataTypeKind::Structure(_) | DataTypeKind::Array(_) => {
                result["members"] = children(dt, role, registry, refs);
            }
            DataTypeKind::Vector(_) => {
                panic!("unexpected vector in a CP implementation definition")
            }
        }
        result
    }
    fn children(
        dt: &DataType,
        role: &str,
        registry: &Registry,
        refs: &mut BTreeMap<String, String>,
    ) -> Value {
        match &dt.kind {
            DataTypeKind::Structure(structure) => json!(
                structure
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        let target = registry
                            .scoped
                            .get(&field.type_ref)
                            .expect("missing scoped implementation member");
                        member(target, &format!("{role}.{index}"), registry, refs)
                    })
                    .collect::<Vec<_>>()
            ),
            DataTypeKind::Array(array) => {
                let target = registry
                    .scoped
                    .get(&array.element_ref)
                    .expect("missing scoped array element");
                let mut value = member(target, &format!("{role}.0"), registry, refs);
                let bounds = array
                    .bounds
                    .as_ref()
                    .expect("missing CP implementation array bounds");
                value["ARRAY-SIZE"] = json!(bounds.max_size);
                value["ARRAY-SIZE-SEMANTICS"] = json!(if bounds.is_dynamic {
                    "VARIABLE-SIZE"
                } else {
                    "FIXED-SIZE"
                });
                if let Some(handling) = &bounds.size_handling {
                    value["ARRAY-SIZE-HANDLING"] = json!(handling);
                }
                json!([value])
            }
            _ => panic!("expected an implementation composite"),
        }
    }
    let mut refs = BTreeMap::new();
    let members = children(dt, "member", registry, &mut refs);
    (
        json!({"name": dt.short_name, "category": dt.category,
            "definition": {"kind": "cp_implementation", "members": members}}),
        refs,
    )
}

fn runtime_base(dt: &DataType) -> Option<Value> {
    let DataTypeKind::TypeReference(reference) = &dt.kind else {
        return None;
    };
    let (bits, encoding) = match resolve_basic_type(reference)? {
        BasicType::Uint8 => (8, "NONE"),
        BasicType::Uint16 => (16, "NONE"),
        BasicType::Uint32 => (32, "NONE"),
        BasicType::Uint64 => (64, "NONE"),
        BasicType::Int8 => (8, "2C"),
        BasicType::Int16 => (16, "2C"),
        BasicType::Int32 => (32, "2C"),
        BasicType::Int64 => (64, "2C"),
        BasicType::Float => (32, "IEEE754"),
        BasicType::Double => (64, "IEEE754"),
        BasicType::Boolean => (8, "BOOLEAN"),
        BasicType::String | BasicType::FixedLengthString(_) => (8, "UTF-8"),
    };
    Some(json!({"bits": bits, "encoding": encoding}))
}

fn raw_references(dt: &DataType) -> BTreeMap<String, String> {
    match &dt.kind {
        DataTypeKind::Unsupported { .. } => BTreeMap::new(),
        DataTypeKind::TypeReference(reference) => {
            BTreeMap::from([("base".into(), reference.type_name.clone())])
        }
        DataTypeKind::Array(array) => {
            BTreeMap::from([("element".into(), array.element_ref.clone())])
        }
        DataTypeKind::Vector(vector) => {
            BTreeMap::from([("element".into(), vector.element_ref.clone())])
        }
        DataTypeKind::Structure(structure) => structure
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| (format!("field.{index}"), field.type_ref.clone()))
            .collect(),
    }
}

fn add_difference(differences: &mut Vec<Value>, field: &str, expected: &Value, actual: &Value) {
    if expected != actual {
        differences.push(json!({"field": field, "expected": expected, "actual": actual}));
    }
}

fn identity_key(row: &Value) -> String {
    let element = text(row, "element");
    let name = short(text(row, "path"));
    let (group, key) = match element {
        "IMPLEMENTATION-DATA-TYPE" => ("implementation", name.to_string()),
        "STD-CPP-IMPLEMENTATION-DATA-TYPE" => ("ap", name.to_lowercase()),
        "SW-BASE-TYPE" => ("base", name.to_string()),
        _ => ("application", name.to_lowercase()),
    };
    format!("{group}:{key}")
}

fn compare(row: &Value, registry: &Registry, collisions: &HashMap<String, usize>) -> Value {
    let mut result = row.clone();
    if text(row, "status") != "ok" {
        return result;
    }
    if let Some(error) = &registry.error {
        result["status"] = json!("rust_error");
        result["rust_error"] = json!(error);
        return result;
    }
    let Some(dt) = registry.get(row) else {
        result["status"] = json!("unrepresented");
        result["reason"] = json!(
            "No definition in the existing Rust parser registry; builtin fallback was not used."
        );
        return result;
    };
    let is_base = text(row, "element") == "SW-BASE-TYPE";
    let (actual, refs) = if is_base {
        let traits = runtime_base(dt).unwrap_or(Value::Null);
        (
            json!({"name": dt.short_name, "definition": {
                "kind": "base", "bits": traits["bits"], "encoding": traits["encoding"],
            }}),
            BTreeMap::new(),
        )
    } else if text(row, "element") == "IMPLEMENTATION-DATA-TYPE"
        && matches!(
            &dt.kind,
            DataTypeKind::Structure(_) | DataTypeKind::Array(_)
        )
    {
        implementation_output(dt, registry)
    } else {
        (normalize(dt), raw_references(dt))
    };
    let mut differences = Vec::new();
    let mut gaps = BTreeSet::new();
    let mut normalizations = BTreeSet::new();
    let mut expected = row["definition"].clone();
    if is_base {
        // The runtime stores terminal primitive semantics, not the source
        // FIXED_LENGTH category. Audit name, bit width and encoding here.
        expected.as_object_mut().unwrap().remove("category");
        normalizations.insert("base_category_lowered_to_primitive".to_string());
        add_difference(
            &mut differences,
            "runtime_category",
            &json!("SW-BASE-TYPE"),
            &json!(dt.category),
        );
    }
    add_difference(&mut differences, "definition", &expected, &actual);
    if collisions[&identity_key(row)] > 1 && !registry.scoped.contains_key(text(row, "path")) {
        gaps.insert("root_path_identity_not_retained".to_string());
    }
    for reference in row["facts"]["references"].as_array().unwrap() {
        let role = text(reference, "role");
        if role == "implementation_mapping" {
            normalizations.insert("implementation_mapping_resolved".to_string());
        } else if let Some(raw) = refs.get(role).filter(|raw| raw.starts_with('/')) {
            add_difference(
                &mut differences,
                &format!("reference.{role}"),
                &reference["path"],
                &json!(raw),
            );
        } else {
            // Normalized definitions already compare the short target names.
            // Full source paths are not part of the runtime contract.
            normalizations.insert(format!("short_reference:{role}"));
        }
    }
    // Resolve through the same retained base declarations used by decoding;
    // source references stay unchanged for the separate identity comparison.
    let resolved =
        arxml_converter::decoder::Decoder::new().resolve_ref(&registry.scoped, text(row, "path"));
    let base = runtime_base(resolved.as_ref().unwrap_or(dt));
    if !row["facts"]["base"].is_null() {
        add_difference(
            &mut differences,
            "base_semantics",
            &row["facts"]["base"],
            &base.clone().unwrap_or(Value::Null),
        );
    }
    if let Some(native) = row["facts"].get("native_base") {
        // Expected traits come from the supported builtin contract, independently
        // of the Rust resolver. Repeated SW-BASE-TYPE declarations are optional.
        let expected = match text(native, "path") {
            "/AUTOSAR/StdTypes/int32_t" => Some(json!({"bits": 32, "encoding": "2C"})),
            "/AUTOSAR/StdTypes/uint16_t" => Some(json!({"bits": 16, "encoding": "NONE"})),
            "/AUTOSAR/StdTypes/uint32_t" => Some(json!({"bits": 32, "encoding": "NONE"})),
            "/AUTOSAR/StdTypes/float" => Some(json!({"bits": 32, "encoding": "IEEE754"})),
            "/AUTOSAR/StdTypes/String" => Some(json!({"bits": 8, "encoding": "UTF-8"})),
            _ => None,
        };
        if let Some(expected) = expected {
            add_difference(
                &mut differences,
                "native_base_semantics",
                &expected,
                &base.clone().unwrap_or(Value::Null),
            );
            normalizations.insert("native_base_uses_builtin_contract".to_string());
        } else {
            gaps.insert("native_base_not_in_builtin_contract".to_string());
        }
    }
    let constraints = registry.application_constraints.get(text(row, "path"));
    for (key, expected) in row["facts"]["constraints"].as_object().unwrap() {
        let retained = match (key.as_str(), constraints) {
            ("maximum_elements", Some(ApplicationTypeConstraints::Array { max_elements })) => {
                Some(json!(max_elements))
            }
            ("SW-MAX-TEXT-SIZE", Some(ApplicationTypeConstraints::String { max_text_size })) => {
                Some(json!(max_text_size))
            }
            ("ARRAY-SIZE-SEMANTICS", _)
                if dt.category == "STRING"
                    && matches!(&dt.kind, DataTypeKind::TypeReference(reference)
                        if reference.string_size == Some(0)) =>
            {
                Some(json!("VARIABLE-SIZE"))
            }
            _ => None,
        };
        if let Some(actual) = retained {
            add_difference(
                &mut differences,
                &format!("constraints.{key}"),
                expected,
                &actual,
            );
        } else {
            gaps.insert(format!("constraint_not_retained:{key}"));
        }
    }
    if let Some(ApplicationTypeConstraints::Array { max_elements }) = constraints
        && row["facts"]["constraints"]
            .get("maximum_elements")
            .is_none()
    {
        add_difference(
            &mut differences,
            "constraints.maximum_elements",
            &row["definition"]["definition"]["size"],
            &json!(max_elements),
        );
    }
    let status = if !differences.is_empty() {
        "different"
    } else if !gaps.is_empty() {
        "partial"
    } else {
        "matched"
    };
    result["status"] = json!(status);
    result["rust"] = actual;
    result["rust_references"] = json!(refs);
    result["runtime_base"] = json!(base);
    result["differences"] = json!(differences);
    result["coverage_gaps"] = json!(gaps);
    result["expected_normalizations"] = json!(normalizations);
    match constraints {
        Some(ApplicationTypeConstraints::Array { max_elements }) => {
            result["application_constraints"] = json!({"max_array_elements": max_elements});
        }
        Some(ApplicationTypeConstraints::String { max_text_size }) => {
            result["application_constraints"] = json!({"max_text_size": max_text_size});
        }
        None => {}
    }
    result
}

// Regression checksum only. Fixture provenance uses SHA-256 from the Python
// report; this checksum is not used for security or integrity decisions.
fn checksum(value: &Value) -> String {
    fn canonical(value: &Value) -> String {
        match value {
            Value::Object(map) => {
                let mut keys = map.keys().collect::<Vec<_>>();
                keys.sort();
                format!(
                    "{{{}}}",
                    keys.into_iter()
                        .map(|key| {
                            format!(
                                "{}:{}",
                                serde_json::to_string(key).unwrap(),
                                canonical(&map[key])
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            Value::Array(items) => format!(
                "[{}]",
                items.iter().map(canonical).collect::<Vec<_>>().join(",")
            ),
            _ => value.to_string(),
        }
    }
    let hash = canonical(value)
        .bytes()
        .fold(0xcbf29ce484222325u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
        });
    format!("{hash:016x}")
}

#[test]
#[ignore = "requires ARXML_REFERENCE_PYTHON with the pinned autosar-data dependency"]
fn all_arxml_types() {
    let directory = root().join("tests/reference");
    let manifest: Value =
        serde_json::from_str(&std::fs::read_to_string(directory.join("inventory.json")).unwrap())
            .unwrap();
    let python = std::env::var_os("ARXML_REFERENCE_PYTHON").expect("set ARXML_REFERENCE_PYTHON");
    let output = Command::new(python)
        .arg(directory.join("autosar_inventory.py"))
        .arg(directory.join("inventory.json"))
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .expect("could not start the Python inventory adapter");
    assert!(
        output.status.success(),
        "inventory process failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reference: Value = serde_json::from_slice(&output.stdout).expect("invalid inventory JSON");
    let documents = manifest["documents"].as_array().unwrap();
    assert!(!documents.is_empty());
    let reference_documents = reference["documents"].as_array().unwrap();
    assert_eq!(documents.len(), reference_documents.len());
    let mut report = json!({"metadata": reference["metadata"], "documents": []});
    let mut baseline = json!({"version": 1, "documents": []});
    let mut document_ids = BTreeSet::new();
    for (document, source) in documents.iter().zip(reference_documents) {
        let id = text(document, "id");
        assert!(document_ids.insert(id));
        assert_eq!(document["id"], source["id"]);
        assert_eq!(document["file"], source["file"]);
        let xml = std::fs::read_to_string(directory.join(text(document, "file"))).unwrap();
        let registry = Registry::load(&xml, text(document, "parser"));
        let rows = source["types"].as_array().unwrap();
        let mut paths = BTreeSet::new();
        let mut collisions = HashMap::new();
        for row in rows {
            assert!(paths.insert(text(row, "path")), "duplicate source path");
            *collisions.entry(identity_key(row)).or_insert(0) += 1;
        }
        let compared = rows
            .iter()
            .map(|row| compare(row, &registry, &collisions))
            .collect::<Vec<_>>();
        let mut counts = BTreeMap::<String, usize>::new();
        let mut categories = BTreeMap::<String, BTreeMap<String, usize>>::new();
        for row in &compared {
            let status = text(row, "status").to_string();
            *counts.entry(status.clone()).or_default() += 1;
            let category = format!("{}:{}", text(row, "element"), text(row, "category"));
            *categories
                .entry(category)
                .or_default()
                .entry(status)
                .or_default() += 1;
        }
        assert_eq!(counts.values().sum::<usize>(), rows.len());
        if source["load"]["status"] == "ok" {
            assert!(
                !rows.is_empty(),
                "loaded document has no inventoried types: {id}"
            );
        }
        let result = json!({"id": id, "file": source["file"], "sha256": source["sha256"],
            "parser": document["parser"], "strict": document["strict"],
            "load": source["load"], "rust_load_error": registry.error,
            "type_count": rows.len(), "counts": counts, "categories": categories, "types": compared});
        let expected = json!({"id": id, "sha256": source["sha256"], "load": source["load"],
            "type_count": rows.len(), "counts": counts, "categories": categories,
            "results_checksum": checksum(&result)});
        println!(
            "INVENTORY {id}: {} types; load {}; {counts:?}",
            rows.len(),
            source["load"]["status"]
        );
        report["documents"].as_array_mut().unwrap().push(result);
        baseline["documents"].as_array_mut().unwrap().push(expected);
    }
    let report_path = std::env::var_os("ARXML_INVENTORY_REPORT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| root().join("../../../target/arxml-reference/type_inventory.json"));
    if let Some(parent) = report_path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        &report_path,
        serde_json::to_string_pretty(&report).unwrap() + "\n",
    )
    .unwrap();
    println!("FULL INVENTORY REPORT: {}", report_path.display());
    let baseline_path = directory.join("inventory_baseline.json");
    if std::env::var("ARXML_UPDATE_INVENTORY").as_deref() == Ok("1") {
        std::fs::write(
            baseline_path,
            serde_json::to_string_pretty(&baseline).unwrap() + "\n",
        )
        .unwrap();
        println!(
            "Wrote inventory baseline; review the full report, then rerun without ARXML_UPDATE_INVENTORY."
        );
    } else {
        let expected: Value =
            serde_json::from_str(&std::fs::read_to_string(baseline_path).unwrap()).unwrap();
        assert_eq!(
            baseline,
            expected,
            "inventory changed; inspect {} before updating expectations",
            report_path.display()
        );
    }
    if std::env::var("ARXML_REQUIRE_FULL_MATCH").as_deref() == Ok("1") {
        for document in report["documents"].as_array().unwrap() {
            assert_eq!(
                document["load"]["status"], "ok",
                "reference could not load {}",
                document["id"]
            );
            for row in document["types"].as_array().unwrap() {
                assert_eq!(
                    row["status"], "matched",
                    "{}: {}",
                    document["id"], row["path"]
                );
            }
        }
    }
}
