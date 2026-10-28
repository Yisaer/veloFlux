//! Table-driven ARXML input/output checks and opt-in Python differential checks.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Command;

use arxml_converter::ast::types::{DataType, DataTypeKind};
use arxml_converter::{ArxmlCodec, parser::datatypes::DataTypesParser};
use serde_json::{Value, json};

#[path = "reference/type_inventory.rs"]
mod type_inventory;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn cases() -> Vec<Value> {
    let input = std::fs::read_to_string(root().join("tests/reference/cases.json")).unwrap();
    let manifest: Value = serde_json::from_str(&input).unwrap();
    assert_eq!(manifest["version"], 1);
    let cases = manifest["cases"].as_array().unwrap().clone();
    assert!(!cases.is_empty(), "the case table must not be empty");
    let mut ids = HashSet::new();
    for case in &cases {
        let id = text(case, "id");
        assert!(
            !id.is_empty() && ids.insert(id.to_string()),
            "invalid or duplicate case ID: {id}"
        );
    }
    cases
}

fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key]
        .as_str()
        .unwrap_or_else(|| panic!("missing {key}"))
}

fn number(value: &Value, key: &str) -> u16 {
    u16::try_from(value[key].as_u64().unwrap()).unwrap()
}

fn short(reference: &str) -> &str {
    reference.rsplit('/').next().unwrap()
}

fn normalize(dt: &DataType) -> Value {
    let definition = match &dt.kind {
        DataTypeKind::Unsupported { reason } => json!({"kind": "unsupported", "reason": reason}),
        DataTypeKind::TypeReference(tr) => json!({
            "kind": "reference",
            "target": short(&tr.type_name).to_lowercase(),
            "string_size": tr.string_size,
        }),
        DataTypeKind::Array(array) => json!({
            "kind": "array", "size": array.size,
            "element": short(&array.element_ref), "in_place": array.in_place,
        }),
        DataTypeKind::Vector(vector) => json!({
            "kind": "vector", "element": short(&vector.element_ref),
        }),
        DataTypeKind::Structure(structure) => json!({
            "kind": "structure",
            "fields": structure.fields.iter().map(|field| json!({
                "name": field.name, "target": short(&field.type_ref),
                "in_place": field.in_place,
            })).collect::<Vec<_>>(),
        }),
    };
    json!({"name": dt.short_name, "category": dt.category, "definition": definition})
}

fn cp_mappings(doc: &roxmltree::Document<'_>) -> Result<HashMap<String, String>, String> {
    doc.descendants()
        .filter(|node| node.has_tag_name("DATA-TYPE-MAP"))
        .map(|node| {
            let reference = |tag| {
                node.children()
                    .find(|child| child.has_tag_name(tag))
                    .and_then(|child| child.text())
                    .ok_or_else(|| format!("missing {tag}"))
            };
            Ok((
                reference("APPLICATION-DATA-TYPE-REF")?.to_string(),
                reference("IMPLEMENTATION-DATA-TYPE-REF")?.to_string(),
            ))
        })
        .collect()
}

fn inspect(case: &Value, path: &Path) -> Result<Value, String> {
    let query = &case["input"]["query"];
    if matches!(
        text(query, "operation"),
        "cp_type" | "implementation_type" | "ap_type"
    ) {
        let xml = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        let doc = roxmltree::Document::parse(&xml).map_err(|e| e.to_string())?;
        if text(query, "operation") == "ap_type" {
            let package = doc
                .descendants()
                .find(|node| {
                    node.has_tag_name("AR-PACKAGE")
                        && node.children().any(|child| {
                            child.has_tag_name("SHORT-NAME") && child.text() == Some("dataTypes")
                        })
                })
                .ok_or("missing dataTypes package")?;
            let types = arxml_converter::parser::ap::datatypes::parse_data_types(package)?;
            let name = short(text(query, "type_path")).to_lowercase();
            return types
                .get(&name)
                .map(normalize)
                .ok_or_else(|| "type not found".into());
        }
        let mut parser = DataTypesParser::new(cp_mappings(&doc)?);
        parser.parse_data_types(doc.root_element())?;
        let types = parser.merged_data_types();
        let name = short(text(query, "type_path"));
        let dt = types
            .get(text(query, "type_path"))
            .ok_or_else(|| format!("implementation type {name} not found"))?;
        return Ok(normalize(dt));
    }
    let codec = ArxmlCodec::load(path)?;
    let dt = match text(query, "operation") {
        "resolve" => codec.resolve(number(query, "service_id"), number(query, "entry_id"))?,
        "resolve_cp" => codec.resolve_cp(
            number(query, "service_id"),
            u32::try_from(query["header_id"].as_u64().unwrap()).unwrap(),
        )?,
        "type" => {
            let dt = codec
                .resolve_ref(text(query, "type_path"))
                .ok_or_else(|| "type not found".to_string())?;
            return Ok(normalize(&dt));
        }
        other => return Err(format!("unknown operation {other}")),
    };
    Ok(normalize(dt))
}

fn assert_output(case: &Value, side: &str, actual: &Value) {
    let expected = case["expected"]
        .get(side)
        .or_else(|| case["expected"].get("both"))
        .expect("missing expected output");
    assert!(
        !expected.is_null(),
        "{}: missing {side} expectation",
        case["id"]
    );
    if let Some(fragment) = expected["error_contains"].as_str() {
        let error = actual["error"].as_str().unwrap_or_else(|| {
            panic!("{} ({side}): expected an error, got {actual:#}", case["id"])
        });
        assert!(error.contains(fragment), "{} ({side}): {error}", case["id"]);
    } else {
        assert_eq!(actual, expected, "{} ({side})", case["id"]);
    }
}

fn rust_output(case: &Value) -> Value {
    let path = root()
        .join("tests/reference")
        .join(text(&case["input"], "file"));
    match inspect(case, &path) {
        Ok(value) => json!({"ok": value}),
        Err(error) => json!({"error": error}),
    }
}

#[test]
fn arxml_input_output_cases() {
    for case in cases() {
        assert!(
            case["source"]["url"].is_string(),
            "missing source: {}",
            case["id"]
        );
        let actual = rust_output(&case);
        println!(
            "CASE {}\nINPUT {:#}\nRUST {actual:#}",
            case["id"], case["input"]
        );
        assert_output(&case, "rust", &actual);
    }
}

#[test]
#[ignore = "requires ARXML_REFERENCE_PYTHON with tests/reference/requirements.txt installed"]
fn arxml_python_differential_cases() {
    let python = std::env::var_os("ARXML_REFERENCE_PYTHON")
        .expect("set ARXML_REFERENCE_PYTHON to the reference virtualenv Python executable");
    let output = Command::new(python)
        .arg(root().join("tests/reference/autosar_reference.py"))
        .arg(root().join("tests/reference/cases.json"))
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .expect("failed to start reference Python");
    assert!(
        output.status.success(),
        "reference runner failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).expect("invalid reference JSON");
    println!("REFERENCE METADATA {:#}", report["metadata"]);
    let rows = report["cases"].as_array().expect("missing reference cases");
    let cases = cases();
    assert_eq!(rows.len(), cases.len());
    for (case, reference) in cases.iter().zip(rows) {
        assert_eq!(reference["id"], case["id"]);
        let rust = rust_output(case);
        let python = &reference["output"];
        println!(
            "CASE {}\nINPUT {:#}\nRUST {rust:#}\nPYTHON {python:#}\nLOAD {:#}\nREFERENCE PATHS {:#}",
            case["id"], case["input"], reference["load"], reference["reference_paths"]
        );
        assert_output(case, "rust", &rust);
        assert_output(case, "python", python);
        match text(case, "comparison") {
            "equal" => assert_eq!(&rust, python, "{}: differential mismatch", case["id"]),
            "both_error" => {
                assert!(rust["error"].is_string() && python["error"].is_string());
            }
            "known_difference" => {
                assert!(case["reason"].as_str().is_some_and(|s| !s.is_empty()));
                assert_ne!(
                    &rust, python,
                    "{}: resolved difference; update this case",
                    case["id"]
                );
                println!("KNOWN DIFFERENCE: {}", case["reason"]);
            }
            other => panic!("unknown comparison {other}"),
        }
    }
}
