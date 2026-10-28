//! CP implementation composites using the existing shared type model.

use std::collections::HashMap;

use arxml_converter::ArxmlCodec;
use arxml_converter::ast::types::{ApplicationTypeConstraints, DataType, DataTypeKind};
use arxml_converter::decoder::{Decoder, Value};
use arxml_converter::parser::datatypes::DataTypesParser;

const XML: &str = include_str!("test_data/implementation_types.arxml");
const PREFIX: &str = "/DataTypes/ImplementationDataTypes/";
const ARBITRARY_BASE_XML: &str = include_str!("reference/fixtures/upstream_value.arxml");

fn load_cp_xml(xml: &str) -> Result<ArxmlCodec, String> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT_FILE: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "veloflux-review-cp-{}-{}.arxml",
        std::process::id(),
        NEXT_FILE.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&path, xml).unwrap();
    let result = ArxmlCodec::load(&path);
    std::fs::remove_file(path).unwrap();
    result
}

fn scoped_mapping_xml(reverse: bool) -> String {
    let mut xml = include_str!("test_data/minimal_cp.arxml").to_string();
    let insertion = {
        let doc = roxmltree::Document::parse(&xml).unwrap();
        let package = doc
            .descendants()
            .find(|n| {
                n.has_tag_name("AR-PACKAGE")
                    && n.children()
                        .any(|c| c.has_tag_name("SHORT-NAME") && c.text() == Some("DataTypes"))
            })
            .unwrap();
        package
            .children()
            .find(|n| n.has_tag_name("AR-PACKAGES"))
            .unwrap()
            .range()
            .end
            - "</AR-PACKAGES>".len()
    };
    let package = |name: &str, base: &str| {
        format!(
            r#"
      <AR-PACKAGE><SHORT-NAME>{name}</SHORT-NAME><ELEMENTS>
        <IMPLEMENTATION-DATA-TYPE><SHORT-NAME>Value</SHORT-NAME><CATEGORY>VALUE</CATEGORY>
          <SW-DATA-DEF-PROPS><SW-DATA-DEF-PROPS-VARIANTS><SW-DATA-DEF-PROPS-CONDITIONAL>
            <BASE-TYPE-REF>/BaseTypes/{base}</BASE-TYPE-REF>
          </SW-DATA-DEF-PROPS-CONDITIONAL></SW-DATA-DEF-PROPS-VARIANTS></SW-DATA-DEF-PROPS>
        </IMPLEMENTATION-DATA-TYPE>
        <IMPLEMENTATION-DATA-TYPE><SHORT-NAME>Shared</SHORT-NAME><CATEGORY>TYPE_REFERENCE</CATEGORY>
          <SW-DATA-DEF-PROPS><SW-DATA-DEF-PROPS-VARIANTS><SW-DATA-DEF-PROPS-CONDITIONAL>
            <IMPLEMENTATION-DATA-TYPE-REF>/DataTypes/{name}/Value</IMPLEMENTATION-DATA-TYPE-REF>
          </SW-DATA-DEF-PROPS-CONDITIONAL></SW-DATA-DEF-PROPS-VARIANTS></SW-DATA-DEF-PROPS>
        </IMPLEMENTATION-DATA-TYPE>
      </ELEMENTS></AR-PACKAGE>"#
        )
    };
    let left = package("Left", "uint8");
    let right = package("Right", "uint16");
    let other_app = r#"<AR-PACKAGE><SHORT-NAME>OtherApp</SHORT-NAME><ELEMENTS>
      <APPLICATION-PRIMITIVE-DATA-TYPE><SHORT-NAME>SpeedType</SHORT-NAME><CATEGORY>VALUE</CATEGORY></APPLICATION-PRIMITIVE-DATA-TYPE>
      </ELEMENTS></AR-PACKAGE>"#;
    xml.insert_str(
        insertion,
        &if reverse {
            format!("{right}{left}{other_app}")
        } else {
            format!("{left}{right}{other_app}")
        },
    );
    xml = xml.replace(
        "<IMPLEMENTATION-DATA-TYPE-REF>/DataTypes/Impl/uint32_impl</IMPLEMENTATION-DATA-TYPE-REF>",
        "<IMPLEMENTATION-DATA-TYPE-REF>/DataTypes/Left/Shared</IMPLEMENTATION-DATA-TYPE-REF>",
    );
    xml.replace("</DATA-TYPE-MAPS>", r#"
      <DATA-TYPE-MAP><APPLICATION-DATA-TYPE-REF>/DataTypes/OtherApp/SpeedType</APPLICATION-DATA-TYPE-REF>
      <IMPLEMENTATION-DATA-TYPE-REF>/DataTypes/Right/Shared</IMPLEMENTATION-DATA-TYPE-REF></DATA-TYPE-MAP></DATA-TYPE-MAPS>"#)
}

#[test]
fn scoped_application_mappings_do_not_use_same_named_implementation_aliases() {
    for reverse in [false, true] {
        let xml = scoped_mapping_xml(reverse);
        let doc = roxmltree::Document::parse(&xml).unwrap();
        let mut parser = arxml_converter::parser::cp::CpParser::new();
        parser.parse(&doc).unwrap();
        let types = parser.merged_data_types();
        let decoder = Decoder::new();
        for (path, expected) in [
            ("/DataTypes/App/SpeedType", (1, Value::U8(1))),
            ("/DataTypes/OtherApp/SpeedType", (2, Value::U16(258))),
        ] {
            assert_eq!(
                decoder.decode(&types, &[1, 2, 0x7F], &types[path]).unwrap(),
                expected,
                "{path}, reverse={reverse}"
            );
        }
        assert!(
            !types.contains_key("speedtype"),
            "ambiguous application short name"
        );
        assert!(decoder.resolve_ref(&types, "Shared").is_none());
        let codec = load_cp_xml(&xml).unwrap();
        assert_eq!(
            codec.decode_cp(10, 100, &[1, 2, 0x7F]).unwrap(),
            Value::U8(1)
        );
        for (from, to, diagnostic) in [
            (
                "/DataTypes/Left/Shared",
                "/Other/Shared",
                "no implementation data type",
            ),
            (
                "/DataTypes/Left/Shared",
                "Shared",
                "ambiguous implementation data type",
            ),
            (
                "/DataTypes/App/SpeedType</APPLICATION-DATA-TYPE-REF>",
                "SpeedType</APPLICATION-DATA-TYPE-REF>",
                "ambiguous application mapping key",
            ),
        ] {
            let error = load_cp_xml(&xml.replace(from, to)).unwrap_err();
            assert!(error.contains(diagnostic), "{diagnostic}: {error}");
        }
    }
}

#[test]
fn compiled_plans_are_immutable_and_do_not_cross_registries() {
    let decoder = Decoder::new();
    let root = DataType::new_array("Root".into(), "ARRAY".into(), "/Types/Element".into(), 1);
    let mut types = HashMap::from([(
        "/Types/Element".into(),
        DataType::new_type_reference("Element".into(), "VALUE".into(), "uint8".into()),
    )]);
    let first = decoder.compile(&types, &root);
    types.insert(
        "/Types/Element".into(),
        DataType::new_type_reference("Element".into(), "VALUE".into(), "uint16".into()),
    );
    let second = decoder.compile(&types, &root);
    for _ in 0..3 {
        assert_eq!(
            first.decode(&[1, 2]).unwrap(),
            (1, Value::Array(vec![Value::U8(1)]))
        );
        assert!(second.decode(&[1]).is_err());
        assert_eq!(
            second.decode(&[1, 2]).unwrap(),
            (2, Value::Array(vec![Value::U16(258)]))
        );
    }
    assert_eq!(
        decoder.decode(&types, &[1, 2], &root).unwrap(),
        (2, Value::Array(vec![Value::U16(258)]))
    );
    drop(types);
    assert_eq!(
        first.decode(&[9]).unwrap(),
        (1, Value::Array(vec![Value::U8(9)]))
    );
}

fn client_server_request_xml(arguments: Option<&str>) -> String {
    let mut xml = include_str!("test_data/minimal_cp.arxml").replace(
        "<HEADER-ID>100</HEADER-ID>",
        &format!("<HEADER-ID>{}</HEADER-ID>", (10u32 << 16) | 100),
    );
    let interface = {
        let doc = roxmltree::Document::parse(&xml).unwrap();
        doc.descendants()
            .find(|n| n.has_tag_name("SENDER-RECEIVER-INTERFACE"))
            .unwrap()
            .range()
    };
    let arguments = arguments
        .map(|a| format!("<ARGUMENTS>{a}</ARGUMENTS>"))
        .unwrap_or_default();
    xml.replace_range(interface, &format!(r#"
        <CLIENT-SERVER-INTERFACE><SHORT-NAME>SensorIf</SHORT-NAME><OPERATIONS>
          <CLIENT-SERVER-OPERATION><SHORT-NAME>Speed</SHORT-NAME>{arguments}</CLIENT-SERVER-OPERATION>
        </OPERATIONS></CLIENT-SERVER-INTERFACE>"#));
    let mapping = {
        let doc = roxmltree::Document::parse(&xml).unwrap();
        doc.descendants()
            .find(|n| n.has_tag_name("SENDER-RECEIVER-TO-SIGNAL-MAPPING"))
            .unwrap()
            .range()
    };
    xml.replace_range(mapping, r#"
        <CLIENT-SERVER-TO-SIGNAL-MAPPING><CALL-SIGNAL-REF>/Sys/SS1</CALL-SIGNAL-REF>
          <CLIENT-SERVER-OPERATION-IREF><TARGET-OPERATION-REF>/SoftwareTypes/Interfaces/SensorIf/Speed</TARGET-OPERATION-REF></CLIENT-SERVER-OPERATION-IREF>
        </CLIENT-SERVER-TO-SIGNAL-MAPPING>"#);
    xml
}

#[test]
fn client_server_request_arguments_preserve_direction_and_order() {
    let argument = |name: &str, direction: &str, target: &str| {
        format!(
            "<ARGUMENT-DATA-PROTOTYPE><SHORT-NAME>{name}</SHORT-NAME><DIRECTION>{direction}</DIRECTION><TYPE-TREF>{target}</TYPE-TREF></ARGUMENT-DATA-PROTOTYPE>"
        )
    };
    let input = argument("first", "IN", "/DataTypes/Impl/uint32_impl");
    let output = argument("result", "OUT", "/DataTypes/App/HeaderType");
    let inout = argument("both", "INOUT", "/DataTypes/Impl/uint16_impl");
    let cases: Vec<(Option<String>, Vec<u8>, Value)> = vec![
        (None, vec![], Value::Struct(vec![])),
        (Some(String::new()), vec![], Value::Struct(vec![])),
        (Some(output.clone()), vec![], Value::Struct(vec![])),
        (Some(input.clone()), vec![0, 0, 0, 42], Value::U32(42)),
        (
            Some(format!("{input}{output}")),
            vec![0, 0, 0, 42],
            Value::U32(42),
        ),
        (
            Some(format!("{output}{input}")),
            vec![0, 0, 0, 42],
            Value::U32(42),
        ),
        (Some(inout.clone()), vec![0, 9], Value::U16(9)),
        (
            Some(format!("{input}{output}{inout}")),
            vec![0, 0, 0, 42, 0, 9],
            Value::Struct(vec![
                ("first".into(), Value::U32(42)),
                ("both".into(), Value::U16(9)),
            ]),
        ),
    ];
    for (arguments, bytes, expected) in cases {
        let xml = client_server_request_xml(arguments.as_deref());
        let codec = load_cp_xml(&xml).unwrap();
        assert!(codec.known_entries().contains(&(10, 100)));
        assert_eq!(
            codec.decode(10, 100, &bytes).unwrap(),
            expected,
            "{arguments:?}"
        );
        assert_eq!(
            codec.decode_cp(10, (10u32 << 16) | 100, &bytes).unwrap(),
            expected
        );
        if bytes.is_empty() {
            assert!(
                codec
                    .decode(10, 100, &[0, 9])
                    .unwrap_err()
                    .contains("zero payload bytes")
            );
        } else {
            assert!(codec.decode(10, 100, &bytes[..bytes.len() - 1]).is_err());
        }
    }
    let xml = client_server_request_xml(Some(&argument(
        "bad",
        "SIDEWAYS",
        "/DataTypes/Impl/uint32_impl",
    )));
    assert!(
        load_cp_xml(&xml)
            .unwrap_err()
            .contains("invalid argument direction")
    );
}

#[test]
fn configured_entries_preserve_messages_with_resolution_errors() {
    let source = include_str!("test_data/schema_message_entries.arxml");
    for target in [
        "/DataTypes/Impl/OptionalPayload",
        "/DataTypes/Impl/UnsupportedPayload",
        "/DataTypes/Impl/UnsupportedAlias",
        "/Other/Missing",
    ] {
        let xml = source.replace(
            "<TYPE-TREF>/DataTypes/Impl/OptionalPayload</TYPE-TREF>",
            &format!("<TYPE-TREF>{target}</TYPE-TREF>"),
        );
        let codec = load_cp_xml(&xml).unwrap();
        assert_eq!(
            codec.configured_entries(),
            vec![(10, 100), (10, 101)],
            "{target}"
        );
        assert!(codec.known_entries().contains(&(10, 100)));
        assert_eq!(
            codec.known_entries().contains(&(10, 101)),
            target == "/DataTypes/Impl/OptionalPayload",
            "{target}"
        );
        assert_eq!(
            codec.decode(10, 100, &[0, 0, 0, 42]).unwrap(),
            Value::Struct(vec![("value".into(), Value::U32(42))])
        );
    }
}

#[test]
fn unused_unsupported_implementation_types_do_not_block_supported_messages() {
    let mut xml = include_str!("test_data/minimal_cp.arxml").to_string();
    let insertion = {
        let doc = roxmltree::Document::parse(&xml).unwrap();
        let package = doc
            .descendants()
            .find(|node| {
                node.has_tag_name("AR-PACKAGE")
                    && node
                        .children()
                        .any(|c| c.has_tag_name("SHORT-NAME") && c.text() == Some("Impl"))
            })
            .unwrap();
        let elements = package
            .children()
            .find(|node| node.has_tag_name("ELEMENTS"))
            .unwrap();
        elements.range().end - "</ELEMENTS>".len()
    };
    xml.insert_str(
        insertion,
        r#"
      <IMPLEMENTATION-DATA-TYPE>
        <SHORT-NAME>UnusedUnion</SHORT-NAME><CATEGORY>UNION</CATEGORY>
      </IMPLEMENTATION-DATA-TYPE>
      <IMPLEMENTATION-DATA-TYPE>
        <SHORT-NAME>UnionAlias</SHORT-NAME><CATEGORY>TYPE_REFERENCE</CATEGORY>
        <SW-DATA-DEF-PROPS><SW-DATA-DEF-PROPS-VARIANTS><SW-DATA-DEF-PROPS-CONDITIONAL>
          <IMPLEMENTATION-DATA-TYPE-REF>/DataTypes/Impl/UnusedUnion</IMPLEMENTATION-DATA-TYPE-REF>
        </SW-DATA-DEF-PROPS-CONDITIONAL></SW-DATA-DEF-PROPS-VARIANTS></SW-DATA-DEF-PROPS>
      </IMPLEMENTATION-DATA-TYPE>
      <IMPLEMENTATION-DATA-TYPE>
        <SHORT-NAME>UnionContainer</SHORT-NAME><CATEGORY>STRUCTURE</CATEGORY>
        <SUB-ELEMENTS><IMPLEMENTATION-DATA-TYPE-ELEMENT>
          <SHORT-NAME>choice</SHORT-NAME><CATEGORY>UNION</CATEGORY>
        </IMPLEMENTATION-DATA-TYPE-ELEMENT></SUB-ELEMENTS>
      </IMPLEMENTATION-DATA-TYPE>
    "#,
    );
    let codec = load_cp_xml(&xml).unwrap();
    assert_eq!(
        codec.decode_cp(10, 100, &[0, 0, 0, 42]).unwrap(),
        Value::U32(42)
    );
    let union = codec.resolve_ref("/DataTypes/Impl/UnusedUnion").unwrap();
    assert!(matches!(union.kind, DataTypeKind::Unsupported { .. }));

    // The same document remains loadable when a message selects an unsupported
    // definition, an alias to it, a structure with an unsupported inline member,
    // or an application VALUE mapped to it. Use reports the source diagnostic.
    for target in ["UnusedUnion", "UnionAlias", "UnionContainer"] {
        let xml = xml.replace(
            "<TYPE-TREF>/DataTypes/App/SpeedType</TYPE-TREF>",
            &format!("<TYPE-TREF>/DataTypes/Impl/{target}</TYPE-TREF>"),
        );
        let codec = load_cp_xml(&xml).unwrap();
        let error = codec.decode_cp(10, 100, &[0, 0, 0, 42]).unwrap_err();
        assert!(
            error.contains("unsupported implementation category: UNION"),
            "{target}: {error}"
        );
        assert!(error.contains("/DataTypes/Impl/"), "{error}");
    }
    let xml = xml.replace(
        "<IMPLEMENTATION-DATA-TYPE-REF>/DataTypes/Impl/uint32_impl</IMPLEMENTATION-DATA-TYPE-REF>",
        "<IMPLEMENTATION-DATA-TYPE-REF>/DataTypes/Impl/UnusedUnion</IMPLEMENTATION-DATA-TYPE-REF>",
    );
    let codec = load_cp_xml(&xml).unwrap();
    assert!(
        codec
            .resolve_cp(10, 100)
            .unwrap_err()
            .contains("UnusedUnion")
    );
}

#[test]
fn application_arrays_propagate_layout_errors_and_keep_payload_boundaries() {
    let mut types = parse(XML).unwrap().merged_data_types();
    let decoder = Decoder::new();
    types.insert(
        "dynamic_alias".into(),
        DataType::new_type_reference(
            "alias".into(),
            "TYPE_REFERENCE".into(),
            format!("{PREFIX}DynamicEnvelope/items"),
        ),
    );
    for reference in [
        format!("{PREFIX}DynamicEnvelope/items"),
        format!("{PREFIX}DynamicEnvelope"),
        "dynamic_alias".into(),
    ] {
        let direct = decoder
            .decode(&types, &[0, 1, 2], &types[&reference])
            .unwrap_err();
        assert!(
            direct.contains("serialization length policy"),
            "{reference}: {direct}"
        );
        let outer = DataType::new_array("outer".into(), "ARRAY".into(), reference, 0);
        assert_eq!(
            decoder.decode(&types, &[0, 1, 2], &outer).unwrap_err(),
            direct
        );
        types.insert("outer".into(), outer);
        let nested = DataType::new_array("nested".into(), "ARRAY".into(), "outer".into(), 0);
        assert_eq!(
            decoder.decode(&types, &[0, 1, 2], &nested).unwrap_err(),
            direct
        );
    }
    types.insert(
        "unsupported".into(),
        DataType {
            short_name: "Unsupported".into(),
            category: "UNION".into(),
            kind: DataTypeKind::Unsupported {
                reason: "unsupported implementation category: UNION at /Types/Unsupported".into(),
            },
        },
    );
    let outer = DataType::new_array("outer".into(), "ARRAY".into(), "unsupported".into(), 0);
    assert!(
        decoder
            .decode(&types, &[1], &outer)
            .unwrap_err()
            .contains("UNION")
    );
    let invalid = DataType::new_array("outer".into(), "ARRAY".into(), "/Other/Missing".into(), 0);
    types.insert("invalid".into(), invalid);
    let outer = DataType::new_array("outer".into(), "ARRAY".into(), "invalid".into(), 0);
    assert!(
        decoder
            .decode(&types, &[1], &outer)
            .unwrap_err()
            .contains("unknown array element type")
    );

    // Payload truncation keeps the previous stop-at-last-complete-element rule.
    let supported = DataType::new_array(
        "supported".into(),
        "ARRAY".into(),
        format!("{PREFIX}U16"),
        0,
    );
    assert_eq!(
        decoder.decode(&types, &[0, 1, 0xAA], &supported).unwrap(),
        (2, Value::Array(vec![Value::U16(1)]))
    );
    assert_eq!(
        decoder.decode(&types, &[0xAA], &supported).unwrap(),
        (0, Value::Array(vec![]))
    );
}

#[test]
fn missing_qualified_paths_never_decode_a_same_named_type() {
    let mut types = parse(XML).unwrap().merged_data_types();
    types.insert(
        "uint32".into(),
        DataType::new_type_reference("uint32".into(), "VALUE".into(), "uint8".into()),
    );
    let decoder = Decoder::new();
    for reference in [
        "/Other/Pair",
        "/Other/U8",
        "/Other/Word",
        "/Other/uint32",
        "/DataTypes/Left/word",
    ] {
        assert!(
            decoder.resolve_ref(&types, reference).is_none(),
            "{reference}"
        );
        let alias =
            DataType::new_type_reference("alias".into(), "TYPE_REFERENCE".into(), reference.into());
        types.insert("alias".into(), alias.clone());
        assert!(
            decoder.resolve_ref(&types, "alias").is_none(),
            "{reference}"
        );
        assert!(
            decoder.decode(&types, &[0; 8], &alias).is_err(),
            "{reference}"
        );
        let array = DataType::new_array("array".into(), "ARRAY".into(), reference.into(), 1);
        assert!(
            decoder.decode(&types, &[0; 8], &array).is_err(),
            "{reference}"
        );
    }
    for reference in ["Pair", "/DataTypes/ImplementationDataTypes/Pair"] {
        let dt = decoder.resolve_ref(&types, reference).unwrap();
        assert_eq!(
            decoder.decode(&types, &[1, 0, 2], &dt).unwrap(),
            (3, pair(1, 2))
        );
    }
    assert!(decoder.resolve_ref(&types, "Word").is_none());
    assert!(decoder.resolve_ref(&types, "/BaseTypes/uint16").is_some());
    assert!(
        decoder.resolve_ref(&types, "/BaseTypes/uint32").is_none(),
        "unregistered builtin path"
    );
}

#[test]
fn base_declarations_determine_binary_layout() {
    for (name, owner) in [
        ("SwBaseType", "ImplValue"),
        ("uint32", "ImplValue"),
        ("SwBaseType", "int32"),
    ] {
        let xml = ARBITRARY_BASE_XML
            .replace("SwBaseType", name)
            .replace("ImplValue", owner);
        let types = parse(&xml).unwrap().merged_data_types();
        let path = format!("/package/{owner}");
        let dt = &types[&path];
        // The source identity is retained even when its name suggests unsigned.
        let DataTypeKind::TypeReference(reference) = &dt.kind else {
            panic!("expected VALUE")
        };
        assert_eq!(reference.type_name, format!("/package/{name}"));
        assert_eq!(
            Decoder::new()
                .decode(&types, &[0xFF, 0xFF, 0xFF, 0xFE, 0xAA], dt)
                .unwrap(),
            (4, Value::I32(-2)),
        );
        let resolved = Decoder::new().resolve_ref(&types, &path).unwrap();
        let DataTypeKind::TypeReference(reference) = resolved.kind else {
            panic!("expected primitive")
        };
        assert_eq!(reference.type_name, "int32");
        assert!(Decoder::new().decode(&types, &[0xFF; 3], dt).is_err());
    }
}

#[test]
fn unsupported_base_declarations_do_not_fall_back_to_names() {
    let xml = ARBITRARY_BASE_XML.replace("SwBaseType", "uint32");
    for (from, to, error) in [
        (
            "<BASE-TYPE-SIZE>32",
            "<BASE-TYPE-SIZE>24",
            "unsupported base type",
        ),
        (
            "<BASE-TYPE-ENCODING>2C",
            "<BASE-TYPE-ENCODING>1C",
            "unsupported base type",
        ),
        (
            "<BASE-TYPE-SIZE>32",
            "<BASE-TYPE-SIZE>invalid",
            "invalid BASE-TYPE-SIZE",
        ),
        (
            "<BASE-TYPE-ENCODING>2C</BASE-TYPE-ENCODING>",
            "",
            "BASE-TYPE-ENCODING",
        ),
    ] {
        let error_message = parse(&xml.replace(from, to)).unwrap_err();
        assert!(error_message.contains(error), "{error_message}");
    }
}

#[test]
fn missing_base_definition_keeps_legacy_name_resolution() {
    let start = ARBITRARY_BASE_XML.find("<SW-BASE-TYPE>").unwrap();
    let end = ARBITRARY_BASE_XML.find("</SW-BASE-TYPE>").unwrap() + "</SW-BASE-TYPE>".len();
    let mut xml = ARBITRARY_BASE_XML.to_string();
    xml.replace_range(start..end, "");
    xml = xml.replace("/package/SwBaseType", "/BaseTypes/int32");
    let types = parse(&xml).unwrap().merged_data_types();
    assert_eq!(
        Decoder::new()
            .decode(
                &types,
                &[0xFF, 0xFF, 0xFF, 0xFE],
                &types["/package/ImplValue"]
            )
            .unwrap(),
        (4, Value::I32(-2)),
    );
}

fn parse(xml: &str) -> Result<DataTypesParser, String> {
    let doc = roxmltree::Document::parse(xml).unwrap();
    let mut parser = DataTypesParser::new(HashMap::new());
    parser.parse_data_types(doc.root_element())?;
    Ok(parser)
}

fn pair(a: u8, b: u16) -> Value {
    Value::Struct(vec![
        ("a".into(), Value::U8(a)),
        ("b".into(), Value::U16(b)),
    ])
}

#[test]
fn fixed_implementation_input_output_cases() {
    let parser = parse(XML).unwrap();
    let types = parser.merged_data_types();
    let decoder = Decoder::new();
    let cases: &[(&str, &[u8], usize, Value)] = &[
        ("AliasWord", &[0, 2, 0xAA], 2, Value::U16(2)),
        ("Pair", &[1, 0, 2, 0xAA], 3, pair(1, 2)),
        ("PairAlias", &[1, 0, 2, 0xAA], 3, pair(1, 2)),
        (
            "PairArray",
            &[1, 0, 2, 3, 0, 4, 0xAA],
            6,
            Value::Array(vec![pair(1, 2), pair(3, 4)]),
        ),
        ("Empty", &[0xAA], 0, Value::Array(vec![])),
        (
            "Envelope",
            &[0, 9, 1, 2, 3, 0x7F, 0xAA],
            6,
            Value::Struct(vec![
                (
                    "payload".into(),
                    Value::Struct(vec![("x".into(), Value::U16(9))]),
                ),
                (
                    "samples".into(),
                    Value::Array(vec![Value::U8(1), Value::U8(2), Value::U8(3)]),
                ),
                ("tail".into(), Value::U8(0x7F)),
            ]),
        ),
    ];
    let sentinel = DataType::new_type_reference("next".into(), "VALUE".into(), "uint8".into());
    for (name, bytes, size, expected) in cases {
        let path = format!("{PREFIX}{name}");
        let dt = &types[&path];
        let (consumed, actual) = decoder.decode(&types, bytes, dt).unwrap();
        println!("TYPE {path}\nBYTES {bytes:02X?}\nVALUE {actual:#?}");
        assert_eq!(consumed, *size, "{name}");
        assert_eq!(&actual, expected, "{name}");
        let compiled = decoder.compile(&types, dt);
        assert_eq!(
            compiled.decode(bytes).unwrap(),
            (*size, expected.clone()),
            "compiled {name}"
        );
        assert_eq!(
            decoder
                .decode(&types, &bytes[consumed..], &sentinel)
                .unwrap(),
            (1, Value::U8(0xAA)),
            "{name}: next field"
        );
    }
    assert!(
        decoder
            .decode(&types, &[1, 0], &types[&format!("{PREFIX}Pair")])
            .is_err()
    );
    assert!(
        decoder
            .decode(&types, &[1, 0, 2], &types[&format!("{PREFIX}PairArray")])
            .is_err()
    );
}

#[test]
fn dynamic_bounds_are_retained_without_guessing_wire_lengths() {
    let types = parse(XML).unwrap().merged_data_types();
    let path = format!("{PREFIX}DynamicEnvelope/items");
    let DataTypeKind::Array(array) = &types[&path].kind else {
        panic!("expected inline array")
    };
    let bounds = array.bounds.as_ref().unwrap();
    assert!(bounds.is_dynamic);
    assert_eq!(array.size, 0);
    assert_eq!(bounds.max_size, 5);
    assert_eq!(
        bounds.size_handling.as_deref(),
        Some("ALL-INDICES-SAME-ARRAY-SIZE")
    );
    let error = Decoder::new()
        .decode(
            &types,
            &[0, 0, 0x7F],
            &types[&format!("{PREFIX}DynamicEnvelope")],
        )
        .unwrap_err();
    assert!(error.contains("serialization length policy"), "{error}");
}

#[test]
fn scoped_names_and_application_precedence_are_preserved() {
    let mut parser = parse(XML).unwrap();
    parser.application_data_types.insert(
        "pair".into(),
        DataType::new_type_reference("ApplicationPair".into(), "VALUE".into(), "uint8".into()),
    );
    let types = parser.merged_data_types();
    let decoder = Decoder::new();
    for (path, bytes, expected) in [
        ("/DataTypes/Left/Word", vec![7], Value::U8(7)),
        ("/DataTypes/Right/Word", vec![0, 8], Value::U16(8)),
    ] {
        let dt = decoder.resolve_ref(&types, path).unwrap();
        assert_eq!(decoder.decode(&types, &bytes, &dt).unwrap().1, expected);
    }
    assert!(
        decoder.resolve_ref(&types, "Word").is_none(),
        "ambiguous short name"
    );
    assert_eq!(
        decoder.decode(&types, &[9], &types["pair"]).unwrap().1,
        Value::U8(9)
    );
    assert_eq!(
        decoder
            .decode(&types, &[1, 0, 2], &types[&format!("{PREFIX}Pair")])
            .unwrap()
            .1,
        pair(1, 2)
    );
    assert!(
        !types.contains_key("samples"),
        "inline definitions must not become global short aliases"
    );
    let doc = roxmltree::Document::parse(XML).unwrap();
    let mut mapped = DataTypesParser::new(HashMap::from([("Pair".into(), "U8".into())]));
    mapped.parse_data_types(doc.root_element()).unwrap();
    let types = mapped.merged_data_types();
    assert_eq!(
        decoder.decode(&types, &[9], &types["pair"]).unwrap().1,
        Value::U8(9),
        "application mapping fallback must precede new implementation short aliases"
    );
    assert_eq!(
        decoder
            .decode(&types, &[1, 0, 2], &types[&format!("{PREFIX}Pair")])
            .unwrap()
            .1,
        pair(1, 2)
    );
}

#[test]
fn invalid_implementation_references_are_rejected() {
    for (xml, expected) in [
        (
            XML.replace(&format!("{PREFIX}U16"), "/Missing"),
            "unknown implementation type reference",
        ),
        (
            XML.replace(&format!("{PREFIX}U16"), &format!("{PREFIX}AliasWord")),
            "cyclic implementation type reference",
        ),
    ] {
        let error = parse(&xml).unwrap_err();
        assert!(error.contains(expected), "{error}");
    }
    let types = HashMap::from([
        (
            "/Cycle/A".into(),
            DataType::new_type_reference("A".into(), "TYPE_REFERENCE".into(), "/Cycle/B".into()),
        ),
        (
            "/Cycle/B".into(),
            DataType::new_type_reference("B".into(), "TYPE_REFERENCE".into(), "/Cycle/A".into()),
        ),
    ]);
    let decoder = Decoder::new();
    assert!(decoder.resolve_ref(&types, "/Cycle/A").is_none());
    assert!(
        decoder
            .decode(&types, &[0], &types["/Cycle/A"])
            .unwrap_err()
            .contains("cyclic type reference")
    );
}

#[test]
fn service_can_reference_an_implementation_composite() {
    let mut xml = include_str!("test_data/minimal_cp.arxml").to_string();
    let replacement = roxmltree::Document::parse(XML).unwrap();
    let package = |doc: &roxmltree::Document<'_>| {
        doc.descendants()
            .find(|node| {
                node.has_tag_name("AR-PACKAGE")
                    && node.children().any(|child| {
                        child.has_tag_name("SHORT-NAME") && child.text() == Some("DataTypes")
                    })
            })
            .unwrap()
            .range()
    };
    let original_range = {
        let original = roxmltree::Document::parse(&xml).unwrap();
        package(&original)
    };
    xml.replace_range(original_range, &XML[package(&replacement)]);
    xml = xml.replace(
        "<TYPE-TREF>/DataTypes/App/SpeedType</TYPE-TREF>",
        &format!("<TYPE-TREF>{PREFIX}PairAlias</TYPE-TREF>"),
    );
    let header = (10u32 << 16) | 100;
    xml = xml.replace(
        "<HEADER-ID>100</HEADER-ID>",
        &format!("<HEADER-ID>{header}</HEADER-ID>"),
    );
    let path = std::env::temp_dir().join(format!(
        "veloflux-implementation-service-{}.arxml",
        std::process::id()
    ));
    std::fs::write(&path, xml).unwrap();
    let result = ArxmlCodec::load(&path);
    std::fs::remove_file(path).unwrap();
    let codec = result.unwrap();
    assert_eq!(codec.resolve(10, 100).unwrap().short_name, "Pair");
    assert!(codec.known_entries().contains(&(10, 100)));
    assert_eq!(codec.decode(10, 100, &[1, 0, 2]).unwrap(), pair(1, 2));
}

#[test]
fn application_limits_are_metadata_without_decoder_enforcement() {
    let codec = ArxmlCodec::load(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/test_data/baq.arxml"
    ))
    .unwrap();
    for (path, expected) in [
        (
            "/DataTypes/ApplicationDataTypes/ADT_ApnArray",
            ApplicationTypeConstraints::Array { max_elements: 5 },
        ),
        (
            "/DataTypes/ApplicationDataTypes/ADT_UUIDCode",
            ApplicationTypeConstraints::String { max_text_size: 128 },
        ),
    ] {
        assert_eq!(
            codec.application_type_constraints(path),
            Some(&expected),
            "{path}"
        );
    }
    assert!(
        codec
            .application_type_constraints("/unknown/ADT_UUIDCode")
            .is_none()
    );
    let array = codec
        .resolve_ref("/DataTypes/ApplicationDataTypes/ADT_ApnArray")
        .unwrap();
    let DataTypeKind::Array(array) = &array.kind else {
        panic!("expected array")
    };
    assert_eq!(array.size, 0);
    assert!(
        array.bounds.is_none(),
        "application metadata must not activate implementation-array length policy"
    );
    let string = codec
        .resolve_ref("/DataTypes/ApplicationDataTypes/ADT_UUIDCode")
        .unwrap();
    let DataTypeKind::TypeReference(reference) = &string.kind else {
        panic!("expected string")
    };
    assert_eq!(reference.string_size, Some(0));
    let payload = vec![b'a'; 129];
    let (consumed, decoded) = Decoder::new()
        .decode(&HashMap::new(), &payload, &string)
        .unwrap();
    assert_eq!(consumed, 129);
    assert_eq!(
        decoded,
        Value::Str("a".repeat(129)),
        "retaining a maximum must not introduce enforcement"
    );
    let ap = ArxmlCodec::load(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/test_data/s1_ap_test.xml"
    ))
    .unwrap();
    assert!(
        ap.application_type_constraints("/dataTypes/WiFiApName")
            .is_none()
    );
}
