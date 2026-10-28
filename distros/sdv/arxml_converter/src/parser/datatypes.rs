//! CP DataTypes parser — extracts IMPLEMENTATION-DATA-TYPE and
//! APPLICATION-*-DATA-TYPE definitions from ARXML.
//!
//! Corresponds to the Go `cp/parser/datatypes/` package.

use std::collections::{HashMap, HashSet};

use roxmltree::Node;

use crate::ast::resolver::{legacy_builtin_name, resolve_basic_type};
use crate::ast::types::{
    ApplicationTypeConstraints, ArrayBounds, DataType, DataTypeKind, StructureField, TypeReference,
};
use crate::util::xml::named_type_path;
use crate::util::{convert, xml};

/// Parses the two-tier AUTOSAR data-type system:
///
/// # Fields
///
/// - `data_type_mappings`: application reference → implementation reference
///   map populated by the `DataTypeMappingSet` parser.
///
/// 1. **Implementation data types** — map to concrete platform types
///    (e.g. `uint8`, `float`).
/// 2. **Application data types** — semantic types that reference
///    implementation types via a `DataTypeMappingSet`.
#[derive(Debug)]
pub struct DataTypesParser {
    /// Parsed application data types keyed by short-name.
    pub application_data_types: HashMap<String, DataType>,
    application_paths: HashMap<String, DataType>,
    ambiguous_application_names: HashSet<String>,
    /// Optional source limits keyed by full application-type path. Kept
    /// separately from the lowered types so decoding behavior is unchanged.
    pub application_constraints: HashMap<String, ApplicationTypeConstraints>,
    /// Legacy short-name view of top-level implementation types. The merged
    /// runtime map also retains scoped paths and inline definitions.
    pub implementation_data_types: HashMap<String, DataType>,
    /// Top-level and inline implementation definitions keyed by full path.
    implementation_paths: HashMap<String, DataType>,
    implementation_root_paths: HashSet<String>,
    /// Full-path base declarations, resolved only when referenced by a VALUE.
    base_types: HashMap<String, Result<DataType, String>>,
    /// application reference → implementation reference map from
    /// `DataTypeMappingSets`.
    data_type_mappings: HashMap<String, String>,
}

impl DataTypesParser {
    /// `data_type_mappings`: application reference → implementation reference,
    /// populated by the `DataTypeMappingSet` parser earlier in the
    /// pipeline.
    pub fn new(data_type_mappings: HashMap<String, String>) -> Self {
        Self {
            application_data_types: HashMap::new(),
            application_paths: HashMap::new(),
            ambiguous_application_names: HashSet::new(),
            application_constraints: HashMap::new(),
            implementation_data_types: HashMap::new(),
            implementation_paths: HashMap::new(),
            implementation_root_paths: HashSet::new(),
            base_types: HashMap::new(),
            data_type_mappings,
        }
    }

    /// Return the map of resolved application data types.
    pub fn get_application_data_types(&self) -> &HashMap<String, DataType> {
        &self.application_data_types
    }

    pub(super) fn resolve_type_reference(&self, reference: &str) -> Result<&DataType, String> {
        let mut dt = self
            .implementation_paths
            .get(reference)
            .or_else(|| self.application_paths.get(reference))
            .or_else(|| {
                (!reference.contains('/'))
                    .then(|| self.legacy_type_reference(reference))
                    .flatten()
            })
            .ok_or_else(|| format!("no data type for {reference}"))?;
        // The implementation graph was validated before application types were
        // built. Return the terminal type so service planning sees its layout.
        while let DataTypeKind::TypeReference(alias) = &dt.kind {
            if dt.category != "TYPE_REFERENCE" {
                break;
            }
            let Some(target) = self.implementation_paths.get(&alias.type_name) else {
                break;
            };
            dt = target;
        }
        if let DataTypeKind::Unsupported { reason } = &dt.kind {
            return Err(reason.clone());
        }
        Ok(dt)
    }

    fn legacy_type_reference(&self, reference: &str) -> Option<&DataType> {
        let name = convert::extract_last(reference);
        if self
            .ambiguous_application_names
            .contains(&name.to_lowercase())
        {
            return None;
        }
        if let Some(dt) = self.application_data_types.get(&name.to_lowercase()) {
            return Some(dt);
        }
        let mut candidates = self
            .implementation_root_paths
            .iter()
            .filter(|path| convert::extract_last(path) == name);
        let path = candidates.next()?;
        if candidates.next().is_some() {
            return None;
        }
        self.implementation_paths.get(path)
    }

    // ---- public entry point ----

    /// Parse both implementation and application data types from the
    /// `DataTypes` AR-PACKAGE element.
    pub fn parse_data_types(&mut self, root: Node) -> Result<(), String> {
        let ar_packages = xml::require_child(root, "AR-PACKAGES")?;
        // Bases may live outside the selected DataTypes package.
        for base in root
            .document()
            .descendants()
            .filter(|n| n.has_tag_name("SW-BASE-TYPE"))
        {
            let path = named_type_path(base)?;
            let name = xml::get_shortname(base)?;
            let resolved = resolve_base_declaration(base).map(|primitive| {
                let reference = |name: &str| TypeReference {
                    type_name: name.into(),
                    string_size: None,
                };
                // Keep existing public field names when they agree with the
                // declaration. A conflicting or arbitrary name cannot decide layout.
                let runtime_name = if resolve_basic_type(&reference(name))
                    == resolve_basic_type(&reference(primitive))
                {
                    name.to_lowercase()
                } else {
                    primitive.into()
                };
                DataType::new_type_reference(name.into(), "SW-BASE-TYPE".into(), runtime_name)
            });
            if self.base_types.insert(path.clone(), resolved).is_some() {
                return Err(format!("duplicate base type path: {path}"));
            }
        }
        self.parse_implementation_data_types(ar_packages)?;
        self.validate_implementation_references()?;
        self.parse_application_data_types(ar_packages)?;
        Ok(())
    }

    // ---- implementation data types ----

    fn parse_implementation_data_types(&mut self, node: Node) -> Result<(), String> {
        for (i, idt) in node
            .descendants()
            .filter(|n| n.tag_name().name() == "IMPLEMENTATION-DATA-TYPE")
            .enumerate()
        {
            let dt = self
                .parse_implementation_node(idt)
                .map_err(|e| format!("parse {i} IMPLEMENTATION-DATA-TYPE failed: {e}"))?;
            self.implementation_root_paths.insert(named_type_path(idt)?);
            self.implementation_data_types
                .insert(dt.short_name.clone(), dt);
        }
        // Compatibility views never pick one of several scoped definitions.
        let mut counts = HashMap::<&str, usize>::new();
        for path in &self.implementation_root_paths {
            *counts.entry(convert::extract_last(path)).or_default() += 1;
        }
        self.implementation_data_types
            .retain(|name, _| counts.get(name.as_str()) == Some(&1));
        Ok(())
    }

    fn parse_implementation_node(&mut self, root: Node) -> Result<DataType, String> {
        let sn = xml::get_shortname(root)?;
        let category = xml::get_category(root)?;
        let dt = match category {
            "VALUE" | "TYPE_REFERENCE" => {
                let props = xml::get_sw_data_def_props_conditional(root)?;
                let tag = if category == "VALUE" {
                    "BASE-TYPE-REF"
                } else {
                    "IMPLEMENTATION-DATA-TYPE-REF"
                };
                let reference = xml::require_child(props, tag)?;
                let target = reference.text().ok_or_else(|| format!("empty {tag}"))?;
                if category == "VALUE" {
                    if let Some(declaration) = self.base_types.get(target) {
                        declaration.as_ref().map_err(Clone::clone)?;
                    } else {
                        xml::valid_basic_type(target)?;
                        if target.contains('/') {
                            let name = legacy_builtin_name(target)
                                .ok_or_else(|| format!("unknown base type reference: {target}"))?;
                            self.base_types.insert(
                                target.into(),
                                Ok(DataType::new_type_reference(
                                    convert::extract_last(target).into(),
                                    "SW-BASE-TYPE".into(),
                                    name,
                                )),
                            );
                        }
                    }
                }
                DataType::new_type_reference(sn.into(), category.into(), target.into())
            }
            "STRUCTURE" => {
                let mut fields = Vec::new();
                for element in xml::require_child(root, "SUB-ELEMENTS")?
                    .children()
                    .filter(|node| node.has_tag_name("IMPLEMENTATION-DATA-TYPE-ELEMENT"))
                {
                    let member = self.parse_implementation_node(element)?;
                    fields.push(StructureField {
                        name: member.short_name,
                        type_ref: named_type_path(element)?,
                        in_place: matches!(
                            member.kind,
                            DataTypeKind::Array(_) | DataTypeKind::Structure(_)
                        ),
                    });
                }
                DataType::new_structure(sn.into(), category.into(), fields)
            }
            "ARRAY" => {
                let elements = xml::require_child(root, "SUB-ELEMENTS")?;
                let mut children = elements
                    .children()
                    .filter(|node| node.has_tag_name("IMPLEMENTATION-DATA-TYPE-ELEMENT"));
                let element = children
                    .next()
                    .ok_or("implementation array requires one element")?;
                if children.next().is_some() {
                    return Err("implementation array requires exactly one element".into());
                }
                self.parse_implementation_node(element)?;
                let is_dynamic = xml::get_array_size_semantics(element)?;
                let raw = xml::child_text(element, "ARRAY-SIZE").ok_or("missing ARRAY-SIZE")?;
                let max_size =
                    convert::to_u64(raw).map_err(|e| format!("invalid ARRAY-SIZE: {e}"))?;
                let mut dt = DataType::new_array(
                    sn.into(),
                    category.into(),
                    named_type_path(element)?,
                    if is_dynamic { 0 } else { max_size },
                );
                if let DataTypeKind::Array(array) = &mut dt.kind {
                    array.bounds = Some(ArrayBounds {
                        max_size,
                        is_dynamic,
                        size_handling: xml::child_text(element, "ARRAY-SIZE-HANDLING")
                            .map(str::to_string),
                    });
                }
                dt
            }
            other => DataType {
                short_name: sn.into(),
                category: other.into(),
                kind: DataTypeKind::Unsupported {
                    reason: format!(
                        "unsupported implementation category: {other} at {}",
                        named_type_path(root)?
                    ),
                },
            },
        };
        let path = named_type_path(root)?;
        if self
            .implementation_paths
            .insert(path.clone(), dt.clone())
            .is_some()
        {
            return Err(format!("duplicate implementation type path: {path}"));
        }
        Ok(dt)
    }

    fn validate_implementation_references(&self) -> Result<(), String> {
        fn visit<'a>(
            path: &'a str,
            types: &'a HashMap<String, DataType>,
            active: &mut HashSet<&'a str>,
            complete: &mut HashSet<&'a str>,
        ) -> Result<(), String> {
            if complete.contains(path) {
                return Ok(());
            }
            if !active.insert(path) {
                return Err(format!("cyclic implementation type reference: {path}"));
            }
            let dt = types
                .get(path)
                .ok_or_else(|| format!("unknown implementation type reference: {path}"))?;
            let targets = match &dt.kind {
                DataTypeKind::TypeReference(reference) if dt.category == "TYPE_REFERENCE" => {
                    vec![reference.type_name.as_str()]
                }
                DataTypeKind::Structure(structure) => structure
                    .fields
                    .iter()
                    .map(|field| field.type_ref.as_str())
                    .collect(),
                DataTypeKind::Array(array) => vec![array.element_ref.as_str()],
                _ => Vec::new(),
            };
            for target in targets {
                visit(target, types, active, complete)?;
            }
            active.remove(path);
            complete.insert(path);
            Ok(())
        }
        let mut complete = HashSet::new();
        for path in self.implementation_paths.keys() {
            visit(
                path,
                &self.implementation_paths,
                &mut HashSet::new(),
                &mut complete,
            )?;
        }
        Ok(())
    }

    // ---- application data types ----

    fn parse_application_data_types(&mut self, node: Node) -> Result<(), String> {
        let mut names = HashMap::<String, usize>::new();
        for application in node.descendants().filter(|n| {
            matches!(
                n.tag_name().name(),
                "APPLICATION-PRIMITIVE-DATA-TYPE"
                    | "APPLICATION-ARRAY-DATA-TYPE"
                    | "APPLICATION-RECORD-DATA-TYPE"
            )
        }) {
            *names
                .entry(xml::get_shortname(application)?.to_lowercase())
                .or_default() += 1;
        }
        self.ambiguous_application_names = names
            .into_iter()
            .filter_map(|(name, count)| (count > 1).then_some(name))
            .collect();
        for key in self.data_type_mappings.keys() {
            if !key.contains('/')
                && self
                    .ambiguous_application_names
                    .contains(&key.to_lowercase())
            {
                return Err(format!(
                    "ambiguous application mapping key '{key}'; use a full path"
                ));
            }
        }
        // APPLICATION-PRIMITIVE-DATA-TYPE
        for (i, apdt) in node
            .descendants()
            .filter(|n| n.tag_name().name() == "APPLICATION-PRIMITIVE-DATA-TYPE")
            .enumerate()
        {
            self.parse_application_data_type(apdt)
                .map_err(|e| format!("parse {i} APPLICATION-PRIMITIVE-DATA-TYPE: {e}"))?;
        }
        // APPLICATION-ARRAY-DATA-TYPE
        for (i, aadt) in node
            .descendants()
            .filter(|n| n.tag_name().name() == "APPLICATION-ARRAY-DATA-TYPE")
            .enumerate()
        {
            self.parse_application_data_type(aadt)
                .map_err(|e| format!("parse {i} APPLICATION-ARRAY-DATA-TYPE: {e}"))?;
        }
        // APPLICATION-RECORD-DATA-TYPE
        for (i, ardt) in node
            .descendants()
            .filter(|n| n.tag_name().name() == "APPLICATION-RECORD-DATA-TYPE")
            .enumerate()
        {
            self.parse_application_data_type(ardt)
                .map_err(|e| format!("parse {i} APPLICATION-RECORD-DATA-TYPE: {e}"))?;
        }
        self.application_data_types
            .retain(|name, _| !self.ambiguous_application_names.contains(name));
        Ok(())
    }

    fn parse_application_data_type(&mut self, root: Node) -> Result<(), String> {
        let sn = xml::get_shortname(root)?;
        let category = xml::get_category(root)?;

        match category {
            "STRING" => self.parse_application_string(root, sn, category),
            "VALUE" => self.resolve_value_type(root, sn),
            "ARRAY" => self.parse_application_array(root, sn, category),
            "STRUCTURE" => self.parse_application_structure(root, sn, category),
            other => Err(format!("unknown category: {other}")),
        }?;
        let path = named_type_path(root)?;
        let dt = self.application_data_types[&sn.to_lowercase()].clone();
        if self.application_paths.insert(path.clone(), dt).is_some() {
            return Err(format!("duplicate application type path: {path}"));
        }
        Ok(())
    }

    fn parse_application_string(
        &mut self,
        root: Node,
        sn: &str,
        category: &str,
    ) -> Result<(), String> {
        let sddpc = xml::get_sw_data_def_props_conditional(root)?;
        let stp = xml::require_child(sddpc, "SW-TEXT-PROPS")?;

        let is_dynamic = xml::get_array_size_semantics(stp)?;
        if !is_dynamic {
            return Err("fixed-length string not supported yet".into());
        }

        let btr = xml::require_child(stp, "BASE-TYPE-REF")?;
        let btr_raw = btr.text().ok_or("empty BASE-TYPE-REF")?;
        if !btr_raw.to_uppercase().contains("UTF_8") {
            return Err(format!("BASE-TYPE-REF should be UTF_8, got: {btr_raw}"));
        }

        if let Some(max_text_size) = optional_limit(stp, "SW-MAX-TEXT-SIZE")? {
            self.application_constraints.insert(
                named_type_path(root)?,
                ApplicationTypeConstraints::String { max_text_size },
            );
        }

        self.application_data_types.insert(
            sn.to_lowercase(),
            DataType::new_string(sn.to_string(), category.to_string(), 0),
        );
        Ok(())
    }

    fn implementation_reference(&self, reference: &str) -> Result<&DataType, String> {
        if reference.contains('/') {
            return self
                .implementation_paths
                .get(reference)
                .ok_or_else(|| format!("no implementation data type '{reference}'"));
        }
        let mut candidates = self
            .implementation_root_paths
            .iter()
            .filter(|path| convert::extract_last(path) == reference);
        let path = candidates
            .next()
            .ok_or_else(|| format!("no implementation data type '{reference}'"))?;
        if candidates.next().is_some() {
            return Err(format!(
                "ambiguous implementation data type '{reference}'; use a full path"
            ));
        }
        Ok(&self.implementation_paths[path])
    }

    fn resolve_value_type(&mut self, root: Node, sn: &str) -> Result<(), String> {
        let path = named_type_path(root)?;
        let idtr_key = self
            .data_type_mappings
            .get(&path)
            .or_else(|| self.data_type_mappings.get(sn))
            .ok_or_else(|| format!("no DataTypeMapping for application type {path}"))?;

        let mut dt = self.implementation_reference(idtr_key)?.clone();

        dt.short_name = sn.to_string();
        self.application_data_types.insert(sn.to_lowercase(), dt);
        Ok(())
    }

    fn parse_application_array(
        &mut self,
        root: Node,
        sn: &str,
        category: &str,
    ) -> Result<(), String> {
        let element = xml::require_child(root, "ELEMENT")?;
        let type_ref = xml::require_child(element, "TYPE-TREF")?;
        let array_ref = type_ref.text().ok_or("empty TYPE-TREF")?;

        let is_dynamic = xml::get_array_size_semantics(element)?;
        let maximum = optional_limit(element, "MAX-NUMBER-OF-ELEMENTS")?;
        let size = if !is_dynamic {
            maximum.ok_or("missing MAX-NUMBER-OF-ELEMENTS")?
        } else {
            0
        };

        if let Some(max_elements) = maximum {
            self.application_constraints.insert(
                named_type_path(root)?,
                ApplicationTypeConstraints::Array { max_elements },
            );
        }

        self.application_data_types.insert(
            sn.to_lowercase(),
            DataType::new_array(
                sn.to_string(),
                category.to_string(),
                array_ref.to_string(),
                size,
            ),
        );
        Ok(())
    }

    fn parse_application_structure(
        &mut self,
        root: Node,
        sn: &str,
        category: &str,
    ) -> Result<(), String> {
        let elements = xml::require_child(root, "ELEMENTS")?;

        let mut fields = Vec::new();
        for record in elements
            .children()
            .filter(|n| n.tag_name().name() == "APPLICATION-RECORD-ELEMENT")
        {
            let field_name = xml::get_shortname(record)?;
            let type_ref = xml::require_child(record, "TYPE-TREF")?;
            let ref_text = type_ref.text().ok_or("empty TYPE-TREF")?;

            fields.push(crate::ast::types::StructureField {
                name: field_name.to_string(),
                type_ref: ref_text.to_string(),
                in_place: false,
            });
        }

        self.application_data_types.insert(
            sn.to_lowercase(),
            DataType::new_structure(sn.to_string(), category.to_string(), fields),
        );
        Ok(())
    }

    /// Build a runtime map with exact application/implementation paths and short
    /// aliases. Existing application short names keep precedence, including
    /// their DataTypeMappingSet fallback.
    pub fn merged_data_types(&self) -> HashMap<String, DataType> {
        let mut merged = self.application_data_types.clone();
        for (adt_name, idt_name) in &self.data_type_mappings {
            let adt_key = adt_name.to_lowercase();
            if !adt_name.contains('/')
                && !self.ambiguous_application_names.contains(&adt_key)
                && !merged.contains_key(&adt_key)
                && let Ok(idt) = self.implementation_reference(idt_name)
            {
                merged.insert(adt_key, idt.clone());
            }
        }
        merged.extend(self.implementation_paths.clone());
        merged.extend(self.application_paths.clone());
        merged.extend(self.base_types.iter().filter_map(|(path, result)| {
            result.as_ref().ok().map(|dt| (path.clone(), dt.clone()))
        }));
        let mut aliases = HashMap::<String, Vec<&DataType>>::new();
        for path in &self.implementation_root_paths {
            let dt = &self.implementation_paths[path];
            // Inline definitions are addressed only by their scoped paths.
            aliases
                .entry(dt.short_name.to_lowercase())
                .or_default()
                .push(dt);
        }
        for (name, candidates) in aliases {
            if self.ambiguous_application_names.contains(&name) {
                continue;
            }
            if let [dt] = candidates.as_slice() {
                merged.entry(name).or_insert_with(|| (*dt).clone());
            }
        }
        merged
    }
}

fn resolve_base_declaration(base: Node) -> Result<&'static str, String> {
    let size = xml::require_child_text(base, "BASE-TYPE-SIZE")?;
    let bits = convert::to_u64(size).map_err(|e| format!("invalid BASE-TYPE-SIZE: {e}"))?;
    let encoding = xml::require_child_text(base, "BASE-TYPE-ENCODING")?;
    match (bits, encoding) {
        (8, "NONE") => Ok("uint8"),
        (16, "NONE") => Ok("uint16"),
        (32, "NONE") => Ok("uint32"),
        (64, "NONE") => Ok("uint64"),
        (8, "2C") => Ok("int8"),
        (16, "2C") => Ok("int16"),
        (32, "2C") => Ok("int32"),
        (64, "2C") => Ok("int64"),
        (32, "IEEE754") => Ok("float"),
        (64, "IEEE754") => Ok("double"),
        (8, "BOOLEAN") => Ok("boolean"),
        (8, "UTF-8") => Ok("string"),
        _ => Err(format!(
            "unsupported base type {}: {bits} bits, encoding {encoding}",
            named_type_path(base)?
        )),
    }
}

fn optional_limit(node: Node, tag: &str) -> Result<Option<u64>, String> {
    xml::find_child(node, tag)
        .map(|element| {
            let raw = element.text().ok_or_else(|| format!("empty {tag}"))?;
            convert::to_u64(raw).map_err(|error| format!("invalid {tag}: {error}"))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use roxmltree::Document;

    /// Minimal ARXML with one IMPLEMENTATION-DATA-TYPE (VALUE) and
    /// one APPLICATION-PRIMITIVE-DATA-TYPE (VALUE) wired together by
    /// a DataTypeMapping.
    const MINI_ARXML: &str = r#"
<AUTOSAR>
  <AR-PACKAGES>
    <AR-PACKAGE>
      <SHORT-NAME>DataTypes</SHORT-NAME>
      <AR-PACKAGES>
        <AR-PACKAGE>
          <SHORT-NAME>ImplementationDataTypes</SHORT-NAME>
          <ELEMENTS>
            <IMPLEMENTATION-DATA-TYPE>
              <SHORT-NAME>uint32_impl</SHORT-NAME>
              <CATEGORY>VALUE</CATEGORY>
              <SW-DATA-DEF-PROPS>
                <SW-DATA-DEF-PROPS-VARIANTS>
                  <SW-DATA-DEF-PROPS-CONDITIONAL>
                    <BASE-TYPE-REF>/BaseTypes/uint32</BASE-TYPE-REF>
                  </SW-DATA-DEF-PROPS-CONDITIONAL>
                </SW-DATA-DEF-PROPS-VARIANTS>
              </SW-DATA-DEF-PROPS>
            </IMPLEMENTATION-DATA-TYPE>
          </ELEMENTS>
        </AR-PACKAGE>
        <AR-PACKAGE>
          <SHORT-NAME>ApplicationDataTypes</SHORT-NAME>
          <ELEMENTS>
            <APPLICATION-PRIMITIVE-DATA-TYPE>
              <SHORT-NAME>MySpeed</SHORT-NAME>
              <CATEGORY>VALUE</CATEGORY>
            </APPLICATION-PRIMITIVE-DATA-TYPE>
          </ELEMENTS>
        </AR-PACKAGE>
      </AR-PACKAGES>
    </AR-PACKAGE>
  </AR-PACKAGES>
</AUTOSAR>
"#;

    fn test_mappings() -> HashMap<String, String> {
        [("MySpeed".into(), "uint32_impl".into())]
            .into_iter()
            .collect()
    }

    #[test]
    fn parse_value_type_through_mapping() {
        let doc = Document::parse(MINI_ARXML).unwrap();
        let mut parser = DataTypesParser::new(test_mappings());
        parser.parse_data_types(doc.root_element()).unwrap();

        let dt = parser.application_data_types.get("myspeed").unwrap();
        assert_eq!(dt.short_name, "MySpeed");
        assert_eq!(dt.category, "VALUE");
    }

    #[test]
    fn missing_mapping_is_error() {
        let doc = Document::parse(MINI_ARXML).unwrap();
        let mut parser = DataTypesParser::new(HashMap::new());
        let err = parser.parse_data_types(doc.root_element()).unwrap_err();
        assert!(err.contains("DataTypeMapping"));
    }

    #[test]
    fn qualified_type_references_require_exact_identity() {
        let doc = Document::parse(MINI_ARXML).unwrap();
        let mut parser = DataTypesParser::new(test_mappings());
        parser.parse_data_types(doc.root_element()).unwrap();
        for reference in [
            "/DataTypes/ApplicationDataTypes/MySpeed",
            "/DataTypes/ImplementationDataTypes/uint32_impl",
            "MySpeed",
            "uint32_impl",
        ] {
            assert!(
                parser.resolve_type_reference(reference).is_ok(),
                "{reference}"
            );
        }
        for reference in [
            "/Other/MySpeed",
            "/Other/uint32_impl",
            "/Other/uint32",
            "/datatypes/ApplicationDataTypes/MySpeed",
        ] {
            assert!(
                parser.resolve_type_reference(reference).is_err(),
                "{reference}"
            );
        }
        // An undeclared primitive-looking path is not a legacy namespace.
        let xml = MINI_ARXML.replace("/BaseTypes/uint32", "/Other/uint32");
        let doc = Document::parse(&xml).unwrap();
        let mut parser = DataTypesParser::new(test_mappings());
        assert!(
            parser
                .parse_data_types(doc.root_element())
                .unwrap_err()
                .contains("unknown base type reference")
        );
    }
}
