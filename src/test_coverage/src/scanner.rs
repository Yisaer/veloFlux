use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use proc_macro2::Span;
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit::Visit;
use syn::{
    Expr, ExprArray, ExprLit, ExprMacro, ExprReference, ExprStruct, Item, ItemFn, ItemMod,
    ItemStruct, Lit, Member, Stmt, Token,
};
use walkdir::WalkDir;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoverageKind {
    Function,
    TestCase,
}

#[derive(Debug, Clone)]
pub struct SourceLocation {
    pub path: PathBuf,
    pub line: usize,
}

#[derive(Debug, Clone)]
pub struct CoverageRecord {
    pub id: String,
    pub kind: CoverageKind,
    pub features: Vec<String>,
    pub location: SourceLocation,
}

#[derive(Debug, Clone)]
pub struct ScanError {
    pub location: SourceLocation,
    pub message: String,
}

#[derive(Debug, Default)]
pub struct ScanResult {
    pub records: Vec<CoverageRecord>,
    pub errors: Vec<ScanError>,
}

pub fn scan_repository(repo_root: &Path) -> Result<ScanResult, String> {
    let test_files = collect_test_files(repo_root)?;
    let mut result = ScanResult::default();

    for file in test_files {
        let relative_path = file.strip_prefix(repo_root).unwrap_or(&file).to_path_buf();
        let content = fs::read_to_string(&file)
            .map_err(|err| format!("failed to read {}: {err}", file.display()))?;

        let mut file_result = scan_function_comments(&relative_path, &content);
        result.records.append(&mut file_result.records);
        result.errors.append(&mut file_result.errors);

        let parsed = syn::parse_file(&content)
            .map_err(|err| format!("failed to parse Rust file {}: {err}", file.display()))?;
        let mut visitor = TestCaseVisitor::new(relative_path);
        visitor.visit_file(&parsed);
        result.records.extend(visitor.records);
        result.errors.extend(visitor.errors);
    }

    Ok(result)
}

fn collect_test_files(repo_root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut files = BTreeSet::new();

    let src_root = repo_root.join("src");
    if src_root.exists() {
        for entry in WalkDir::new(&src_root).into_iter() {
            let entry = entry.map_err(|err| format!("failed to walk src/: {err}"))?;
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.into_path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                continue;
            }
            files.insert(path);
        }
    }

    let tests_root = repo_root.join("tests");
    if tests_root.exists() {
        for entry in WalkDir::new(&tests_root).into_iter() {
            let entry = entry.map_err(|err| format!("failed to walk tests/: {err}"))?;
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.into_path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
                files.insert(path);
            }
        }
    }

    Ok(files.into_iter().collect())
}

fn scan_function_comments(path: &Path, content: &str) -> ScanResult {
    let mut result = ScanResult::default();
    let lines: Vec<&str> = content.lines().collect();

    for (index, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if !trimmed.starts_with("// coverage-covers:") {
            continue;
        }

        let location = SourceLocation {
            path: path.to_path_buf(),
            line: index + 1,
        };

        let Some((_, feature_list)) = trimmed.split_once(':') else {
            result.errors.push(ScanError {
                location,
                message: "malformed `coverage-covers` comment".to_string(),
            });
            continue;
        };

        let features = parse_feature_list(feature_list.trim());
        if features.is_empty() {
            result.errors.push(ScanError {
                location,
                message: "malformed `coverage-covers` comment: missing feature ids".to_string(),
            });
            continue;
        }

        match find_next_function_name(&lines, index + 1) {
            Some(function_name) => result.records.push(CoverageRecord {
                id: format!("{}::{}", path.display(), function_name),
                kind: CoverageKind::Function,
                features,
                location,
            }),
            None => result.errors.push(ScanError {
                location,
                message: "malformed `coverage-covers` comment: no following test function"
                    .to_string(),
            }),
        }
    }

    result
}

fn parse_feature_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|feature| !feature.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn find_next_function_name(lines: &[&str], start_index: usize) -> Option<String> {
    let mut saw_attribute = false;

    for line in lines.iter().skip(start_index) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with("#[") {
            saw_attribute = true;
            continue;
        }
        if !saw_attribute {
            return None;
        }
        if let Some(name) = extract_function_name(trimmed) {
            return Some(name);
        }
        return None;
    }

    None
}

fn extract_function_name(line: &str) -> Option<String> {
    let fn_index = line.find("fn ")?;
    let after_fn = &line[fn_index + 3..];
    let identifier: String = after_fn
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .collect();
    if identifier.is_empty() {
        None
    } else {
        Some(identifier)
    }
}

struct TestCaseVisitor {
    path: PathBuf,
    tracked_types: BTreeSet<(String, String)>,
    module_path: Vec<String>,
    current_function: Option<String>,
    records: Vec<CoverageRecord>,
    errors: Vec<ScanError>,
    seen_case_names: BTreeMap<(String, String), SourceLocation>,
}

impl TestCaseVisitor {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            tracked_types: BTreeSet::new(),
            module_path: Vec::new(),
            current_function: None,
            records: Vec::new(),
            errors: Vec::new(),
            seen_case_names: BTreeMap::new(),
        }
    }

    fn current_scope_id(&self) -> String {
        let mut parts = vec![self.path.display().to_string()];
        parts.extend(self.module_path.iter().cloned());
        if let Some(function_name) = &self.current_function {
            parts.push(function_name.clone());
        }
        parts.join("::")
    }

    fn current_module_scope_id(&self) -> String {
        let mut parts = vec![self.path.display().to_string()];
        parts.extend(self.module_path.iter().cloned());
        parts.join("::")
    }

    fn track_type(&mut self, struct_name: impl Into<String>) {
        self.tracked_types
            .insert((self.current_scope_id(), struct_name.into()));
    }

    fn is_tracked_type(&self, struct_name: &str) -> bool {
        let current_scope = self.current_scope_id();
        let module_scope = self.current_module_scope_id();
        let struct_name = struct_name.to_string();
        self.tracked_types
            .contains(&(current_scope, struct_name.clone()))
            || self.tracked_types.contains(&(module_scope, struct_name))
    }

    fn push_case_record(
        &mut self,
        struct_name: &str,
        expr: &ExprStruct,
        case_name: Option<String>,
        features: Vec<String>,
    ) {
        let line = span_line(expr.span());
        let location = SourceLocation {
            path: self.path.clone(),
            line,
        };

        let case_suffix = case_name
            .clone()
            .unwrap_or_else(|| format!("<anonymous:{}>", struct_name));
        let scope_id = format!("{}::{}", self.current_scope_id(), case_suffix);

        if let Some(case_name) = case_name {
            let key = (self.current_scope_id(), case_name.clone());
            if let Some(previous) = self.seen_case_names.get(&key) {
                self.errors.push(ScanError {
                    location: location.clone(),
                    message: format!(
                        "duplicate testcase name `{}` in the same test group; first seen at {}:{}",
                        case_name,
                        previous.path.display(),
                        previous.line
                    ),
                });
            } else {
                self.seen_case_names.insert(key, location.clone());
            }
        }

        self.records.push(CoverageRecord {
            id: scope_id,
            kind: CoverageKind::TestCase,
            features,
            location,
        });
    }

    fn record_expr_struct_if_tracked(&mut self, expr: &ExprStruct) -> bool {
        let Some(struct_name) = expr
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
        else {
            return false;
        };

        if !self.is_tracked_type(&struct_name) {
            return false;
        }

        let mut case_name = None;
        let mut covers = None;

        for field in &expr.fields {
            let Member::Named(ident) = &field.member else {
                continue;
            };
            match ident.to_string().as_str() {
                "name" => {
                    if let Some(value) = string_literal_from_expr(&field.expr) {
                        case_name = Some(value);
                    }
                }
                "covers" => match covers_from_expr(&field.expr) {
                    Ok(values) => covers = Some(values),
                    Err(message) => self.errors.push(ScanError {
                        location: SourceLocation {
                            path: self.path.clone(),
                            line: span_line(field.expr.span()),
                        },
                        message,
                    }),
                },
                _ => {}
            }
        }

        if let Some(features) = covers {
            self.push_case_record(&struct_name, expr, case_name, features);
        } else {
            self.errors.push(ScanError {
                location: SourceLocation {
                    path: self.path.clone(),
                    line: span_line(expr.span()),
                },
                message: format!(
                    "tracked testcase `{}` is missing required `covers` field",
                    struct_name
                ),
            });
        }

        true
    }

    fn visit_macro_expression_list(&mut self, expr_macro: &ExprMacro) {
        let parser = Punctuated::<Expr, Token![,]>::parse_terminated;
        let Ok(expressions) = parser.parse2(expr_macro.mac.tokens.clone()) else {
            return;
        };

        for expression in expressions {
            self.visit_expr(&expression);
        }
    }
}

impl<'ast> Visit<'ast> for TestCaseVisitor {
    fn visit_stmt(&mut self, stmt: &'ast Stmt) {
        if let Stmt::Item(Item::Struct(item_struct)) = stmt {
            let has_covers_field = item_struct.fields.iter().any(|field| {
                field
                    .ident
                    .as_ref()
                    .map(|ident| ident == "covers")
                    .unwrap_or(false)
            });
            if has_covers_field {
                self.track_type(item_struct.ident.to_string());
            }
        }
        syn::visit::visit_stmt(self, stmt);
    }

    fn visit_item_struct(&mut self, item_struct: &'ast ItemStruct) {
        let has_covers_field = item_struct.fields.iter().any(|field| {
            field
                .ident
                .as_ref()
                .map(|ident| ident == "covers")
                .unwrap_or(false)
        });
        if has_covers_field {
            self.track_type(item_struct.ident.to_string());
        }
        syn::visit::visit_item_struct(self, item_struct);
    }

    fn visit_item_mod(&mut self, item_mod: &'ast ItemMod) {
        if let Some((_, items)) = &item_mod.content {
            self.module_path.push(item_mod.ident.to_string());
            for item in items {
                self.visit_item(item);
            }
            self.module_path.pop();
        }
    }

    fn visit_item_fn(&mut self, item_fn: &'ast ItemFn) {
        let previous_function = self.current_function.replace(item_fn.sig.ident.to_string());
        syn::visit::visit_item_fn(self, item_fn);
        self.current_function = previous_function;
    }

    fn visit_expr_struct(&mut self, expr: &'ast ExprStruct) {
        self.record_expr_struct_if_tracked(expr);
        syn::visit::visit_expr_struct(self, expr);
    }

    fn visit_expr_macro(&mut self, expr_macro: &'ast ExprMacro) {
        self.visit_macro_expression_list(expr_macro);
        syn::visit::visit_expr_macro(self, expr_macro);
    }
}

fn string_literal_from_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(value),
            ..
        }) => Some(value.value()),
        _ => None,
    }
}

fn covers_from_expr(expr: &Expr) -> Result<Vec<String>, String> {
    match expr {
        Expr::Reference(ExprReference { expr, .. }) => covers_from_expr(expr),
        Expr::Array(ExprArray { elems, .. }) => elems
            .iter()
            .map(|elem| match elem {
                Expr::Lit(ExprLit {
                    lit: Lit::Str(value),
                    ..
                }) => Ok(value.value()),
                _ => Err("`covers` must use string literals only".to_string()),
            })
            .collect(),
        _ => Err("`covers` must use the form `&[\"feature.id\"]`".to_string()),
    }
}

fn span_line(span: Span) -> usize {
    span.start().line
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_feature_list_with_multiple_entries() {
        let features = parse_feature_list("a.b.c, d.e.f");
        assert_eq!(features, vec!["a.b.c".to_string(), "d.e.f".to_string()]);
    }

    #[test]
    fn extracts_function_name_from_async_signature() {
        let function_name = extract_function_name("async fn sample_test() {");
        assert_eq!(function_name.as_deref(), Some("sample_test"));
    }

    #[test]
    fn parses_string_literal_covers() {
        let expr: Expr = syn::parse_str("&[\"planner.logical.rule\"]").expect("parse expr");
        let covers = covers_from_expr(&expr).expect("covers");
        assert_eq!(covers, vec!["planner.logical.rule".to_string()]);
    }

    #[test]
    fn rejects_non_literal_covers() {
        let expr: Expr = syn::parse_str("&[FEATURE_ID]").expect("parse expr");
        let err = covers_from_expr(&expr).expect_err("should reject");
        assert_eq!(err, "`covers` must use string literals only");
    }

    #[test]
    fn scans_testcases_inside_vec_macro() {
        let content = r#"
            #[test]
            fn table_driven_cases() {
                struct Case {
                    name: &'static str,
                    covers: &'static [&'static str],
                }

                let cases = vec![
                    Case {
                        name: "macro_case",
                        covers: &["planner.logical.rule"],
                    },
                ];

                assert_eq!(cases.len(), 1);
            }

            #[test]
            fn unrelated_same_name_case_is_not_tracked() {
                struct Case {
                    name: &'static str,
                }

                let cases = vec![
                    Case {
                        name: "untracked_macro_case",
                    },
                ];

                assert_eq!(cases.len(), 1);
            }
        "#;
        let parsed = syn::parse_file(content).expect("parse test content");
        let mut visitor = TestCaseVisitor::new(PathBuf::from("tests/file.rs"));

        visitor.visit_file(&parsed);

        assert!(visitor.errors.is_empty());
        assert_eq!(visitor.records.len(), 1);
        assert_eq!(
            visitor.records[0].features,
            vec!["planner.logical.rule".to_string()]
        );
        assert!(visitor.records[0].id.ends_with("macro_case"));
    }
}
