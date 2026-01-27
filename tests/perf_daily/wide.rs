pub fn column_names(cols: usize) -> Vec<String> {
    (1..=cols).map(|i| format!("a{i}")).collect()
}

pub fn select_sql(column_names: &[String], stream_name: &str) -> String {
    format!("select {} from {}", column_names.join(","), stream_name)
}
