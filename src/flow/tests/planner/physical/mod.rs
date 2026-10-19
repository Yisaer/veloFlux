mod column_filter_explain_test;
mod plan_explain_table_driven;
mod sink_compress_tests;
mod sink_encrypt_tests;

fn assert_region_suffix(execution: &serde_json::Value, suffix: &[&str]) {
    let stages: std::collections::BTreeMap<_, _> = execution["stages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|stage| {
            (
                stage["index"].as_i64().unwrap(),
                stage["name"].as_str().unwrap().rsplit_once('_').unwrap().0,
            )
        })
        .collect();
    assert!(
        execution["regions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|region| {
                let names: Vec<_> = region["stages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|id| stages[&id.as_i64().unwrap()])
                    .collect();
                names.ends_with(suffix)
            }),
        "missing region suffix {suffix:?}: {execution}"
    );
    for stage in execution["stages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|stage| stage["class"] == "sink" || stage["class"] == "source")
    {
        assert!(execution["regions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|region| region["stages"] == serde_json::json!([stage["index"]])));
    }
}
