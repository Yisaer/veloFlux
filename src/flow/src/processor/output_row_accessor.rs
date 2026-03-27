use crate::model::{Message, Tuple};
use crate::planner::physical::output_schema::{OutputSchema, OutputValueGetter};
use crate::processor::ProcessorError;
use datatypes::Value;
use std::sync::Arc;
use std::time::SystemTime;

#[derive(Clone)]
struct OutputRowColumn {
    name: Arc<str>,
    getter: OutputValueGetter,
}

#[derive(Debug, Clone)]
enum ResolvedGetter {
    MessageByIndex {
        msg_idx: usize,
        key_idx: usize,
        expected_source: Arc<str>,
        expected_key: Arc<str>,
    },
    Affiliate {
        column_name: Arc<str>,
    },
    Missing,
}

struct MessageBuilder {
    source_name: Arc<str>,
    keys: Vec<Arc<str>>,
    values: Vec<Arc<Value>>,
}

#[derive(Clone)]
pub(crate) struct OutputRowAccessor {
    columns: Arc<[OutputRowColumn]>,
    resolved: Option<Vec<ResolvedGetter>>,
}

pub(crate) struct ExtractedOutputRow {
    missing_columns: Vec<Arc<str>>,
    values: Vec<Option<Arc<Value>>>,
}

impl OutputRowAccessor {
    pub(crate) fn from_output_schema(output_schema: &OutputSchema) -> Self {
        let columns = output_schema
            .columns
            .iter()
            .map(|column| OutputRowColumn {
                name: Arc::clone(&column.name),
                getter: column.getter.clone(),
            })
            .collect::<Vec<_>>();
        Self {
            columns: columns.into(),
            resolved: None,
        }
    }

    pub(crate) fn width(&self) -> usize {
        self.columns.len()
    }

    pub(crate) fn column_names(&self) -> Arc<[Arc<str>]> {
        self.columns
            .iter()
            .map(|column| Arc::clone(&column.name))
            .collect::<Vec<_>>()
            .into()
    }

    pub(crate) fn extract_row(
        &mut self,
        tuple: &Tuple,
    ) -> Result<ExtractedOutputRow, ProcessorError> {
        if self.resolved.is_none() {
            self.resolved = Some(resolve_getters(self.columns.as_ref(), tuple));
        }

        let resolved = self.resolved.as_mut().ok_or_else(|| {
            ProcessorError::ProcessingError(
                "output row accessor getter cache was not initialized".to_string(),
            )
        })?;

        let mut values = Vec::with_capacity(self.columns.len());
        let mut missing_columns = Vec::new();
        for (idx, column) in self.columns.iter().enumerate() {
            let resolved_item = resolved.get(idx).cloned();
            let value = match resolved_item {
                Some(ResolvedGetter::Missing) => None,
                Some(getter) => resolved_value(tuple, &getter).or_else(|| {
                    fallback_value(tuple, &column.getter).map(|(value, new_getter)| {
                        if let Some(slot) = resolved.get_mut(idx) {
                            *slot = new_getter;
                        }
                        value
                    })
                }),
                None => fallback_value(tuple, &column.getter).map(|(value, new_getter)| {
                    if idx == resolved.len() {
                        resolved.push(new_getter);
                    } else if let Some(slot) = resolved.get_mut(idx) {
                        *slot = new_getter;
                    }
                    value
                }),
            };

            if value.is_none() {
                missing_columns.push(Arc::clone(&column.name));
            }
            values.push(value);
        }

        Ok(ExtractedOutputRow {
            missing_columns,
            values,
        })
    }

    pub(crate) fn materialize_tuple(
        &self,
        timestamp: SystemTime,
        values: &[Arc<Value>],
        output_mask: Option<Arc<[bool]>>,
    ) -> Tuple {
        debug_assert_eq!(
            values.len(),
            self.columns.len(),
            "materialized row width must match output layout"
        );

        let mut message_builders = Vec::<MessageBuilder>::new();
        let mut affiliate_entries = Vec::<(Arc<String>, Value)>::new();

        for (column, value) in self.columns.iter().zip(values.iter()) {
            match &column.getter {
                OutputValueGetter::MessageByName { source_name, .. } => {
                    if let Some(builder) = message_builders
                        .iter_mut()
                        .find(|builder| builder.source_name.as_ref() == source_name.as_ref())
                    {
                        builder.keys.push(Arc::clone(&column.name));
                        builder.values.push(Arc::clone(value));
                    } else {
                        message_builders.push(MessageBuilder {
                            source_name: Arc::clone(source_name),
                            keys: vec![Arc::clone(&column.name)],
                            values: vec![Arc::clone(value)],
                        });
                    }
                }
                OutputValueGetter::Affiliate { .. } => affiliate_entries.push((
                    Arc::new(column.name.as_ref().to_string()),
                    value.as_ref().clone(),
                )),
            }
        }

        let messages = message_builders
            .into_iter()
            .map(|builder| {
                Arc::new(Message::new_shared_keys(
                    builder.source_name,
                    Arc::from(builder.keys),
                    builder.values,
                ))
            })
            .collect::<Vec<_>>();

        let mut tuple = Tuple::with_timestamp(Arc::from(messages), timestamp);
        if !affiliate_entries.is_empty() {
            tuple.add_affiliate_columns(affiliate_entries);
        }
        if let Some(mask) = output_mask {
            tuple.set_output_mask_shared(mask);
        }
        tuple
    }
}

impl ExtractedOutputRow {
    pub(crate) fn missing_columns(&self) -> &[Arc<str>] {
        self.missing_columns.as_slice()
    }

    pub(crate) fn into_required_values(
        self,
        context: &'static str,
    ) -> Result<Vec<Arc<Value>>, ProcessorError> {
        if !self.missing_columns.is_empty() {
            let missing = self
                .missing_columns
                .iter()
                .map(|name| name.as_ref())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(ProcessorError::ProcessingError(format!(
                "{context} failed to resolve output columns [{missing}] from runtime tuple"
            )));
        }

        Ok(self
            .values
            .into_iter()
            .map(|value| value.expect("missing columns handled above"))
            .collect())
    }

    pub(crate) fn into_values_with_null_fill(self) -> Vec<Arc<Value>> {
        self.values
            .into_iter()
            .map(|value| value.unwrap_or_else(|| Arc::new(Value::Null)))
            .collect()
    }

    pub(crate) fn into_optional_values(self) -> Vec<Option<Arc<Value>>> {
        self.values
    }
}

fn resolve_getters(columns: &[OutputRowColumn], sample: &Tuple) -> Vec<ResolvedGetter> {
    columns
        .iter()
        .map(|column| match &column.getter {
            OutputValueGetter::Affiliate { column_name } => ResolvedGetter::Affiliate {
                column_name: Arc::clone(column_name),
            },
            OutputValueGetter::MessageByName {
                source_name,
                column_name,
            } => sample
                .messages()
                .iter()
                .enumerate()
                .find_map(|(msg_idx, msg)| {
                    if msg.source() != source_name.as_ref() {
                        return None;
                    }
                    msg.entries().enumerate().find_map(|(key_idx, (key, _))| {
                        if key == column_name.as_ref() {
                            Some(ResolvedGetter::MessageByIndex {
                                msg_idx,
                                key_idx,
                                expected_source: Arc::clone(source_name),
                                expected_key: Arc::clone(column_name),
                            })
                        } else {
                            None
                        }
                    })
                })
                .unwrap_or(ResolvedGetter::Missing),
        })
        .collect()
}

fn resolved_value(tuple: &Tuple, getter: &ResolvedGetter) -> Option<Arc<Value>> {
    match getter {
        ResolvedGetter::MessageByIndex {
            msg_idx,
            key_idx,
            expected_source,
            expected_key,
        } => tuple.messages().get(*msg_idx).and_then(|msg| {
            if msg.source() != expected_source.as_ref() {
                return None;
            }
            msg.entry_by_index(*key_idx).and_then(|(key, value)| {
                if key.as_ref() == expected_key.as_ref() {
                    Some(Arc::clone(value))
                } else {
                    None
                }
            })
        }),
        ResolvedGetter::Affiliate { column_name } => tuple
            .affiliate()
            .and_then(|affiliate| affiliate.value(column_name.as_ref()))
            .map(|value| Arc::new(value.clone())),
        ResolvedGetter::Missing => None,
    }
}

fn fallback_value(
    tuple: &Tuple,
    getter: &OutputValueGetter,
) -> Option<(Arc<Value>, ResolvedGetter)> {
    match getter {
        OutputValueGetter::Affiliate { column_name } => tuple
            .affiliate()
            .and_then(|affiliate| affiliate.value(column_name.as_ref()))
            .map(|value| {
                (
                    Arc::new(value.clone()),
                    ResolvedGetter::Affiliate {
                        column_name: Arc::clone(column_name),
                    },
                )
            }),
        OutputValueGetter::MessageByName {
            source_name,
            column_name,
        } => tuple
            .messages()
            .iter()
            .enumerate()
            .find_map(|(msg_idx, msg)| {
                if msg.source() != source_name.as_ref() {
                    return None;
                }
                msg.entries()
                    .enumerate()
                    .find_map(|(key_idx, (key, value))| {
                        if key == column_name.as_ref() {
                            Some((
                                Arc::new(value.clone()),
                                ResolvedGetter::MessageByIndex {
                                    msg_idx,
                                    key_idx,
                                    expected_source: Arc::clone(source_name),
                                    expected_key: Arc::clone(column_name),
                                },
                            ))
                        } else {
                            None
                        }
                    })
            }),
    }
}
