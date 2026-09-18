use std::sync::Arc;

use crate::catalog::{TableDefinition, TableProps};
use crate::table::provider::TableProvider;

use super::{FlowInstance, FlowInstanceError};

impl FlowInstance {
    pub async fn create_table(
        &self,
        definition: TableDefinition,
    ) -> Result<Arc<TableDefinition>, FlowInstanceError> {
        self.validate_table_definition(&definition)?;
        let decoder = self
            .decoder_registry
            .instantiate(definition.decoder(), definition.id(), definition.schema())
            .map_err(|err| FlowInstanceError::Invalid(err.to_string()))?;
        let provider = crate::table::create_table_provider(
            definition.id(),
            definition.schema(),
            definition.props(),
            decoder,
        )
        .map_err(|err| FlowInstanceError::Invalid(err.to_string()))?;
        self.create_table_with_provider(definition, provider).await
    }

    async fn create_table_with_provider(
        &self,
        definition: TableDefinition,
        provider: Arc<dyn TableProvider>,
    ) -> Result<Arc<TableDefinition>, FlowInstanceError> {
        self.validate_table_definition(&definition)?;
        let created = self
            .catalog
            .insert_table(definition)
            .map_err(FlowInstanceError::from)?;
        if let Err(err) = self.catalog.register_table_provider(created.id(), provider) {
            let _ = self.catalog.remove_table(created.id());
            return Err(FlowInstanceError::from(err));
        }
        Ok(created)
    }

    pub async fn get_table(
        &self,
        table_id: &str,
    ) -> Result<Option<Arc<TableDefinition>>, FlowInstanceError> {
        Ok(self.catalog.get_table(table_id))
    }

    pub async fn list_tables(&self) -> Result<Vec<Arc<TableDefinition>>, FlowInstanceError> {
        Ok(self.catalog.list_tables())
    }

    pub async fn delete_table(&self, table_id: &str) -> Result<(), FlowInstanceError> {
        self.catalog
            .remove_table(table_id)
            .map_err(FlowInstanceError::from)
    }

    fn validate_table_definition(
        &self,
        definition: &TableDefinition,
    ) -> Result<(), FlowInstanceError> {
        match definition.props() {
            TableProps::History(props) => {
                if props.datasource.trim().is_empty() {
                    return Err(FlowInstanceError::Invalid(format!(
                        "table '{}' history datasource must not be empty",
                        definition.id()
                    )));
                }
                if props.topic.trim().is_empty() {
                    return Err(FlowInstanceError::Invalid(format!(
                        "table '{}' history topic must not be empty",
                        definition.id()
                    )));
                }
                if props.batch_size == Some(0) {
                    return Err(FlowInstanceError::Invalid(format!(
                        "table '{}' history batch_size must be greater than 0",
                        definition.id()
                    )));
                }
            }
            TableProps::MemoryMock(props) => {
                if props.rows.iter().any(|row| !row.is_object()) {
                    return Err(FlowInstanceError::Invalid(format!(
                        "table '{}' memory_mock rows must be JSON objects",
                        definition.id()
                    )));
                }
            }
        }
        Ok(())
    }
}
