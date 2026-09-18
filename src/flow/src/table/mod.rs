pub mod history;
pub mod memory;
pub mod provider;

use std::sync::Arc;

use crate::catalog::TableProps;
use crate::codec::RecordDecoder;
use datatypes::Schema;
use memory::MemoryMockTableProvider;
use provider::{HistoryTableProvider, TableProvider, TableProviderError};

pub fn create_table_provider(
    table_name: &str,
    schema: Arc<Schema>,
    props: &TableProps,
    decoder: Arc<dyn RecordDecoder>,
) -> Result<Arc<dyn TableProvider>, TableProviderError> {
    match props {
        TableProps::History(props) => Ok(Arc::new(HistoryTableProvider::new(
            schema,
            props.clone(),
            decoder,
        ))),
        TableProps::MemoryMock(props) => Ok(Arc::new(MemoryMockTableProvider::from_props(
            schema, props, table_name,
        )?)),
    }
}
