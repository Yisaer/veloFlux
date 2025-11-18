use crate::model::RecordBatch;

impl RecordBatch {
    pub fn debug_print(&self) {
        let columns = self.columns();
        println!(
            "[RecordBatch] rows={}, columns={}",
            self.num_rows(),
            columns.len()
        );
        for column in columns {
            println!(
                "  column {}.{} = {:?}",
                column.source_name(),
                column.name(),
                column.values()
            );
        }
    }
}
