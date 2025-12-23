#[derive(Debug, Default)]
pub struct ColPlaceholderAllocator {
    next_index: usize,
}

impl ColPlaceholderAllocator {
    pub fn new() -> Self {
        Self { next_index: 0 }
    }

    pub fn allocate(&mut self) -> String {
        self.next_index += 1;
        format!("col_{}", self.next_index)
    }
}

