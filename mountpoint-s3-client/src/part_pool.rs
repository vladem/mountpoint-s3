use bytes::Bytes;
use crossbeam::queue::SegQueue;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct PartPool {
    free_parts: Arc<SegQueue<MemoryBlock>>,
    part_size: usize,
}

impl PartPool {
    pub(crate) fn new(part_size: usize) -> Self {
        let free_parts = Default::default();
        Self { free_parts, part_size }
    }

    pub(crate) fn acquire_part(&self, data: &[u8]) -> Bytes {
        // recycle a previously allocated block or allocate a new one
        let mut memory_block = if let Some(memory_block) = self.free_parts.pop() {
            metrics::counter!("memory_blocks.reused").increment(1);
            memory_block
        } else {
            metrics::counter!("memory_blocks.allocated").increment(1);
            vec![0u8; self.part_size]
        };
        memory_block[..data.len()].copy_from_slice(data);
        let part = Part {
            memory_block: Some(memory_block),
            len: data.len(),
            free_parts: self.free_parts.clone(),
        };
        Bytes::from_owner(part)
    }

    pub(crate) fn drain(&self) {
        while let Some(_) = self.free_parts.pop() {}
    }
    // pub(crate) fn allocate_parts(&self, parts_num: usize) {
    //     for _ in 0..parts_num {
    //         self.free_parts.push(vec![0u8; self.part_size]);
    //     }
    // }

    // pub(crate) fn deallocate_parts(&self, parts_num: usize) {
    //     for _ in 0..parts_num {
    //         self.free_parts.pop();
    //     }
    // }
}

pub(crate) struct Part {
    memory_block: Option<MemoryBlock>,
    len: usize,
    free_parts: Arc<SegQueue<MemoryBlock>>,
}

type MemoryBlock = Vec<u8>;

impl Drop for Part {
    fn drop(&mut self) {
        // return part to the pool
        self.free_parts
            .push(self.memory_block.take().expect("part should not be dropped"));
    }
}

impl AsRef<[u8]> for Part {
    fn as_ref(&self) -> &[u8] {
        &self.memory_block.as_ref().expect("part should not be dropped")[..self.len]
    }
}
