use bytes::Bytes;
use crossbeam::queue::SegQueue;
use libc::{mmap, munmap, MAP_ANONYMOUS, MAP_PRIVATE, PROT_READ, PROT_WRITE};
use std::ffi::c_void;
use std::ptr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PartPool {
    free_parts: Arc<SegQueue<MemoryBlock>>,
    part_size: usize,
}

impl PartPool {
    pub fn new(part_size: usize) -> Self {
        let free_parts = Default::default();
        Self { free_parts, part_size }
    }

    pub fn acquire_part(&self, data: &[u8]) -> Bytes {
        // recycle a previously allocated block or allocate a new one
        let mut memory_block = if let Some(memory_block) = self.free_parts.pop() {
            metrics::counter!("memory_blocks.reused").increment(1);
            memory_block
        } else {
            metrics::counter!("memory_blocks.allocated").increment(1);
            MemoryBlock::new(self.part_size)
        };
        memory_block.fill(data);
        let part = Part {
            memory_block: Some(memory_block),
            len: data.len(),
            free_parts: self.free_parts.clone(),
        };
        Bytes::from_owner(part)
    }

    // pub fn drain(&self) {
    //     while let Some(_) = self.free_parts.pop() {}
    // }

    // pub fn allocate_parts(&self, parts_num: usize) {
    //     for _ in 0..parts_num {
    //         self.free_parts.push(vec![0u8; self.part_size]);
    //     }
    // }

    // pub fn deallocate_parts(&self, parts_num: usize) {
    //     for _ in 0..parts_num {
    //         self.free_parts.pop();
    //     }
    // }
}

struct MemoryBlock {
    ptr: *mut c_void,
    size: usize,
}

unsafe impl Send for MemoryBlock {}

impl Drop for MemoryBlock {
    fn drop(&mut self) {
        unsafe {
            munmap(self.ptr, self.size);
        }
    }
}

impl AsMut<[u8]> for MemoryBlock {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr as *mut u8, self.size) }
    }
}

impl AsRef<[u8]> for MemoryBlock {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as *mut u8, self.size) }
    }
}

impl MemoryBlock {
    fn new(size: usize) -> Self {
        let ptr = unsafe {
            let ptr = mmap(
                ptr::null_mut(),
                size,
                PROT_READ | PROT_WRITE, // Read and write permissions
                MAP_ANONYMOUS | MAP_PRIVATE,
                -1,
                0,
            );
            if ptr == libc::MAP_FAILED {
                panic!("Memory allocation failed");
            }
            ptr
        };
        Self { ptr, size }
    }

    fn fill(&mut self, data: &[u8]) {
        self.as_mut()[..data.len()].copy_from_slice(data);
    }
}

pub struct Part {
    memory_block: Option<MemoryBlock>,
    len: usize,
    free_parts: Arc<SegQueue<MemoryBlock>>,
}

impl Drop for Part {
    fn drop(&mut self) {
        let memory_block = self.memory_block.take().expect("part should not be dropped");
        // return part to the pool, trying to make the size of it less than 8GIB
        if self.free_parts.len() < 1024 {
            self.free_parts.push(memory_block);
        }
    }
}

impl AsRef<[u8]> for Part {
    fn as_ref(&self) -> &[u8] {
        &self.memory_block.as_ref().expect("part should not be dropped").as_ref()[..self.len]
    }
}
