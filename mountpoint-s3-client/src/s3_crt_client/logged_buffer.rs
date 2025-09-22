//! Logged wrapper around OwnedBuffer for tracking allocations and deallocations.

use std::ops::Deref;
use tracing::info;

use mountpoint_s3_crt::s3::buffer::OwnedBuffer;

/// A wrapper around OwnedBuffer that logs allocations and deallocations.
///
/// This wrapper tracks buffer lifecycle events with the following fields:
/// - addr: Memory address of the buffer
/// - s3_key: S3 object key associated with this buffer
/// - offset: Offset within the S3 object
/// - payload_size: Size of the buffer payload
#[derive(Debug, Clone)]
pub struct LoggedOwnedBuffer {
    inner: OwnedBuffer,
    s3_key: String,
    offset: u64,
    addr: usize,
}

impl LoggedOwnedBuffer {
    /// Create a new LoggedOwnedBuffer, logging the allocation.
    pub fn new(buffer: OwnedBuffer, s3_key: String, offset: u64) -> Self {
        let addr = buffer.as_ref().as_ptr() as usize;
        let payload_size = buffer.as_ref().len();

        info!(
            addr = addr,
            s3_key = %s3_key,
            offset = offset,
            payload_size = payload_size,
            "buffer_allocated"
        );

        Self {
            inner: buffer,
            s3_key,
            offset,
            addr,
        }
    }

    /// Get the payload size.
    pub fn payload_size(&self) -> usize {
        self.inner.as_ref().len()
    }
}

impl Drop for LoggedOwnedBuffer {
    fn drop(&mut self) {
        info!(
            addr = self.addr,
            s3_key = %self.s3_key,
            offset = self.offset,
            payload_size = self.payload_size(),
            "buffer_deallocated"
        );
    }
}

impl Deref for LoggedOwnedBuffer {
    type Target = OwnedBuffer;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl AsRef<[u8]> for LoggedOwnedBuffer {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

// SAFETY: LoggedOwnedBuffer is just a wrapper around OwnedBuffer, which is Send + Sync
unsafe impl Send for LoggedOwnedBuffer {}
unsafe impl Sync for LoggedOwnedBuffer {}
