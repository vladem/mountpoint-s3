use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::{ops::Range, sync::atomic::Ordering};

use async_channel::{Receiver, Sender, unbounded};
use humansize::make_format;
use tracing::trace;

use crate::mem_limiter::{BufferArea, MemoryLimiter};

use super::PrefetchReadError;

#[derive(Debug)]
pub enum BackpressureFeedbackEvent {
    /// An event where data with a certain length has been read out of the stream
    DataRead { offset: u64, length: usize },
    /// An event indicating part queue stall
    PartQueueStall,
}

pub struct BackpressureConfig {
    /// Backpressure's initial read window size
    pub initial_read_window_size: usize,
    /// Minimum read window size that the backpressure controller is allowed to scale down to
    pub min_read_window_size: usize,
    /// Maximum read window size that the backpressure controller is allowed to scale up to
    pub max_read_window_size: usize,
    /// Factor to increase the read window size by when the part queue is stalled
    pub read_window_size_multiplier: usize,
    /// Request range to apply backpressure
    pub request_range: Range<u64>,
}

#[derive(Debug)]
pub struct BackpressureNotifier {
    feedback_sender: Sender<BackpressureFeedbackEvent>,
    read_window_end_offset: Arc<AtomicU64>,
}

impl BackpressureNotifier {
    pub async fn send_feedback(&mut self, event: BackpressureFeedbackEvent) {
        if self.feedback_sender.send(event).await.is_err() {
            trace!("read window incrementing queue is already closed");
        }
    }

    pub fn read_window_end_offset(&self) -> u64 {
        self.read_window_end_offset.load(Ordering::SeqCst)
    }
}

/// A [BackpressureController] should be given to consumers of a byte stream.
/// It is used to send feedback ([Self::send_feedback]) to its corresponding [BackpressureLimiter],
/// the counterpart which should be leveraged by the stream producer.
#[derive(Debug)]
pub struct BackpressureController {
    /// Amount by which the producer should be producing data ahead of [Self::next_read_offset].
    preferred_read_window_size: usize,
    min_read_window_size: usize,
    max_read_window_size: usize,
    /// Multiplier by which [Self::preferred_read_window_size] is scaled.
    read_window_size_multiplier: usize,
    /// Upper bound of the current read window, relative to the start of the S3 object.
    ///
    /// The request can return data up to this offset *exclusively*.
    /// This value must be advanced to continue fetching new data.
    read_window_end_offset: u64,
    read_window_end_offset_shared: Arc<AtomicU64>,
    /// Next offset of the data to be read, relative to the start of the S3 object.
    next_read_offset: u64,
    /// End offset within the S3 object for the request.
    ///
    /// The request can return data up to this offset *exclusively*.
    request_end_offset: u64,
    /// Memory limiter is used to guide decisions on how much data to prefetch.
    ///
    /// For example, when memory is low we should scale down [Self::preferred_read_window_size].
    mem_limiter: Arc<MemoryLimiter>,
}

/// The [BackpressureLimiter] is used on producer side of a stream, for example,
/// any [super::part_stream::ObjectPartStream] that supports backpressure.
///
/// The producer can call [Self::wait_for_read_window_increment] to wait for feedback from the consumer.
#[derive(Debug)]
pub struct BackpressureLimiter {
    feedback_receiver: Receiver<BackpressureFeedbackEvent>,
    controller: BackpressureController,
}

/// Creates a [BackpressureController] and its related [BackpressureLimiter].
///
/// This pair allows a consumer to send feedback ([BackpressureFeedbackEvent]) when starved or bytes are consumed,
/// informing a producer (a holder of the [BackpressureLimiter]) when it should provide data more aggressively.
pub fn new_backpressure_controller(
    config: BackpressureConfig,
    mem_limiter: Arc<MemoryLimiter>,
) -> (BackpressureNotifier, BackpressureLimiter) {
    // Minimum window size multiplier as the scaling up and down won't work if the multiplier is 1.
    const MIN_WINDOW_SIZE_MULTIPLIER: usize = 2;
    let read_window_end_offset = config.request_range.start + config.initial_read_window_size as u64;
    mem_limiter.reserve(BufferArea::Prefetch, config.initial_read_window_size as u64);

    let (feedback_sender, feedback_receiver) = unbounded();
    let read_window_end_offset_shared = Arc::new(AtomicU64::new(0));

    let notifier = BackpressureNotifier {
        feedback_sender,
        read_window_end_offset: read_window_end_offset_shared.clone(),
    };

    let controller = BackpressureController {
        preferred_read_window_size: config.initial_read_window_size,
        min_read_window_size: config.min_read_window_size,
        max_read_window_size: config.max_read_window_size,
        read_window_size_multiplier: config.read_window_size_multiplier.max(MIN_WINDOW_SIZE_MULTIPLIER),
        read_window_end_offset,
        next_read_offset: config.request_range.start,
        request_end_offset: config.request_range.end,
        mem_limiter,
        read_window_end_offset_shared,
    };

    let limiter = BackpressureLimiter {
        feedback_receiver,
        controller,
    };

    (notifier, limiter)
}

impl BackpressureController {
    fn process_event(&mut self, event: BackpressureFeedbackEvent, request_start: u64, part_size: u64) {
        match event {
            // Note, that this may come from a backwards seek, so offsets observed by this method are not necessarily ascending
            BackpressureFeedbackEvent::DataRead { offset, length } => {
                self.next_read_offset = offset + length as u64;
                self.mem_limiter.release(BufferArea::Prefetch, length as u64);
                let remaining_window = self.read_window_end_offset.saturating_sub(self.next_read_offset) as usize;

                // Increment the read window only if the remaining window reaches some threshold i.e. half of it left.
                // When memory is low the `preferred_read_window_size` will be scaled down so we have to keep trying
                // until we have enough read window.
                while remaining_window < (self.preferred_read_window_size / 2)
                    && self.read_window_end_offset < self.request_end_offset
                {
                    let new_read_window_end_offset = self
                        .next_read_offset
                        .saturating_add(self.preferred_read_window_size as u64);
                    let new_read_window_end_offset =
                        Self::round_up_to_part_boundary(new_read_window_end_offset, request_start, part_size)
                            .min(self.request_end_offset);
                    // We can skip if the new `read_window_end_offset` is less than or equal to the current one, this
                    // could happen after the read window is scaled down.
                    if new_read_window_end_offset <= self.read_window_end_offset {
                        break;
                    }
                    let to_increase = new_read_window_end_offset.saturating_sub(self.read_window_end_offset) as usize;

                    // Force incrementing read window regardless of available memory when we are already at minimum
                    // read window size.
                    if self.preferred_read_window_size <= self.min_read_window_size {
                        self.mem_limiter.reserve(BufferArea::Prefetch, to_increase as u64);
                        self.read_window_end_offset = new_read_window_end_offset;
                        break;
                    }

                    // Try to reserve the memory for the length we want to increase before sending the request,
                    // scale down the read window if it fails.
                    if self.mem_limiter.try_reserve(BufferArea::Prefetch, to_increase as u64) {
                        self.read_window_end_offset = new_read_window_end_offset;
                        break;
                    } else {
                        self.scale_down();
                    }
                }
            }
            BackpressureFeedbackEvent::PartQueueStall => self.scale_up(),
        }
        self.read_window_end_offset_shared
            .store(self.read_window_end_offset, Ordering::SeqCst);
    }

    /// Scale up preferred read window size with a multiplier configured at initialization.
    ///
    /// Fails silently if there is insufficient free memory to perform it according to [Self::mem_limiter].
    fn scale_up(&mut self) {
        if self.preferred_read_window_size < self.max_read_window_size {
            let new_read_window_size = (self.preferred_read_window_size * self.read_window_size_multiplier)
                .max(self.min_read_window_size)
                .min(self.max_read_window_size);
            // Only scale up when there is enough memory. We don't have to reserve the memory here
            // because only `preferred_read_window_size` is increased but the actual read window will
            // be updated later on `DataRead` event (where we do reserve memory).
            let to_increase = (new_read_window_size - self.preferred_read_window_size) as u64;
            let available_mem = self.mem_limiter.available_mem();
            if available_mem >= to_increase {
                let formatter = make_format(humansize::BINARY);
                trace!(
                    prev_size = formatter(self.preferred_read_window_size),
                    new_size = formatter(new_read_window_size),
                    "scaled up preferred read window"
                );
                self.preferred_read_window_size = new_read_window_size;
                metrics::histogram!("prefetch.window_after_increase_mib")
                    .record((self.preferred_read_window_size / 1024 / 1024) as f64);
            }
        }
    }

    /// Scale down [Self::preferred_read_window_size] by a multiplier configured at initialization.
    fn scale_down(&mut self) {
        if self.preferred_read_window_size > self.min_read_window_size {
            assert!(self.read_window_size_multiplier > 1);
            let new_read_window_size = (self.preferred_read_window_size / self.read_window_size_multiplier)
                .max(self.min_read_window_size)
                .min(self.max_read_window_size);
            let formatter = make_format(humansize::BINARY);
            trace!(
                current_size = formatter(self.preferred_read_window_size),
                new_size = formatter(new_read_window_size),
                "scaled down read window"
            );
            self.preferred_read_window_size = new_read_window_size;
            metrics::histogram!("prefetch.window_after_decrease_mib")
                .record((self.preferred_read_window_size / 1024 / 1024) as f64);
        }
    }

    fn finished(&self) -> bool {
        self.read_window_end_offset == self.request_end_offset
    }

    /// Round up `read_window_end` to next part boundary (relative to request start).
    ///
    /// This function ensures that read window end offsets are aligned to part boundaries for the second request,
    /// which helps optimize memory usage.
    ///
    /// If the `read_window_end` is before the second request start or `align_read_window` is false, it returns the `read_window_end` unchanged.
    ///
    /// Note: window excludes the last byte, denoted by `read_window_end`.
    fn round_up_to_part_boundary(read_window_end: u64, request_start: u64, part_size: u64) -> u64 {
        if read_window_end > request_start {
            let relative_end_offset = read_window_end - request_start;
            if !relative_end_offset.is_multiple_of(part_size) {
                let aligned_relative_offset = part_size * (relative_end_offset / part_size + 1);
                request_start + aligned_relative_offset
            } else {
                read_window_end
            }
        } else {
            read_window_end
        }
    }
}

impl Drop for BackpressureController {
    fn drop(&mut self) {
        debug_assert!(
            self.next_read_offset <= self.request_end_offset,
            "invariant: the next read offset should never be larger than the request end offset",
        );
        // Free up memory we have reserved for the read window.
        let remaining_window = self.read_window_end_offset.saturating_sub(self.next_read_offset);
        self.mem_limiter.release(BufferArea::Prefetch, remaining_window);
    }
}

impl BackpressureLimiter {
    pub fn read_window_end_offset(&self) -> u64 {
        self.controller.read_window_end_offset
    }

    /// Wait until the backpressure window moves ahead of the given offset.
    ///
    /// Returns the new read window offset if it has changed, otherwise [None].
    pub async fn wait_for_read_window_increment<E>(
        &mut self,
        next_request_offset: u64,
        request_start: u64,
        part_size: u64,
    ) -> Result<Option<u64>, PrefetchReadError<E>> {
        let prev_read_window_end_offset = self.controller.read_window_end_offset;
        loop {
            // we will only wait for another read if next_request_offset is ahead of read_window_end_offset
            // otherwise process all events that are available now and return to keep reading from the request
            let event = if self.controller.read_window_end_offset <= next_request_offset && !self.controller.finished()
            {
                self.feedback_receiver
                    .recv()
                    .await
                    .map_err(|_| PrefetchReadError::ReadWindowIncrement)?
            } else if let Ok(event) = self.feedback_receiver.try_recv() {
                // todo: ignored closed channel?
                event
            } else {
                break;
            };
            // this call updates read_window_end_offset if the reader advanced far enough (at least half of read window must be not read yet)
            self.controller.process_event(event, request_start, part_size);
        }
        if prev_read_window_end_offset < self.controller.read_window_end_offset {
            Ok(Some(self.controller.read_window_end_offset))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use futures::executor::block_on;
    use mountpoint_s3_client::mock_client::MockClientError;
    use test_case::test_case;

    use crate::mem_limiter::MemoryLimiter;
    use crate::memory::PagedPool;
    use crate::s3::config::INITIAL_READ_WINDOW_SIZE;

    #[test_case(INITIAL_READ_WINDOW_SIZE, 2)] // real config
    #[test_case(3 * 1024 * 1024, 4)]
    #[test_case(8 * 1024 * 1024, 8)]
    #[test_case(2 * 1024 * 1024 * 1024, 2)]
    fn test_read_window_scale_up(initial_read_window_size: usize, read_window_size_multiplier: usize) {
        let request_range = 0..(5 * 1024 * 1024 * 1024);
        let backpressure_config = BackpressureConfig {
            initial_read_window_size,
            min_read_window_size: 8 * 1024 * 1024,
            max_read_window_size: 2 * 1024 * 1024 * 1024,
            read_window_size_multiplier,
            request_range,
        };

        let mut backpressure_controller = new_backpressure_controller_for_test(backpressure_config);
        while backpressure_controller.preferred_read_window_size < backpressure_controller.max_read_window_size {
            backpressure_controller.scale_up();
            assert!(backpressure_controller.preferred_read_window_size >= backpressure_controller.min_read_window_size);
            assert!(backpressure_controller.preferred_read_window_size <= backpressure_controller.max_read_window_size);
        }
        assert_eq!(
            backpressure_controller.preferred_read_window_size, backpressure_controller.max_read_window_size,
            "should have scaled up to max read window size"
        );
    }

    #[test_case(2 * 1024 * 1024 * 1024, 2)]
    #[test_case(15 * 1024 * 1024 * 1024, 2)]
    #[test_case(2 * 1024 * 1024 * 1024, 8)]
    #[test_case(8 * 1024 * 1024, 8)]
    fn test_read_window_scale_down(initial_read_window_size: usize, read_window_size_multiplier: usize) {
        let request_range = 0..(5 * 1024 * 1024 * 1024);
        let backpressure_config = BackpressureConfig {
            initial_read_window_size,
            min_read_window_size: 8 * 1024 * 1024,
            max_read_window_size: 2 * 1024 * 1024 * 1024,
            read_window_size_multiplier,
            request_range,
        };

        let mut backpressure_controller = new_backpressure_controller_for_test(backpressure_config);
        while backpressure_controller.preferred_read_window_size > backpressure_controller.min_read_window_size {
            backpressure_controller.scale_down();
            assert!(backpressure_controller.preferred_read_window_size <= backpressure_controller.max_read_window_size);
            assert!(backpressure_controller.preferred_read_window_size >= backpressure_controller.min_read_window_size);
        }
        assert_eq!(
            backpressure_controller.preferred_read_window_size, backpressure_controller.min_read_window_size,
            "should have scaled down to min read window size"
        );
    }

    #[test]
    #[allow(clippy::identity_op)]
    fn wait_for_read_window_increment_drains_all_events() {
        const KIB: usize = 1024;
        const MIB: usize = 1024 * KIB;
        const GIB: usize = 1024 * MIB;

        // OK, back to basics. Just reproduce what happened, verify it passes after the fix.
        let backpressure_config = BackpressureConfig {
            initial_read_window_size: 8 * MIB,
            min_read_window_size: 8 * MIB,
            max_read_window_size: 2 * GIB,
            read_window_size_multiplier: 2,
            request_range: 0..(5 * GIB as u64),
        };

        let (mut backpressure_notifier, mut backpressure_limiter) =
            new_backpressure_limiter_for_test(backpressure_config);

        block_on(async {
            let expected_offset = 8 * MIB as u64;
            assert_eq!(
                backpressure_limiter.read_window_end_offset(),
                expected_offset,
                "read window end offset should already be {expected_offset} due to initial read window size config",
            );

            // Send more than one increment.
            backpressure_notifier
                .send_feedback(BackpressureFeedbackEvent::DataRead {
                    offset: 0,
                    length: 1 * MIB,
                })
                .await;
            backpressure_notifier
                .send_feedback(BackpressureFeedbackEvent::DataRead {
                    offset: (1 * MIB) as u64,
                    length: 2 * MIB,
                })
                .await;
            backpressure_notifier
                .send_feedback(BackpressureFeedbackEvent::DataRead {
                    offset: (3 * MIB) as u64,
                    length: 2 * MIB,
                })
                .await;

            let curr_offset = backpressure_limiter
                .wait_for_read_window_increment::<MockClientError>(0, 0, (8 * MIB) as u64)
                .await
                .expect("should return OK as we have new values to increment before channels are closed")
                .expect("value should change as we sent increments");
            assert_eq!(
                16 * MIB as u64,
                curr_offset,
                "expected offset did not match offset reported by limiter",
            );
        });
    }

    #[test_case(500, 1000, 100, 500; "offset before second request start")]
    #[test_case(1000, 1000, 512, 1000; "offset at second request start")]
    #[test_case(1500, 1000, 512, 1512; "offset after second request start, needs rounding up")]
    #[test_case(2024, 1000, 512, 2024; "offset after second request start, already aligned")]
    #[test_case(1001, 1000, 512, 1512; "offset just after second request start, needs rounding up")]
    #[test_case(1512, 1000, 512, 1512; "offset exactly at part boundary")]
    #[test_case(1513, 1000, 512, 2024; "offset just past part boundary")]
    fn test_round_up_to_part_boundary(offset: u64, second_req_start: u64, part_size: u64, expected: u64) {
        let result = BackpressureController::round_up_to_part_boundary(offset, second_req_start, part_size);
        assert_eq!(result, expected);
    }

    fn new_backpressure_limiter_for_test(
        backpressure_config: BackpressureConfig,
    ) -> (BackpressureNotifier, BackpressureLimiter) {
        let pool = PagedPool::new_with_candidate_sizes([8 * 1024 * 1024]);
        let mem_limiter = Arc::new(MemoryLimiter::new(
            pool,
            backpressure_config.max_read_window_size as u64,
        ));
        new_backpressure_controller(backpressure_config, mem_limiter.clone())
    }

    fn new_backpressure_controller_for_test(config: BackpressureConfig) -> BackpressureController {
        let pool = PagedPool::new_with_candidate_sizes([8 * 1024 * 1024]);
        let mem_limiter = Arc::new(MemoryLimiter::new(pool, config.max_read_window_size as u64));
        mem_limiter.reserve(BufferArea::Prefetch, config.initial_read_window_size as u64);
        let read_window_end_offset = config.request_range.start + config.initial_read_window_size as u64;
        BackpressureController {
            preferred_read_window_size: config.initial_read_window_size,
            min_read_window_size: config.min_read_window_size,
            max_read_window_size: config.max_read_window_size,
            read_window_size_multiplier: config.read_window_size_multiplier,
            read_window_end_offset,
            next_read_offset: config.request_range.start,
            request_end_offset: config.request_range.end,
            mem_limiter,
            read_window_end_offset_shared: Arc::new(AtomicU64::new(0)),
        }
    }
}
