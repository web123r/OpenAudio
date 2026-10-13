use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::TryLockError;
use std::sync::{Mutex, MutexGuard};

/// Locks a mutex, recovering from poisoning instead of panicking.
/// A poisoned lock means some thread panicked while holding it
/// earlier; for audio state, continuing with the (possibly
/// inconsistent) data is far better than cascading that panic into
/// every other thread that touches the same lock -- especially since
/// some of those locks are read inside OS audio callbacks, where an
/// escaping panic crashes the whole process.
pub fn safe_lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    match m.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

pub(crate) fn trim_interleaved_queue(
    queue: &mut VecDeque<f32>,
    maximum_frames: usize,
    channels: usize,
) {
    assert!(
        channels > 0,
        "interleaved audio must have at least one channel"
    );

    let maximum_samples = maximum_frames.saturating_mul(channels);
    if queue.len() > maximum_samples {
        let excess_samples = queue.len() - maximum_samples;
        let frames_to_drop = excess_samples.div_ceil(channels);
        let samples_to_drop = frames_to_drop.saturating_mul(channels);
        queue.drain(..samples_to_drop);
    }
}

pub(crate) struct BoundedAudioQueue {
    channels: usize,
    maximum_samples: usize,
    state: Mutex<AudioQueueState>,
    dropped_frames: AtomicU64,
}

struct AudioQueueState {
    blocks: VecDeque<QueuedAudioBlock>,
    buffered_samples: usize,
    closed: bool,
}

pub(crate) struct QueuedAudioBlock {
    pub(crate) samples: Vec<f32>,
    pub(crate) presentation_timestamp_ns: u64,
}

impl BoundedAudioQueue {
    pub(crate) fn new(channels: usize, maximum_frames: usize) -> Result<Self, String> {
        if channels == 0 || maximum_frames == 0 {
            return Err("audio queue requires nonzero channels and capacity".to_string());
        }

        let maximum_samples = channels
            .checked_mul(maximum_frames)
            .ok_or_else(|| "audio queue capacity overflow".to_string())?;

        Ok(Self {
            channels,
            maximum_samples,
            state: Mutex::new(AudioQueueState {
                blocks: VecDeque::new(),
                buffered_samples: 0,
                closed: false,
            }),
            dropped_frames: AtomicU64::new(0),
        })
    }

    /// Copies one interleaved block without waiting for the consumer lock.
    /// When full, stale queued blocks are dropped to keep latency bounded.
    pub(crate) fn try_push(
        &self,
        samples: &[f32],
        presentation_timestamp_ns: u64,
    ) -> Result<bool, String> {
        self.try_push_owned(samples.to_vec(), presentation_timestamp_ns)
    }

    pub(crate) fn try_push_owned(
        &self,
        samples: Vec<f32>,
        presentation_timestamp_ns: u64,
    ) -> Result<bool, String> {
        if samples.len() % self.channels != 0 {
            return Err("audio queue block is not frame aligned".to_string());
        }

        let frames = samples.len() / self.channels;
        if frames == 0 {
            return Ok(true);
        }

        if samples.len() > self.maximum_samples {
            self.dropped_frames
                .fetch_add(frames as u64, Ordering::Relaxed);
            return Ok(false);
        }

        let mut state = match self.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => {
                self.dropped_frames
                    .fetch_add(frames as u64, Ordering::Relaxed);
                return Ok(false);
            }
        };

        if state.closed {
            self.dropped_frames
                .fetch_add(frames as u64, Ordering::Relaxed);
            return Ok(false);
        }

        while state.buffered_samples + samples.len() > self.maximum_samples {
            let Some(stale_block) = state.blocks.pop_front() else {
                break;
            };
            state.buffered_samples -= stale_block.samples.len();
            self.dropped_frames.fetch_add(
                (stale_block.samples.len() / self.channels) as u64,
                Ordering::Relaxed,
            );
        }

        state.buffered_samples += samples.len();
        state.blocks.push_back(QueuedAudioBlock {
            samples,
            presentation_timestamp_ns,
        });
        Ok(true)
    }

    pub(crate) fn try_pop(&self) -> Option<QueuedAudioBlock> {
        let mut state = safe_lock(&self.state);
        let block = state.blocks.pop_front()?;
        state.buffered_samples -= block.samples.len();
        Some(block)
    }

    pub(crate) fn close(&self) {
        safe_lock(&self.state).closed = true;
    }

    pub(crate) fn is_closed_and_empty(&self) -> bool {
        let state = safe_lock(&self.state);
        state.closed && state.blocks.is_empty()
    }

    pub(crate) fn take_dropped_frames(&self) -> u64 {
        self.dropped_frames.swap(0, Ordering::Relaxed)
    }

    #[cfg(test)]
    fn buffered_samples(&self) -> usize {
        safe_lock(&self.state).buffered_samples
    }
}

#[cfg(test)]
mod tests {
    use super::{trim_interleaved_queue, BoundedAudioQueue};
    use std::collections::VecDeque;

    #[test]
    fn queue_trim_drops_complete_interleaved_frames() {
        let mut queue = VecDeque::from([0.0, 10.0, 1.0, 11.0, 2.0, 12.0]);

        trim_interleaved_queue(&mut queue, 2, 2);

        assert_eq!(queue, VecDeque::from([1.0, 11.0, 2.0, 12.0]));
        assert_eq!(queue.len() % 2, 0);
    }

    #[test]
    fn queue_trim_keeps_data_when_within_frame_limit() {
        let mut queue = VecDeque::from([0.0, 10.0, 1.0, 11.0]);

        trim_interleaved_queue(&mut queue, 2, 2);

        assert_eq!(queue, VecDeque::from([0.0, 10.0, 1.0, 11.0]));
    }

    #[test]
    fn bounded_audio_queue_drops_oldest_complete_blocks_on_saturation() {
        let queue = BoundedAudioQueue::new(2, 2).unwrap();

        assert!(queue.try_push(&[0.0, 10.0], 1).unwrap());
        assert!(queue.try_push(&[1.0, 11.0], 2).unwrap());
        assert!(queue.try_push(&[2.0, 12.0], 3).unwrap());

        assert_eq!(queue.buffered_samples(), 4);
        let first = queue.try_pop().unwrap();
        assert_eq!(first.samples, vec![1.0, 11.0]);
        assert_eq!(first.presentation_timestamp_ns, 2);
        let second = queue.try_pop().unwrap();
        assert_eq!(second.samples, vec![2.0, 12.0]);
        assert_eq!(second.presentation_timestamp_ns, 3);
        assert_eq!(queue.take_dropped_frames(), 1);
        assert_eq!(queue.buffered_samples(), 0);
    }

    #[test]
    fn bounded_audio_queue_rejects_oversized_or_misaligned_blocks() {
        let queue = BoundedAudioQueue::new(2, 2).unwrap();

        assert!(!queue
            .try_push(&[0.0, 10.0, 1.0, 11.0, 2.0, 12.0], 1)
            .unwrap());
        assert!(queue.try_push(&[0.0], 2).is_err());
        assert_eq!(queue.take_dropped_frames(), 3);
        assert_eq!(queue.buffered_samples(), 0);
    }

    #[test]
    fn bounded_audio_queue_drains_after_close_and_rejects_new_blocks() {
        let queue = BoundedAudioQueue::new(1, 2).unwrap();
        assert!(queue.try_push(&[1.0, 2.0], 1).unwrap());

        queue.close();

        assert!(!queue.try_push(&[3.0], 2).unwrap());
        assert_eq!(queue.try_pop().unwrap().samples, vec![1.0, 2.0]);
        assert!(queue.is_closed_and_empty());
        assert_eq!(queue.take_dropped_frames(), 1);
    }

    #[test]
    fn bounded_audio_queue_sustains_five_seconds_of_64_channel_audio() {
        const CHANNELS: usize = 64;
        const SAMPLE_RATE: usize = 48_000;
        const CALLBACK_FRAMES: usize = 48;
        const DURATION_SECONDS: usize = 5;

        let queue = BoundedAudioQueue::new(CHANNELS, SAMPLE_RATE / 10).unwrap();
        let mut generated_frames = 0usize;
        let mut consumed_frames = 0usize;

        for callback_index in 0..(SAMPLE_RATE * DURATION_SECONDS / CALLBACK_FRAMES) {
            let mut block = Vec::with_capacity(CALLBACK_FRAMES * CHANNELS);
            for frame in 0..CALLBACK_FRAMES {
                let frame_value = ((callback_index * CALLBACK_FRAMES + frame) % 1_000) as f32;
                for channel in 0..CHANNELS {
                    block.push(frame_value + channel as f32 * 0.001);
                }
            }

            assert!(queue
                .try_push(&block, callback_index as u64)
                .expect("valid interleaved callback block"));
            generated_frames += CALLBACK_FRAMES;

            let consumed = queue.try_pop().expect("consumer keeps up");
            assert_eq!(consumed.samples, block);
            consumed_frames += consumed.samples.len() / CHANNELS;
            assert!(queue.buffered_samples() <= SAMPLE_RATE / 10 * CHANNELS);
        }

        assert_eq!(generated_frames, SAMPLE_RATE * DURATION_SECONDS);
        assert_eq!(consumed_frames, generated_frames);
        assert_eq!(queue.take_dropped_frames(), 0);
        assert_eq!(queue.buffered_samples(), 0);
    }
}
