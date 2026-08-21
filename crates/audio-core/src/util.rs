use std::sync::{Mutex, MutexGuard};

/// Locks a mutex, recovering from poisoning instead of panicking.
/// A poisoned lock means some thread panicked while holding it
/// earlier; for audio state, continuing with the (possibly
/// inconsistent) data is far better than cascading that panic into
/// every other thread that touches the same lock -- especially since
/// some of those locks are read inside OS audio callbacks, where an
/// escaping panic crashes the whole process.
pub fn safe_lock<T>(m: &Mutex<T>) -> MutexGuard<T> {
    match m.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}