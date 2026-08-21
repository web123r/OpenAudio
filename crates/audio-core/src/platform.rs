//! OS-level realtime scheduling helpers for audio threads.
//!
//! On Windows, background / unfocused apps can be deprioritized by the
//! scheduler and EcoQoS. Audio work must opt into MMCSS ("Pro Audio")
//! on every thread that touches the realtime path (CPAL callbacks *and*
//! UDP receive loops that feed jitter buffers).

use std::cell::Cell;

thread_local! {
    static THREAD_PREPARED: Cell<bool> = const { Cell::new(false) };
}

/// Target jitter-buffer depth used when priming playback paths.
pub const JITTER_BUFFER_TARGET_SECS: f64 = 0.020;

/// Idempotent per-thread setup: MMCSS + elevated priority.
/// Safe to call from CPAL callbacks and worker threads alike.
pub fn ensure_realtime_audio_thread() {
    THREAD_PREPARED.with(|prepared| {
        if prepared.get() {
            return;
        }
        prepared.set(true);
        #[cfg(windows)]
        prepare_realtime_audio_thread_windows();
    });
}

/// Process-wide setup: disable background throttling on the calling process
/// and prepare the calling thread. Call once from main() before audio starts.
pub fn prepare_realtime_process() {
    #[cfg(windows)]
    disable_process_power_throttling_windows();

    ensure_realtime_audio_thread();
}

/// Back-compat alias used by older call sites.
pub fn boost_audio_thread_priority() {
    ensure_realtime_audio_thread();
}

#[cfg(windows)]
fn prepare_realtime_audio_thread_windows() {
    use windows::core::PCWSTR;
    use windows::Win32::System::Threading::{
        AvSetMmThreadCharacteristicsW, AvSetMmThreadPriority, GetCurrentThread,
        SetThreadPriority, AVRT_PRIORITY_CRITICAL, THREAD_PRIORITY_TIME_CRITICAL,
    };

    unsafe {
        let _ = SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL);

        let task_name: Vec<u16> = "Pro Audio\0".encode_utf16().collect();
        let mut task_index = 0u32;
        if let Ok(handle) = AvSetMmThreadCharacteristicsW(PCWSTR(task_name.as_ptr()), &mut task_index) {
            let _ = AvSetMmThreadPriority(handle, AVRT_PRIORITY_CRITICAL);
        }
    }
}

#[cfg(windows)]
fn disable_process_power_throttling_windows() {
    use windows::Win32::System::Threading::{
        GetCurrentProcess, SetProcessInformation, ProcessPowerThrottling, SetPriorityClass, HIGH_PRIORITY_CLASS,
        PROCESS_POWER_THROTTLING_EXECUTION_SPEED, PROCESS_POWER_THROTTLING_IGNORE_TIMER_RESOLUTION,
        PROCESS_POWER_THROTTLING_STATE,
    };

    unsafe {
        let _ = SetPriorityClass(GetCurrentProcess(), HIGH_PRIORITY_CLASS);
    }

    let mut state = PROCESS_POWER_THROTTLING_STATE {
        Version: 1,
        ControlMask: PROCESS_POWER_THROTTLING_EXECUTION_SPEED
            | PROCESS_POWER_THROTTLING_IGNORE_TIMER_RESOLUTION,
        StateMask: 0,
    };

    unsafe {
        let _ = SetProcessInformation(
            GetCurrentProcess(),
            ProcessPowerThrottling,
            &mut state as *mut _ as *mut _,
            std::mem::size_of::<PROCESS_POWER_THROTTLING_STATE>() as u32,
        );
    }
}
