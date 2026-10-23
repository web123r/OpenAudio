//! OS-level realtime scheduling helpers for audio threads.
//!
//! The public scheduling API is platform-neutral. Windows uses MMCSS
//! ("Pro Audio") and high process priority; other platforms currently use
//! the same no-op-safe entry points while their native scheduling backends
//! are added.

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

        #[cfg(target_os = "linux")]
        prepare_realtime_audio_thread_linux();

        #[cfg(target_os = "macos")]
        prepare_realtime_audio_thread_macos();
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
        AvSetMmThreadCharacteristicsW, AvSetMmThreadPriority, GetCurrentThread, SetThreadPriority,
        AVRT_PRIORITY_CRITICAL, THREAD_PRIORITY_TIME_CRITICAL,
    };

    unsafe {
        let _ = SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL);

        let task_name: Vec<u16> = "Pro Audio\0".encode_utf16().collect();
        let mut task_index = 0u32;
        if let Ok(handle) =
            AvSetMmThreadCharacteristicsW(PCWSTR(task_name.as_ptr()), &mut task_index)
        {
            let _ = AvSetMmThreadPriority(handle, AVRT_PRIORITY_CRITICAL);
        }
    }
}

#[cfg(windows)]
fn disable_process_power_throttling_windows() {
    use windows::Win32::System::Threading::{
        GetCurrentProcess, ProcessPowerThrottling, SetPriorityClass, SetProcessInformation,
        HIGH_PRIORITY_CLASS, PROCESS_POWER_THROTTLING_EXECUTION_SPEED,
        PROCESS_POWER_THROTTLING_IGNORE_TIMER_RESOLUTION, PROCESS_POWER_THROTTLING_STATE,
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

#[cfg(target_os = "linux")]
fn prepare_realtime_audio_thread_linux() {
    use std::io::Error;

    unsafe {
        let thread = libc::pthread_self();
        let mut param = std::mem::zeroed::<libc::sched_param>();
        // Use the maximum priority for SCHED_RR (typically 99, but we can query it)
        let max_priority = libc::sched_get_priority_max(libc::SCHED_RR);
        param.sched_priority = max_priority;

        let result = libc::pthread_setschedparam(thread, libc::SCHED_RR, &param);

        if result != 0 {
            // EPERM is common if the user lacks CAP_SYS_NICE or audio group limits.
            // We log a warning but continue; audio will still work, just with less
            // resistance to underruns under load.
            let err = Error::from_raw_os_error(result);
            eprintln!(
                "audio-core: warning: could not set realtime thread priority: {}",
                err
            );
        }
    }
}

#[cfg(target_os = "macos")]
fn prepare_realtime_audio_thread_macos() {
    use libc::{
        mach_thread_self, thread_policy_set, thread_time_constraint_policy,
        THREAD_TIME_CONSTRAINT_POLICY,
    };
    use std::io::Error;

    #[repr(C)]
    struct MachTimebaseInfo {
        numer: u32,
        denom: u32,
    }

    extern "C" {
        fn mach_timebase_info(info: *mut MachTimebaseInfo) -> i32;
    }

    unsafe {
        let thread = mach_thread_self();

        let mut timebase = MachTimebaseInfo { numer: 0, denom: 0 };
        let (period, computation, constraint) = if mach_timebase_info(&mut timebase) == 0
            && timebase.numer > 0
            && timebase.denom > 0
        {
            // Convert nanoseconds to Mach absolute time units:
            // mach_units = (ns * denom) / numer
            let ns_to_mach = |ns: u64| -> u32 {
                let units = (ns as u128 * timebase.denom as u128) / (timebase.numer as u128);
                units.min(u32::MAX as u128) as u32
            };
            // 10 ms nominal audio cycle (~480 frames @ 48 kHz), 5 ms computation slice, 8 ms deadline constraint
            (
                ns_to_mach(10_000_000),
                ns_to_mach(5_000_000),
                ns_to_mach(8_000_000),
            )
        } else {
            // Fallback default cycles if timebase query fails
            (10_000_000, 5_000_000, 8_000_000)
        };

        let mut policy = thread_time_constraint_policy {
            period,
            computation,
            constraint,
            preemptible: 1,
        };

        let result = thread_policy_set(
            thread,
            THREAD_TIME_CONSTRAINT_POLICY as u32,
            &mut policy as *mut _ as *mut i32,
            std::mem::size_of::<thread_time_constraint_policy>() as u32 / 4,
        );

        if result != 0 {
            let err = Error::from_raw_os_error(result);
            eprintln!(
                "audio-core: warning: could not set macOS realtime thread priority: {}",
                err
            );
        }
    }
}
