use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[derive(Debug, Clone, Copy)]
pub struct NodePing {
    pub rtt_ms: f32,
    pub last_seen: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BrowserAccessMode {
    PasswordProtected,
    OpenLan,
}

pub struct PublishSession {
    pub id: u64,
    pub node_name: String,
    pub stream_name: String,
    pub stream_id: u32,
    pub selected_input: Option<String>,
    pub is_loopback: bool,
    pub record: bool,
    pub channel_labels: Vec<String>,
    pub encrypted: bool,
    pub passphrase: String,
    pub running: Arc<AtomicBool>,
    pub status: Arc<Mutex<String>>,
    pub last_toggle: Instant,
}

pub struct CombinePublishSession {
    pub id: u64,
    pub session_tag: String,
    pub node_name: String,
    pub stream_name: String,
    pub stream_id: u32,
    pub channel_count: usize,
    pub channel_sources: Vec<(Option<String>, bool)>,
    pub channel_labels: Vec<String>,
    pub record: bool,
    pub encrypted: bool,
    pub passphrase: String,
    pub running: Arc<AtomicBool>,
    pub status: Arc<Mutex<String>>,
    pub last_toggle: Instant,
}

pub struct AsioPublishSession {
    pub id: u64,
    pub node_name: String,
    pub stream_name: String,
    pub stream_id: u32,
    pub selected_driver: Option<String>,
    pub driver_channel_count: usize,
    pub channel_indices: Vec<usize>,
    pub channel_labels: Vec<String>,
    pub running: Arc<AtomicBool>,
    pub status: Arc<Mutex<String>>,
    pub last_toggle: Instant,
}

pub struct SubscribeSession {
    pub id: u64,
    pub selected_discovered_node_id: Option<String>,
    pub bind_port: String,
    pub selected_output: Option<String>,
    pub volume: audio_core::VolumeControl,
    pub record: bool,
    pub encrypted: bool,
    pub passphrase: String,
    pub running: Arc<AtomicBool>,
    pub status: Arc<Mutex<String>>,
    pub last_toggle: Instant,
    pub jitter_target_ms: Arc<AtomicU32>,
}

pub struct SplitSubscribeSession {
    pub id: u64,
    pub session_tag: String,
    pub selected_discovered_node_id: Option<String>,
    pub bind_port: String,
    pub channel_devices: Vec<Option<String>>,
    pub running: Arc<AtomicBool>,
    pub status: Arc<Mutex<String>>,
    pub last_toggle: Instant,
    pub record: bool,
    pub encrypted: bool,
    pub passphrase: String,
}
