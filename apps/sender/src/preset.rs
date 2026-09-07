use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct PresetFile {
    pub format: String,
    pub version: u32,
    pub publish_sessions: Vec<PublishPreset>,
    pub combine_publish_sessions: Vec<CombinePublishPreset>,
    pub asio_publish_sessions: Vec<AsioPublishPreset>,
    pub subscribe_sessions: Vec<SubscribePreset>,
    pub split_subscribe_sessions: Vec<SplitSubscribePreset>,
    pub browser: BrowserPreset,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PublishPreset {
    pub node_name: String,
    pub stream_name: String,
    pub stream_id: u32,
    pub selected_input: Option<String>,
    pub is_loopback: bool,
    pub record: bool,
    #[serde(default)]
    pub channel_labels: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CombinePublishPreset {
    pub session_tag: String,
    pub node_name: String,
    pub stream_name: String,
    pub stream_id: u32,
    pub channel_count: usize,
    pub channel_sources: Vec<(Option<String>, bool)>,
    pub record: bool,
    #[serde(default)]
    pub channel_labels: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AsioPublishPreset {
    pub node_name: String,
    pub stream_name: String,
    pub stream_id: u32,
    pub selected_driver: Option<String>,
    pub channel_indices: Vec<usize>,
    #[serde(default)]
    pub channel_labels: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribePreset {
    pub selected_discovered_node_id: Option<String>,
    pub bind_port: String,
    pub selected_output: Option<String>,
    pub volume: f32,
    pub record: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SplitSubscribePreset {
    pub session_tag: String,
    pub selected_discovered_node_id: Option<String>,
    pub bind_port: String,
    pub channel_devices: Vec<Option<String>>,
    pub record: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BrowserPreset {
    pub access_mode: String,
    pub password: String,
}

impl Default for PresetFile {
    fn default() -> Self {
        Self {
            format: "OpenAudio preset".to_string(),
            version: 1,
            publish_sessions: Vec::new(),
            combine_publish_sessions: Vec::new(),
            asio_publish_sessions: Vec::new(),
            subscribe_sessions: Vec::new(),
            split_subscribe_sessions: Vec::new(),
            browser: BrowserPreset {
                access_mode: "password_protected".to_string(),
                password: String::new(),
            },
        }
    }
}
