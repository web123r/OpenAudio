use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EndpointKind {
    Playback,
    Recording,
}

impl EndpointKind {
    fn as_str(self) -> &'static str {
        match self {
            EndpointKind::Playback => "playback",
            EndpointKind::Recording => "recording",
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct SarEndpoint {
    id: String,
    description: String,
    #[serde(rename = "type")]
    endpoint_type: String,
    #[serde(rename = "channelCount")]
    channel_count: u32,
    #[serde(rename = "attachPhysical", skip_serializing_if = "Option::is_none")]
    attach_physical: Option<bool>,
    #[serde(
        rename = "physicalChannelBase",
        skip_serializing_if = "Option::is_none"
    )]
    physical_channel_base: Option<u32>,
}

#[derive(Serialize, Deserialize, Clone)]
struct SarConfig {
    #[serde(rename = "driverClsid")]
    driver_clsid: String,
    #[serde(rename = "enableApplicationRouting")]
    enable_application_routing: bool,
    #[serde(
        rename = "waveRtMinimumFrames",
        skip_serializing_if = "Option::is_none"
    )]
    wave_rt_minimum_frames: Option<u32>,
    endpoints: Vec<SarEndpoint>,
    /// Application routing rules -- we don't need to understand this
    /// schema in detail, just preserve it untouched round-trip.
    #[serde(default)]
    applications: Vec<serde_json::Value>,
}

/// Finds SAR's config file. Checks the location confirmed by direct
/// testing (%APPDATA%\SynchronousAudioRouter\default.json) first, then
/// a %LOCALAPPDATA% variant as a fallback for robustness on other
/// machines/SAR versions we haven't verified against.
pub fn find_sar_config_path() -> Option<PathBuf> {
    let candidates = [
        std::env::var("APPDATA").ok().map(|base| {
            PathBuf::from(base)
                .join("SynchronousAudioRouter")
                .join("default.json")
        }),
        std::env::var("LOCALAPPDATA").ok().map(|base| {
            PathBuf::from(base)
                .join("SynchronousAudioRouter")
                .join("default.json")
        }),
    ];

    candidates.into_iter().flatten().find(|p| p.exists())
}

fn load_config(path: &Path) -> Result<SarConfig, String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("failed to read SAR config at {}: {e}", path.display()))?;
    serde_json::from_str(&content)
        .map_err(|e| format!("failed to parse SAR config JSON: {e}"))
}

fn save_config(path: &Path, config: &SarConfig) -> Result<(), String> {
    // Back up the original before touching it -- this is another
    // program's live config file, not something to risk corrupting
    // without a way back.
    let backup_path = path.with_extension("json.bak");
    if path.exists() {
        fs::copy(path, &backup_path)
            .map_err(|e| format!("failed to back up SAR config to {}: {e}", backup_path.display()))?;
    }

    let json = serde_json::to_string_pretty(config)
        .map_err(|e| format!("failed to serialize SAR config: {e}"))?;
    fs::write(path, json)
        .map_err(|e| format!("failed to write SAR config to {}: {e}", path.display()))?;
    Ok(())
}

/// Generates the predictable display name OpenAudio uses for a given
/// session tag and channel index, e.g. "OpenAudio-Combine1-Ch0".
pub fn openaudio_endpoint_name(session_tag: &str, channel_index: usize) -> String {
    format!("OpenAudio-{session_tag}-Ch{channel_index}")
}

fn openaudio_endpoint_id(session_tag: &str, channel_index: usize) -> String {
    format!("openaudio_{session_tag}_{channel_index}")
}

/// Ensures exactly `channel_count` SAR endpoints of the given `kind`
/// exist for this session tag, removing any stale ones from a
/// previous call with a different count. Returns the display names in
/// channel order, ready to use directly as device targets.
///
/// Requires SAR to have been configured at least once already (so
/// `driverClsid` and the hardware interface are set) -- this function
/// does not fabricate that, since guessing it wrong would break your
/// real hardware routing.
pub fn ensure_openaudio_endpoints(
    session_tag: &str,
    kind: EndpointKind,
    channel_count: usize,
) -> Result<Vec<String>, String> {
    let path = find_sar_config_path().ok_or_else(|| {
        "couldn't find SAR's default.json -- has SAR been configured at least once via its own GUI first?".to_string()
    })?;

    let mut config = load_config(&path)?;

    let id_prefix = format!("openaudio_{session_tag}_");
    config.endpoints.retain(|ep| !ep.id.starts_with(&id_prefix));

    let mut names = Vec::with_capacity(channel_count);
    for i in 0..channel_count {
        let name = openaudio_endpoint_name(session_tag, i);
        config.endpoints.push(SarEndpoint {
            id: openaudio_endpoint_id(session_tag, i),
            description: name.clone(),
            endpoint_type: kind.as_str().to_string(),
            channel_count: 1, // one network channel per endpoint, matching our split/combine design
            attach_physical: None,
            physical_channel_base: None,
        });
        names.push(name);
    }

    save_config(&path, &config)?;
    Ok(names)
}

/// Removes all OpenAudio-created endpoints for a session tag, without
/// adding any replacements. Use when a session is removed entirely.
pub fn remove_openaudio_endpoints(session_tag: &str) -> Result<(), String> {
    let path = find_sar_config_path().ok_or_else(|| {
        "couldn't find SAR's default.json".to_string()
    })?;

    let mut config = load_config(&path)?;
    let id_prefix = format!("openaudio_{session_tag}_");
    config.endpoints.retain(|ep| !ep.id.starts_with(&id_prefix));

    save_config(&path, &config)
}