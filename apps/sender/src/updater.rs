//! Background update checker for OpenAudio.
//! Queries GitHub Releases API periodically and alerts the user when a newer release is published.

use serde::Deserialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub const DEFAULT_GITHUB_REPO: &str = "web123r/OpenAudio";
pub const UPDATE_CHECK_INTERVAL: Duration = Duration::from_secs(3600); // 1 hour

#[derive(Debug, Clone, Deserialize)]
pub struct GitHubAsset {
    pub name: String,
    pub browser_download_url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GitHubRelease {
    pub tag_name: String,
    pub name: Option<String>,
    pub html_url: String,
    pub body: Option<String>,
    #[allow(dead_code)]
    pub draft: Option<bool>,
    #[allow(dead_code)]
    pub prerelease: Option<bool>,
    pub assets: Option<Vec<GitHubAsset>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateStatus {
    Idle,
    Checking,
    UpToDate {
        checked_at: Instant,
        version: String,
    },
    UpdateAvailable {
        current_version: String,
        latest_version: String,
        release_name: String,
        release_url: String,
        download_url: Option<String>,
        release_notes: String,
        checked_at: Instant,
    },
    Error {
        message: String,
        checked_at: Instant,
    },
}

#[derive(Clone)]
pub struct UpdateChecker {
    pub repo: String,
    pub status: Arc<Mutex<UpdateStatus>>,
    pub is_checking: Arc<AtomicBool>,
    pub dismissed: Arc<AtomicBool>,
    pub trigger_check: Arc<AtomicBool>,
}

impl Default for UpdateChecker {
    fn default() -> Self {
        Self::new(DEFAULT_GITHUB_REPO)
    }
}

impl UpdateChecker {
    pub fn new(repo: &str) -> Self {
        Self {
            repo: repo.to_string(),
            status: Arc::new(Mutex::new(UpdateStatus::Idle)),
            is_checking: Arc::new(AtomicBool::new(false)),
            dismissed: Arc::new(AtomicBool::new(false)),
            trigger_check: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn start_background_loop(&self, running: Arc<AtomicBool>) {
        let repo = self.repo.clone();
        let status = self.status.clone();
        let is_checking = self.is_checking.clone();
        let trigger_check = self.trigger_check.clone();

        thread::Builder::new()
            .name("openaudio-update-checker".to_string())
            .spawn(move || {
                // Initial short delay on startup so we don't compete with immediate audio startup
                thread::sleep(Duration::from_millis(2500));

                let mut last_check = Instant::now() - UPDATE_CHECK_INTERVAL;

                while running.load(Ordering::Relaxed) {
                    let triggered = trigger_check.swap(false, Ordering::SeqCst);
                    let interval_elapsed = last_check.elapsed() >= UPDATE_CHECK_INTERVAL;

                    if triggered || interval_elapsed {
                        last_check = Instant::now();
                        is_checking.store(true, Ordering::SeqCst);
                        if let Ok(mut s) = status.lock() {
                            *s = UpdateStatus::Checking;
                        }

                        let res = check_github_release(&repo);
                        let current_ver = env!("CARGO_PKG_VERSION");

                        let new_status = match res {
                            Ok(release) => {
                                let tag = release.tag_name.trim_start_matches('v').trim();
                                if !release.draft.unwrap_or(false) && is_newer_version(tag, current_ver) {
                                    let download_url = release
                                        .assets
                                        .as_ref()
                                        .and_then(|assets| {
                                            assets.iter().find(|a| a.name.ends_with(".zip") || a.name.ends_with(".exe"))
                                        })
                                        .map(|a| a.browser_download_url.clone())
                                        .or_else(|| Some(release.html_url.clone()));

                                    UpdateStatus::UpdateAvailable {
                                        current_version: current_ver.to_string(),
                                        latest_version: tag.to_string(),
                                        release_name: release.name.unwrap_or_else(|| format!("v{tag}")),
                                        release_url: release.html_url,
                                        download_url,
                                        release_notes: release.body.unwrap_or_default(),
                                        checked_at: Instant::now(),
                                    }
                                } else {
                                    UpdateStatus::UpToDate {
                                        checked_at: Instant::now(),
                                        version: current_ver.to_string(),
                                    }
                                }
                            }
                            Err(e) => UpdateStatus::Error {
                                message: e,
                                checked_at: Instant::now(),
                            },
                        };

                        if let Ok(mut s) = status.lock() {
                            *s = new_status;
                        }
                        is_checking.store(false, Ordering::SeqCst);
                    }

                    thread::sleep(Duration::from_millis(500));
                }
            })
            .expect("failed to spawn update checker thread");
    }

    pub fn check_now(&self) {
        self.dismissed.store(false, Ordering::SeqCst);
        self.trigger_check.store(true, Ordering::SeqCst);
    }
}

pub fn check_github_release(repo: &str) -> Result<GitHubRelease, String> {
    let url = format!("https://api.github.com/repos/{repo}/releases/latest");
    let resp = ureq::get(&url)
        .set("User-Agent", "OpenAudio-Desktop-Updater")
        .set("Accept", "application/vnd.github.v3+json")
        .timeout(Duration::from_secs(10))
        .call()
        .map_err(|e| format!("update check failed: {e}"))?;

    let release = resp
        .into_json::<GitHubRelease>()
        .map_err(|e| format!("failed to parse GitHub release: {e}"))?;

    Ok(release)
}

/// Parses semantic version string ("1.2.3" or "v1.2.3") into (major, minor, patch).
pub fn parse_semver(ver: &str) -> Option<(u32, u32, u32)> {
    let clean = ver.trim().trim_start_matches('v');
    let parts: Vec<&str> = clean.split('.').collect();
    if parts.len() < 2 {
        return None;
    }
    let major = parts.get(0)?.parse::<u32>().ok()?;
    let minor = parts.get(1)?.parse::<u32>().ok()?;
    let patch = if parts.len() >= 3 {
        parts[2].split('-').next()?.parse::<u32>().ok()?
    } else {
        0
    };
    Some((major, minor, patch))
}

/// Returns true if remote version is strictly newer than current version.
pub fn is_newer_version(remote: &str, current: &str) -> bool {
    let Some(r) = parse_semver(remote) else {
        return false;
    };
    let Some(c) = parse_semver(current) else {
        return false;
    };

    if r.0 != c.0 {
        return r.0 > c.0;
    }
    if r.1 != c.1 {
        return r.1 > c.1;
    }
    r.2 > c.2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_semver() {
        assert_eq!(parse_semver("0.1.0"), Some((0, 1, 0)));
        assert_eq!(parse_semver("v0.1.0"), Some((0, 1, 0)));
        assert_eq!(parse_semver("v1.20.5"), Some((1, 20, 5)));
        assert_eq!(parse_semver("2.0"), Some((2, 0, 0)));
        assert_eq!(parse_semver("v0.2.0-rc1"), Some((0, 2, 0)));
    }

    #[test]
    fn test_is_newer_version() {
        assert!(is_newer_version("0.2.0", "0.1.0"));
        assert!(is_newer_version("v0.2.0", "0.1.0"));
        assert!(is_newer_version("0.1.1", "0.1.0"));
        assert!(is_newer_version("1.0.0", "0.9.9"));
        assert!(!is_newer_version("0.1.0", "0.1.0"));
        assert!(!is_newer_version("v0.1.0", "0.1.0"));
        assert!(!is_newer_version("0.0.9", "0.1.0"));
    }
}
