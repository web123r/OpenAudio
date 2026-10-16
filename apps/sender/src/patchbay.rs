use std::collections::HashSet;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PatchbayNode {
    pub id: String,
    pub name: String,
    pub channels: usize,
    pub ip_address: String,
    pub is_publisher: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PatchbayMatrix {
    pub publishers: Vec<PatchbayNode>,
    pub receivers: Vec<PatchbayNode>,
    pub active_patches: HashSet<(String, String)>, // (publisher_id, receiver_id)
    pub volume_db: f32,
}

impl Default for PatchbayMatrix {
    fn default() -> Self {
        Self {
            publishers: Vec::new(),
            receivers: Vec::new(),
            active_patches: HashSet::new(),
            volume_db: 0.0,
        }
    }
}

impl PatchbayMatrix {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_publisher(
        &mut self,
        id: impl Into<String>,
        name: impl Into<String>,
        channels: usize,
        ip: impl Into<String>,
    ) {
        self.publishers.push(PatchbayNode {
            id: id.into(),
            name: name.into(),
            channels,
            ip_address: ip.into(),
            is_publisher: true,
        });
    }

    pub fn add_receiver(
        &mut self,
        id: impl Into<String>,
        name: impl Into<String>,
        channels: usize,
        ip: impl Into<String>,
    ) {
        self.receivers.push(PatchbayNode {
            id: id.into(),
            name: name.into(),
            channels,
            ip_address: ip.into(),
            is_publisher: false,
        });
    }

    #[allow(dead_code)]
    pub fn toggle_patch(&mut self, pub_id: &str, rec_id: &str) -> bool {
        let key = (pub_id.to_string(), rec_id.to_string());
        if self.active_patches.contains(&key) {
            self.active_patches.remove(&key);
            false
        } else {
            self.active_patches.insert(key);
            true
        }
    }

    pub fn is_patched(&self, pub_id: &str, rec_id: &str) -> bool {
        self.active_patches
            .contains(&(pub_id.to_string(), rec_id.to_string()))
    }

    pub fn set_patched(&mut self, pub_id: &str, rec_id: &str, patched: bool) {
        let key = (pub_id.to_string(), rec_id.to_string());
        if patched {
            self.active_patches.insert(key);
        } else {
            self.active_patches.remove(&key);
        }
    }

    pub fn clear_patches(&mut self) {
        self.active_patches.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_patchbay_matrix_toggle() {
        let mut matrix = PatchbayMatrix::new();
        matrix.add_publisher("pub-1", "Studio Mic 1", 2, "192.168.1.10");
        matrix.add_receiver("rec-1", "Speakers L/R", 2, "192.168.1.20");

        assert!(!matrix.is_patched("pub-1", "rec-1"));
        let state1 = matrix.toggle_patch("pub-1", "rec-1");
        assert!(state1);
        assert!(matrix.is_patched("pub-1", "rec-1"));

        let state2 = matrix.toggle_patch("pub-1", "rec-1");
        assert!(!state2);
        assert!(!matrix.is_patched("pub-1", "rec-1"));
    }
}
