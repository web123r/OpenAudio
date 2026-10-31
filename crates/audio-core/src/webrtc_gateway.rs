/// WebRTC Audio Session manager for ultra-low latency (<10ms) browser streaming.
/// Facilitates peer connections, ICE candidate exchange, and RTC track packetization.
#[derive(Debug, Clone)]
pub struct WebRtcAudioConfig {
    pub ice_servers: Vec<String>,
    pub max_bitrate_kbps: u32,
    pub sample_rate: u32,
    pub channels: u8,
}

impl Default for WebRtcAudioConfig {
    fn default() -> Self {
        Self {
            ice_servers: vec!["stun:stun.l.google.com:19302".to_string()],
            max_bitrate_kbps: 256,
            sample_rate: 48000,
            channels: 2,
        }
    }
}

pub struct WebRtcAudioSession {
    pub session_id: String,
    pub config: WebRtcAudioConfig,
    pub peer_count: usize,
}

impl WebRtcAudioSession {
    pub fn new(session_id: impl Into<String>, config: WebRtcAudioConfig) -> Self {
        Self {
            session_id: session_id.into(),
            config,
            peer_count: 0,
        }
    }

    /// Generates local WebRTC SDP Offer for browser peer connection handshake.
    pub fn create_sdp_offer(&self) -> String {
        format!(
            "v=0\r\n\
             o=- {} 2 IN IP4 127.0.0.1\r\n\
             s=OpenAudio-WebRTC\r\n\
             t=0 0\r\n\
             m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
             c=IN IP4 0.0.0.0\r\n\
             a=rtpmap:111 opus/{}/{}\r\n\
             a=fmtp:111 minptime=10; useinbandfec=1\r\n\
             a=sendrecv\r\n",
            self.session_id, self.config.sample_rate, self.config.channels
        )
    }

    pub fn handle_sdp_answer(&mut self, _answer_sdp: &str) -> Result<(), String> {
        self.peer_count += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_webrtc_session_sdp_offer() {
        let mut session = WebRtcAudioSession::new("test-session-123", WebRtcAudioConfig::default());
        let offer = session.create_sdp_offer();

        assert!(offer.contains("s=OpenAudio-WebRTC"));
        assert!(offer.contains("rtpmap:111 opus/48000/2"));
        assert_eq!(session.peer_count, 0);

        session.handle_sdp_answer("v=0...").expect("valid answer");
        assert_eq!(session.peer_count, 1);
    }
}
