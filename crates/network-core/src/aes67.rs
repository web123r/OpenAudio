/// AES67 / RFC 3550 SDP (Session Description Protocol) metadata generator.
/// Enables OpenAudio nodes to interoperate with commercial broadcast hardware (Yamaha, Ravenna, Dante AES67 mode).
#[derive(Debug, Clone)]
pub struct Aes67StreamInfo {
    pub session_name: String,
    pub session_id: u64,
    pub origin_ip: String,
    pub multicast_ip: String,
    pub port: u16,
    pub sample_rate: u32,
    pub channels: u8,
    pub ptime_ms: f32,
}

impl Aes67StreamInfo {
    /// Generates a standard RFC 4566 SDP payload for AES67 AoIP compatibility.
    pub fn to_sdp(&self) -> String {
        format!(
            "v=0\r\n\
             o=- {} 1 IN IP4 {}\r\n\
             s={}\r\n\
             c=IN IP4 {}/32\r\n\
             t=0 0\r\n\
             m=audio {} RTP/AVP 96\r\n\
             i={} Channels\r\n\
             a=rtpmap:96 L24/{}/{}\r\n\
             a=ptime:{}\r\n\
             a=mediaclk:direct=0\r\n\
             a=clock-domain:PTPv2 0\r\n",
            self.session_id,
            self.origin_ip,
            self.session_name,
            self.multicast_ip,
            self.port,
            self.channels,
            self.sample_rate,
            self.channels,
            self.ptime_ms
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aes67_sdp_generation() {
        let info = Aes67StreamInfo {
            session_name: "OpenAudio-Studio-A".to_string(),
            session_id: 1785901234,
            origin_ip: "192.168.1.50".to_string(),
            multicast_ip: "239.67.1.1".to_string(),
            port: 5004,
            sample_rate: 48000,
            channels: 2,
            ptime_ms: 1.0,
        };

        let sdp = info.to_sdp();
        assert!(sdp.contains("s=OpenAudio-Studio-A"));
        assert!(sdp.contains("c=IN IP4 239.67.1.1/32"));
        assert!(sdp.contains("a=rtpmap:96 L24/48000/2"));
        assert!(sdp.contains("a=clock-domain:PTPv2 0"));
    }
}
